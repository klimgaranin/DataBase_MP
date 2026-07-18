"""
app/jobs/job_wb_orders.py
ETL-джоб: WB заказы (raw_dedup + norm).
Advisory lock: 4242001.
Расписание (Task Scheduler): каждые N минут.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

# ── bootstrap: sys.path + .env ───────────────────────────────────────────────
_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))          # app/
sys.path.insert(0, str(_THIS.parent.parent.parent))   # DataBase_MP/

from app.utils import (
    setup_sys_path, load_env, setup_logging, tg_send,
    now_iso_utc, now_msk_label,
    ensure_tz, parse_iso_dt, first_run_cursor,
)

setup_sys_path(__file__)
load_env(__file__)

# ── импорты проекта ───────────────────────────────────────────────────────────
from app.clients.http_wb_statistics import iter_orders
from app.db import (
    insert_wb_orders_raw_dedup,
    cleanup_wb_orders_raw_dedup,
    upsert_wb_orders_norm,
    get_job_cursor,
    set_job_cursor,
    insert_job_run,
    get_last_dup_pct,
    try_advisory_lock,
    advisory_unlock,
)
from app.normalize.norm_wb_orders import normalize_wb_order

# ── константы ─────────────────────────────────────────────────────────────────
JOB_NAME = "wb_orders"
LOCK_ID  = 4_242_001


# ── конфиг ────────────────────────────────────────────────────────────────────
def _load_job_config() -> dict:
    first_run_days_back   = max(1,   min(int(os.getenv("WB_FIRST_RUN_DAYS_BACK",   "3")),   30))
    lookback_base         = max(0,   min(int(os.getenv("WB_LOOKBACK_MINUTES",       "10")), 120))
    lookback_max          = max(0,   min(int(os.getenv("WB_LOOKBACK_MAX_MINUTES",   str(max(lookback_base, 20)))), 240))
    raw_retention         = max(1,   min(int(os.getenv("WB_RAW_DEDUP_RETENTION_DAYS", "14")), 365))
    debug_sleep           = max(0,   min(int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0")), 3600))
    log_file              = (os.getenv("WB_ORDERS_LOG_FILE") or "").strip() or None
    return dict(
        first_run_days_back=first_run_days_back,
        lookback_base=lookback_base,
        lookback_max=lookback_max,
        raw_retention=raw_retention,
        debug_sleep=debug_sleep,
        log_file=log_file,
    )


# ── lookback-логика ───────────────────────────────────────────────────────────
def _calc_lookback(cursor_old_iso: str, last_dup_pct: Optional[float],
                   base: int, max_look: int) -> int:
    dt_old = parse_iso_dt(cursor_old_iso)
    if dt_old is None:
        return base
    gap_min = max(int((datetime.now(dt_old.tzinfo or timezone.utc) - dt_old).total_seconds() // 60), 0)

    if   gap_min <= 20:  look = min(base, 2)
    elif gap_min <= 60:  look = min(base, 5)
    elif gap_min <= 360: look = min(base, 10)
    else:                look = min(max_look, max(base, 15))

    if last_dup_pct is not None:
        if   last_dup_pct >= 30.0: look = min(look, 2)
        elif last_dup_pct >= 15.0: look = min(look, 5)
        elif last_dup_pct <= 3.0:  look = min(max_look, look + 5)
        elif last_dup_pct <= 7.0:  look = min(max_look, look + 2)

    return max(0, int(look))


def _apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = parse_iso_dt(cursor_old_iso)
    if dt is None:
        return ensure_tz(cursor_old_iso)
    return (dt - timedelta(minutes=max(0, minutes))).replace(microsecond=0).isoformat()


def _max_cursor_from_rows(rows: list[dict[str, Any]], fallback: str) -> str:
    best_dt: Optional[datetime] = None
    best_raw: Optional[str]     = None
    for r in rows:
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = parse_iso_dt(str(c))
        if dt and (best_dt is None or dt > best_dt):
            best_dt  = dt
            best_raw = str(c)
    return ensure_tz(best_raw) if best_raw else ensure_tz(fallback)


def _dedupe_by_srid(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = parse_iso_dt(str(c))
        if dt is None:
            continue
        key  = str(srid)
        prev = best.get(key)
        if prev is None or dt > prev[0]:
            best[key] = (dt, r)
    return [pair[1] for pair in best.values()]


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"])

    if not try_advisory_lock(LOCK_ID):
        log.info("[ЗАДАЧА] пропуск: предыдущий запуск ещё выполняется (лок занят)")
        return 0

    if cfg["debug_sleep"] > 0:
        log.info("[ЗАДАЧА] отладочная пауза после лока: %s сек.", cfg["debug_sleep"])
        time.sleep(cfg["debug_sleep"])

    started_at    = now_iso_utc()
    status        = "ok"
    error: Optional[str] = None
    api_rows      = 0
    raw_new       = 0
    norm_upserted = 0
    duplicates    = 0
    dup_pct       = 0.0
    cursor_old    = ""
    cursor_used   = ""
    cursor_new    = ""

    try:
        cursor_old   = get_job_cursor(JOB_NAME) or ""
        last_dup_pct = get_last_dup_pct(JOB_NAME)

        if not cursor_old:
            cursor_used = first_run_cursor(cfg["first_run_days_back"])
            log.info("[ЗАДАЧА] первый запуск, cursor_used=%s", cursor_used)
        else:
            look_min    = _calc_lookback(cursor_old, last_dup_pct, cfg["lookback_base"], cfg["lookback_max"])
            cursor_used = _apply_lookback(cursor_old, look_min)
            log.info(
                "[ЗАДАЧА] cursor_old=%s look_min=%d cursor_used=%s last_dup_pct=%s",
                cursor_old, look_min, cursor_used, last_dup_pct,
            )

        try:
            rows_raw = iter_orders(cursor_used, flag=0)
        except Exception as e:
            raise RuntimeError(f"WB API error: {e}") from e

        api_rows = len(rows_raw)
        log.info("[ЗАДАЧА] API вернул строк: %d", api_rows)

        if rows_raw:
            rows_deduped  = _dedupe_by_srid(rows_raw)
            raw_new       = insert_wb_orders_raw_dedup(rows_deduped)
            duplicates    = len(rows_deduped) - raw_new
            dup_pct       = round(duplicates / api_rows * 100, 2) if api_rows else 0.0

            norm_rows     = [
                nr
                for r in rows_deduped
                if r.get("srid")
                for nr in [normalize_wb_order(r)]
                if nr is not None
            ]
            norm_upserted = upsert_wb_orders_norm(norm_rows)
            cursor_new    = _max_cursor_from_rows(rows_raw, cursor_used)
            set_job_cursor(JOB_NAME, cursor_new)

            log.info(
                "[ЗАДАЧА] raw_new=%d duplicates=%d dup_pct=%.1f%% norm_upserted=%d cursor_new=%s",
                raw_new, duplicates, dup_pct, norm_upserted, cursor_new,
            )
        else:
            cursor_new = cursor_used
            log.info("[ЗАДАЧА] API вернул 0 строк, курсор не сдвигаем")

        deleted = cleanup_wb_orders_raw_dedup(cfg["raw_retention"])
        log.info("[ЗАДАЧА] cleanup raw_dedup: удалено=%d строк", deleted)

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("[ЗАДАЧА] ОШИБКА — %s", error)
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            insert_job_run(
                job_name         = JOB_NAME,
                started_at_iso   = started_at,
                finished_at_iso  = finished_at,
                status           = status,
                api_rows         = api_rows,
                raw_new_versions = raw_new,
                norm_upserted    = norm_upserted,
                duplicates       = duplicates,
                dup_pct          = dup_pct,
                cursor_old       = cursor_old  or None,
                cursor_used      = cursor_used or None,
                cursor_new       = cursor_new  or None,
                error            = error,
            )
        except Exception as je:
            log.warning("[ЗАДАЧА] insert_job_run failed: %s", je)

        ts = now_msk_label()
        if status == "ok":
            msg = (
                f"\u2705 WB_Orders_Sync | {ts} | OK\n\n"
                f"\u27a1 WB Api вернул строк: {api_rows}\n\n"
                f"\U0001f504 Обновлено строк: {norm_upserted}"
            )
        else:
            short_err = (error or "unknown")[:200]
            msg = (
                f"\u274C WB_Orders_Sync | {ts} | FAIL\n"
                f"{short_err}"
            )
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())