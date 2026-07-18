"""
app/jobs/job_wb_stocks.py
Джоб: получить текущие остатки с WB и сохранить в PostgreSQL.
Advisory lock: 4242002 (не пересекается с wb_orders 4242001).
Расписание (Task Scheduler): каждые 30 минут.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# ── bootstrap: sys.path + .env ───────────────────────────────────────────────
_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))          # app/
sys.path.insert(0, str(_THIS.parent.parent.parent))   # DataBase_MP/

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

# ── импорты проекта ───────────────────────────────────────────────────────────
from app.db import (
    try_advisory_lock, advisory_unlock, insert_job_run,
    insert_wb_stocks_raw, upsert_wb_stocks_snap, cleanup_wb_stocks_raw,
)
from app.clients.http_wb_stocks import fetch_all_stocks
from app.normalize.norm_wb_stocks import normalize_wb_stock

# ── константы ─────────────────────────────────────────────────────────────────
JOB_NAME           = "wb_stocks"
LOCK_ID            = 4_242_002
RAW_RETENTION_DAYS = int(os.getenv("WB_STOCKS_RAW_RETENTION_DAYS", "30"))
DEBUG_SLEEP        = int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0"))


def main() -> int:
    log = setup_logging(
        JOB_NAME,
        log_file=(os.getenv("WB_STOCKS_LOG_FILE") or "").strip() or None,
    )

    if not try_advisory_lock(LOCK_ID):
        log.info("WB stocks: lock занят, выходим.")
        return 0

    if DEBUG_SLEEP > 0:
        log.info("WB stocks: DEBUG_SLEEP %d сек.", DEBUG_SLEEP)
        time.sleep(DEBUG_SLEEP)

    snapped_at    = now_iso_utc()
    started_at    = snapped_at
    api_rows      = 0
    raw_inserted  = 0
    snap_upserted = 0
    status        = "ok"
    error: Optional[str] = None

    try:
        log.info("WB stocks: старт, snapped_at=%s", snapped_at)

        with requests.Session() as sess:
            raw_items = fetch_all_stocks(session=sess)

        api_rows = len(raw_items)
        log.info("WB stocks: получено %d строк из API", api_rows)

        if not raw_items:
            # Не выходим раньше finally — пусть job_run запишется и алерт уйдёт.
            # Статус оставляем "ok", но snap_upserted=0 видно в алерте.
            log.warning("WB stocks: API вернул 0 строк.")
        else:
            norm_rows     = [normalize_wb_stock(r) for r in raw_items]
            raw_inserted  = insert_wb_stocks_raw(raw_items, snapped_at)
            snap_upserted = upsert_wb_stocks_snap(norm_rows, snapped_at)

            log.info(
                "WB stocks: raw_inserted=%d snap_upserted=%d snapped_at=%s",
                raw_inserted, snap_upserted, snapped_at,
            )

            deleted = cleanup_wb_stocks_raw(RAW_RETENTION_DAYS)
            log.info("WB stocks: cleanup raw > %d дней, удалено %d строк", RAW_RETENTION_DAYS, deleted)

        return 0

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("WB stocks: ОШИБКА — %s", error)
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
                raw_new_versions = raw_inserted,
                norm_upserted    = snap_upserted,
                duplicates       = 0,
                dup_pct          = 0.0,
                cursor_old       = None,
                cursor_used      = None,
                cursor_new       = None,
                error            = error,
            )
        except Exception as je:
            log.warning("WB stocks: insert_job_run failed: %s", je)

        ts = now_msk_label()
        if status == "ok":
            msg = (
                f"\u2705 WB_Stocks_Sync | {ts} | OK\n\n"
                f"\u27a1 WB Api вернул строк: {api_rows}\n\n"
                f"\U0001f504 Обновлено строк: {snap_upserted}"
            )
        else:
            short_err = (error or "unknown")[:200]
            msg = (
                f"\u274C WB_Stocks_Sync | {ts} | FAIL\n"
                f"{short_err}"
            )
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())