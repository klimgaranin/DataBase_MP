from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, List, Dict

import psycopg2
import requests
from dotenv import load_dotenv

from clients.wb_statistics import iter_orders
from db import (
    get_dsn,
    insert_wb_orders_raw_dedup,
    cleanup_wb_orders_raw_dedup,
    upsert_wb_orders_norm,
    get_job_cursor,
    set_job_cursor,
    insert_job_run,
    try_advisory_lock,
    advisory_unlock,
)
from wb_normalize import normalize_wb_order

JOB_NAME = "wb_orders"
LOCK_ID = 4242001


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

def setup_logging() -> None:
    level_str = (os.getenv("LOG_LEVEL") or "INFO").upper().strip()
    level = getattr(logging, level_str, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


log = logging.getLogger(JOB_NAME)


# ---------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_tz(iso_str: str) -> str:
    """
    WB: если TZ не указан — Московское время (UTC+3).
    Здесь делаем строго: если TZ нет, дописываем +03:00.
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # already has +03:00 / -05:00
    if len(s) >= 6 and (s[-6] in ["+", "-"]) and s[-3] == ":":
        return s
    return s + "+03:00"


def parse_iso_dt(iso_str: str) -> Optional[datetime]:
    s = ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ---------------------------------------------------------------------
# DB helper (dup_pct from last run)
# ---------------------------------------------------------------------

def get_last_dup_pct(job_name: str) -> Optional[float]:
    """
    Берём dup_pct (в процентах 0..100) из последнего job_runs.
    Если БД недоступна или таблица ещё не создана — вернём None (job не падает).
    """
    dsn = get_dsn()
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select dup_pct
                    from job_runs
                    where job_name = %s
                    order by started_at desc
                    limit 1
                    """,
                    (job_name,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                val = row[0]
                if val is None:
                    return None
                return float(val)
    except Exception:
        return None


# ---------------------------------------------------------------------
# Lookback logic
# ---------------------------------------------------------------------

def calc_lookback_minutes(cursor_old_iso: str, last_dup_pct: Optional[float]) -> int:
    """
    Авто-настройка окна догрузки (lookback), чтобы:
    - не потерять “догоняющие” изменения WB;
    - держать дубли под контролем.

    Настройки:
    - WB_LOOKBACK_MINUTES: базовое окно (дефолт 10)
    - WB_LOOKBACK_MAX_MINUTES: верхняя граница авто-увеличения (дефолт max(base, 20))
    """
    base = int(os.getenv("WB_LOOKBACK_MINUTES", "10"))
    base = max(0, min(base, 120))

    max_auto = int(os.getenv("WB_LOOKBACK_MAX_MINUTES", str(max(base, 20))))
    max_auto = max(0, min(max_auto, 240))

    dt_old = parse_iso_dt(cursor_old_iso)
    if dt_old is None:
        return base

    now_same_tz = datetime.now(dt_old.tzinfo or timezone.utc)
    gap_min = max(int((now_same_tz - dt_old).total_seconds() // 60), 0)

    # 1) Базовая логика по gap: чем ближе к real-time, тем меньше окно
    if gap_min <= 20:
        look = min(base, 2)
    elif gap_min <= 60:
        look = min(base, 5)
    elif gap_min <= 360:
        look = min(base, 10)
    else:
        look = min(max_auto, max(base, 15))

    # 2) Подстройка по прошлому dup_pct (в процентах)
    if last_dup_pct is not None:
        if last_dup_pct >= 30.0:
            look = min(look, 2)
        elif last_dup_pct >= 15.0:
            look = min(look, 5)
        elif last_dup_pct <= 3.0:
            look = min(max_auto, look + 5)
        elif last_dup_pct <= 7.0:
            look = min(max_auto, look + 2)

    return max(0, int(look))


def apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = parse_iso_dt(cursor_old_iso)
    if dt is None:
        return ensure_tz(cursor_old_iso)
    dt2 = dt - timedelta(minutes=max(0, minutes))
    return dt2.replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------
# Cursor + page helpers
# ---------------------------------------------------------------------

def max_cursor_from_rows(rows: List[Dict[str, Any]], fallback: str) -> str:
    best_dt: Optional[datetime] = None
    best_raw: Optional[str] = None

    for r in rows:
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = parse_iso_dt(str(c))
        if dt is None:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_raw = str(c)

    return ensure_tz(best_raw) if best_raw else ensure_tz(fallback)


def dedupe_for_norm(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    В рамках одной страницы оставляем по srid самую свежую версию.
    Это уменьшает лишние upsert'ы в NORM, не меняя смысл данных.
    """
    best_by_srid: Dict[str, tuple[datetime, Dict[str, Any]]] = {}

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

        key = str(srid)
        prev = best_by_srid.get(key)
        if prev is None or dt > prev[0]:
            best_by_srid[key] = (dt, r)

    return [pair[1] for pair in best_by_srid.values()]


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":
    load_dotenv()
    setup_logging()

    if not try_advisory_lock(LOCK_ID):
        log.info("[JOB] skip: previous run still active (lock busy)")
        raise SystemExit(0)

    # Для теста lock (чтобы успеть запустить вторую копию)
    debug_sleep = int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0"))
    if debug_sleep > 0:
        log.info("[JOB] debug sleep after lock: %s seconds", debug_sleep)
        time.sleep(debug_sleep)

    started_at = now_utc_iso()

    cursor_old: Optional[str] = None
    cursor_used: Optional[str] = None
    cursor_new: Optional[str] = None

    api_rows = 0
    raw_new_versions = 0
    norm_upserted = 0

    status = "ok"
    error: Optional[str] = None

    try:
        cursor_old = get_job_cursor(JOB_NAME)

        # Первый запуск: 3 дня назад (MSK)
        if not cursor_old:
            tz_msk = timezone(timedelta(hours=3))
            cursor_old = (datetime.now(tz_msk) - timedelta(days=3)).replace(microsecond=0).isoformat()

        last_dup_pct = get_last_dup_pct(JOB_NAME)
        lookback_min = calc_lookback_minutes(cursor_old, last_dup_pct=last_dup_pct)
        cursor_used = apply_lookback(cursor_old, minutes=lookback_min)

        log.info("[JOB] cursor_old=%s lookback_min=%s cursor_used=%s", cursor_old, lookback_min, cursor_used)

        cursor_max_seen = cursor_old

        with requests.Session() as sess:
            for chunk in iter_orders(cursor_used, flag=0, session=sess):
                if not chunk:
                    break

                api_rows += len(chunk)

                # RAW dedup
                raw_new_versions += insert_wb_orders_raw_dedup(chunk)

                # NORM upsert (схлопываем по srid в рамках страницы)
                compact = dedupe_for_norm(chunk)
                norm_rows = [normalize_wb_order(r) for r in compact]
                upsert_wb_orders_norm(norm_rows)
                norm_upserted += len(norm_rows)

                # max cursor по данным
                cursor_max_seen = max_cursor_from_rows(chunk, fallback=cursor_max_seen)

        cursor_new = ensure_tz(cursor_max_seen)

        # курсор не откатываем назад
        dt_old = parse_iso_dt(cursor_old)
        dt_new = parse_iso_dt(cursor_new)

        if dt_old is not None and dt_new is not None and dt_new < dt_old:
            log.warning("[JOB] cursor_new would go backwards, keep cursor_old (cursor_new=%s)", cursor_new)
            cursor_new = cursor_old
            dt_new = dt_old

        duplicates = max(api_rows - raw_new_versions, 0)
        dup_ratio = (duplicates / api_rows) if api_rows else 0.0  # 0..1

        log.info(
            "[REPORT] api_rows=%s raw_new_versions=%s duplicates=%s dup_pct=%.2f%% "
            "norm_upserted=%s cursor_old=%s cursor_used=%s cursor_new=%s",
            api_rows,
            raw_new_versions,
            duplicates,
            dup_ratio * 100.0,
            norm_upserted,
            cursor_old,
            cursor_used,
            cursor_new,
        )

        # Обновляем курсор только если есть движение вперёд
        if dt_old is not None and dt_new is not None and dt_new > dt_old:
            set_job_cursor(JOB_NAME, cursor_new)
        else:
            log.info("[JOB] cursor not advanced (skip cursor update)")

        deleted = cleanup_wb_orders_raw_dedup(14)
        log.info("[JOB] cleanup_raw_dedup_deleted=%s", deleted)

    except Exception as e:
        status = "fail"
        error = repr(e)
        log.exception("[JOB] ERROR: %s", error)

    finally:
        finished_at = now_utc_iso()
        try:
            duplicates = max(api_rows - raw_new_versions, 0)
            dup_ratio = (duplicates / api_rows) if api_rows else 0.0

            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=api_rows,
                raw_new_versions=raw_new_versions,
                norm_upserted=norm_upserted,
                duplicates=duplicates,
                dup_pct=dup_ratio * 100.0,  # проценты 0..100
                cursor_old=cursor_old,
                cursor_used=cursor_used,
                cursor_new=cursor_new,
                error=error,
            )
        finally:
            advisory_unlock(LOCK_ID)
