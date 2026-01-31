from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from clients.wb_statistics import fetch_orders_page
from db import (
    insert_wb_orders_raw_dedup,
    cleanup_wb_orders_raw_dedup,
    upsert_wb_orders_norm,
    set_job_cursor,
    try_advisory_lock,
    advisory_unlock,
)
from wb_normalize import normalize_wb_order

JOB_NAME_MAIN = "wb_orders"
JOB_NAME_BACKFILL = "wb_orders_backfill"
LOCK_ID = 4242001

MAX_ROWS_PER_RESPONSE = 80000  # условный лимит при flag=0 [web:197]
SLEEP_SEC = 61                 # 1 запрос в минуту [web:197]


def ensure_tz(iso_str: str) -> str:
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    if len(s) >= 6 and (s[-6] in ["+", "-"]) and s[-3] == ":":
        return s
    return s + "+03:00"


def add_months(dt: datetime, months: int) -> datetime:
    y = dt.year
    m = dt.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return dt.replace(year=y, month=m)


def calc_start_cursor_iso() -> str:
    tz = timezone(timedelta(hours=3))
    now = datetime.now(tz)
    start = add_months(now, -2).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


def run_backfill_full():
    load_dotenv()

    cursor = calc_start_cursor_iso()
    print(f"[PIZ] start_cursor={cursor}")

    total_rows = 0
    total_norm = 0
    pages = 0

    import time

    while True:
        pages += 1
        print(f"[PIZ] page={pages} dateFrom={cursor}")

        chunk = fetch_orders_page(cursor, flag=0)
        print(f"[PIZ] page={pages} rows={len(chunk)}")

        if not chunk:
            print("[PIZ] done: got empty []")
            break

        insert_wb_orders_raw_dedup(chunk)
        total_rows += len(chunk)

        norm_rows = [normalize_wb_order(r) for r in chunk if r.get("srid")]
        upsert_wb_orders_norm(norm_rows)
        total_norm += len(norm_rows)

        last = chunk[-1]
        new_cursor = last.get("lastChangeDate") or last.get("date")
        if not new_cursor:
            print("[PIZ] stop: last record has no lastChangeDate/date")
            break

        new_cursor = ensure_tz(new_cursor)

        print(f"[PIZ] totals: raw_seen={total_rows} norm_upserted={total_norm} new_cursor={new_cursor}")

        set_job_cursor(JOB_NAME_BACKFILL, new_cursor)
        set_job_cursor(JOB_NAME_MAIN, new_cursor)

        # Финальная страница: МЕНЬШЕ лимита => можно завершать без лишнего запроса. [web:197]
        if len(chunk) < MAX_ROWS_PER_RESPONSE:
            print("[PIZ] stop: rows < 80000 (final page likely reached)")
            break

        # Иначе строк >= лимита: возможно есть продолжение
        if new_cursor == cursor:
            print("[PIZ] stop: cursor not advancing")
            break

        cursor = new_cursor
        time.sleep(SLEEP_SEC)

    deleted = cleanup_wb_orders_raw_dedup(14)
    print(f"[PIZ] cleanup_raw_dedup_deleted={deleted}")


if __name__ == "__main__":
    load_dotenv()

    if not try_advisory_lock(LOCK_ID):
        print("[PIZ] skip: lock busy (hourly job or previous run is active)")
        raise SystemExit(0)

    try:
        run_backfill_full()
    finally:
        advisory_unlock(LOCK_ID)
