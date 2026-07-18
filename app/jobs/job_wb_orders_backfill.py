"""
app/jobs/job_wb_orders_backfill.py
Ручной бэкфилл заказов WB: загружает ВСЮ историю с указанной даты постранично.
Каждая страница сразу пишется в БД — память не копится.
После каждой страницы курсор сохраняется — можно возобновить после обрыва.

Запуск:
    python app/jobs/job_wb_orders_backfill.py 2026-01-01
"""
from __future__ import annotations

import sys
import time
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

_THIS = Path(__file__).resolve()

def _add_sys_path() -> None:
    # jobs/ → app/ → DataBase_MP/
    for p in (_THIS.parent.parent, _THIS.parent.parent.parent):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)

_add_sys_path()

for _c in (
    _THIS.parent.parent.parent / ".env",   # DataBase_MP/.env
    _THIS.parent.parent / ".env",          # app/.env (запасной)
):
    if _c.exists():
        load_dotenv(dotenv_path=_c)
        break
else:
    load_dotenv()

try:
    from app.clients.http_wb_statistics import iter_orders          # type: ignore
    from app.db import (                                             # type: ignore
        insert_wb_orders_raw_dedup, cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm, set_job_cursor,
        try_advisory_lock, advisory_unlock,
    )
    from app.normalize.norm_wb_orders import normalize_wb_order     # type: ignore
except Exception:
    from clients.http_wb_statistics import iter_orders              # type: ignore
    from db import (                                                 # type: ignore
        insert_wb_orders_raw_dedup, cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm, set_job_cursor,
        try_advisory_lock, advisory_unlock,
    )
    from normalize.norm_wb_orders import normalize_wb_order         # type: ignore

JOB_NAME_MAIN     = "wb_orders"
JOB_NAME_BACKFILL = "wb_orders_backfill"
LOCK_ID           = 4_242_001

SLEEP_SEC  = 62        # лимит WB: 1 запрос/мин
THRESHOLD  = 79_000    # если строк >= порога — есть следующая страница

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("backfill")


def _ensure_tz(s: str) -> str:
    s = (s or "").strip()
    if not s or s.endswith("Z"):
        return s
    if len(s) >= 6 and s[-6] in "+-" and s[-3] == ":":
        return s
    return s + "+03:00"


def run_backfill(date_from: str = "2026-01-01T00:00:00+03:00") -> None:
    cursor = _ensure_tz(date_from)
    log.info("Бэкфилл старт: dateFrom=%s", cursor)

    total_api  = 0
    total_norm = 0
    page       = 0

    import requests
    with requests.Session() as sess:
        while True:
            page += 1
            log.info("Страница=%s dateFrom=%s", page, cursor)

            chunk = iter_orders(cursor, flag=0, session=sess)
            log.info("Страница=%s строк=%s", page, len(chunk))

            if not chunk:
                log.info("Пустой ответ — бэкфилл завершён")
                break

            insert_wb_orders_raw_dedup(chunk)
            total_api += len(chunk)

            norm_rows = [
                nr
                for r in chunk
                if r.get("srid")
                for nr in [normalize_wb_order(r)]
                if nr is not None
            ]
            upsert_wb_orders_norm(norm_rows)
            total_norm += len(norm_rows)

            last_change = chunk[-1].get("lastChangeDate") or chunk[-1].get("date")
            if not last_change:
                log.warning("Нет lastChangeDate в последней строке — стоп")
                break

            new_cursor = _ensure_tz(str(last_change))

            log.info(
                "Итого: api=%s norm=%s | курсор_старый=%s курсор_новый=%s",
                total_api, total_norm, cursor, new_cursor,
            )

            # Сохраняем оба курсора — чтобы основной джоб не откатился назад
            set_job_cursor(JOB_NAME_BACKFILL, new_cursor)
            set_job_cursor(JOB_NAME_MAIN,     new_cursor)

            if len(chunk) < THRESHOLD:
                log.info("Строк=%s < порога=%s — финальная страница, стоп", len(chunk), THRESHOLD)
                break

            if new_cursor == cursor:
                log.warning("Курсор не сдвинулся — стоп (защита от зацикливания)")
                break

            cursor = new_cursor
            log.info("Жду %s сек (лимит WB 1 запрос/мин)...", SLEEP_SEC)
            time.sleep(SLEEP_SEC)

    deleted = cleanup_wb_orders_raw_dedup(14)
    log.info("Очистка raw_dedup: удалено=%s", deleted)
    log.info("Бэкфилл завершён: api_rows=%s norm_upserted=%s pages=%s", total_api, total_norm, page)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_arg = sys.argv[1].strip()
        if "T" not in date_arg:
            date_arg = date_arg + "T00:00:00+03:00"
        start_date = date_arg
    else:
        start_date = "2026-01-01T00:00:00+03:00"

    if not try_advisory_lock(LOCK_ID):
        log.warning("Пропуск: лок занят (основной джоб работает)")
        raise SystemExit(0)

    try:
        run_backfill(date_from=start_date)
    finally:
        advisory_unlock(LOCK_ID)
