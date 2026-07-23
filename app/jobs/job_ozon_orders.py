"""
app/jobs/job_ozon_orders.py
ETL-джоб: Ozon FBO заказы.
Первый запуск: с 1 января текущего года.
Дальше: актуализация от job_cursors с lookback.
"""
from __future__ import annotations

import os
import sys
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

from app.clients.http_ozon_seller import OzonSellerClient, iter_fbo_postings, response_sha256
from app.normalize.norm_ozon_orders import normalize_ozon_fbo_order_items_full, normalize_ozon_fbo_posting


JOB_NAME = "ozon_orders"
ALERT_NAME = "Ozon_Orders_Sync"
LOCK_ID = 4_242_201


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        get_job_cursor,
        insert_job_run,
        insert_raw_api_responses,
        set_job_cursor,
        try_advisory_lock,
        upsert_ozon_fbo_order_items_full,
        upsert_ozon_fbo_postings,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "get_job_cursor": get_job_cursor,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "set_job_cursor": set_job_cursor,
        "try_advisory_lock": try_advisory_lock,
        "upsert_ozon_fbo_order_items_full": upsert_ozon_fbo_order_items_full,
        "upsert_ozon_fbo_postings": upsert_ozon_fbo_postings,
    }


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _first_run_start(today: date | None = None) -> datetime:
    current = today or datetime.now(timezone.utc).date()
    configured = (os.getenv("OZON_ORDERS_FIRST_RUN_DATE") or "").strip()
    if configured:
        return datetime.combine(date.fromisoformat(configured), time.min, tzinfo=timezone.utc)
    return datetime(current.year, 1, 1, tzinfo=timezone.utc)


def _apply_lookback(cursor: datetime, minutes: int) -> datetime:
    return cursor - timedelta(minutes=max(0, minutes))


def _max_cursor_from_postings(rows: list[dict], fallback: datetime) -> datetime:
    best = fallback
    for row in rows:
        for field in ("in_process_at", "shipment_date"):
            parsed = _parse_dt(row.get(field))
            if parsed and parsed > best:
                best = parsed
    return best


def _load_job_config() -> dict[str, object]:
    first_run_start = _first_run_start()
    since_override = _parse_dt(os.getenv("OZON_ORDERS_SINCE"))
    until_override = _parse_dt(os.getenv("OZON_ORDERS_UNTIL"))
    lookback_minutes = max(0, min(int(os.getenv("OZON_ORDERS_LOOKBACK_MINUTES", "180")), 24 * 60))
    debug_sleep = max(0, min(int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0")), 3600))
    dry_run = os.getenv("OZON_ORDERS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    log_file = (os.getenv("OZON_ORDERS_LOG_FILE") or "").strip() or "logs/job_ozon_orders.log"
    return {
        "first_run_start": first_run_start,
        "since_override": since_override,
        "until_override": until_override,
        "lookback_minutes": lookback_minutes,
        "debug_sleep": debug_sleep,
        "dry_run": dry_run,
        "log_file": log_file,
    }


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)

    if cfg["dry_run"]:
        since = cfg["since_override"] or cfg["first_run_start"]
        until = cfg["until_override"] or datetime.now(timezone.utc).replace(microsecond=0)
        log.info("Заказы Ozon: сухой запуск, период=%s..%s", since.isoformat(), until.isoformat())
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Заказы Ozon: задача уже выполняется, выходим.")
        return 0

    if int(cfg["debug_sleep"]) > 0:
        log.info("Заказы Ozon: отладочная пауза после лока: %s сек.", cfg["debug_sleep"])
        time_module.sleep(int(cfg["debug_sleep"]))

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    raw_versions_inserted = 0
    item_upserted = 0
    cursor_old = ""
    cursor_used = ""
    cursor_new = ""
    http_logs: list[dict] = []

    try:
        client = OzonSellerClient()
        postings_raw: list[dict] = []
        posting_rows: list[dict] = []
        full_item_rows: list[dict] = []

        cursor_old = db["get_job_cursor"](JOB_NAME) or ""
        until = cfg["until_override"] or datetime.now(timezone.utc).replace(microsecond=0)
        if cfg["since_override"]:
            since = cfg["since_override"]
        elif cursor_old:
            parsed_cursor = _parse_dt(cursor_old)
            since = _apply_lookback(parsed_cursor or cfg["first_run_start"], int(cfg["lookback_minutes"]))
        else:
            since = cfg["first_run_start"]
        cursor_used = since.isoformat()

        log.info("Заказы Ozon: старт, период=%s..%s, старый курсор=%s", since.isoformat(), until.isoformat(), cursor_old or "-")
        for page_items, response_log in iter_fbo_postings(client, since=since, until=until):
            api_rows += len(page_items)
            postings_raw.extend(page_items)
            http_logs.append(
                {
                    "run_id": started_at,
                    "marketplace": "ozon",
                    "method_name": response_log.method_name,
                    "http_method": response_log.http_method,
                    "url": response_log.url,
                    "request_payload": response_log.request_payload,
                    "response_status": response_log.response_status,
                    "response_payload": response_log.response_payload,
                    "response_sha256": response_sha256(response_log.response_payload),
                    "duration_ms": response_log.duration_ms,
                    "attempt": response_log.attempt,
                    "error": response_log.error,
                }
            )
            log.info("Заказы Ozon: получена страница, строк=%d, всего=%d", len(page_items), api_rows)

        for row in postings_raw:
            posting = normalize_ozon_fbo_posting(row)
            if posting is not None:
                posting_rows.append(posting)
            full_item_rows.extend(normalize_ozon_fbo_order_items_full(row))

        log.info(
            "Заказы Ozon: начинаю запись в БД, отправлений=%d, полных товарных строк=%d, HTTP-логов=%d",
            len(posting_rows),
            len(full_item_rows),
            len(http_logs),
        )
        raw_versions_inserted = db["upsert_ozon_fbo_postings"](posting_rows, run_id=started_at)
        log.info("Заказы Ozon: raw postings обновлены, новых/изменённых версий=%d", raw_versions_inserted)
        item_upserted = db["upsert_ozon_fbo_order_items_full"](full_item_rows, run_id=started_at)
        log.info("Заказы Ozon: полная техническая таблица обновлена, новых/изменённых строк=%d", item_upserted)
        db["insert_raw_api_responses"](http_logs)
        log.info("Заказы Ozon: raw HTTP logs записаны, строк=%d", len(http_logs))
        new_cursor_dt = _max_cursor_from_postings(postings_raw, until)
        cursor_new = max(new_cursor_dt, until).isoformat()
        if not cfg["since_override"] and not cfg["until_override"]:
            db["set_job_cursor"](JOB_NAME, cursor_new)

        log.info(
            "Заказы Ozon: новых/изменённых raw-версий=%d, новых/изменённых товарных строк=%d, HTTP-логов=%d, новый курсор=%s",
            raw_versions_inserted,
            item_upserted,
            len(http_logs),
            cursor_new,
        )
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Заказы Ozon: ОШИБКА - %s", error)
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            db["insert_job_run"](
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=api_rows,
                raw_new_versions=raw_versions_inserted,
                norm_upserted=item_upserted,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=cursor_old or None,
                cursor_used=cursor_used or None,
                cursor_new=cursor_new or None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Заказы Ozon: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            msg = (
                f"✅ {ALERT_NAME} | {ts} | OK\n\n"
                f"➡ Ozon API строк: {api_rows}\n\n"
                f"🧾 Новых/изменённых версий: {raw_versions_inserted}\n\n"
                f"🔄 Товарных строк: {item_upserted}"
            )
        else:
            msg = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(msg, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
