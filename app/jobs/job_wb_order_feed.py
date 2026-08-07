"""Накопительная ETL-джоба WB Analytics API: Лента заказов за последние 31 сутки."""
from __future__ import annotations

import os
import sys
import time as time_module
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import load_env, now_iso_utc, now_msk_label, setup_logging, setup_sys_path, tg_send

setup_sys_path(__file__)
load_env(__file__)

from app.clients.http_wb_order_feed import WbOrderFeedClient, iter_order_feed, response_sha256
from app.normalize.norm_wb_order_feed import normalize_wb_order_feed_order, parse_dt


JOB_NAME = "wb_order_feed"
ALERT_NAME = "WB_Order_Feed_Sync"
LOCK_ID = 4_242_005
MAX_PERIOD_DAYS = 31


def _resolve_log_file(value: str | None) -> str:
    path = Path((value or "").strip()) if (value or "").strip() else _THIS.parent.parent.parent / "logs" / "job_wb_order_feed.log"
    if not path.is_absolute():
        path = _THIS.parent.parent.parent / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _parse_env_dt(name: str) -> datetime | None:
    return parse_dt(os.getenv(name))


def _period(now: datetime, *, days: int, since_override: datetime | None, until_override: datetime | None) -> tuple[datetime, datetime]:
    until = until_override or now
    since = since_override or (until - timedelta(days=days))
    if since > until:
        raise ValueError("WB_ORDER_FEED_SINCE не может быть позже WB_ORDER_FEED_UNTIL")
    if until - since > timedelta(days=MAX_PERIOD_DAYS):
        raise ValueError("WB Order Feed позволяет запросить максимум 31 день")
    return since, until


def _load_job_config() -> dict[str, Any]:
    days = max(1, min(int(os.getenv("WB_ORDER_FEED_PERIOD_DAYS", str(MAX_PERIOD_DAYS))), MAX_PERIOD_DAYS))
    return {
        "period_days": days,
        "since_override": _parse_env_dt("WB_ORDER_FEED_SINCE"),
        "until_override": _parse_env_dt("WB_ORDER_FEED_UNTIL"),
        "timezone_name": (os.getenv("WB_ORDER_FEED_TIMEZONE") or "UTC").strip() or "UTC",
        "page_limit": max(1, min(int(os.getenv("WB_ORDER_FEED_PAGE_LIMIT", "10000")), 10_000)),
        "max_pages": max(1, min(int(os.getenv("WB_ORDER_FEED_MAX_PAGES", "100")), 1000)),
        "dry_run": (os.getenv("WB_ORDER_FEED_DRY_RUN") or "0").strip().lower() in {"1", "true", "yes"},
        "debug_sleep": max(0, min(int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0")), 3600)),
        "log_file": _resolve_log_file(os.getenv("WB_ORDER_FEED_LOG_FILE")),
    }


def _dedupe_by_srid(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Убирает повторы srid внутри одного snapshot, оставляя наиболее поздний updatedAt."""
    unique: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for row in rows:
        srid = str(row.get("srid") or "").strip()
        if not srid:
            continue
        previous = unique.get(srid)
        if previous is None:
            unique[srid] = row
            continue
        duplicates += 1
        previous_at = previous.get("status_updated_at") or datetime.min.replace(tzinfo=timezone.utc)
        current_at = row.get("status_updated_at") or datetime.min.replace(tzinfo=timezone.utc)
        if current_at > previous_at:
            unique[srid] = row
    return list(unique.values()), duplicates


def _db_functions() -> dict[str, Any]:
    from app.db import (
        advisory_unlock,
        delete_wb_order_feed_versions_for_run,
        get_job_cursor,
        get_wb_order_feed_raw_run_rows,
        insert_job_run,
        insert_raw_api_responses,
        set_job_cursor,
        try_advisory_lock,
        upsert_wb_order_feed_orders,
        upsert_wb_order_feed_orders_full,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "delete_wb_order_feed_versions_for_run": delete_wb_order_feed_versions_for_run,
        "get_job_cursor": get_job_cursor,
        "get_wb_order_feed_raw_run_rows": get_wb_order_feed_raw_run_rows,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "set_job_cursor": set_job_cursor,
        "try_advisory_lock": try_advisory_lock,
        "upsert_wb_order_feed_orders": upsert_wb_order_feed_orders,
        "upsert_wb_order_feed_orders_full": upsert_wb_order_feed_orders_full,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WB Order Feed sync")
    parser.add_argument("--rebuild-from-raw-run", default="", help="пересобрать состояние из raw HTTP run_id без API-вызова")
    args = parser.parse_args(argv)
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"])
    now = datetime.now(timezone.utc).replace(microsecond=0)
    since, until = _period(now, days=cfg["period_days"], since_override=cfg["since_override"], until_override=cfg["until_override"])

    if cfg["dry_run"]:
        log.info("Лента заказов WB: сухой запуск, период=%s..%s, timezone=%s", since.isoformat(), until.isoformat(), cfg["timezone_name"])
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Лента заказов WB: предыдущий запуск ещё выполняется, пропуск.")
        return 0

    if cfg["debug_sleep"]:
        log.info("Лента заказов WB: отладочная пауза после лока: %s сек.", cfg["debug_sleep"])
        time_module.sleep(cfg["debug_sleep"])

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    raw_versions = 0
    staging_changed = 0
    pages = 0
    cursor_old = ""
    cursor_used = since.isoformat()
    cursor_new = ""

    try:
        cursor_old = db["get_job_cursor"](JOB_NAME) or ""
        normalized_rows: list[dict[str, Any]] = []
        run_id_for_rows = started_at
        force_history = False

        if args.rebuild_from_raw_run:
            run_id_for_rows = args.rebuild_from_raw_run
            cursor_used = run_id_for_rows
            cached_rows = db["get_wb_order_feed_raw_run_rows"](run_id_for_rows)
            api_rows = len(cached_rows)
            normalized_rows = [
                item
                for cached in cached_rows
                for item in [normalize_wb_order_feed_order(cached["payload"], currency=cached["currency"])]
                if item is not None
            ]
            deleted = db["delete_wb_order_feed_versions_for_run"](run_id_for_rows)
            force_history = True
            log.info("Лента заказов WB: восстановление из raw run_id=%s, строк=%d, старых версий удалено=%d", run_id_for_rows, api_rows, deleted)
        else:
            client = WbOrderFeedClient()
            log.info(
                "Лента заказов WB: старт, период=%s..%s, timezone=%s, старый курсор=%s",
                since.isoformat(), until.isoformat(), cfg["timezone_name"], cursor_old or "-",
            )
            for orders, currency, response_log in iter_order_feed(
                client,
                start=since,
                end=until,
                timezone_name=cfg["timezone_name"],
                limit=cfg["page_limit"],
                max_pages=cfg["max_pages"],
            ):
                pages += 1
                api_rows += len(orders)
                normalized_rows.extend(
                    item
                    for row in orders
                    for item in [normalize_wb_order_feed_order(row, currency=currency)]
                    if item is not None
                )
                # Raw HTTP-страница фиксируется сразу: источник для восстановления не потеряется при обрыве.
                db["insert_raw_api_responses"]([
                    {
                        "run_id": started_at,
                        "marketplace": "wb",
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
                ])
                log.info("Лента заказов WB: страница=%d, API=%d, всего API=%d; raw HTTP сохранён", pages, len(orders), api_rows)

        normalized, duplicate_rows = _dedupe_by_srid(normalized_rows)
        raw_versions = db["upsert_wb_order_feed_orders"](normalized, run_id=run_id_for_rows, force_history=force_history)
        staging_changed = db["upsert_wb_order_feed_orders_full"](normalized, run_id=run_id_for_rows)
        log.info(
            "Лента заказов WB: уникальных srid=%d, повторов в snapshot=%d, новых/изменённых версий=%d, технических строк=%d",
            len(normalized), duplicate_rows, raw_versions, staging_changed,
        )

        cursor_new = until.isoformat()
        if not args.rebuild_from_raw_run and not cfg["since_override"] and not cfg["until_override"]:
            db["set_job_cursor"](JOB_NAME, cursor_new)
        log.info("Лента заказов WB: завершена, страниц=%d, строк=%d, версий=%d", pages, api_rows, raw_versions)
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Лента заказов WB: ОШИБКА - %s", error)
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
                raw_new_versions=raw_versions,
                norm_upserted=staging_changed,
                cursor_old=cursor_old or None,
                cursor_used=cursor_used,
                cursor_new=cursor_new or None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Лента заказов WB: не удалось записать job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            message = (
                f"✅ {ALERT_NAME} | {ts} | OK\n\n"
                f"➡ Строк WB API: {api_rows}\n"
                f"➡ Страниц: {pages}\n\n"
                f"🧾 Новых/изменённых версий: {raw_versions}\n"
                f"🔄 Технических строк: {staging_changed}"
            )
        else:
            message = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(message, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
