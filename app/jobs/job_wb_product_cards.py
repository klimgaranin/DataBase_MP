"""ETL-джоб: WB карточки товаров и официальные ссылки на фотографии."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import load_env, now_iso_utc, now_msk_label, setup_logging, setup_sys_path, tg_send

setup_sys_path(__file__)
load_env(__file__)

from app.clients.http_wb_content import WbContentClient, iter_cards_list, response_sha256
from app.normalize.norm_product_cards import normalize_wb_content_card


JOB_NAME = "wb_product_cards"
ALERT_NAME = "WB_Product_Cards"
LOCK_ID = 4_242_205


def _resolve_log_file(value: str | None) -> str:
    path = Path((value or "").strip()) if (value or "").strip() else _THIS.parent.parent.parent / "logs" / "job_wb_product_cards.log"
    if not path.is_absolute():
        path = _THIS.parent.parent.parent / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, Any]:
    return {
        "dry_run": (os.getenv("WB_PRODUCT_CARDS_DRY_RUN") or "0").strip().lower() in {"1", "true", "yes"},
        "page_limit": max(1, min(int(os.getenv("WB_PRODUCT_CARDS_PAGE_LIMIT", "100")), 100)),
        "max_pages": max(1, min(int(os.getenv("WB_PRODUCT_CARDS_MAX_PAGES", "1000")), 5000)),
        "log_file": _resolve_log_file(os.getenv("WB_PRODUCT_CARDS_LOG_FILE")),
    }


def _db_functions() -> dict[str, Any]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_raw_api_responses,
        try_advisory_lock,
        upsert_marketplace_product_cards_current,
        upsert_wb_content_cards,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "try_advisory_lock": try_advisory_lock,
        "upsert_marketplace_product_cards_current": upsert_marketplace_product_cards_current,
        "upsert_wb_content_cards": upsert_wb_content_cards,
    }


def _http_log(run_id: str, response_log) -> dict[str, Any]:
    return {
        "run_id": run_id,
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


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"])
    if cfg["dry_run"]:
        log.info("Карточки WB: сухой запуск, API и БД не вызываются")
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Карточки WB: предыдущий запуск ещё выполняется, пропуск.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_rows = 0
    pages = 0
    with_photos = 0

    try:
        client = WbContentClient()
        normalized_rows: list[dict[str, Any]] = []

        log.info("Карточки WB: старт обновления")
        for cards, response_log in iter_cards_list(
            client,
            limit=int(cfg["page_limit"]),
            max_pages=int(cfg["max_pages"]),
            with_photo=-1,
        ):
            pages += 1
            api_rows += len(cards)
            normalized_rows.extend(
                item for row in cards for item in [normalize_wb_content_card(row)] if item is not None
            )
            db["insert_raw_api_responses"]([_http_log(started_at, response_log)])
            log.info("Карточки WB: страница=%d, API строк=%d, всего=%d; raw HTTP сохранён", pages, len(cards), api_rows)

        with_photos = sum(1 for row in normalized_rows if row.get("primary_image"))
        norm_rows += db["upsert_wb_content_cards"](normalized_rows, run_id=started_at)
        norm_rows += db["upsert_marketplace_product_cards_current"](normalized_rows, run_id=started_at)
        log.info("Карточки WB: строк API=%d, с фото=%d, записано строк=%d", api_rows, with_photos, norm_rows)
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Карточки WB: ОШИБКА - %s", error)
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
                raw_new_versions=api_rows,
                norm_upserted=norm_rows,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=None,
                cursor_used=None,
                cursor_new=None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Карточки WB: не удалось записать job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            message = f"✅ {ALERT_NAME} | {ts} | OK\n\n➡ API строк: {api_rows}\n➡ С фото: {with_photos}\n\n🔄 Обновлено строк: {norm_rows}"
        else:
            message = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(message, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
