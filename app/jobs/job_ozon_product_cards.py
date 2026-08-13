"""ETL-джоб: Ozon карточки товаров и официальные ссылки на фотографии."""
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

from app.clients.http_ozon_seller import (
    OzonSellerClient,
    chunked,
    fetch_product_info_list,
    iter_product_list,
    response_sha256,
)
from app.normalize.norm_ozon_stocks import normalize_product_info_item, normalize_product_list_item
from app.normalize.norm_product_cards import normalize_ozon_product_card


JOB_NAME = "ozon_product_cards"
ALERT_NAME = "Ozon_Product_Cards"
LOCK_ID = 4_242_206


def _resolve_log_file(value: str | None) -> str:
    path = Path((value or "").strip()) if (value or "").strip() else _THIS.parent.parent.parent / "logs" / "job_ozon_product_cards.log"
    if not path.is_absolute():
        path = _THIS.parent.parent.parent / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, Any]:
    return {
        "dry_run": (os.getenv("OZON_PRODUCT_CARDS_DRY_RUN") or "0").strip().lower() in {"1", "true", "yes"},
        "log_file": _resolve_log_file(os.getenv("OZON_PRODUCT_CARDS_LOG_FILE")),
    }


def _db_functions() -> dict[str, Any]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_raw_api_responses,
        try_advisory_lock,
        upsert_core_ozon_marketplace_products,
        upsert_marketplace_product_cards_current,
        upsert_ozon_product_info_items,
        upsert_ozon_product_list_items,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "try_advisory_lock": try_advisory_lock,
        "upsert_core_ozon_marketplace_products": upsert_core_ozon_marketplace_products,
        "upsert_marketplace_product_cards_current": upsert_marketplace_product_cards_current,
        "upsert_ozon_product_info_items": upsert_ozon_product_info_items,
        "upsert_ozon_product_list_items": upsert_ozon_product_list_items,
    }


def _http_log(run_id: str, response_log) -> dict[str, Any]:
    return {
        "run_id": run_id,
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


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"])
    if cfg["dry_run"]:
        log.info("Карточки Ozon: сухой запуск, API и БД не вызываются")
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Карточки Ozon: предыдущий запуск ещё выполняется, пропуск.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_rows = 0
    with_photos = 0

    try:
        client = OzonSellerClient()
        product_list_rows: list[dict[str, Any]] = []

        log.info("Карточки Ozon: старт обновления")
        for visibility in ("ALL", "ARCHIVED"):
            for items, response_log in iter_product_list(client, visibility=visibility):
                product_list_rows.extend(items)
                db["insert_raw_api_responses"]([_http_log(started_at, response_log)])
                log.info(
                    "Карточки Ozon: список товаров, видимость=%s, строк на странице=%d, всего=%d",
                    visibility,
                    len(items),
                    len(product_list_rows),
                )

        product_list_norm = [
            item for row in product_list_rows for item in [normalize_product_list_item(row)] if item is not None
        ]
        product_ids = sorted({row["product_id"] for row in product_list_norm if row.get("product_id")})
        product_info_raw: list[dict[str, Any]] = []
        for product_id_chunk in chunked(product_ids, 1000):
            items, response_log = fetch_product_info_list(client, product_ids=product_id_chunk)
            product_info_raw.extend(items)
            db["insert_raw_api_responses"]([_http_log(started_at, response_log)])
            log.info("Карточки Ozon: карточки товаров, строк на странице=%d, всего=%d", len(items), len(product_info_raw))

        product_info_norm = [
            item for row in product_info_raw for item in [normalize_product_info_item(row)] if item is not None
        ]
        product_card_norm = [
            item for row in product_info_raw for item in [normalize_ozon_product_card(row)] if item is not None
        ]
        with_photos = sum(1 for row in product_card_norm if row.get("primary_image"))
        api_rows = len(product_list_rows) + len(product_info_raw)
        norm_rows += db["upsert_ozon_product_list_items"](product_list_norm, run_id=started_at)
        norm_rows += db["upsert_ozon_product_info_items"](product_info_norm, run_id=started_at)
        norm_rows += db["upsert_core_ozon_marketplace_products"](product_info_norm)
        norm_rows += db["upsert_marketplace_product_cards_current"](product_card_norm, run_id=started_at)
        log.info("Карточки Ozon: строк API=%d, с фото=%d, записано строк=%d", api_rows, with_photos, norm_rows)
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Карточки Ozon: ОШИБКА - %s", error)
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
            log.warning("Карточки Ozon: не удалось записать job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            message = f"✅ {ALERT_NAME} | {ts} | OK\n\n➡ API строк: {api_rows}\n➡ С фото: {with_photos}\n\n🔄 Обновлено строк: {norm_rows}"
        else:
            message = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(message, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
