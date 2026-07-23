"""
app/jobs/job_ozon_stocks.py
ETL-джоб: Ozon товары/SKU и аналитика остатков.
Расписание: 2 раза в день.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

from app.clients.http_ozon_seller import (
    OzonSellerClient,
    chunked,
    fetch_analytics_stocks_batch,
    fetch_product_info_list,
    iter_product_list,
    response_sha256,
)
from app.normalize.norm_ozon_stocks import (
    merge_analytics_stock_rows,
    normalize_analytics_stock,
    normalize_product_info_item,
    normalize_product_list_item,
)


JOB_NAME = "ozon_stocks"
LOCK_ID = 4_242_202


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_ozon_analytics_stocks,
        insert_ozon_stock_details,
        insert_raw_api_responses,
        try_advisory_lock,
        upsert_core_ozon_marketplace_products,
        upsert_ozon_product_info_items,
        upsert_ozon_product_list_items,
        upsert_ozon_stock_by_cluster,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_ozon_analytics_stocks": insert_ozon_analytics_stocks,
        "insert_ozon_stock_details": insert_ozon_stock_details,
        "insert_raw_api_responses": insert_raw_api_responses,
        "try_advisory_lock": try_advisory_lock,
        "upsert_core_ozon_marketplace_products": upsert_core_ozon_marketplace_products,
        "upsert_ozon_product_info_items": upsert_ozon_product_info_items,
        "upsert_ozon_product_list_items": upsert_ozon_product_list_items,
        "upsert_ozon_stock_by_cluster": upsert_ozon_stock_by_cluster,
    }


def _load_job_config() -> dict[str, object]:
    return {
        "dry_run": os.getenv("OZON_STOCKS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"},
        "log_file": (os.getenv("OZON_STOCKS_LOG_FILE") or "").strip() or None,
        "stock_batch_workers": max(1, min(int(os.getenv("OZON_STOCKS_BATCH_WORKERS", "20")), 50)),
    }


def _http_log(run_id: str, response_log) -> dict:
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
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)
    if cfg["dry_run"]:
        log.info("Остатки Ozon: сухой запуск, API и БД не вызываются")
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Остатки Ozon: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    snapped_at = started_at
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_rows = 0
    http_logs: list[dict] = []

    try:
        client = OzonSellerClient()
        product_list_rows = []
        for visibility in ("ALL", "ARCHIVED"):
            for items, response_log in iter_product_list(client, visibility=visibility):
                product_list_rows.extend(items)
                http_logs.append(_http_log(started_at, response_log))
                log.info(
                    "Остатки Ozon: список товаров, видимость=%s, строк на странице=%d, всего=%d",
                    visibility,
                    len(items),
                    len(product_list_rows),
                )

        product_list_norm = [
            item for row in product_list_rows for item in [normalize_product_list_item(row)] if item is not None
        ]
        product_ids = sorted({row["product_id"] for row in product_list_norm if row.get("product_id")})

        product_info_raw = []
        for product_id_chunk in chunked(product_ids, 1000):
            items, response_log = fetch_product_info_list(client, product_ids=product_id_chunk)
            product_info_raw.extend(items)
            http_logs.append(_http_log(started_at, response_log))
            log.info("Остатки Ozon: карточки товаров, строк на странице=%d, всего=%d", len(items), len(product_info_raw))

        product_info_norm = [
            item for row in product_info_raw for item in [normalize_product_info_item(row)] if item is not None
        ]
        skus = sorted({row["sku"] for row in product_info_norm if row.get("sku")})

        sku_chunks = list(chunked(skus, 100))
        log.info("Остатки Ozon: подготовлено запросов к analytics/stocks=%d", len(sku_chunks))
        stock_raw, stock_response_logs = fetch_analytics_stocks_batch(
            client,
            sku_chunks=sku_chunks,
            max_workers=int(cfg["stock_batch_workers"]),
        )
        http_logs.extend(_http_log(started_at, response_log) for response_log in stock_response_logs)
        log.info("Остатки Ozon: получены остатки по SKU, строк=%d, HTTP-ответов=%d", len(stock_raw), len(stock_response_logs))

        stock_norm = [item for row in stock_raw for item in [normalize_analytics_stock(row)] if item is not None]
        stock_staging_rows = merge_analytics_stock_rows(stock_norm)
        api_rows = len(product_list_rows) + len(product_info_raw) + len(stock_raw)

        norm_rows += db["upsert_ozon_product_list_items"](product_list_norm, run_id=started_at)
        norm_rows += db["upsert_ozon_product_info_items"](product_info_norm, run_id=started_at)
        norm_rows += db["upsert_core_ozon_marketplace_products"](product_info_norm)
        norm_rows += db["insert_ozon_analytics_stocks"](stock_norm, run_id=started_at, snapped_at=snapped_at)
        norm_rows += db["insert_ozon_stock_details"](stock_norm, run_id=started_at, snapped_at=snapped_at)
        norm_rows += db["upsert_ozon_stock_by_cluster"](stock_staging_rows, run_id=started_at, snapped_at=snapped_at)
        db["insert_raw_api_responses"](http_logs)

        log.info("Остатки Ozon: строк API=%d, записано строк=%d, HTTP-логов=%d", api_rows, norm_rows, len(http_logs))
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Остатки Ozon: ОШИБКА - %s", error)
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
            log.warning("Остатки Ozon: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            msg = f"✅ Ozon_Stocks | {ts} | OK\n\n➡ Ozon API строк: {api_rows}\n\n🔄 Обновлено строк: {norm_rows}"
        else:
            msg = f"❌ Ozon_Stocks | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(msg, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
