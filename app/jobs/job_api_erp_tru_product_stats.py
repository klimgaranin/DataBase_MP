"""
ETL-джоб: ERP/TRU статистика товаров.

Период по умолчанию: с такого же числа прошлого месяца по сегодня.
Рабочая таблица в БД полностью заменяется каждым запуском.
"""
from __future__ import annotations

import os
import sys
from calendar import monthrange
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

from app.clients.http_api_erp_tru import ApiErpTruClient, response_sha256
from app.normalize.norm_api_erp_tru import normalize_product_stat_row


JOB_NAME = "api_erp_tru_product_stats"
ALERT_NAME = "API_ERP_TRU_Product_Stats"
LOCK_ID = 4_242_303


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_raw_api_responses,
        replace_api_erp_tru_product_stats,
        try_advisory_lock,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "replace_api_erp_tru_product_stats": replace_api_erp_tru_product_stats,
        "try_advisory_lock": try_advisory_lock,
    }


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else _THIS.parent.parent.parent / "logs" / "job_api_erp_tru_product_stats.log"
    if not path.is_absolute():
        path = _THIS.parent.parent.parent / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def previous_month_same_day(today: date) -> date:
    month = today.month - 1
    year = today.year
    if month == 0:
        month = 12
        year -= 1
    day = min(today.day, monthrange(year, month)[1])
    return date(year, month, day)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value.strip())


def _load_job_config() -> dict[str, object]:
    today = datetime.now(timezone.utc).date()
    date_to = _parse_date(os.getenv("API_ERP_TRU_DATE_TO")) or today
    date_from = _parse_date(os.getenv("API_ERP_TRU_DATE_FROM")) or previous_month_same_day(date_to)
    if date_from > date_to:
        raise RuntimeError(f"Некорректный период ERP/TRU: {date_from} > {date_to}")
    return {
        "date_from": date_from,
        "date_to": date_to,
        "wo_sets": os.getenv("API_ERP_TRU_WO_SETS", "false").strip().lower() in {"1", "true", "yes", "да"},
        "dry_run": os.getenv("API_ERP_TRU_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "да"},
        "log_file": _resolve_log_file(os.getenv("API_ERP_TRU_LOG_FILE")),
    }


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)
    date_from = cfg["date_from"]
    date_to = cfg["date_to"]
    if not isinstance(date_from, date) or not isinstance(date_to, date):
        raise RuntimeError("Некорректные даты ERP/TRU")

    if cfg["dry_run"]:
        log.info("ERP/TRU статистика товаров: сухой запуск, период=%s..%s", date_from.isoformat(), date_to.isoformat())
        return 0

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("ERP/TRU статистика товаров: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_rows = 0
    grouped_articles = 0

    try:
        client = ApiErpTruClient()
        log.info("ERP/TRU статистика товаров: старт, период=%s..%s", date_from.isoformat(), date_to.isoformat())
        raw_rows, response_log = client.request_product_stats(
            date_from=date_from,
            date_to=date_to,
            wo_sets=bool(cfg["wo_sets"]),
        )
        if response_log.error:
            raise RuntimeError(response_log.error)
        api_rows = len(raw_rows)
        normalized = [
            item
            for row in raw_rows
            for item in [normalize_product_stat_row(row)]
            if item is not None
        ]
        grouped_articles = len({row["article"] for row in normalized})
        log.info(
            "ERP/TRU статистика товаров: получено API строк=%d, нормализовано=%d, уникальных артикулов=%d",
            api_rows,
            len(normalized),
            grouped_articles,
        )

        db["insert_raw_api_responses"]([
            {
                "run_id": started_at,
                "marketplace": "erp_tru",
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
        _, norm_rows = db["replace_api_erp_tru_product_stats"](
            normalized,
            run_id=started_at,
            period_from=date_from,
            period_to=date_to,
        )
        log.info("ERP/TRU статистика товаров: БД обновлена, строк=%d", norm_rows)
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("ERP/TRU статистика товаров: ОШИБКА - %s", error)
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
                duplicates=max(0, api_rows - grouped_articles),
                dup_pct=(round((max(0, api_rows - grouped_articles) / api_rows) * 100, 2) if api_rows else 0.0),
                cursor_old=None,
                cursor_used=f"{date_from.isoformat()}..{date_to.isoformat()}",
                cursor_new=None,
                error=error,
            )
        except Exception as job_error:
            log.warning("ERP/TRU статистика товаров: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            msg = (
                f"✅ {ALERT_NAME} | {ts} | OK\n\n"
                f"➡ Период: {date_from.isoformat()}..{date_to.isoformat()}\n"
                f"➡ API строк: {api_rows}\n"
                f"➡ Уникальных артикулов: {grouped_articles}\n\n"
                f"🔄 Обновлено строк БД: {norm_rows}"
            )
        else:
            msg = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:500]}"
        tg_send(msg, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
