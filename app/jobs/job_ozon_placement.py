"""
app/jobs/job_ozon_placement.py
ETL-джоб: Ozon отчёт стоимости размещения по товарам.
Расписание: 1 раз утром.
"""
from __future__ import annotations

import hashlib
import os
import sys
import time
from datetime import date, timedelta
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
    create_placement_by_products_report,
    download_report_file,
    fetch_report_info,
    response_sha256,
)
from app.normalize.norm_ozon_placement import parse_placement_xlsx


JOB_NAME = "ozon_placement"
LOCK_ID = 4_242_203


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_raw_api_responses,
        replace_ozon_placement_cells,
        replace_ozon_placement_rows,
        try_advisory_lock,
        upsert_ozon_placement_report,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_raw_api_responses": insert_raw_api_responses,
        "replace_ozon_placement_cells": replace_ozon_placement_cells,
        "replace_ozon_placement_rows": replace_ozon_placement_rows,
        "try_advisory_lock": try_advisory_lock,
        "upsert_ozon_placement_report": upsert_ozon_placement_report,
    }


def _load_job_config() -> dict[str, object]:
    yesterday = date.today() - timedelta(days=1)
    date_from = date.fromisoformat(os.getenv("OZON_PLACEMENT_DATE_FROM", yesterday.isoformat()))
    date_to = date.fromisoformat(os.getenv("OZON_PLACEMENT_DATE_TO", yesterday.isoformat()))
    return {
        "date_from": date_from,
        "date_to": date_to,
        "poll_attempts": max(1, min(int(os.getenv("OZON_PLACEMENT_POLL_ATTEMPTS", "20")), 120)),
        "poll_sleep_seconds": max(5, min(int(os.getenv("OZON_PLACEMENT_POLL_SLEEP_SECONDS", "30")), 300)),
        "dry_run": os.getenv("OZON_PLACEMENT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"},
        "log_file": (os.getenv("OZON_PLACEMENT_LOG_FILE") or "").strip() or None,
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


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)
    if cfg["dry_run"]:
        log.info("Стоимость размещения Ozon: сухой запуск, период=%s..%s", cfg["date_from"], cfg["date_to"])
        return 0

    if (cfg["date_to"] - cfg["date_from"]).days > 30:
        raise SystemExit("Стоимость размещения Ozon: максимальный период отчёта 31 день")

    db = _db_functions()
    if not db["try_advisory_lock"](LOCK_ID):
        log.info("Стоимость размещения Ozon: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_rows = 0
    code = ""
    http_logs: list[dict] = []

    try:
        client = OzonSellerClient()
        code, response_log = create_placement_by_products_report(client, date_from=cfg["date_from"], date_to=cfg["date_to"])
        http_logs.append(_http_log(started_at, response_log))
        if not code:
            raise RuntimeError("Стоимость размещения Ozon: API не вернул код отчёта")
        log.info("Стоимость размещения Ozon: отчёт создан, код=%s, период=%s..%s", code, cfg["date_from"], cfg["date_to"])

        info = {}
        for attempt in range(1, int(cfg["poll_attempts"]) + 1):
            info, response_log = fetch_report_info(client, code=code)
            http_logs.append(_http_log(started_at, response_log))
            report_status = info.get("status")
            log.info("Стоимость размещения Ozon: проверка готовности %d, статус=%s", attempt, report_status)
            if report_status == "success":
                break
            if report_status == "failed":
                raise RuntimeError(f"Стоимость размещения Ozon: отчёт завершился ошибкой: {info.get('error')}")
            time.sleep(int(cfg["poll_sleep_seconds"]))
        else:
            raise RuntimeError("Стоимость размещения Ozon: отчёт не был готов за отведённое время")

        file_url = info.get("file") or ""
        if not file_url:
            raise RuntimeError("Стоимость размещения Ozon: отчёт готов, но ссылка на файл отсутствует")
        file_content = download_report_file(file_url)
        file_sha = _sha256(file_content)
        rows = parse_placement_xlsx(file_content)
        api_rows = len(rows)

        db["upsert_ozon_placement_report"](
            {
                "code": code,
                "date_from": cfg["date_from"],
                "date_to": cfg["date_to"],
                "status": info.get("status"),
                "file_url": file_url,
                "file_sha256": file_sha,
                "payload": info,
            },
            run_id=started_at,
        )
        norm_rows = db["replace_ozon_placement_rows"](rows, report_code=code, run_id=started_at)
        placement_cells = db["replace_ozon_placement_cells"](rows, report_code=code, run_id=started_at)
        db["insert_raw_api_responses"](http_logs)

        log.info(
            "Стоимость размещения Ozon: загружено строк=%d, ячеек=%d, SHA-256 файла=%s, HTTP-логов=%d",
            norm_rows,
            placement_cells,
            file_sha,
            len(http_logs),
        )
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Стоимость размещения Ozon: ОШИБКА - %s", error)
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
                raw_new_versions=1 if code else 0,
                norm_upserted=norm_rows,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=None,
                cursor_used=str(cfg["date_from"]),
                cursor_new=str(cfg["date_to"]),
                error=error,
            )
        except Exception as job_error:
            log.warning("Стоимость размещения Ozon: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            msg = f"✅ Ozon_Placement | {ts} | OK\n\n➡ Строк отчёта: {api_rows}\n\n🔄 Загружено строк: {norm_rows}"
        else:
            msg = f"❌ Ozon_Placement | {ts} | FAIL\n{(error or 'unknown')[:200]}"
        tg_send(msg, logger=log)
        db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
