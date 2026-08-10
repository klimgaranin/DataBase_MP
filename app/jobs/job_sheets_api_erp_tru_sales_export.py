"""
Джоб: выгрузить ERP/TRU продажи из PostgreSQL в Google Sheets.
"""
from __future__ import annotations

import os
import sys
from html import escape
from pathlib import Path
from typing import Optional

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

from app.db import advisory_unlock, insert_job_run, try_advisory_lock
from app.ops.sheets_export import ApiErpTruSalesExportResult, run_api_erp_tru_sales_to_sheets


JOB_NAME = "sheets_api_erp_tru_sales_export"
ALERT_NAME = "Sheets_API_ERP_TRU_Sales_Export"
LOCK_ID = 4_242_304
PROJECT_ROOT = _THIS.parent.parent.parent


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_sheets_api_erp_tru_sales_export.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "dry_run": os.getenv("SHEETS_API_ERP_TRU_SALES_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "да"},
        "mode": (os.getenv("SHEETS_API_ERP_TRU_SALES_EXPORT_MODE") or "replace").strip().lower(),
        "log_file": _resolve_log_file(os.getenv("SHEETS_API_ERP_TRU_SALES_EXPORT_LOG_FILE")),
    }


def _format_result(result: ApiErpTruSalesExportResult) -> str:
    sync = result.sync
    if sync is None:
        return "нет результата"
    return (
        f"строк={result.rows_count}, лист={result.sheet_name}!{result.start_cell}, "
        f"режим={sync.mode}, без изменений={sync.unchanged_rows}, "
        f"обновлено={sync.changed_rows}, добавлено={sync.appended_rows}, "
        f"устаревших={sync.stale_rows}, строк листа добавлено={sync.added_sheet_rows}, "
        f"ячеек={sync.updated_cells}"
    )


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)

    mode = str(cfg["mode"])
    if mode not in {"upsert", "replace"}:
        log.error("Выгрузка ERP/TRU продаж в Sheets: неизвестный режим %s", mode)
        return 2

    if not try_advisory_lock(LOCK_ID):
        log.info("Выгрузка ERP/TRU продаж в Sheets: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    result: ApiErpTruSalesExportResult | None = None

    try:
        log.info("Выгрузка ERP/TRU продаж в Sheets: старт, режим=%s, dry_run=%s", mode, bool(cfg["dry_run"]))
        result = run_api_erp_tru_sales_to_sheets(
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        log.info("Выгрузка ERP/TRU продаж в Sheets: %s", _format_result(result))
        return 0
    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Выгрузка ERP/TRU продаж в Sheets: ОШИБКА - %s", error)
        return 2
    finally:
        finished_at = now_iso_utc()
        try:
            sync = result.sync if result is not None else None
            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=sync.updated_cells if sync is not None else 0,
                raw_new_versions=0,
                norm_upserted=result.rows_count if result is not None else 0,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=None,
                cursor_used=None,
                cursor_new=None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Выгрузка ERP/TRU продаж в Sheets: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok" and result is not None:
            msg = f"✅ {ALERT_NAME} | {ts} | OK\n\n➡ {_format_result(result)}"
        else:
            msg = f"❌ {ALERT_NAME} | {ts} | FAIL\n{escape((error or 'unknown')[:200])}"
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
