"""
Джоб: выгрузить себестоимость 1С по WB/Ozon в Google Sheets.
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

from app.db import advisory_unlock, insert_job_run, try_advisory_lock
from app.ops.telegram_alerts import JobAlert, render_job_alert, sheet_rows_metric, sheet_sync_warnings
from app.ops.sheets_export import DEFAULT_MP_COST_SPREADSHEET_ID, SourceBlockExportResult, run_source_block_to_sheets


JOB_NAME = "sheets_source_costs_export"
ALERT_NAME = "Sheets_Source_Costs_Export"
LOCK_ID = 4_242_402
PROJECT_ROOT = _THIS.parent.parent.parent


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_sheets_source_costs_export.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "dry_run": os.getenv("SHEETS_SOURCE_COSTS_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "да"},
        "mode": (os.getenv("SHEETS_SOURCE_COSTS_EXPORT_MODE") or "replace").strip().lower(),
        "spreadsheet_id": (os.getenv("SHEETS_SOURCE_COSTS_SPREADSHEET_ID") or DEFAULT_MP_COST_SPREADSHEET_ID).strip(),
        "log_file": _resolve_log_file(os.getenv("SHEETS_SOURCE_COSTS_EXPORT_LOG_FILE")),
    }


def _format_result(label: str, result: SourceBlockExportResult) -> str:
    sync = result.sync
    if sync is None:
        return f"{label}: нет результата"
    return (
        f"{label}: строк={result.rows_count}, лист={result.sheet_name}!{result.start_cell}, "
        f"режим={sync.mode}, обновлено={sync.changed_rows}, добавлено={sync.appended_rows}, "
        f"устаревших={sync.stale_rows}, ячеек={sync.updated_cells}"
    )


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)

    mode = str(cfg["mode"])
    if mode not in {"upsert", "replace"}:
        log.error("Выгрузка себестоимости в Sheets: неизвестный режим %s", mode)
        return 2

    if not try_advisory_lock(LOCK_ID):
        log.info("Выгрузка себестоимости в Sheets: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    general: SourceBlockExportResult | None = None
    ozon: SourceBlockExportResult | None = None
    wb: SourceBlockExportResult | None = None

    try:
        log.info("Выгрузка себестоимости в Sheets: старт, режим=%s, dry_run=%s", mode, bool(cfg["dry_run"]))
        general = run_source_block_to_sheets(
            block="source-cost-general",
            spreadsheet_id=str(cfg["spreadsheet_id"]),
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        ozon = run_source_block_to_sheets(
            block="source-cost-ozon",
            spreadsheet_id=str(cfg["spreadsheet_id"]),
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        wb = run_source_block_to_sheets(
            block="source-cost-wb",
            spreadsheet_id=str(cfg["spreadsheet_id"]),
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        log.info("Выгрузка себестоимости в Sheets: %s", _format_result("Общий", general))
        log.info("Выгрузка себестоимости в Sheets: %s", _format_result("Ozon", ozon))
        log.info("Выгрузка себестоимости в Sheets: %s", _format_result("WB", wb))
        return 0
    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Выгрузка себестоимости в Sheets: ОШИБКА - %s", error)
        return 2
    finally:
        finished_at = now_iso_utc()
        try:
            ozon_sync = ozon.sync if ozon is not None else None
            wb_sync = wb.sync if wb is not None else None
            general_sync = general.sync if general is not None else None
            updated_cells = (
                (general_sync.updated_cells if general_sync is not None else 0)
                + (ozon_sync.updated_cells if ozon_sync is not None else 0)
                + (wb_sync.updated_cells if wb_sync is not None else 0)
            )
            rows_count = (
                (general.rows_count if general is not None else 0)
                + (ozon.rows_count if ozon is not None else 0)
                + (wb.rows_count if wb is not None else 0)
            )
            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=updated_cells,
                raw_new_versions=0,
                norm_upserted=rows_count,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=None,
                cursor_used=None,
                cursor_new=None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Выгрузка себестоимости в Sheets: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok" and general is not None and ozon is not None and wb is not None:
            msg = render_job_alert(
                JobAlert(
                    job_name=ALERT_NAME,
                    timestamp=ts,
                    status="OK",
                    metrics=(
                        sheet_rows_metric("Общий", general),
                        sheet_rows_metric("Ozon", ozon),
                        sheet_rows_metric("WB", wb),
                    ),
                    warnings=sheet_sync_warnings((general, ozon, wb)),
                )
            )
        else:
            msg = render_job_alert(JobAlert(job_name=ALERT_NAME, timestamp=ts, status="FAIL", error=error))
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
