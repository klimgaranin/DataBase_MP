"""
Джоб: выгрузить внутренние файловые блоки из PostgreSQL в Google Sheets.
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
from app.ops.sheets_export import SourceBlockExportResult, run_source_block_to_sheets


JOB_NAME = "sheets_source_files_export"
ALERT_NAME = "Sheets_Source_Files_Export"
LOCK_ID = 4_242_305
PROJECT_ROOT = _THIS.parent.parent.parent


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_sheets_source_files_export.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "dry_run": os.getenv("SHEETS_SOURCE_FILES_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "да"},
        "mode": (os.getenv("SHEETS_SOURCE_FILES_EXPORT_MODE") or "replace").strip().lower(),
        "log_file": _resolve_log_file(os.getenv("SHEETS_SOURCE_FILES_EXPORT_LOG_FILE")),
    }


def _format_result(label: str, result: SourceBlockExportResult) -> str:
    sync = result.sync
    if sync is None:
        return f"{label}: нет результата"
    return (
        f"{label}: строк={result.rows_count}, лист={result.sheet_name}!{result.start_cell}, "
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
        log.error("Выгрузка файловых блоков в Sheets: неизвестный режим %s", mode)
        return 2

    if not try_advisory_lock(LOCK_ID):
        log.info("Выгрузка файловых блоков в Sheets: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    inventory: SourceBlockExportResult | None = None
    pipeline: SourceBlockExportResult | None = None
    specs: SourceBlockExportResult | None = None

    try:
        log.info("Выгрузка файловых блоков в Sheets: старт, режим=%s, dry_run=%s", mode, bool(cfg["dry_run"]))
        inventory = run_source_block_to_sheets(
            block="production-inventory",
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        pipeline = run_source_block_to_sheets(
            block="supply-pipeline",
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        specs = run_source_block_to_sheets(
            block="supply-order-specs",
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        log.info("Выгрузка файловых блоков в Sheets: %s", _format_result("остатки МП", inventory))
        log.info("Выгрузка файловых блоков в Sheets: %s", _format_result("список заказов", pipeline))
        log.info("Выгрузка файловых блоков в Sheets: %s", _format_result("спецификации заказов", specs))
        return 0
    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Выгрузка файловых блоков в Sheets: ОШИБКА - %s", error)
        return 2
    finally:
        finished_at = now_iso_utc()
        try:
            inventory_sync = inventory.sync if inventory is not None else None
            pipeline_sync = pipeline.sync if pipeline is not None else None
            specs_sync = specs.sync if specs is not None else None
            updated_cells = (inventory_sync.updated_cells if inventory_sync is not None else 0) + (
                pipeline_sync.updated_cells if pipeline_sync is not None else 0
            ) + (
                specs_sync.updated_cells if specs_sync is not None else 0
            )
            rows_count = (
                (inventory.rows_count if inventory is not None else 0)
                + (pipeline.rows_count if pipeline is not None else 0)
                + (specs.rows_count if specs is not None else 0)
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
            log.warning("Выгрузка файловых блоков в Sheets: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok" and inventory is not None and pipeline is not None and specs is not None:
            msg = render_job_alert(
                JobAlert(
                    job_name=ALERT_NAME,
                    timestamp=ts,
                    status="OK",
                    metrics=(
                        sheet_rows_metric("Остатки МП", inventory),
                        sheet_rows_metric("Список заказов", pipeline),
                        sheet_rows_metric("Спецификации", specs),
                    ),
                    warnings=sheet_sync_warnings((inventory, pipeline, specs)),
                )
            )
        else:
            msg = render_job_alert(JobAlert(job_name=ALERT_NAME, timestamp=ts, status="FAIL", error=error))
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
