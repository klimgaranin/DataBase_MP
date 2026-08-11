"""
app/jobs/job_sheets_ozon_placement_export.py
Джоб: выгрузить Ozon платное хранение из PostgreSQL в Google Sheets.
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
from app.ops.telegram_alerts import AlertWarning, JobAlert, render_job_alert, sheet_rows_metric, sheet_sync_warnings
from app.ops.sheets_export import PlacementExportResult, run_ozon_placement_to_sheets


JOB_NAME = "sheets_ozon_placement_export"
ALERT_NAME = "Sheets_Ozon_Placement_Export"
LOCK_ID = 4_242_302
PROJECT_ROOT = _THIS.parent.parent.parent


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_sheets_ozon_placement_export.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if path.is_absolute() and PROJECT_ROOT not in path.parents and not path.parent.exists():
        path = PROJECT_ROOT / "logs" / "job_sheets_ozon_placement_export.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "dry_run": os.getenv("SHEETS_OZON_PLACEMENT_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"},
        "mode": (os.getenv("SHEETS_OZON_PLACEMENT_EXPORT_MODE") or "replace").strip().lower(),
        "log_file": _resolve_log_file(os.getenv("SHEETS_OZON_PLACEMENT_EXPORT_LOG_FILE")),
    }


def _format_result(result: PlacementExportResult) -> str:
    sync = result.sync
    report_part = ""
    if result.report_date is not None:
        expected = f", ожидаемая дата={result.expected_report_date}" if result.expected_report_date is not None else ""
        report_part = f", отчёт={result.report_date}{expected}"
    if sync is None:
        return f"нет результата{report_part}"
    return (
        f"строк={result.rows_count}, лист={result.sheet_name}!{result.start_cell}, "
        f"режим={sync.mode}, без изменений={sync.unchanged_rows}, "
        f"обновлено={sync.changed_rows}, добавлено={sync.appended_rows}, "
        f"устаревших={sync.stale_rows}, строк листа добавлено={sync.added_sheet_rows}, "
        f"ячеек={sync.updated_cells}{report_part}"
    )


def _fallback_warning(result: PlacementExportResult) -> str:
    if result.report_date is None or result.expected_report_date is None:
        return ""
    if result.report_date >= result.expected_report_date:
        return ""
    return (
        "\n\n⚠ Использован не сегодняшний отчёт Ozon placement: "
        f"{result.report_date}, ожидали {result.expected_report_date}.\n"
        "Повторная попытка должна сработать через несколько часов."
    )


def _fallback_alert_warning(result: PlacementExportResult) -> AlertWarning | None:
    if result.report_date is None or result.expected_report_date is None:
        return None
    if result.report_date >= result.expected_report_date:
        return None
    return AlertWarning(
        "Использован не сегодняшний отчёт Ozon placement: "
        f"{result.report_date}, ожидали {result.expected_report_date}. "
        "Повторная попытка должна сработать через несколько часов."
    )


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)

    mode = str(cfg["mode"])
    if mode not in {"upsert", "replace"}:
        log.error("Выгрузка Ozon хранения в Sheets: неизвестный режим %s", mode)
        return 2

    if not try_advisory_lock(LOCK_ID):
        log.info("Выгрузка Ozon хранения в Sheets: задача уже выполняется, выходим.")
        return 0

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    result: PlacementExportResult | None = None

    try:
        log.info("Выгрузка Ozon хранения в Sheets: старт, режим=%s, dry_run=%s", mode, bool(cfg["dry_run"]))
        result = run_ozon_placement_to_sheets(
            mode=mode,  # type: ignore[arg-type]
            dry_run=bool(cfg["dry_run"]),
            verbose=False,
        )
        log.info("Выгрузка Ozon хранения в Sheets: %s", _format_result(result))
        warning = _fallback_warning(result)
        if warning:
            log.warning("Выгрузка Ozon хранения в Sheets: %s", warning.replace("\n", " "))
        return 0
    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Выгрузка Ozon хранения в Sheets: ОШИБКА - %s", error)
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
            log.warning("Выгрузка Ozon хранения в Sheets: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok" and result is not None:
            warnings = list(sheet_sync_warnings((result,)))
            fallback_warning = _fallback_alert_warning(result)
            if fallback_warning is not None:
                warnings.append(fallback_warning)
            msg = render_job_alert(
                JobAlert(
                    job_name=ALERT_NAME,
                    timestamp=ts,
                    status="OK",
                    metrics=(sheet_rows_metric("Ozon хранение", result),),
                    warnings=tuple(warnings),
                )
            )
        else:
            msg = render_job_alert(JobAlert(job_name=ALERT_NAME, timestamp=ts, status="FAIL", error=error))
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
