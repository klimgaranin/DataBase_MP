"""
app/jobs/job_sheets_orders_export.py
Джоб: выгрузить агрегированные заказы Ozon и WB из PostgreSQL в Google Sheets.
"""
from __future__ import annotations

import os
import sys
import time
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
from app.ops.sheets_export import OrderExportResult, run_orders_to_sheets


JOB_NAME = "sheets_orders_export"
ALERT_NAME = "Sheets_Orders_Export"
LOCK_ID = 4_242_301
PROJECT_ROOT = _THIS.parent.parent.parent


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_sheets_orders_export.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if path.is_absolute() and PROJECT_ROOT not in path.parents and not path.parent.exists():
        path = PROJECT_ROOT / "logs" / "job_sheets_orders_export.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "debug_sleep": max(0, min(int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0")), 3600)),
        "dry_run": os.getenv("SHEETS_ORDERS_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"},
        "mode": (os.getenv("SHEETS_ORDERS_EXPORT_MODE") or "upsert").strip().lower(),
        "log_file": _resolve_log_file(os.getenv("SHEETS_ORDERS_EXPORT_LOG_FILE")),
    }


def _format_result(result: OrderExportResult) -> str:
    sync = result.sync
    if sync is None:
        return f"{result.marketplace}: нет результата"
    return (
        f"{result.marketplace}: строк={result.rows_count}, "
        f"лист={result.sheet_name}!{result.start_cell}, "
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
        log.error("Sheets orders export: неизвестный режим %s", mode)
        return 2

    if not try_advisory_lock(LOCK_ID):
        log.info("Sheets orders export: задача уже выполняется, выходим.")
        return 0

    if int(cfg["debug_sleep"]) > 0:
        log.info("Sheets orders export: отладочная пауза после лока: %s сек.", cfg["debug_sleep"])
        time.sleep(int(cfg["debug_sleep"]))

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    api_rows = 0
    norm_upserted = 0
    results: list[OrderExportResult] = []

    try:
        log.info("Sheets orders export: старт, режим=%s, dry_run=%s", mode, bool(cfg["dry_run"]))
        for marketplace in ("ozon", "wb"):
            result = run_orders_to_sheets(
                marketplace=marketplace,
                mode=mode,  # type: ignore[arg-type]
                dry_run=bool(cfg["dry_run"]),
                verbose=False,
            )
            results.append(result)
            norm_upserted += result.rows_count
            sync = result.sync
            if sync is not None:
                api_rows += sync.updated_cells
            log.info("Sheets orders export: %s", _format_result(result))

        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Sheets orders export: ОШИБКА - %s", error)
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=api_rows,
                raw_new_versions=0,
                norm_upserted=norm_upserted,
                duplicates=0,
                dup_pct=0.0,
                cursor_old=None,
                cursor_used=None,
                cursor_new=None,
                error=error,
            )
        except Exception as job_error:
            log.warning("Sheets orders export: не удалось записать итог запуска в job_runs: %s", job_error)

        ts = now_msk_label()
        if status == "ok":
            details = "\n".join(f"➡ {_format_result(result)}" for result in results)
            msg = f"✅ {ALERT_NAME} | {ts} | OK\n\n{details}"
        else:
            msg = f"❌ {ALERT_NAME} | {ts} | FAIL\n{escape((error or 'unknown')[:200])}"
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
