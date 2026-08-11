"""
Джоб: себестоимость 1С из файла СС_общий.txt.
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

from app.clients.local_source_cost_file import DEFAULT_COST_FILE_PATH, read_source_cost_file
from app.clients.local_source_files import file_sha256
from app.normalize.norm_source_costs import normalize_source_cost_row


JOB_NAME = "source_costs"
ALERT_NAME = "Source_Costs"
LOCK_ID = 4_242_401
PROJECT_ROOT = _THIS.parent.parent.parent
SOURCE_NAME = "СС_общий"
TABLE_NAME = "source_cost_by_warehouse"


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        get_latest_source_file_sha256,
        insert_job_run,
        insert_source_file_snapshots,
        replace_source_cost_by_warehouse_current,
        try_advisory_lock,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "get_latest_source_file_sha256": get_latest_source_file_sha256,
        "insert_job_run": insert_job_run,
        "insert_source_file_snapshots": insert_source_file_snapshots,
        "replace_source_cost_by_warehouse_current": replace_source_cost_by_warehouse_current,
        "try_advisory_lock": try_advisory_lock,
    }


def _resolve_log_file(value: str | None) -> str:
    configured = (value or "").strip()
    path = Path(configured) if configured else PROJECT_ROOT / "logs" / "job_source_costs.log"
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _load_job_config() -> dict[str, object]:
    return {
        "path": os.getenv("SOURCE_COSTS_FILE_PATH", DEFAULT_COST_FILE_PATH).strip(),
        "skip_unchanged": os.getenv("SOURCE_COSTS_SKIP_UNCHANGED", "1").strip().lower() in {"1", "true", "yes", "да"},
        "dry_run": os.getenv("SOURCE_COSTS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "да"},
        "no_changes_exit_code": int(os.getenv("SOURCE_COSTS_NO_CHANGES_EXIT_CODE", "0") or "0"),
        "log_file": _resolve_log_file(os.getenv("SOURCE_COSTS_LOG_FILE")),
    }


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)
    db = None if cfg["dry_run"] else _db_functions()
    lock_acquired = False
    if not cfg["dry_run"]:
        if not db["try_advisory_lock"](LOCK_ID):
            log.info("Себестоимость 1С: задача уже выполняется, выходим.")
            return 0
        lock_acquired = True

    started_at = now_iso_utc()
    status = "ok"
    error: Optional[str] = None
    raw_inserted = 0
    norm_rows = 0
    api_rows = 0
    no_changes = False

    try:
        path = Path(str(cfg["path"]))
        sha256 = file_sha256(path)
        if cfg["skip_unchanged"] and not cfg["dry_run"]:
            previous = db["get_latest_source_file_sha256"](source_name=SOURCE_NAME, table_name=TABLE_NAME)
            if previous == sha256:
                no_changes = True
                log.info("Себестоимость 1С: файл не изменился, БД и Sheets не трогаем: %s", path)
                return int(cfg.get("no_changes_exit_code") or 0)

        parsed = read_source_cost_file(path)
        raw_rows = parsed["warehouse_rows"]
        normalized = [
            row
            for source_row in raw_rows
            for row in [normalize_source_cost_row(source_row)]
            if row is not None
        ]
        api_rows = int(parsed["data_row_count"])

        if cfg["dry_run"]:
            log.info(
                "Себестоимость 1С: dry-run, файл=%s, строк товара=%d, складских строк=%d, нормализовано=%d",
                path,
                api_rows,
                len(raw_rows),
                len(normalized),
            )
            norm_rows = len(normalized)
        else:
            raw_inserted = db["insert_source_file_snapshots"](
                run_id=started_at,
                source_name=SOURCE_NAME,
                file_path=str(path),
                file_sha256=sha256,
                tables=[
                    {
                        "table_name": TABLE_NAME,
                        "row_count": len(raw_rows),
                        "payload": parsed,
                    }
                ],
            )
            norm_rows = db["replace_source_cost_by_warehouse_current"](normalized, run_id=started_at)

        log.info(
            "Себестоимость 1С: raw snapshot=%d, товаров=%d, складских строк=%d, current строк=%d",
            raw_inserted,
            api_rows,
            len(raw_rows),
            norm_rows,
        )
        return 0
    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Себестоимость 1С: ОШИБКА - %s", error)
        return 2
    finally:
        finished_at = now_iso_utc()
        try:
            if not cfg["dry_run"] and not no_changes:
                db["insert_job_run"](
                    job_name=JOB_NAME,
                    started_at_iso=started_at,
                    finished_at_iso=finished_at,
                    status=status,
                    api_rows=api_rows,
                    raw_new_versions=raw_inserted,
                    norm_upserted=norm_rows,
                    duplicates=0,
                    dup_pct=0.0,
                    cursor_old=None,
                    cursor_used=None,
                    cursor_new=None,
                    error=error,
                )
        except Exception as job_error:
            log.warning("Себестоимость 1С: не удалось записать итог запуска в job_runs: %s", job_error)

        if not cfg["dry_run"] and not no_changes:
            ts = now_msk_label()
            if status == "ok":
                msg = (
                    f"✅ {ALERT_NAME} | {ts} | OK\n\n"
                    f"➡ Товаров строк: {api_rows}\n"
                    f"➡ Складских строк: {norm_rows}\n\n"
                    f"🔄 Current-слой заменён"
                )
            else:
                msg = f"❌ {ALERT_NAME} | {ts} | FAIL\n{(error or 'unknown')[:200]}"
            tg_send(msg, logger=log)

        if lock_acquired:
            db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
