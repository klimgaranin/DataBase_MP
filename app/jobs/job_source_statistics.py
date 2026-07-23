"""
app/jobs/job_source_statistics.py
ETL-джоб: файловая статистика из Excel "Статистика.xlsm".

Не заменяет действующие WB jobs. По умолчанию WB-таблицы из XLSM пропускаются,
потому что WB заказы и остатки уже грузятся в PostgreSQL отдельными jobs.
"""
from __future__ import annotations

import hashlib
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

from app.clients.source_statistics_excel import ExcelTableData, read_excel_tables
from app.clients.local_source_files import (
    read_production_inventory_rows,
    read_supply_pipeline_rows,
    resolve_latest_file,
    resolve_latest_file_preferred,
)
from app.normalize.norm_source_statistics import (
    normalize_order_daily,
    normalize_ozon_storage,
    normalize_production_inventory,
    normalize_stock_summary,
    normalize_supply_pipeline,
)


JOB_NAME = "source_statistics"
LOCK_ID = 4_242_101
ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SOURCE_PATH = ROOT / "local" / "source_exports" / "Статистика.xlsm"

ACTIVE_INTERNAL_TABLES = {"Остатки_СМП_ОСН_СОХ_СВХ_ТС", "Список_заказов"}


def _db_functions() -> dict[str, object]:
    from app.db import (
        advisory_unlock,
        insert_job_run,
        insert_production_inventory_snapshot,
        insert_source_file_snapshots,
        try_advisory_lock,
        upsert_ozon_storage_costs,
        upsert_source_orders_daily,
        upsert_source_stock_summary,
        upsert_supply_pipeline_current,
    )

    return {
        "advisory_unlock": advisory_unlock,
        "insert_job_run": insert_job_run,
        "insert_production_inventory_snapshot": insert_production_inventory_snapshot,
        "insert_source_file_snapshots": insert_source_file_snapshots,
        "try_advisory_lock": try_advisory_lock,
        "upsert_ozon_storage_costs": upsert_ozon_storage_costs,
        "upsert_source_orders_daily": upsert_source_orders_daily,
        "upsert_source_stock_summary": upsert_source_stock_summary,
        "upsert_supply_pipeline_current": upsert_supply_pipeline_current,
    }


def _load_job_config() -> dict[str, object]:
    source_path = Path(os.getenv("SOURCE_STATISTICS_FILE", str(DEFAULT_SOURCE_PATH))).expanduser()
    if not source_path.is_absolute():
        source_path = ROOT / source_path
    include_wb_tables = (os.getenv("SOURCE_STATISTICS_INCLUDE_WB_TABLES", "0").strip().lower() in {"1", "true", "yes"})
    dry_run = os.getenv("SOURCE_STATISTICS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    log_file = (os.getenv("SOURCE_STATISTICS_LOG_FILE") or "").strip() or None
    orders_list_path = os.getenv("SOURCE_STATISTICS_ORDERS_LIST_PATH", r"\\tsclient\P\Список заказов").strip()
    stocks_1c_path = os.getenv("SOURCE_STATISTICS_1C_STOCKS_PATH", r"\\tsclient\S\МП").strip()
    return {
        "source_path": source_path,
        "include_wb_tables": include_wb_tables,
        "dry_run": dry_run,
        "log_file": log_file,
        "orders_list_path": orders_list_path,
        "stocks_1c_path": stocks_1c_path,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_snapshots(tables: list[ExcelTableData]) -> list[dict]:
    return [
        {
            "table_name": table.name,
            "row_count": len(table.rows),
            "payload": {
                "sheet_name": table.sheet_name,
                "ref": table.ref,
                "rows": table.rows,
            },
        }
        for table in tables
    ]


def _normalize_tables(tables: list[ExcelTableData]) -> dict[str, list[dict]]:
    table_by_name = {table.name: table for table in tables}

    ozon_orders = [
        row
        for source_row in table_by_name.get("Заказы_OZON", ExcelTableData("", "", "", [])).rows
        for row in [normalize_order_daily(source_row, source_system="ozon")]
        if row is not None
    ]
    yandex_orders = [
        row
        for source_row in table_by_name.get("Заказы_YM", ExcelTableData("", "", "", [])).rows
        for row in [normalize_order_daily(source_row, source_system="yandex_market")]
        if row is not None
    ]
    ozon_stocks = [
        row
        for source_row in table_by_name.get("Остатки_OZON", ExcelTableData("", "", "", [])).rows
        for row in [normalize_stock_summary(source_row, source_system="ozon")]
        if row is not None
    ]
    yandex_stocks = [
        row
        for source_row in table_by_name.get("Остатки_YM", ExcelTableData("", "", "", [])).rows
        for row in [normalize_stock_summary(source_row, source_system="yandex_market")]
        if row is not None
    ]
    ozon_storage = [
        row
        for source_row in table_by_name.get("Хранение_OZON", ExcelTableData("", "", "", [])).rows
        for row in [normalize_ozon_storage(source_row)]
        if row is not None
    ]
    production_inventory = [
        row
        for source_row in table_by_name.get("Остатки_СМП_ОСН_СОХ_СВХ_ТС", ExcelTableData("", "", "", [])).rows
        for row in [normalize_production_inventory(source_row)]
        if row is not None
    ]
    supply_pipeline = [
        row
        for source_row in table_by_name.get("Список_заказов", ExcelTableData("", "", "", [])).rows
        for row in [normalize_supply_pipeline(source_row)]
        if row is not None
    ]
    return {
        "orders": ozon_orders + yandex_orders,
        "stocks": ozon_stocks + yandex_stocks,
        "ozon_storage": ozon_storage,
        "production_inventory": production_inventory,
        "supply_pipeline": supply_pipeline,
    }


def _apply_direct_file_sources(normalized: dict[str, list[dict]], cfg: dict[str, object], log) -> None:
    try:
        orders_path = resolve_latest_file(str(cfg["orders_list_path"]), patterns=("Список*.xlsx", "*.xlsx"))
        rows = read_supply_pipeline_rows(orders_path)
        direct_rows = [row for source_row in rows for row in [normalize_supply_pipeline(source_row)] if row is not None]
        if direct_rows:
            normalized["supply_pipeline"] = direct_rows
            log.info("Файловая статистика: список заказов прочитан из файла=%s, строк=%d", orders_path, len(direct_rows))
    except Exception as exc:
        log.warning("Файловая статистика: список заказов не прочитан напрямую, используем данные из XLSM: %s", exc)

    try:
        stocks_path = resolve_latest_file_preferred(
            str(cfg["stocks_1c_path"]),
            patterns=("Остатки*.txt", "*.txt", "Остатки*.xls", "*.xls"),
        )
        rows = read_production_inventory_rows(stocks_path)
        direct_rows = [row for source_row in rows for row in [normalize_production_inventory(source_row)] if row is not None]
        if direct_rows:
            normalized["production_inventory"] = direct_rows
            log.info("Файловая статистика: остатки 1С прочитаны из файла=%s, строк=%d", stocks_path, len(direct_rows))
    except Exception as exc:
        log.warning("Файловая статистика: остатки 1С не прочитаны напрямую, используем данные из XLSM: %s", exc)


def main() -> int:
    cfg = _load_job_config()
    log = setup_logging(JOB_NAME, log_file=cfg["log_file"] if isinstance(cfg["log_file"], str) else None)

    db = None if cfg["dry_run"] else _db_functions()
    lock_acquired = False
    if not cfg["dry_run"]:
        if not db["try_advisory_lock"](LOCK_ID):
            log.info("Файловая статистика: задача уже выполняется, выходим.")
            return 0
        lock_acquired = True

    started_at = now_iso_utc()
    snapped_at = started_at
    status = "ok"
    error: Optional[str] = None
    raw_inserted = 0
    norm_upserted = 0
    source_rows = 0

    try:
        source_path = cfg["source_path"]
        if not isinstance(source_path, Path):
            raise RuntimeError("Некорректный SOURCE_STATISTICS_FILE")
        if not source_path.exists():
            raise FileNotFoundError(f"Файл статистики не найден: {source_path}")

        all_tables = read_excel_tables(source_path)
        if cfg["include_wb_tables"]:
            tables = all_tables
        else:
            tables = [table for table in all_tables if table.name in ACTIVE_INTERNAL_TABLES]
        source_rows = sum(len(table.rows) for table in tables)
        log.info(
            "Файловая статистика: прочитано таблиц=%d, строк=%d, включены старые WB/Ozon/Yandex таблицы=%s",
            len(tables),
            source_rows,
            cfg["include_wb_tables"],
        )

        file_sha = _file_sha256(source_path)
        normalized = _normalize_tables(tables)
        _apply_direct_file_sources(normalized, cfg, log)
        if cfg["dry_run"]:
            log.info("Файловая статистика: сухой запуск, запись в БД пропущена, SHA-256 XLSM=%s", file_sha)
            norm_upserted = sum(len(rows) for rows in normalized.values())
        else:
            raw_inserted = db["insert_source_file_snapshots"](
                run_id=started_at,
                source_name="Статистика.xlsm",
                file_path=str(source_path),
                file_sha256=file_sha,
                tables=_raw_snapshots(tables),
            )

            norm_upserted += db["upsert_source_orders_daily"](normalized["orders"], run_id=started_at)
            norm_upserted += db["upsert_source_stock_summary"](normalized["stocks"], run_id=started_at, snapped_at=snapped_at)
            norm_upserted += db["upsert_ozon_storage_costs"](normalized["ozon_storage"], run_id=started_at, snapped_at=snapped_at)
            norm_upserted += db["insert_production_inventory_snapshot"](normalized["production_inventory"], snapped_at=snapped_at)
            norm_upserted += db["upsert_supply_pipeline_current"](normalized["supply_pipeline"], run_id=started_at, snapped_at=snapped_at)

        log.info(
            "Файловая статистика: raw snapshot=%d, строк к записи=%d, старые заказы=%d, старые остатки=%d, старое хранение Ozon=%d, остатки 1С=%d, список заказов=%d",
            raw_inserted,
            norm_upserted,
            len(normalized["orders"]),
            len(normalized["stocks"]),
            len(normalized["ozon_storage"]),
            len(normalized["production_inventory"]),
            len(normalized["supply_pipeline"]),
        )
        return 0

    except Exception as exc:
        status = "fail"
        error = repr(exc)
        log.exception("Файловая статистика: ОШИБКА - %s", error)
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            if not cfg.get("dry_run"):
                db["insert_job_run"](
                    job_name=JOB_NAME,
                    started_at_iso=started_at,
                    finished_at_iso=finished_at,
                    status=status,
                    api_rows=source_rows,
                    raw_new_versions=raw_inserted,
                    norm_upserted=norm_upserted,
                    duplicates=0,
                    dup_pct=0.0,
                    cursor_old=None,
                    cursor_used=None,
                    cursor_new=None,
                    error=error,
                )
        except Exception as job_error:
            log.warning("Файловая статистика: не удалось записать итог запуска в job_runs: %s", job_error)

        if cfg.get("dry_run"):
            pass
        else:
            ts = now_msk_label()
            if status == "ok":
                msg = (
                    f"✅ Source_Statistics | {ts} | OK\n\n"
                    f"➡ Файл строк: {source_rows}\n\n"
                    f"🔄 Обновлено строк: {norm_upserted}"
                )
            else:
                msg = f"❌ Source_Statistics | {ts} | FAIL\n{(error or 'unknown')[:200]}"
            tg_send(msg, logger=log)

        if lock_acquired:
            db["advisory_unlock"](LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
