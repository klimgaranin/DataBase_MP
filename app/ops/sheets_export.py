from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Literal, Sequence

from app.config import ROOT, get_config
from app.db import connect


ORDER_EXPORT_HEADERS = ["Дата", "Артикул", "Кол-во", "Сумма"]
OZON_PLACEMENT_EXPORT_HEADERS = ["Артикул", "Платно, шт", "Платно, л", "Списано в день, RUB", "Дней до первой платности"]
API_ERP_TRU_SALES_EXPORT_HEADERS = ["Артикул", "Кол-во"]
SOURCE_MARKETPLACE_COST_EXPORT_HEADERS = ["Артикул", "С/с BYN"]
SOURCE_PRODUCTION_INVENTORY_EXPORT_HEADERS = ["Артикул", "СМП", "ОСН", "СОХ", "СВХ", "ТС"]
SOURCE_SUPPLY_PIPELINE_EXPORT_HEADERS = ["Артикул", "СОГЛ Заказа", "В ПРОИЗВ", "ГОТОВ", "В ПУТИ", "МИНСК"]
SOURCE_SUPPLY_ORDER_SPECS_EXPORT_HEADERS = ["Артикул", "LOT", "Дата производства"]
DEFAULT_ORDERS_SHEET_NAME = "DATA"
DEFAULT_SOURCE_SPECS_SHEET_NAME = "DATA 2"
DEFAULT_MP_COST_SPREADSHEET_ID = "1vFXRJTGkfW1_NSWzThDYLGKpSOMUCTnZGEc6P8BZ4dg"
DEFAULT_OZON_START_CELL = "A1"
DEFAULT_WB_START_CELL = "F1"
DEFAULT_SOURCE_COST_OZON_START_CELL = "AX1"
DEFAULT_SOURCE_COST_WB_START_CELL = "BB1"
DEFAULT_SOURCE_COST_GENERAL_START_CELL = "BK1"
DEFAULT_OZON_PLACEMENT_START_CELL = "K1"
DEFAULT_SOURCE_PRODUCTION_INVENTORY_START_CELL = "Q1"
DEFAULT_SOURCE_SUPPLY_PIPELINE_START_CELL = "X1"
DEFAULT_SOURCE_SUPPLY_ORDER_SPECS_START_CELL = "H1"
DEFAULT_API_ERP_TRU_SALES_START_CELL = "AE1"
OZON_ORDER_EXPORT_TIME_ZONE = "UTC"
WB_ORDER_EXPORT_TIME_ZONE = "UTC"
PLACEMENT_REPORT_TZ = timezone(timedelta(hours=3), name="Europe/Minsk")


@dataclass(frozen=True)
class OrderSheetRow:
    order_date: date
    article: str
    quantity: int
    amount: Decimal


@dataclass(frozen=True)
class OzonPlacementSheetRow:
    article: str
    paid_qty: int
    paid_liters: Decimal
    daily_writeoff_rub: Decimal
    days_until_first_paid: int | None


@dataclass(frozen=True)
class ApiErpTruSalesSheetRow:
    article: str
    sales_count: int


@dataclass(frozen=True)
class SourceMarketplaceCostSheetRow:
    article: str
    unit_cost_byn: Decimal


@dataclass(frozen=True)
class SourceProductionInventorySheetRow:
    article: str
    smp_qty: Decimal
    osn_qty: Decimal
    soh_qty: Decimal
    svh_qty: Decimal
    ts_qty: Decimal


@dataclass(frozen=True)
class SourceSupplyPipelineSheetRow:
    article: str
    approved_order_qty: int
    in_production_qty: int
    ready_qty: int
    in_way_qty: int
    minsk_date: date | None


@dataclass(frozen=True)
class SourceSupplyOrderSpecSheetRow:
    article: str
    specification: str
    production_date: date | None


@dataclass(frozen=True)
class SheetSyncResult:
    mode: str
    prepared_rows: int
    existing_rows: int
    unchanged_rows: int
    changed_rows: int
    appended_rows: int
    stale_rows: int
    header_updated: bool
    cleared: bool
    updated_range: str | None
    updated_cells: int
    added_sheet_rows: int = 0


@dataclass(frozen=True)
class OrderExportResult:
    marketplace: Literal["ozon", "wb"]
    sheet_name: str
    start_cell: str
    date_from: date
    date_to: date
    rows_count: int
    sync: SheetSyncResult | None
    dry_run: bool


@dataclass(frozen=True)
class PlacementExportResult:
    sheet_name: str
    start_cell: str
    rows_count: int
    sync: SheetSyncResult | None
    dry_run: bool
    report_date: date | None = None
    expected_report_date: date | None = None
    product_report_code: str | None = None
    product_report_rows: int | None = None


@dataclass(frozen=True)
class ApiErpTruSalesExportResult:
    sheet_name: str
    start_cell: str
    rows_count: int
    sync: SheetSyncResult | None
    dry_run: bool


@dataclass(frozen=True)
class SourceBlockExportResult:
    block: Literal["production-inventory", "supply-pipeline", "supply-order-specs", "source-cost-ozon", "source-cost-wb", "source-cost-general"]
    sheet_name: str
    start_cell: str
    rows_count: int
    sync: SheetSyncResult | None
    dry_run: bool


OzonOrderSheetRow = OrderSheetRow


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return ROOT / path


def _add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + (value.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def default_orders_date_from(today: date | None = None) -> date:
    current = today or date.today()
    current_month_start = date(current.year, current.month, 1)
    return _add_months(current_month_start, -2)


def default_orders_date_to(today: date | None = None) -> date:
    return today or date.today()


def format_sheet_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def _amount_to_sheet_value(value: Decimal) -> int | float:
    amount = value.quantize(Decimal("0.01"))
    if amount == amount.to_integral_value():
        return int(amount)
    return float(amount)


def _volume_to_sheet_value(value: Decimal) -> int | float:
    amount = value.quantize(Decimal("0.001"))
    if amount == amount.to_integral_value():
        return int(amount)
    return float(amount)


def build_order_sheet_values(
    rows: Sequence[OrderSheetRow],
    *,
    marketplace: Literal["ozon", "wb"],
) -> list[list[Any]]:
    values: list[list[Any]] = [ORDER_EXPORT_HEADERS]
    for row in rows:
        values.append(
            [
                format_sheet_date(row.order_date),
                row.article,
                row.quantity,
                _amount_to_sheet_value(row.amount),
            ]
        )
    return values


def build_ozon_order_sheet_values(rows: Sequence[OrderSheetRow]) -> list[list[Any]]:
    return build_order_sheet_values(rows, marketplace="ozon")


def build_ozon_placement_sheet_values(rows: Sequence[OzonPlacementSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [OZON_PLACEMENT_EXPORT_HEADERS]
    for row in rows:
        values.append(
            [
                row.article,
                row.paid_qty,
                _volume_to_sheet_value(row.paid_liters),
                _amount_to_sheet_value(row.daily_writeoff_rub),
                "" if row.days_until_first_paid is None else row.days_until_first_paid,
            ]
        )
    return values


def build_api_erp_tru_sales_sheet_values(rows: Sequence[ApiErpTruSalesSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [API_ERP_TRU_SALES_EXPORT_HEADERS]
    for row in rows:
        values.append([row.article, row.sales_count])
    return values


def build_source_marketplace_cost_sheet_values(rows: Sequence[SourceMarketplaceCostSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [SOURCE_MARKETPLACE_COST_EXPORT_HEADERS]
    for row in rows:
        values.append([row.article, _amount_to_sheet_value(row.unit_cost_byn)])
    return values


def build_source_production_inventory_sheet_values(rows: Sequence[SourceProductionInventorySheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [SOURCE_PRODUCTION_INVENTORY_EXPORT_HEADERS]
    for row in rows:
        values.append(
            [
                row.article,
                _quantity_to_sheet_value(row.smp_qty),
                _quantity_to_sheet_value(row.osn_qty),
                _quantity_to_sheet_value(row.soh_qty),
                _quantity_to_sheet_value(row.svh_qty),
                _quantity_to_sheet_value(row.ts_qty),
            ]
        )
    return values


def _quantity_to_sheet_value(value: Decimal) -> int | float | str:
    amount = Decimal(value or 0).quantize(Decimal("0.001"))
    if amount == 0:
        return ""
    if amount == amount.to_integral_value():
        return int(amount)
    return float(amount.normalize())


def build_source_supply_pipeline_sheet_values(rows: Sequence[SourceSupplyPipelineSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [SOURCE_SUPPLY_PIPELINE_EXPORT_HEADERS]
    for row in rows:
        values.append(
            [
                row.article,
                row.approved_order_qty,
                row.in_production_qty,
                row.ready_qty,
                row.in_way_qty,
                "" if row.minsk_date is None else format_sheet_date(row.minsk_date),
            ]
        )
    return values


def build_source_supply_order_specs_sheet_values(rows: Sequence[SourceSupplyOrderSpecSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [SOURCE_SUPPLY_ORDER_SPECS_EXPORT_HEADERS]
    for row in rows:
        values.append(
            [
                row.article,
                row.specification,
                "" if row.production_date is None else format_sheet_date(row.production_date),
            ]
        )
    return values


def _parse_start_cell(start_cell: str) -> tuple[str, int]:
    letters = "".join(char for char in start_cell if char.isalpha()).upper()
    digits = "".join(char for char in start_cell if char.isdigit())
    if not letters or not digits:
        raise RuntimeError(f"Некорректная стартовая ячейка: {start_cell}")
    return letters, int(digits)


def _column_to_number(column: str) -> int:
    result = 0
    for char in column.upper():
        result = result * 26 + ord(char) - ord("A") + 1
    return result


def _number_to_column(number: int) -> str:
    chars = []
    while number:
        number, rem = divmod(number - 1, 26)
        chars.append(chr(ord("A") + rem))
    return "".join(reversed(chars))


def _target_columns(start_cell: str, width: int) -> tuple[str, str, int]:
    start_column, start_row = _parse_start_cell(start_cell)
    end_column = _number_to_column(_column_to_number(start_column) + width - 1)
    return start_column, end_column, start_row


def _source_block_number_formats(
    block: Literal["production-inventory", "supply-pipeline", "supply-order-specs", "source-cost-ozon", "source-cost-wb", "source-cost-general"],
) -> list[dict[str, str]]:
    if block in {"source-cost-ozon", "source-cost-wb", "source-cost-general"}:
        return [
            {"type": "TEXT"},
            {"type": "NUMBER"},
        ]
    if block == "production-inventory":
        return [
            {"type": "TEXT"},
            {"type": "NUMBER"},
            {"type": "NUMBER"},
            {"type": "NUMBER"},
            {"type": "NUMBER"},
            {"type": "NUMBER"},
        ]
    if block == "supply-order-specs":
        return [
            {"type": "TEXT"},
            {"type": "TEXT"},
            {"type": "DATE", "pattern": "dd.mm.yyyy"},
        ]
    return [
        {"type": "TEXT"},
        {"type": "NUMBER", "pattern": "0"},
        {"type": "NUMBER", "pattern": "0"},
        {"type": "NUMBER", "pattern": "0"},
        {"type": "NUMBER", "pattern": "0"},
        {"type": "DATE", "pattern": "dd.mm.yyyy"},
    ]


def _apply_source_block_formats(
    *,
    client: Any,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    block: Literal["production-inventory", "supply-pipeline", "supply-order-specs", "source-cost-ozon", "source-cost-wb", "source-cost-general"],
) -> None:
    format_columns = getattr(client, "set_column_number_formats", None)
    if not callable(format_columns):
        return
    start_column, _, start_row = _target_columns(start_cell, len(_source_block_number_formats(block)))
    format_columns(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        start_column_index=_column_to_number(start_column) - 1,
        start_row_index=start_row - 1,
        number_formats=_source_block_number_formats(block),
    )


def _ensure_sheet_has_rows(
    *,
    client: Any,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    values: list[list[Any]],
    headers: Sequence[str],
) -> int:
    _, _, header_row = _target_columns(start_cell, len(headers))
    required_rows = header_row + max(0, len(values) - 1)
    ensure_rows = getattr(client, "ensure_sheet_rows", None)
    if not callable(ensure_rows):
        return 0
    return int(ensure_rows(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, min_rows=required_rows) or 0)


def _row_key(row: Sequence[Any], *, key_columns: int) -> tuple[str, ...]:
    padded = list(row) + [""] * key_columns
    return tuple(_normalize_cell(value) for value in padded[:key_columns])


def _normalize_cell(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    numeric_text = text.replace(",", ".")
    if re.fullmatch(r"-?\d+(?:\.\d+)?", numeric_text):
        amount = Decimal(numeric_text).normalize()
        if amount == amount.to_integral_value():
            return str(int(amount))
        return format(amount, "f").rstrip("0").rstrip(".")
    return str(value).strip()


def _normalize_row(row: Sequence[Any], *, width: int) -> list[str]:
    padded = list(row) + [""] * width
    return [_normalize_cell(value) for value in padded[:width]]


def sync_sheet_table(
    *,
    client: Any,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    values: list[list[Any]],
    mode: Literal["upsert", "replace"],
    headers: Sequence[str] | None = None,
    key_columns: int = 2,
    replace_on_order_change: bool = False,
) -> SheetSyncResult:
    table_headers = list(headers or ORDER_EXPORT_HEADERS)
    width = len(table_headers)
    start_column, end_column, header_row = _target_columns(start_cell, width)
    full_column_range = f"{start_column}:{end_column}"
    added_sheet_rows = _ensure_sheet_has_rows(
        client=client,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=start_cell,
        values=values,
        headers=table_headers,
    )
    if mode == "replace":
        client.clear_values(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, a1_range=full_column_range)
        result = client.update_values(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_cell=start_cell,
            values=values,
        )
        return SheetSyncResult(
            mode=mode,
            prepared_rows=max(0, len(values) - 1),
            existing_rows=0,
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=0,
            header_updated=True,
            cleared=True,
            updated_range=result.get("updatedRange"),
            updated_cells=int(result.get("updatedCells") or 0),
            added_sheet_rows=added_sheet_rows,
        )

    existing = client.get_values(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=full_column_range,
    )
    updates: list[tuple[str, list[list[Any]]]] = []
    header_updated = False
    if not existing or _normalize_row(existing[header_row - 1] if len(existing) >= header_row else [], width=width) != table_headers:
        updates.append((f"{start_column}{header_row}:{end_column}{header_row}", [table_headers]))
        header_updated = True

    existing_by_key: dict[tuple[str, ...], tuple[int, list[Any]]] = {}
    for index, row in enumerate(existing[header_row:], start=header_row + 1):
        key = _row_key(row, key_columns=key_columns)
        if all(key) and key not in existing_by_key:
            existing_by_key[key] = (index, row)

    target_keys = {_row_key(value_row, key_columns=key_columns) for value_row in values[1:] if all(_row_key(value_row, key_columns=key_columns))}
    stale_keys = set(existing_by_key) - target_keys
    if stale_keys:
        client.clear_values(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, a1_range=full_column_range)
        result = client.update_values(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_cell=start_cell,
            values=values,
        )
        return SheetSyncResult(
            mode="replace-stale",
            prepared_rows=max(0, len(values) - 1),
            existing_rows=max(0, len(existing) - header_row),
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=len(stale_keys),
            header_updated=True,
            cleared=True,
            updated_range=result.get("updatedRange"),
            updated_cells=int(result.get("updatedCells") or 0),
            added_sheet_rows=added_sheet_rows,
        )

    target_key_order = [_row_key(value_row, key_columns=key_columns) for value_row in values[1:] if all(_row_key(value_row, key_columns=key_columns))]
    existing_key_order = [_row_key(row, key_columns=key_columns) for row in existing[header_row:] if all(_row_key(row, key_columns=key_columns))]
    if replace_on_order_change and existing_key_order != target_key_order:
        client.clear_values(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name, a1_range=full_column_range)
        result = client.update_values(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_cell=start_cell,
            values=values,
        )
        return SheetSyncResult(
            mode="replace-order",
            prepared_rows=max(0, len(values) - 1),
            existing_rows=max(0, len(existing) - header_row),
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=0,
            header_updated=True,
            cleared=True,
            updated_range=result.get("updatedRange"),
            updated_cells=int(result.get("updatedCells") or 0),
            added_sheet_rows=added_sheet_rows,
        )

    changed_rows = 0
    unchanged_rows = 0
    appended_rows: list[list[Any]] = []
    for value_row in values[1:]:
        key = _row_key(value_row, key_columns=key_columns)
        found = existing_by_key.get(key)
        if found is None:
            appended_rows.append(value_row)
            continue
        row_number, existing_row = found
        if _normalize_row(existing_row, width=width) == _normalize_row(value_row, width=width):
            unchanged_rows += 1
            continue
        updates.append((f"{start_column}{row_number}:{end_column}{row_number}", [value_row]))
        changed_rows += 1

    if appended_rows:
        append_start_row = max(len(existing), header_row) + 1
        append_end_row = append_start_row + len(appended_rows) - 1
        updates.append((f"{start_column}{append_start_row}:{end_column}{append_end_row}", appended_rows))

    updated_cells = 0
    if updates:
        result = client.batch_update_values(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            updates=updates,
        )
        updated_cells = int(result.get("totalUpdatedCells") or 0)

    return SheetSyncResult(
        mode=mode,
        prepared_rows=max(0, len(values) - 1),
        existing_rows=max(0, len(existing) - header_row),
        unchanged_rows=unchanged_rows,
        changed_rows=changed_rows,
        appended_rows=len(appended_rows),
        stale_rows=0,
        header_updated=header_updated,
        cleared=False,
        updated_range=None,
        updated_cells=updated_cells,
        added_sheet_rows=added_sheet_rows,
    )


def plan_sheet_table_sync(
    *,
    existing: list[list[Any]],
    start_cell: str,
    values: list[list[Any]],
    mode: Literal["upsert", "replace"],
    headers: Sequence[str] | None = None,
    key_columns: int = 2,
    replace_on_order_change: bool = False,
) -> SheetSyncResult:
    table_headers = list(headers or ORDER_EXPORT_HEADERS)
    width = len(table_headers)
    start_column, _, header_row = _target_columns(start_cell, width)
    if mode == "replace":
        return SheetSyncResult(
            mode=mode,
            prepared_rows=max(0, len(values) - 1),
            existing_rows=max(0, len(existing) - header_row),
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=0,
            header_updated=True,
            cleared=True,
            updated_range=None,
            updated_cells=len(values) * width,
        )

    header_updated = not existing or _normalize_row(existing[header_row - 1] if len(existing) >= header_row else [], width=width) != table_headers
    existing_by_key: dict[tuple[str, ...], list[Any]] = {}
    for row in existing[header_row:]:
        key = _row_key(row, key_columns=key_columns)
        if all(key) and key not in existing_by_key:
            existing_by_key[key] = row

    target_keys = {_row_key(value_row, key_columns=key_columns) for value_row in values[1:] if all(_row_key(value_row, key_columns=key_columns))}
    stale_keys = set(existing_by_key) - target_keys
    if stale_keys:
        return SheetSyncResult(
            mode="replace-stale",
            prepared_rows=max(0, len(values) - 1),
            existing_rows=max(0, len(existing) - header_row),
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=len(stale_keys),
            header_updated=True,
            cleared=True,
            updated_range=None,
            updated_cells=len(values) * width,
        )

    target_key_order = [_row_key(value_row, key_columns=key_columns) for value_row in values[1:] if all(_row_key(value_row, key_columns=key_columns))]
    existing_key_order = [_row_key(row, key_columns=key_columns) for row in existing[header_row:] if all(_row_key(row, key_columns=key_columns))]
    if replace_on_order_change and existing_key_order != target_key_order:
        return SheetSyncResult(
            mode="replace-order",
            prepared_rows=max(0, len(values) - 1),
            existing_rows=max(0, len(existing) - header_row),
            unchanged_rows=0,
            changed_rows=max(0, len(values) - 1),
            appended_rows=max(0, len(values) - 1),
            stale_rows=0,
            header_updated=True,
            cleared=True,
            updated_range=None,
            updated_cells=len(values) * width,
        )

    changed_rows = 0
    unchanged_rows = 0
    appended_rows = 0
    for value_row in values[1:]:
        existing_row = existing_by_key.get(_row_key(value_row, key_columns=key_columns))
        if existing_row is None:
            appended_rows += 1
        elif _normalize_row(existing_row, width=width) == _normalize_row(value_row, width=width):
            unchanged_rows += 1
        else:
            changed_rows += 1

    header_cells = width if header_updated else 0
    updated_cells = header_cells + (changed_rows + appended_rows) * width
    return SheetSyncResult(
        mode=mode,
        prepared_rows=max(0, len(values) - 1),
        existing_rows=max(0, len(existing) - header_row),
        unchanged_rows=unchanged_rows,
        changed_rows=changed_rows,
        appended_rows=appended_rows,
        stale_rows=0,
        header_updated=header_updated,
        cleared=False,
        updated_range=f"{start_column}:{start_column}",
        updated_cells=updated_cells,
    )


def fetch_ozon_order_sheet_rows(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
) -> list[OrderSheetRow]:
    start = date_from or default_orders_date_from()
    end = date_to or default_orders_date_to()
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = [start, end]
    if limit is not None:
        params.append(limit)

    sql = f"""
        SELECT
            (COALESCE(created_at, in_process_at) AT TIME ZONE '{OZON_ORDER_EXPORT_TIME_ZONE}')::date AS order_date,
            COALESCE(NULLIF(product_offer_id, ''), product_sku::text, '') AS article,
            SUM(product_quantity)::int AS quantity,
            SUM(product_price_amount * product_quantity)::numeric(14,2) AS amount
        FROM staging.ozon_fbo_order_items_full
        WHERE (COALESCE(created_at, in_process_at) AT TIME ZONE '{OZON_ORDER_EXPORT_TIME_ZONE}')::date BETWEEN %s AND %s
          AND COALESCE(status, '') <> 'cancelled'
        GROUP BY 1, 2
        ORDER BY 1 DESC, MAX(COALESCE(created_at, in_process_at)) DESC, 2 ASC
        {limit_sql}
    """
    return _fetch_order_rows(sql, params)


def fetch_wb_order_sheet_rows(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
) -> list[OrderSheetRow]:
    start = date_from or default_orders_date_from()
    end = date_to or default_orders_date_to()
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = [start, end]
    if limit is not None:
        params.append(limit)

    sql = f"""
        SELECT
            (date_ts AT TIME ZONE '{WB_ORDER_EXPORT_TIME_ZONE}')::date AS order_date,
            COALESCE(NULLIF(supplier_article, ''), nm_id::text, '') AS article,
            COUNT(*)::int AS quantity,
            SUM(COALESCE(price_with_disc, finished_price, total_price, 0))::numeric(14,2) AS amount
        FROM wb_orders_norm
        WHERE (date_ts AT TIME ZONE '{WB_ORDER_EXPORT_TIME_ZONE}')::date BETWEEN %s AND %s
          AND COALESCE(is_cancel, FALSE) = FALSE
        GROUP BY 1, 2
        ORDER BY 1 DESC, MAX(date_ts) DESC, 2 ASC
        {limit_sql}
    """
    return _fetch_order_rows(sql, params)


def _fetch_order_rows(sql: str, params: Sequence[Any]) -> list[OrderSheetRow]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = []
            for order_date, article, quantity, amount in cur.fetchall():
                result.append(
                    OrderSheetRow(
                        order_date=order_date,
                        article=str(article or ""),
                        quantity=int(quantity or 0),
                        amount=Decimal(amount or 0),
                    )
                )
            return result


def fetch_ozon_placement_sheet_rows(*, limit: int | None = None) -> list[OzonPlacementSheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = []
    if limit is not None:
        params.append(limit)
    sql = f"""
        SELECT
            article,
            paid_qty,
            paid_liters,
            daily_writeoff_rub,
            days_until_first_paid
        FROM analytics.ozon_placement_latest_for_sheets
        ORDER BY article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                OzonPlacementSheetRow(
                    article=str(article or ""),
                    paid_qty=int(paid_qty or 0),
                    paid_liters=Decimal(paid_liters or 0),
                    daily_writeoff_rub=Decimal(daily_writeoff_rub or 0),
                    days_until_first_paid=None if days_until_first_paid is None else int(days_until_first_paid),
                )
                for article, paid_qty, paid_liters, daily_writeoff_rub, days_until_first_paid in cur.fetchall()
            ]


def fetch_api_erp_tru_sales_sheet_rows(*, limit: int | None = None) -> list[ApiErpTruSalesSheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = []
    if limit is not None:
        params.append(limit)
    sql = f"""
        SELECT article, sales_count
        FROM analytics.api_erp_tru_sales_for_sheets
        ORDER BY article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                ApiErpTruSalesSheetRow(article=str(article or ""), sales_count=int(sales_count or 0))
                for article, sales_count in cur.fetchall()
            ]


def fetch_source_marketplace_cost_sheet_rows(
    *,
    marketplace: Literal["ozon", "wb", "general"],
    limit: int | None = None,
) -> list[SourceMarketplaceCostSheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = [marketplace]
    if limit is not None:
        params.append(limit)
    sql = f"""
        SELECT article, unit_cost_byn
        FROM analytics.source_cost_marketplace_for_sheets
        WHERE marketplace = %s
          AND unit_cost_byn IS NOT NULL
        ORDER BY article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                SourceMarketplaceCostSheetRow(
                    article=str(article or ""),
                    unit_cost_byn=Decimal(unit_cost_byn or 0),
                )
                for article, unit_cost_byn in cur.fetchall()
            ]


def fetch_source_production_inventory_sheet_rows(*, limit: int | None = None) -> list[SourceProductionInventorySheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = []
    if limit is not None:
        params.append(limit)
    sql = f"""
        WITH latest AS (
            SELECT MAX(snapped_at) AS snapped_at
            FROM core.production_inventory_snapshot
        )
        SELECT p.article, p.smp_qty, p.osn_qty, p.soh_qty, p.svh_qty, p.ts_qty
        FROM core.production_inventory_snapshot p
        JOIN latest l ON l.snapped_at = p.snapped_at
        ORDER BY p.article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                SourceProductionInventorySheetRow(
                    article=str(article or ""),
                    smp_qty=Decimal(smp_qty or 0),
                    osn_qty=Decimal(osn_qty or 0),
                    soh_qty=Decimal(soh_qty or 0),
                    svh_qty=Decimal(svh_qty or 0),
                    ts_qty=Decimal(ts_qty or 0),
                )
                for article, smp_qty, osn_qty, soh_qty, svh_qty, ts_qty in cur.fetchall()
            ]


def fetch_source_supply_pipeline_sheet_rows(*, limit: int | None = None) -> list[SourceSupplyPipelineSheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = []
    if limit is not None:
        params.append(limit)
    sql = f"""
        SELECT article, approved_order_qty, in_production_qty, ready_qty, in_way_qty, minsk_date
        FROM staging.supply_pipeline_current
        ORDER BY article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                SourceSupplyPipelineSheetRow(
                    article=str(article or ""),
                    approved_order_qty=int(approved_order_qty or 0),
                    in_production_qty=int(in_production_qty or 0),
                    ready_qty=int(ready_qty or 0),
                    in_way_qty=int(in_way_qty or 0),
                    minsk_date=minsk_date,
                )
                for article, approved_order_qty, in_production_qty, ready_qty, in_way_qty, minsk_date in cur.fetchall()
            ]


def fetch_source_supply_order_specs_sheet_rows(*, limit: int | None = None) -> list[SourceSupplyOrderSpecSheetRow]:
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = []
    if limit is not None:
        params.append(limit)
    sql = f"""
        SELECT article, specification, production_date
        FROM staging.supply_order_specs_current
        ORDER BY source_sheet, source_row_number, article
        {limit_sql}
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                SourceSupplyOrderSpecSheetRow(
                    article=str(article or ""),
                    specification=str(specification or ""),
                    production_date=production_date,
                )
                for article, specification, production_date in cur.fetchall()
            ]


def default_placement_expected_date(now: datetime | None = None) -> date:
    current = now or datetime.now(PLACEMENT_REPORT_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=PLACEMENT_REPORT_TZ)
    return current.astimezone(PLACEMENT_REPORT_TZ).date()


def fetch_ozon_placement_report_selection() -> dict[str, Any] | None:
    sql = """
        SELECT p.code, p.date_to, COUNT(r.*)::int AS rows_count
        FROM raw.ozon_placement_reports p
        JOIN raw.ozon_placement_report_rows r ON r.report_code = p.code
        WHERE p.status = 'success'
        GROUP BY p.code, p.date_to, p.updated_at
        HAVING COUNT(r.*) > 0
        ORDER BY p.date_to DESC, p.updated_at DESC
        LIMIT 1
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                return None
            code, report_date, rows_count = row
            return {
                "code": str(code),
                "report_date": report_date,
                "rows_count": int(rows_count or 0),
            }


def export_orders_to_sheets(
    *,
    marketplace: Literal["ozon", "wb"],
    spreadsheet_id: str | None = None,
    sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
    start_cell: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
    mode: Literal["upsert", "replace"] = "upsert",
    dry_run: bool = False,
) -> int:
    result = run_orders_to_sheets(
        marketplace=marketplace,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=start_cell,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        mode=mode,
        dry_run=dry_run,
        verbose=True,
    )
    return 0 if result is not None else 1


def run_orders_to_sheets(
    *,
    marketplace: Literal["ozon", "wb"],
    spreadsheet_id: str | None = None,
    sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
    start_cell: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
    mode: Literal["upsert", "replace"] = "upsert",
    dry_run: bool = False,
    verbose: bool = False,
) -> OrderExportResult:
    config = get_config()
    target_spreadsheet_id = spreadsheet_id or config.analytics_mp_spreadsheet_id
    target_start_cell = start_cell or (DEFAULT_OZON_START_CELL if marketplace == "ozon" else DEFAULT_WB_START_CELL)
    start = date_from or default_orders_date_from()
    end = date_to or default_orders_date_to()

    if marketplace == "ozon":
        rows = fetch_ozon_order_sheet_rows(date_from=start, date_to=end, limit=limit)
    else:
        rows = fetch_wb_order_sheet_rows(date_from=start, date_to=end, limit=limit)
    values = build_order_sheet_values(rows, marketplace=marketplace)

    marketplace_label = "Ozon" if marketplace == "ozon" else "WB"
    if verbose:
        print(f"{marketplace_label} заказы: период {start.isoformat()} - {end.isoformat()}")
        print(f"{marketplace_label} заказы: подготовлено агрегированных строк: {len(rows)}")
        print(f"Лист: {sheet_name}, стартовая ячейка: {target_start_cell}, режим: {mode}")
        if rows[:3]:
            print("Первые строки:")
            for value_row in values[1:4]:
                print(" | ".join(str(value) for value in value_row))

    from app.clients.google_sheets import GoogleSheetsClient

    client = GoogleSheetsClient(credentials_path=_resolve_project_path(config.google_application_credentials))
    if dry_run:
        start_column, end_column, _ = _target_columns(target_start_cell, len(ORDER_EXPORT_HEADERS))
        existing = client.get_values(
            spreadsheet_id=target_spreadsheet_id,
            sheet_name=sheet_name,
            a1_range=f"{start_column}:{end_column}",
        )
        plan = plan_sheet_table_sync(
            existing=existing,
            start_cell=target_start_cell,
            values=values,
            mode=mode,
            replace_on_order_change=True,
        )
        if verbose:
            print(
                "Dry-run план Google Sheets: "
                f"существующих строк {plan.existing_rows}, "
                f"без изменений {plan.unchanged_rows}, "
                f"будет обновлено {plan.changed_rows}, "
                f"будет добавлено {plan.appended_rows}, "
                f"устаревших строк {plan.stale_rows}, "
                f"очистка блока: {'да' if plan.cleared else 'нет'}, "
                f"ожидаемо ячеек к изменению {plan.updated_cells}"
            )
            print("Dry-run: запись в Google Sheets не выполнялась")
        return OrderExportResult(
            marketplace=marketplace,
            sheet_name=sheet_name,
            start_cell=target_start_cell,
            date_from=start,
            date_to=end,
            rows_count=len(rows),
            sync=plan,
            dry_run=True,
        )

    result = sync_sheet_table(
        client=client,
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=target_start_cell,
        values=values,
        mode=mode,
        replace_on_order_change=True,
    )
    if verbose:
        if result.cleared:
            print(f"Google Sheets: диапазон перезаписан: {result.updated_range}")
        print(
            "Google Sheets: "
            f"существующих строк {result.existing_rows}, "
            f"без изменений {result.unchanged_rows}, "
            f"обновлено {result.changed_rows}, "
            f"добавлено {result.appended_rows}, "
            f"устаревших убрано {result.stale_rows}, "
            f"строк листа добавлено {result.added_sheet_rows}, "
            f"ячеек изменено {result.updated_cells}"
        )
    return OrderExportResult(
        marketplace=marketplace,
        sheet_name=sheet_name,
        start_cell=target_start_cell,
        date_from=start,
        date_to=end,
        rows_count=len(rows),
        sync=result,
        dry_run=False,
    )


def export_ozon_orders_to_sheets(**kwargs: Any) -> int:
    return export_orders_to_sheets(marketplace="ozon", **kwargs)


def export_wb_orders_to_sheets(**kwargs: Any) -> int:
    return export_orders_to_sheets(marketplace="wb", **kwargs)


def export_ozon_placement_to_sheets(**kwargs: Any) -> int:
    result = run_ozon_placement_to_sheets(verbose=True, **kwargs)
    return 0 if result is not None else 1


def export_api_erp_tru_sales_to_sheets(**kwargs: Any) -> int:
    result = run_api_erp_tru_sales_to_sheets(verbose=True, **kwargs)
    return 0 if result is not None else 1


def export_source_block_to_sheets(
    *,
    block: Literal["production-inventory", "supply-pipeline", "supply-order-specs", "source-cost-ozon", "source-cost-wb", "source-cost-general"],
    **kwargs: Any,
) -> int:
    result = run_source_block_to_sheets(block=block, verbose=True, **kwargs)
    return 0 if result is not None else 1


def run_source_block_to_sheets(
    *,
    block: Literal["production-inventory", "supply-pipeline", "supply-order-specs", "source-cost-ozon", "source-cost-wb", "source-cost-general"],
    spreadsheet_id: str | None = None,
    sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
    start_cell: str | None = None,
    limit: int | None = None,
    mode: Literal["upsert", "replace"] = "replace",
    dry_run: bool = False,
    verbose: bool = False,
) -> SourceBlockExportResult:
    config = get_config()
    target_spreadsheet_id = spreadsheet_id or config.analytics_mp_spreadsheet_id

    if block == "production-inventory":
        target_start_cell = start_cell or DEFAULT_SOURCE_PRODUCTION_INVENTORY_START_CELL
        headers = SOURCE_PRODUCTION_INVENTORY_EXPORT_HEADERS
        rows = fetch_source_production_inventory_sheet_rows(limit=limit)
        values = build_source_production_inventory_sheet_values(rows)
        label = "Остатки МП"
    elif block == "supply-pipeline":
        target_start_cell = start_cell or DEFAULT_SOURCE_SUPPLY_PIPELINE_START_CELL
        headers = SOURCE_SUPPLY_PIPELINE_EXPORT_HEADERS
        rows = fetch_source_supply_pipeline_sheet_rows(limit=limit)
        values = build_source_supply_pipeline_sheet_values(rows)
        label = "Список заказов"
    elif block == "supply-order-specs":
        sheet_name = DEFAULT_SOURCE_SPECS_SHEET_NAME if sheet_name == DEFAULT_ORDERS_SHEET_NAME else sheet_name
        target_start_cell = start_cell or DEFAULT_SOURCE_SUPPLY_ORDER_SPECS_START_CELL
        headers = SOURCE_SUPPLY_ORDER_SPECS_EXPORT_HEADERS
        rows = fetch_source_supply_order_specs_sheet_rows(limit=limit)
        values = build_source_supply_order_specs_sheet_values(rows)
        label = "Спецификации заказов"
    elif block == "source-cost-ozon":
        target_spreadsheet_id = spreadsheet_id or DEFAULT_MP_COST_SPREADSHEET_ID
        target_start_cell = start_cell or DEFAULT_SOURCE_COST_OZON_START_CELL
        headers = SOURCE_MARKETPLACE_COST_EXPORT_HEADERS
        rows = fetch_source_marketplace_cost_sheet_rows(marketplace="ozon", limit=limit)
        values = build_source_marketplace_cost_sheet_values(rows)
        label = "Себестоимость Ozon"
    elif block == "source-cost-wb":
        target_spreadsheet_id = spreadsheet_id or DEFAULT_MP_COST_SPREADSHEET_ID
        target_start_cell = start_cell or DEFAULT_SOURCE_COST_WB_START_CELL
        headers = SOURCE_MARKETPLACE_COST_EXPORT_HEADERS
        rows = fetch_source_marketplace_cost_sheet_rows(marketplace="wb", limit=limit)
        values = build_source_marketplace_cost_sheet_values(rows)
        label = "Себестоимость WB"
    else:
        target_spreadsheet_id = spreadsheet_id or DEFAULT_MP_COST_SPREADSHEET_ID
        target_start_cell = start_cell or DEFAULT_SOURCE_COST_GENERAL_START_CELL
        headers = SOURCE_MARKETPLACE_COST_EXPORT_HEADERS
        rows = fetch_source_marketplace_cost_sheet_rows(marketplace="general", limit=limit)
        values = build_source_marketplace_cost_sheet_values(rows)
        label = "Себестоимость общая"

    if verbose:
        print(f"{label}: подготовлено строк: {len(rows)}")
        print(f"Лист: {sheet_name}, стартовая ячейка: {target_start_cell}, режим: {mode}")
        if rows[:3]:
            print("Первые строки:")
            for value_row in values[1:4]:
                print(" | ".join(str(value) for value in value_row))

    from app.clients.google_sheets import GoogleSheetsClient

    client = GoogleSheetsClient(credentials_path=_resolve_project_path(config.google_application_credentials))
    if dry_run:
        start_column, end_column, _ = _target_columns(target_start_cell, len(headers))
        existing = client.get_values(
            spreadsheet_id=target_spreadsheet_id,
            sheet_name=sheet_name,
            a1_range=f"{start_column}:{end_column}",
        )
        plan = plan_sheet_table_sync(
            existing=existing,
            start_cell=target_start_cell,
            values=values,
            mode=mode,
            headers=headers,
            key_columns=1,
            replace_on_order_change=True,
        )
        if verbose:
            print(
                "Dry-run план Google Sheets: "
                f"существующих строк {plan.existing_rows}, "
                f"без изменений {plan.unchanged_rows}, "
                f"будет обновлено {plan.changed_rows}, "
                f"будет добавлено {plan.appended_rows}, "
                f"устаревших строк {plan.stale_rows}, "
                f"очистка блока: {'да' if plan.cleared else 'нет'}, "
                f"ожидаемо ячеек к изменению {plan.updated_cells}"
            )
            print("Dry-run: запись в Google Sheets не выполнялась")
        return SourceBlockExportResult(
            block=block,
            sheet_name=sheet_name,
            start_cell=target_start_cell,
            rows_count=len(rows),
            sync=plan,
            dry_run=True,
        )

    _apply_source_block_formats(
        client=client,
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=target_start_cell,
        block=block,
    )
    sync = sync_sheet_table(
        client=client,
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=target_start_cell,
        values=values,
        mode=mode,
        headers=headers,
        key_columns=1,
        replace_on_order_change=True,
    )
    if verbose:
        if sync.cleared:
            print(f"Google Sheets: диапазон перезаписан: {sync.updated_range}")
        print(
            "Google Sheets: "
            f"существующих строк {sync.existing_rows}, "
            f"без изменений {sync.unchanged_rows}, "
            f"обновлено {sync.changed_rows}, "
            f"добавлено {sync.appended_rows}, "
            f"устаревших убрано {sync.stale_rows}, "
            f"строк листа добавлено={sync.added_sheet_rows}, "
            f"ячеек изменено {sync.updated_cells}"
        )
    return SourceBlockExportResult(
        block=block,
        sheet_name=sheet_name,
        start_cell=target_start_cell,
        rows_count=len(rows),
        sync=sync,
        dry_run=False,
    )


def run_api_erp_tru_sales_to_sheets(
    *,
    spreadsheet_id: str | None = None,
    sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
    start_cell: str = DEFAULT_API_ERP_TRU_SALES_START_CELL,
    limit: int | None = None,
    mode: Literal["upsert", "replace"] = "replace",
    dry_run: bool = False,
    verbose: bool = False,
) -> ApiErpTruSalesExportResult:
    config = get_config()
    target_spreadsheet_id = spreadsheet_id or config.analytics_mp_spreadsheet_id
    rows = fetch_api_erp_tru_sales_sheet_rows(limit=limit)
    values = build_api_erp_tru_sales_sheet_values(rows)

    if verbose:
        print(f"ERP/TRU продажи: подготовлено строк: {len(rows)}")
        print(f"Лист: {sheet_name}, стартовая ячейка: {start_cell}, режим: {mode}")
        if rows[:3]:
            print("Первые строки:")
            for value_row in values[1:4]:
                print(" | ".join(str(value) for value in value_row))

    from app.clients.google_sheets import GoogleSheetsClient

    client = GoogleSheetsClient(credentials_path=_resolve_project_path(config.google_application_credentials))
    if dry_run:
        start_column, end_column, _ = _target_columns(start_cell, len(API_ERP_TRU_SALES_EXPORT_HEADERS))
        existing = client.get_values(
            spreadsheet_id=target_spreadsheet_id,
            sheet_name=sheet_name,
            a1_range=f"{start_column}:{end_column}",
        )
        plan = plan_sheet_table_sync(
            existing=existing,
            start_cell=start_cell,
            values=values,
            mode=mode,
            headers=API_ERP_TRU_SALES_EXPORT_HEADERS,
            key_columns=1,
            replace_on_order_change=True,
        )
        if verbose:
            print(
                "Dry-run план Google Sheets: "
                f"существующих строк {plan.existing_rows}, "
                f"без изменений {plan.unchanged_rows}, "
                f"будет обновлено {plan.changed_rows}, "
                f"будет добавлено {plan.appended_rows}, "
                f"устаревших строк {plan.stale_rows}, "
                f"очистка блока: {'да' if plan.cleared else 'нет'}, "
                f"ожидаемо ячеек к изменению {plan.updated_cells}"
            )
            print("Dry-run: запись в Google Sheets не выполнялась")
        return ApiErpTruSalesExportResult(
            sheet_name=sheet_name,
            start_cell=start_cell,
            rows_count=len(rows),
            sync=plan,
            dry_run=True,
        )

    sync = sync_sheet_table(
        client=client,
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=start_cell,
        values=values,
        mode=mode,
        headers=API_ERP_TRU_SALES_EXPORT_HEADERS,
        key_columns=1,
        replace_on_order_change=True,
    )
    if verbose:
        if sync.cleared:
            print(f"Google Sheets: диапазон перезаписан: {sync.updated_range}")
        print(
            "Google Sheets: "
            f"существующих строк {sync.existing_rows}, "
            f"без изменений {sync.unchanged_rows}, "
            f"обновлено {sync.changed_rows}, "
            f"добавлено {sync.appended_rows}, "
            f"устаревших убрано {sync.stale_rows}, "
            f"строк листа добавлено {sync.added_sheet_rows}, "
            f"ячеек изменено {sync.updated_cells}"
        )
    return ApiErpTruSalesExportResult(
        sheet_name=sheet_name,
        start_cell=start_cell,
        rows_count=len(rows),
        sync=sync,
        dry_run=False,
    )


def run_ozon_placement_to_sheets(
    *,
    spreadsheet_id: str | None = None,
    sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
    start_cell: str = DEFAULT_OZON_PLACEMENT_START_CELL,
    limit: int | None = None,
    mode: Literal["upsert", "replace"] = "replace",
    dry_run: bool = False,
    verbose: bool = False,
) -> PlacementExportResult:
    config = get_config()
    target_spreadsheet_id = spreadsheet_id or config.analytics_mp_spreadsheet_id
    selection = fetch_ozon_placement_report_selection()
    expected_report_date = default_placement_expected_date()
    rows = fetch_ozon_placement_sheet_rows(limit=limit)
    values = build_ozon_placement_sheet_values(rows)

    if verbose:
        print(f"Ozon хранение: подготовлено строк: {len(rows)}")
        print(f"Лист: {sheet_name}, стартовая ячейка: {start_cell}, режим: {mode}")
        if rows[:3]:
            print("Первые строки:")
            for value_row in values[1:4]:
                print(" | ".join(str(value) for value in value_row))

    from app.clients.google_sheets import GoogleSheetsClient

    client = GoogleSheetsClient(credentials_path=_resolve_project_path(config.google_application_credentials))
    if dry_run:
        start_column, end_column, _ = _target_columns(start_cell, len(OZON_PLACEMENT_EXPORT_HEADERS))
        existing = client.get_values(
            spreadsheet_id=target_spreadsheet_id,
            sheet_name=sheet_name,
            a1_range=f"{start_column}:{end_column}",
        )
        plan = plan_sheet_table_sync(
            existing=existing,
            start_cell=start_cell,
            values=values,
            mode=mode,
            headers=OZON_PLACEMENT_EXPORT_HEADERS,
            key_columns=1,
        )
        if verbose:
            print(
                "Dry-run план Google Sheets: "
                f"существующих строк {plan.existing_rows}, "
                f"без изменений {plan.unchanged_rows}, "
                f"будет обновлено {plan.changed_rows}, "
                f"будет добавлено {plan.appended_rows}, "
                f"устаревших строк {plan.stale_rows}, "
                f"очистка блока: {'да' if plan.cleared else 'нет'}, "
                f"ожидаемо ячеек к изменению {plan.updated_cells}"
            )
            print("Dry-run: запись в Google Sheets не выполнялась")
        return PlacementExportResult(
            sheet_name=sheet_name,
            start_cell=start_cell,
            rows_count=len(rows),
            sync=plan,
            dry_run=True,
            report_date=selection["report_date"] if selection else None,
            expected_report_date=expected_report_date,
            product_report_code=selection["code"] if selection else None,
            product_report_rows=selection["rows_count"] if selection else None,
        )

    sync = sync_sheet_table(
        client=client,
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=start_cell,
        values=values,
        mode=mode,
        headers=OZON_PLACEMENT_EXPORT_HEADERS,
        key_columns=1,
    )
    if verbose:
        if sync.cleared:
            print(f"Google Sheets: диапазон перезаписан: {sync.updated_range}")
        print(
            "Google Sheets: "
            f"существующих строк {sync.existing_rows}, "
            f"без изменений {sync.unchanged_rows}, "
            f"обновлено {sync.changed_rows}, "
            f"добавлено {sync.appended_rows}, "
            f"устаревших убрано {sync.stale_rows}, "
            f"строк листа добавлено {sync.added_sheet_rows}, "
            f"ячеек изменено {sync.updated_cells}"
        )
    return PlacementExportResult(
        sheet_name=sheet_name,
        start_cell=start_cell,
        rows_count=len(rows),
        sync=sync,
        dry_run=False,
        report_date=selection["report_date"] if selection else None,
        expected_report_date=expected_report_date,
        product_report_code=selection["code"] if selection else None,
        product_report_rows=selection["rows_count"] if selection else None,
    )


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _add_common_order_args(parser: argparse.ArgumentParser, *, default_start_cell: str) -> None:
    parser.add_argument("--spreadsheet-id")
    parser.add_argument("--sheet-name", default=DEFAULT_ORDERS_SHEET_NAME)
    parser.add_argument("--start-cell", default=default_start_cell)
    parser.add_argument("--date-from", help="YYYY-MM-DD; по умолчанию начало позапрошлого месяца")
    parser.add_argument("--date-to", help="YYYY-MM-DD; по умолчанию сегодня")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--mode", choices=("upsert", "replace"), default="upsert")
    parser.add_argument("--dry-run", action="store_true")


def _add_common_sheet_args(
    parser: argparse.ArgumentParser,
    *,
    default_start_cell: str,
    default_mode: str,
    default_sheet_name: str = DEFAULT_ORDERS_SHEET_NAME,
) -> None:
    parser.add_argument("--spreadsheet-id")
    parser.add_argument("--sheet-name", default=default_sheet_name)
    parser.add_argument("--start-cell", default=default_start_cell)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--mode", choices=("upsert", "replace"), default=default_mode)
    parser.add_argument("--dry-run", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Экспорт данных DataBase_MP в Google Sheets")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ozon_orders = subparsers.add_parser(
        "ozon-orders",
        help="выгрузить Ozon FBO заказы в формат Дата/Артикул/Кол-во/Сумма",
    )
    _add_common_order_args(ozon_orders, default_start_cell=DEFAULT_OZON_START_CELL)

    wb_orders = subparsers.add_parser(
        "wb-orders",
        help="выгрузить WB заказы в формат Дата/Артикул/Кол-во/Сумма",
    )
    _add_common_order_args(wb_orders, default_start_cell=DEFAULT_WB_START_CELL)

    ozon_placement = subparsers.add_parser(
        "ozon-placement",
        help="выгрузить Ozon платное хранение в DATA",
    )
    _add_common_sheet_args(ozon_placement, default_start_cell=DEFAULT_OZON_PLACEMENT_START_CELL, default_mode="replace")

    api_erp_tru_sales = subparsers.add_parser(
        "api-erp-tru-sales",
        help="выгрузить ERP/TRU продажи в DATA",
    )
    _add_common_sheet_args(api_erp_tru_sales, default_start_cell=DEFAULT_API_ERP_TRU_SALES_START_CELL, default_mode="replace")

    source_inventory = subparsers.add_parser(
        "source-production-inventory",
        help="выгрузить внутренние остатки МП в DATA",
    )
    _add_common_sheet_args(source_inventory, default_start_cell=DEFAULT_SOURCE_PRODUCTION_INVENTORY_START_CELL, default_mode="replace")

    source_pipeline = subparsers.add_parser(
        "source-supply-pipeline",
        help="выгрузить список заказов в DATA",
    )
    _add_common_sheet_args(source_pipeline, default_start_cell=DEFAULT_SOURCE_SUPPLY_PIPELINE_START_CELL, default_mode="replace")

    source_specs = subparsers.add_parser(
        "source-supply-order-specs",
        help="выгрузить LOT и даты производства в DATA 2",
    )
    _add_common_sheet_args(
        source_specs,
        default_start_cell=DEFAULT_SOURCE_SUPPLY_ORDER_SPECS_START_CELL,
        default_mode="replace",
        default_sheet_name=DEFAULT_SOURCE_SPECS_SHEET_NAME,
    )

    source_cost_ozon = subparsers.add_parser(
        "source-cost-ozon",
        help="выгрузить себестоимость Ozon из 1С в DATA",
    )
    _add_common_sheet_args(source_cost_ozon, default_start_cell=DEFAULT_SOURCE_COST_OZON_START_CELL, default_mode="replace")

    source_cost_wb = subparsers.add_parser(
        "source-cost-wb",
        help="выгрузить себестоимость WB из 1С в DATA",
    )
    _add_common_sheet_args(source_cost_wb, default_start_cell=DEFAULT_SOURCE_COST_WB_START_CELL, default_mode="replace")

    source_cost_general = subparsers.add_parser(
        "source-cost-general",
        help="выгрузить общую себестоимость 1С в DATA",
    )
    _add_common_sheet_args(source_cost_general, default_start_cell=DEFAULT_SOURCE_COST_GENERAL_START_CELL, default_mode="replace")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in {"ozon-orders", "wb-orders"}:
        common_kwargs = {
            "spreadsheet_id": args.spreadsheet_id,
            "sheet_name": args.sheet_name,
            "start_cell": args.start_cell,
            "date_from": _parse_date(args.date_from),
            "date_to": _parse_date(args.date_to),
            "limit": args.limit,
            "mode": args.mode,
            "dry_run": args.dry_run,
        }
        if args.command == "ozon-orders":
            return export_ozon_orders_to_sheets(**common_kwargs)
        if args.command == "wb-orders":
            return export_wb_orders_to_sheets(**common_kwargs)
    if args.command == "ozon-placement":
        return export_ozon_placement_to_sheets(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "api-erp-tru-sales":
        return export_api_erp_tru_sales_to_sheets(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-production-inventory":
        return export_source_block_to_sheets(
            block="production-inventory",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-supply-pipeline":
        return export_source_block_to_sheets(
            block="supply-pipeline",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-supply-order-specs":
        return export_source_block_to_sheets(
            block="supply-order-specs",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-cost-ozon":
        return export_source_block_to_sheets(
            block="source-cost-ozon",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-cost-wb":
        return export_source_block_to_sheets(
            block="source-cost-wb",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    if args.command == "source-cost-general":
        return export_source_block_to_sheets(
            block="source-cost-general",
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            limit=args.limit,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
