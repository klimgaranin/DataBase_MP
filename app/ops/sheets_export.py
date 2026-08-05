from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Literal, Sequence

from app.config import ROOT, get_config
from app.db import connect


ORDER_EXPORT_HEADERS = ["Дата", "Артикул", "Кол-во", "Сумма"]
DEFAULT_ORDERS_SHEET_NAME = "DATA"
DEFAULT_OZON_START_CELL = "A1"
DEFAULT_WB_START_CELL = "F1"
OZON_ORDER_EXPORT_TIME_ZONE = "UTC"
WB_ORDER_EXPORT_TIME_ZONE = "UTC"


@dataclass(frozen=True)
class OrderSheetRow:
    order_date: date
    article: str
    quantity: int
    amount: Decimal


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


def _row_key(row: Sequence[Any]) -> tuple[str, str]:
    padded = list(row) + [""] * (len(ORDER_EXPORT_HEADERS) - len(row))
    return (_normalize_cell(padded[0]), _normalize_cell(padded[1]))


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


def _normalize_row(row: Sequence[Any]) -> list[str]:
    padded = list(row) + [""] * (len(ORDER_EXPORT_HEADERS) - len(row))
    return [_normalize_cell(value) for value in padded[: len(ORDER_EXPORT_HEADERS)]]


def sync_sheet_table(
    *,
    client: Any,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    values: list[list[Any]],
    mode: Literal["upsert", "replace"],
) -> SheetSyncResult:
    start_column, end_column, header_row = _target_columns(start_cell, len(ORDER_EXPORT_HEADERS))
    full_column_range = f"{start_column}:{end_column}"
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
        )

    existing = client.get_values(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        a1_range=full_column_range,
    )
    updates: list[tuple[str, list[list[Any]]]] = []
    header_updated = False
    if not existing or _normalize_row(existing[header_row - 1] if len(existing) >= header_row else []) != ORDER_EXPORT_HEADERS:
        updates.append((f"{start_column}{header_row}:{end_column}{header_row}", [ORDER_EXPORT_HEADERS]))
        header_updated = True

    existing_by_key: dict[tuple[str, str], tuple[int, list[Any]]] = {}
    for index, row in enumerate(existing[header_row:], start=header_row + 1):
        key = _row_key(row)
        if all(key) and key not in existing_by_key:
            existing_by_key[key] = (index, row)

    target_keys = {_row_key(value_row) for value_row in values[1:] if all(_row_key(value_row))}
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
        )

    changed_rows = 0
    unchanged_rows = 0
    appended_rows: list[list[Any]] = []
    for value_row in values[1:]:
        key = _row_key(value_row)
        found = existing_by_key.get(key)
        if found is None:
            appended_rows.append(value_row)
            continue
        row_number, existing_row = found
        if _normalize_row(existing_row) == _normalize_row(value_row):
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
    )


def plan_sheet_table_sync(
    *,
    existing: list[list[Any]],
    start_cell: str,
    values: list[list[Any]],
    mode: Literal["upsert", "replace"],
) -> SheetSyncResult:
    start_column, _, header_row = _target_columns(start_cell, len(ORDER_EXPORT_HEADERS))
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
            updated_cells=len(values) * len(ORDER_EXPORT_HEADERS),
        )

    header_updated = not existing or _normalize_row(existing[header_row - 1] if len(existing) >= header_row else []) != ORDER_EXPORT_HEADERS
    existing_by_key: dict[tuple[str, str], list[Any]] = {}
    for row in existing[header_row:]:
        key = _row_key(row)
        if all(key) and key not in existing_by_key:
            existing_by_key[key] = row

    target_keys = {_row_key(value_row) for value_row in values[1:] if all(_row_key(value_row))}
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
            updated_cells=len(values) * len(ORDER_EXPORT_HEADERS),
        )

    changed_rows = 0
    unchanged_rows = 0
    appended_rows = 0
    for value_row in values[1:]:
        existing_row = existing_by_key.get(_row_key(value_row))
        if existing_row is None:
            appended_rows += 1
        elif _normalize_row(existing_row) == _normalize_row(value_row):
            unchanged_rows += 1
        else:
            changed_rows += 1

    header_cells = len(ORDER_EXPORT_HEADERS) if header_updated else 0
    updated_cells = header_cells + (changed_rows + appended_rows) * len(ORDER_EXPORT_HEADERS)
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
        ORDER BY 1 DESC, 2 ASC
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
        ORDER BY 1 DESC, 2 ASC
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
        plan = plan_sheet_table_sync(existing=existing, start_cell=target_start_cell, values=values, mode=mode)
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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
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
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
