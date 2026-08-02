from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from app.config import ROOT, get_config
from app.db import connect


OZON_ORDER_EXPORT_HEADERS = ["Дата", "Артикул", "Кол-во", "Сумма", "Статус"]

OZON_STATUS_LABELS = {
    "awaiting_packaging": "Ожидает сборки",
    "awaiting_deliver": "Ожидает отгрузки",
    "delivering": "Доставляется",
    "delivered": "Доставлен",
    "cancelled": "Отменён",
}


@dataclass(frozen=True)
class OzonOrderSheetRow:
    order_date: date
    article: str
    quantity: int
    amount: Decimal
    status: str


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return ROOT / path


def format_ozon_status(status: str) -> str:
    return OZON_STATUS_LABELS.get(status, status)


def format_sheet_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def build_ozon_order_sheet_values(rows: Sequence[OzonOrderSheetRow]) -> list[list[Any]]:
    values: list[list[Any]] = [OZON_ORDER_EXPORT_HEADERS]
    for row in rows:
        amount = row.amount.quantize(Decimal("0.01"))
        amount_value: int | float
        if amount == amount.to_integral_value():
            amount_value = int(amount)
        else:
            amount_value = float(amount)
        values.append(
            [
                format_sheet_date(row.order_date),
                row.article,
                row.quantity,
                amount_value,
                format_ozon_status(row.status),
            ]
        )
    return values


def fetch_ozon_order_sheet_rows(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
) -> list[OzonOrderSheetRow]:
    where_parts = ["COALESCE(created_at, in_process_at)::date IS NOT NULL"]
    params: list[Any] = []
    if date_from is not None:
        where_parts.append("COALESCE(created_at, in_process_at)::date >= %s")
        params.append(date_from)
    if date_to is not None:
        where_parts.append("COALESCE(created_at, in_process_at)::date <= %s")
        params.append(date_to)

    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params.append(limit)

    sql = f"""
        SELECT
            COALESCE(created_at, in_process_at)::date AS order_date,
            COALESCE(NULLIF(product_offer_id, ''), product_sku::text, '') AS article,
            SUM(product_quantity)::int AS quantity,
            SUM(product_price_amount * product_quantity)::numeric(14,2) AS amount,
            COALESCE(status, '') AS status
        FROM staging.ozon_fbo_order_items_full
        WHERE {" AND ".join(where_parts)}
        GROUP BY 1, 2, 5
        ORDER BY 1 DESC, 2 ASC, 5 ASC
        {limit_sql}
    """

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = []
            for order_date, article, quantity, amount, status in cur.fetchall():
                result.append(
                    OzonOrderSheetRow(
                        order_date=order_date,
                        article=str(article or ""),
                        quantity=int(quantity or 0),
                        amount=Decimal(amount or 0),
                        status=str(status or ""),
                    )
                )
            return result


def export_ozon_orders_to_sheets(
    *,
    spreadsheet_id: str | None = None,
    sheet_name: str = "DATA 2",
    start_cell: str = "H1",
    clear_range: str = "H:L",
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> int:
    config = get_config()
    target_spreadsheet_id = spreadsheet_id or config.analytics_mp_spreadsheet_id
    rows = fetch_ozon_order_sheet_rows(date_from=date_from, date_to=date_to, limit=limit)
    values = build_ozon_order_sheet_values(rows)

    print(f"Ozon заказы: подготовлено строк данных: {len(rows)}")
    print(f"Лист: {sheet_name}, стартовая ячейка: {start_cell}, очистка: {clear_range}")
    if rows[:3]:
        print("Первые строки:")
        for value_row in values[1:4]:
            print(" | ".join(str(value) for value in value_row))

    if dry_run:
        print("Dry-run: запись в Google Sheets не выполнялась")
        return 0

    from app.clients.google_sheets import GoogleSheetsClient

    client = GoogleSheetsClient(credentials_path=_resolve_project_path(config.google_application_credentials))
    if clear_range:
        client.clear_values(spreadsheet_id=target_spreadsheet_id, sheet_name=sheet_name, a1_range=clear_range)
    result = client.update_values(
        spreadsheet_id=target_spreadsheet_id,
        sheet_name=sheet_name,
        start_cell=start_cell,
        values=values,
    )

    print(f"Google Sheets: диапазон обновлён: {result.get('updatedRange')}")
    print(f"Google Sheets: строк: {result.get('updatedRows')}, ячеек: {result.get('updatedCells')}")
    return 0


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Экспорт данных DataBase_MP в Google Sheets")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ozon_orders = subparsers.add_parser(
        "ozon-orders",
        help="выгрузить Ozon FBO заказы в формат Дата/Артикул/Кол-во/Сумма/Статус",
    )
    ozon_orders.add_argument("--spreadsheet-id")
    ozon_orders.add_argument("--sheet-name", default="DATA 2")
    ozon_orders.add_argument("--start-cell", default="H1")
    ozon_orders.add_argument("--clear-range", default="H:L")
    ozon_orders.add_argument("--date-from", help="YYYY-MM-DD")
    ozon_orders.add_argument("--date-to", help="YYYY-MM-DD")
    ozon_orders.add_argument("--limit", type=int)
    ozon_orders.add_argument("--dry-run", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "ozon-orders":
        return export_ozon_orders_to_sheets(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            start_cell=args.start_cell,
            clear_range=args.clear_range,
            date_from=_parse_date(args.date_from),
            date_to=_parse_date(args.date_to),
            limit=args.limit,
            dry_run=args.dry_run,
        )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
