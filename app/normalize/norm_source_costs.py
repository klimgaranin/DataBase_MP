from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


OZON_COST_WAREHOUSE = "OZON - товар, переданный на склад МП"
WB_COST_WAREHOUSE = "Wildberries- товар, переданный на склад МП"


def normalize_source_cost_row(row: dict[str, Any]) -> dict[str, Any] | None:
    article = normalize_article(row.get("article"))
    warehouse_name = str(row.get("warehouse_name") or "").strip()
    if not article or not warehouse_name:
        return None

    quantity = parse_decimal(row.get("quantity"))
    unit_cost_byn = parse_decimal(row.get("unit_cost_byn"))
    total_cost_byn = parse_decimal(row.get("total_cost_byn"))
    if quantity == 0 and unit_cost_byn == 0 and total_cost_byn == 0:
        return None

    return {
        "row_number": int(row.get("row_number") or 0),
        "code": str(row.get("code") or "").strip(),
        "product_name": str(row.get("product_name") or "").strip(),
        "article": article,
        "tnved_code": str(row.get("tnved_code") or "").strip(),
        "warehouse_name": warehouse_name,
        "quantity": quantity,
        "unit_cost_byn": unit_cost_byn,
        "total_cost_byn": total_cost_byn,
        "payload": row.get("payload", row),
    }


def normalize_article(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    if text.isdigit():
        return str(int(text))
    return text


def parse_decimal(value: Any) -> Decimal:
    text = str(value or "").replace(" ", "").replace("\xa0", "").replace(",", ".").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")
