from __future__ import annotations

from typing import Any


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def normalize_product_stat_row(row: dict[str, Any]) -> dict[str, Any] | None:
    article = str(row.get("article") or "").strip()
    if not article:
        return None
    return {
        "external_id": parse_int(row.get("id")),
        "article": article,
        "series_name": _text(row.get("series_name")),
        "brand_name": _text(row.get("brand_name")),
        "name_1s": _text(row.get("name_1s")),
        "barcode": _text(row.get("barcode")),
        "remains_warehouse_count": parse_int(row.get("remains_warehouse_count")) or 0,
        "warehouse_count": parse_int(row.get("warehouse_count")) or 0,
        "presence_count": parse_int(row.get("presence_count")) or 0,
        "for_marketplaces_count": parse_int(row.get("for_marketplaces_count")) or 0,
        "reserved_total_count": parse_int(row.get("reserved_total_count")) or 0,
        "reserved_invoice_count": parse_int(row.get("reserved_invoice_count")) or 0,
        "reserved_cash_count": parse_int(row.get("reserved_cash_count")) or 0,
        "avg_price": parse_float(row.get("avg_price")),
        "sales_count": parse_int(row.get("sales_count")) or 0,
        "sales_sum": parse_float(row.get("sales_sum")) or 0.0,
        "payload": row,
    }


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None
