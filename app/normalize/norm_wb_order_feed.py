"""Нормализация строк WB Analytics API: Лента заказов."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes"}:
        return True
    if lowered in {"0", "false", "no"}:
        return False
    return None


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def normalize_wb_order_feed_order(row: dict[str, Any], *, currency: str | None) -> dict[str, Any] | None:
    """Раскрывает все поля объекта Order из свежего Swagger WB без потери payload."""
    srid = str(row.get("srid") or "").strip()
    if not srid:
        return None
    return {
        "srid": srid,
        "nm_id": parse_int(row.get("nmId")),
        "chrt_id": parse_int(row.get("chrtId")),
        "created_at": parse_dt(row.get("createdAt")),
        "status_updated_at": parse_dt(row.get("updatedAt")),
        "status": str(row.get("status") or "") or None,
        "cancel_type": str(row.get("cancelType") or "") or None,
        "warehouse_name": str(row.get("warehouseName") or "") or None,
        "warehouse_region": str(row.get("warehouseRegion") or "") or None,
        "is_mp": parse_bool(row.get("isMp")),
        "destination_city": str(row.get("destinationCity") or "") or None,
        "destination_district": str(row.get("destinationDistrict") or "") or None,
        "seller_price": parse_float(row.get("sellerPrice")),
        "currency": currency,
        "is_b2b": parse_bool(row.get("isB2b")),
        "payload": row,
    }
