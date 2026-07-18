"""
app/wb_stocks_db.py
Нормализация строк WB Stocks API.

Реальная структура ответа /analytics/v1/stocks-report/wb-warehouses:
{
    "nmId": 168149837,
    "chrtId": 279566746,
    "warehouseId": 120762,
    "warehouseName": "Электросталь",
    "regionName": "Центральный",
    "quantity": 39,
    "inWayToClient": 0,
    "inWayFromClient": 0
}
"""
from __future__ import annotations
from typing import Any, Dict


def normalize_wb_stock(row: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует одну строку из WB Stocks API в snake_case для БД."""
    def _int(v: Any) -> int | None:
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    return {
        "nm_id":              _int(row.get("nmId")),
        "chrt_id":            _int(row.get("chrtId")),
        "warehouse_id":       _int(row.get("warehouseId")),
        "warehouse_name":     row.get("warehouseName") or None,
        "region_name":        row.get("regionName") or None,
        "quantity":           _int(row.get("quantity")),
        "in_way_to_client":   _int(row.get("inWayToClient")),
        "in_way_from_client": _int(row.get("inWayFromClient")),
    }