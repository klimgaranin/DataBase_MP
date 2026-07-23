from __future__ import annotations

from datetime import datetime
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


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "да"}
    return bool(value)


def normalize_product_list_item(row: dict[str, Any]) -> dict[str, Any] | None:
    product_id = parse_int(row.get("product_id"))
    if product_id is None:
        return None
    return {
        "product_id": product_id,
        "offer_id": str(row.get("offer_id") or "").strip() or None,
        "sku": parse_int(row.get("sku")),
        "archived": parse_bool(row.get("archived")),
        "has_fbo_stocks": parse_bool(row.get("has_fbo_stocks")),
        "has_fbs_stocks": parse_bool(row.get("has_fbs_stocks")),
        "is_discounted": parse_bool(row.get("is_discounted")),
        "quants_count": len(row.get("quants") or []) if isinstance(row.get("quants"), list) else 0,
        "payload": row,
    }


def normalize_product_info_item(row: dict[str, Any]) -> dict[str, Any] | None:
    product_id = parse_int(row.get("id") or row.get("product_id"))
    if product_id is None:
        return None
    return {
        "product_id": product_id,
        "offer_id": str(row.get("offer_id") or "").strip() or None,
        "sku": parse_int(row.get("sku")),
        "name": row.get("name"),
        "currency_code": row.get("currency_code"),
        "price": parse_float(row.get("price")),
        "old_price": parse_float(row.get("old_price")),
        "min_price": parse_float(row.get("min_price")),
        "vat": parse_float(row.get("vat")),
        "volume_weight": parse_float(row.get("volume_weight")),
        "description_category_id": parse_int(row.get("description_category_id")),
        "type_id": parse_int(row.get("type_id")),
        "primary_image": _first_value(row.get("primary_image")),
        "images_count": len(row.get("images") or []) if isinstance(row.get("images"), list) else 0,
        "barcodes_count": len(row.get("barcodes") or []) if isinstance(row.get("barcodes"), list) else 0,
        "commissions_count": len(row.get("commissions") or []) if isinstance(row.get("commissions"), list) else 0,
        "is_archived": parse_bool(row.get("is_archived")),
        "is_autoarchived": parse_bool(row.get("is_autoarchived")),
        "is_discounted": parse_bool(row.get("is_discounted")),
        "has_discounted_fbo_item": parse_bool(row.get("has_discounted_fbo_item")),
        "is_kgt": parse_bool(row.get("is_kgt")),
        "is_prepayment_allowed": parse_bool(row.get("is_prepayment_allowed")),
        "is_seasonal": parse_bool(row.get("is_seasonal")),
        "is_super": parse_bool(row.get("is_super")),
        "status": _nested_text(row, "statuses", "status"),
        "status_name": _nested_text(row, "statuses", "status_name"),
        "status_description": _nested_text(row, "statuses", "status_description"),
        "moderate_status": _nested_text(row, "statuses", "moderate_status"),
        "validation_status": _nested_text(row, "statuses", "validation_status"),
        "status_updated_at": _parse_dt(_nested_value(row, "statuses", "status_updated_at")),
        "has_price": _nested_bool(row, "visibility_details", "has_price"),
        "has_stock": _nested_bool(row, "visibility_details", "has_stock"),
        "model_id": parse_int(_nested_value(row, "model_info", "model_id")),
        "model_count": parse_int(_nested_value(row, "model_info", "count")),
        "updated_at": _parse_dt(row.get("updated_at")),
        "created_at": _parse_dt(row.get("created_at")),
        "payload": row,
    }


def normalize_analytics_stock(row: dict[str, Any]) -> dict[str, Any] | None:
    sku = parse_int(row.get("sku"))
    if sku is None:
        return None
    requested = parse_int(row.get("requested_stock_count")) or 0
    transit = parse_int(row.get("transit_stock_count")) or 0
    returns = parse_int(row.get("return_from_customer_stock_count")) or 0
    return {
        "sku": sku,
        "offer_id": str(row.get("offer_id") or "").strip() or None,
        "cluster_id": parse_int(row.get("cluster_id")),
        "cluster_name": row.get("cluster_name"),
        "available_stock_count": parse_int(row.get("available_stock_count")) or 0,
        "valid_stock_count": parse_int(row.get("valid_stock_count")) or 0,
        "other_stock_count": parse_int(row.get("other_stock_count")) or 0,
        "requested_stock_count": requested,
        "transit_stock_count": transit,
        "return_from_customer_stock_count": returns,
        "return_to_seller_stock_count": parse_int(row.get("return_to_seller_stock_count")) or 0,
        "stock_defect_stock_count": parse_int(row.get("stock_defect_stock_count")) or 0,
        "transit_defect_stock_count": parse_int(row.get("transit_defect_stock_count")) or 0,
        "expiring_stock_count": parse_int(row.get("expiring_stock_count")) or 0,
        "waiting_docs_stock_count": parse_int(row.get("waiting_docs_stock_count")) or 0,
        "waiting_docs_to_export_stock_count": parse_int(row.get("waiting_docs_to_export_stock_count")) or 0,
        "excess_stock_count": parse_int(row.get("excess_stock_count")) or 0,
        "in_way_to_warehouse_count": requested + transit + returns,
        "ads": parse_float(row.get("ads")),
        "ads_cluster": parse_float(row.get("ads_cluster")),
        "idc": parse_float(row.get("idc")),
        "idc_cluster": parse_float(row.get("idc_cluster")),
        "days_without_sales": parse_int(row.get("days_without_sales")),
        "days_without_sales_cluster": parse_int(row.get("days_without_sales_cluster")),
        "turnover_grade": row.get("turnover_grade"),
        "turnover_grade_cluster": row.get("turnover_grade_cluster"),
        "macrolocal_cluster_id": parse_int(row.get("macrolocal_cluster_id")),
        "warehouse_id": parse_int(row.get("warehouse_id")),
        "warehouse_name": row.get("warehouse_name"),
        "placement_zone": row.get("placement_zone"),
        "name": row.get("name"),
        "item_tags_count": len(row.get("item_tags") or []) if isinstance(row.get("item_tags"), list) else 0,
        "payload": row,
    }


def merge_analytics_stock_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[int, str, int], dict[str, Any]] = {}
    for row in rows:
        sku = row.get("sku")
        if sku is None:
            continue
        offer_id = str(row.get("offer_id") or "")
        cluster_id = row.get("cluster_id") or 0
        key = (int(sku), offer_id, int(cluster_id))
        current = merged.get(key)
        if current is None:
            current = dict(row)
            current["payload"] = {"merged_rows": [row.get("payload", {})]}
            merged[key] = current
            continue
        for field in (
            "available_stock_count",
            "requested_stock_count",
            "transit_stock_count",
            "return_from_customer_stock_count",
            "in_way_to_warehouse_count",
            "valid_stock_count",
            "other_stock_count",
            "return_to_seller_stock_count",
            "stock_defect_stock_count",
            "transit_defect_stock_count",
            "expiring_stock_count",
            "waiting_docs_stock_count",
            "waiting_docs_to_export_stock_count",
            "excess_stock_count",
        ):
            current[field] = (current.get(field) or 0) + (row.get(field) or 0)
        for field in (
            "cluster_name", "ads", "ads_cluster", "idc", "idc_cluster",
            "days_without_sales", "days_without_sales_cluster",
            "turnover_grade", "turnover_grade_cluster",
            "macrolocal_cluster_id", "warehouse_id", "warehouse_name",
            "placement_zone", "name", "item_tags_count",
        ):
            if current.get(field) in (None, "") and row.get(field) not in (None, ""):
                current[field] = row.get(field)
        current.setdefault("payload", {}).setdefault("merged_rows", []).append(row.get("payload", {}))
    return list(merged.values())


def _nested_value(row: dict[str, Any], group: str, key: str) -> Any:
    value = row.get(group)
    if isinstance(value, dict):
        return value.get(key)
    return None


def _nested_text(row: dict[str, Any], group: str, key: str) -> str | None:
    value = _nested_value(row, group, key)
    return str(value) if value not in (None, "") else None


def _nested_bool(row: dict[str, Any], group: str, key: str) -> bool:
    return parse_bool(_nested_value(row, group, key))


def _first_value(value: Any) -> str | None:
    if isinstance(value, list) and value:
        first = value[0]
        return str(first) if first not in (None, "") else None
    if value not in (None, ""):
        return str(value)
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
