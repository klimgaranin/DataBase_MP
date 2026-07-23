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


def parse_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, dict):
        value = value.get("amount")
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "да"}
    return bool(value)


def price_currency(value: Any, fallback: Any = None) -> str | None:
    if isinstance(value, dict):
        currency = value.get("currency")
        if currency:
            return str(currency)
    if fallback:
        return str(fallback)
    return None


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalize_ozon_fbo_posting(row: dict[str, Any]) -> dict[str, Any] | None:
    posting_number = row.get("posting_number")
    if not posting_number:
        return None
    analytics = as_dict(row.get("analytics_data"))
    cancellation = as_dict(row.get("cancellation"))
    financial = as_dict(row.get("financial_data"))
    legal = as_dict(row.get("legal_info"))
    external_order = as_dict(row.get("external_order"))
    additional_data = as_list(row.get("additional_data"))
    products = as_list(row.get("products"))
    financial_products = as_list(financial.get("products"))
    return {
        "posting_number": str(posting_number),
        "order_id": parse_int(row.get("order_id")),
        "order_number": row.get("order_number"),
        "status": row.get("status"),
        "substatus": row.get("substatus"),
        "created_at": parse_dt(row.get("created_at")),
        "in_process_at": parse_dt(row.get("in_process_at")),
        "shipment_date": parse_dt(row.get("shipment_date")),
        "cancel_reason_id": parse_int(row.get("cancel_reason_id")),
        "cancel_reason": cancellation.get("cancel_reason"),
        "cancellation_initiator": cancellation.get("cancellation_initiator"),
        "cancellation_type": cancellation.get("cancellation_type"),
        "analytics_city": analytics.get("city"),
        "analytics_delivery_type": analytics.get("delivery_type"),
        "analytics_is_legal": parse_bool(analytics.get("is_legal")),
        "analytics_is_premium": parse_bool(analytics.get("is_premium")),
        "analytics_payment_type_group_name": analytics.get("payment_type_group_name"),
        "analytics_warehouse_id": parse_int(analytics.get("warehouse_id")),
        "analytics_warehouse_name": analytics.get("warehouse_name"),
        "analytics_client_delivery_date_begin": parse_dt(analytics.get("client_delivery_date_begin")),
        "analytics_client_delivery_date_end": parse_dt(analytics.get("client_delivery_date_end")),
        "financial_cluster_from": financial.get("cluster_from"),
        "financial_cluster_to": financial.get("cluster_to"),
        "legal_company_name": legal.get("company_name"),
        "legal_inn": legal.get("inn"),
        "legal_kpp": legal.get("kpp"),
        "external_order_is_external": parse_bool(external_order.get("is_external")),
        "external_order_platform_name": external_order.get("platform_name"),
        "products_count": len(products),
        "financial_products_count": len(financial_products),
        "additional_data_count": len(additional_data),
        "additional_data": additional_data,
        "payload": row,
    }


def normalize_ozon_fbo_order_items_full(row: dict[str, Any]) -> list[dict[str, Any]]:
    posting = normalize_ozon_fbo_posting(row)
    if posting is None:
        return []
    financial = as_dict(row.get("financial_data"))
    financial_products = as_list(financial.get("products"))
    result: list[dict[str, Any]] = []
    for line_number, product in enumerate(as_list(row.get("products")), start=1):
        product_data = as_dict(product)
        financial_product = as_dict(financial_products[line_number - 1] if line_number <= len(financial_products) else None)
        commission = as_dict(financial_product.get("commission"))
        actions = as_list(financial_product.get("actions"))
        digital_codes = as_list(product_data.get("digital_codes"))
        result.append(
            {
                **{key: posting.get(key) for key in (
                    "posting_number",
                    "order_id",
                    "order_number",
                    "status",
                    "substatus",
                    "created_at",
                    "in_process_at",
                    "shipment_date",
                    "cancel_reason_id",
                    "cancel_reason",
                    "cancellation_initiator",
                    "cancellation_type",
                    "analytics_city",
                    "analytics_delivery_type",
                    "analytics_is_legal",
                    "analytics_is_premium",
                    "analytics_payment_type_group_name",
                    "analytics_warehouse_id",
                    "analytics_warehouse_name",
                    "analytics_client_delivery_date_begin",
                    "analytics_client_delivery_date_end",
                    "financial_cluster_from",
                    "financial_cluster_to",
                    "legal_company_name",
                    "legal_inn",
                    "legal_kpp",
                    "external_order_is_external",
                    "external_order_platform_name",
                    "products_count",
                    "financial_products_count",
                    "additional_data_count",
                    "additional_data",
                )},
                "line_number": line_number,
                "product_is_marketplace_buyout": parse_bool(product_data.get("is_marketplace_buyout")),
                "product_offer_id": str(product_data.get("offer_id") or "").strip(),
                "product_name": product_data.get("name"),
                "product_sku": parse_int(product_data.get("sku")),
                "product_quantity": parse_int(product_data.get("quantity")) or 0,
                "product_price_amount": parse_float(product_data.get("price")),
                "product_price_currency": price_currency(product_data.get("price"), product_data.get("currency_code")),
                "product_digital_codes_count": len(digital_codes),
                "product_digital_codes": digital_codes,
                "financial_product_id": parse_int(financial_product.get("product_id")),
                "financial_payout": parse_float(financial_product.get("payout")),
                "financial_old_price": parse_float(financial_product.get("old_price")),
                "financial_price": parse_float(financial_product.get("price")),
                "financial_total_discount_value": parse_float(financial_product.get("total_discount_value")),
                "financial_total_discount_percent": parse_float(financial_product.get("total_discount_percent")),
                "financial_actions_count": len(actions),
                "financial_actions": actions,
                "financial_commission_amount": parse_float(commission.get("amount")),
                "financial_commission_percent": parse_float(commission.get("percent")),
                "financial_commission_currency": commission.get("currency"),
                "posting_payload": row,
                "product_payload": product_data,
                "financial_product_payload": financial_product,
            }
        )
    return result
