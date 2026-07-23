from __future__ import annotations

from datetime import date, datetime
from typing import Any


def parse_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(str(value).replace(" ", "").replace(",", ".")))
    except (TypeError, ValueError):
        return 0


def parse_decimal(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def article(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_order_daily(row: dict[str, Any], *, source_system: str) -> dict[str, Any] | None:
    fact_date = parse_date(row.get("Дата"))
    item_article = article(row.get("Артикул"))
    if not fact_date or not item_article:
        return None
    return {
        "source_system": source_system,
        "article": item_article,
        "fact_date": fact_date,
        "status": str(row.get("Статус") or "").strip(),
        "orders_qty": parse_int(row.get("Кол-во")),
        "revenue": parse_decimal(row.get("Сумма")),
        "payload": row,
    }


def normalize_stock_summary(row: dict[str, Any], *, source_system: str) -> dict[str, Any] | None:
    item_article = article(row.get("Артикул"))
    if not item_article:
        return None
    return {
        "source_system": source_system,
        "article": item_article,
        "quantity": parse_int(row.get("Остаток, шт", row.get("Остаток"))),
        "in_way_qty": parse_int(row.get("В пути, шт")),
        "payload": row,
    }


def normalize_ozon_storage(row: dict[str, Any]) -> dict[str, Any] | None:
    item_article = article(row.get("Артикул"))
    if not item_article:
        return None
    return {
        "article": item_article,
        "paid_qty": parse_int(row.get("Платно, шт")),
        "paid_liters": parse_decimal(row.get("Платно, л")),
        "daily_writeoff_rub": parse_decimal(row.get("Списано в день, RUB")),
        "days_until_first_paid": parse_int(row.get("Дней до первой платности")) if row.get("Дней до первой платности") not in (None, "") else None,
        "payload": row,
    }


def normalize_production_inventory(row: dict[str, Any]) -> dict[str, Any] | None:
    item_article = article(row.get("Артикул"))
    if not item_article:
        return None
    return {
        "article": item_article,
        "smp_qty": parse_int(row.get("СМП")),
        "osn_qty": parse_int(row.get("ОСН")),
        "soh_qty": parse_int(row.get("СОХ")),
        "svh_qty": parse_int(row.get("СВХ")),
        "ts_qty": parse_int(row.get("ТС")),
        "payload": row,
    }


def normalize_supply_pipeline(row: dict[str, Any]) -> dict[str, Any] | None:
    item_article = article(row.get("Артикул"))
    if not item_article:
        return None
    return {
        "article": item_article,
        "approved_order_qty": parse_int(row.get("СОГЛ Заказа")),
        "in_production_qty": parse_int(row.get("В ПРОИЗВ")),
        "ready_qty": parse_int(row.get("ГОТОВ")),
        "in_way_qty": parse_int(row.get("В ПУТИ")),
        "minsk_date": parse_date(row.get("МИНСК")),
        "payload": row,
    }
