from __future__ import annotations

from datetime import date, datetime
from typing import Any


RU_MONTHS = {
    "январь": 1,
    "января": 1,
    "февраль": 2,
    "февраля": 2,
    "март": 3,
    "марта": 3,
    "апрель": 4,
    "апреля": 4,
    "май": 5,
    "мая": 5,
    "июнь": 6,
    "июня": 6,
    "июль": 7,
    "июля": 7,
    "август": 8,
    "августа": 8,
    "сентябрь": 9,
    "сентября": 9,
    "октябрь": 10,
    "октября": 10,
    "ноябрь": 11,
    "ноября": 11,
    "декабрь": 12,
    "декабря": 12,
}


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
        return parse_ru_month_year(text)


def parse_ru_month_year(value: str) -> date | None:
    parts = value.lower().replace(",", " ").split()
    year = None
    month = None
    for part in parts:
        cleaned = part.strip(".")
        if cleaned in RU_MONTHS:
            month = RU_MONTHS[cleaned]
        elif cleaned.isdigit() and len(cleaned) == 4:
            year = int(cleaned)
    if year and month:
        return date(year, month, 1)
    return None


def article(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    if text.isdigit():
        return str(int(text))
    return text


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
        "smp_qty": parse_decimal(row.get("СМП")),
        "osn_qty": parse_decimal(row.get("ОСН")),
        "soh_qty": parse_decimal(row.get("СОХ")),
        "svh_qty": parse_decimal(row.get("СВХ")),
        "ts_qty": parse_decimal(row.get("ТС")),
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


def normalize_supply_order_spec(row: dict[str, Any]) -> dict[str, Any] | None:
    item_article = article(row.get("Артикул"))
    if not item_article:
        return None
    return {
        "source_sheet": str(row.get("Лист") or "").strip(),
        "source_row_number": parse_int(row.get("Номер строки")),
        "article": item_article,
        "specification": str(row.get("LOT") or row.get("Спец-ия") or row.get("Спецификация") or "").strip(),
        "production_date": parse_date(row.get("Дата производства") or row.get("Дата производсвта")),
        "payload": row,
    }
