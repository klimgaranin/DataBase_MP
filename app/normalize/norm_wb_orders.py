from __future__ import annotations
from datetime import datetime, date
from typing import Any, Optional

def parse_bool(v: Any) -> Optional[bool]:
    if v is None: return None
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"): return True
    if s in ("false", "0", "no"): return False
    return None

def parse_int(v: Any) -> Optional[int]:
    if v is None or v == "": return None
    try: return int(v)
    except: return None

def parse_float_ru(v: Any) -> Optional[float]:
    if v is None or v == "": return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try: return float(s)
    except: return None

def parse_date(v: Any) -> Optional[date]:
    if v is None or v == "": return None
    s = str(v).strip()
    # WB часто отдаёт ISO; но на вход может попасть "YYYY-MM-DD ..."
    s = s.replace(" ", "T")
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except:
        return None

def parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "": return None
    s = str(v).strip().replace(" ", "T")
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except:
        return None

def normalize_wb_order(row: dict) -> dict:
    return {
        "srid": str(row.get("srid") or ""),

        "is_cancel": parse_bool(row.get("isCancel")),
        "date_ts": parse_dt(row.get("date")),
        "last_change_ts": parse_dt(row.get("lastChangeDate")),

        "warehouse_name": row.get("warehouseName"),
        "warehouse_type": row.get("warehouseType"),
        "country_name": row.get("countryName"),
        "oblast_okrug_name": row.get("oblastOkrugName"),
        "region_name": row.get("regionName"),

        "supplier_article": row.get("supplierArticle"),
        "nm_id": parse_int(row.get("nmId")),
        "barcode": row.get("barcode"),

        "category": row.get("category"),
        "subject": row.get("subject"),
        "brand": row.get("brand"),
        "tech_size": row.get("techSize"),

        "income_id": parse_int(row.get("incomeID")),
        "is_supply": parse_bool(row.get("isSupply")),
        "is_realization": parse_bool(row.get("isRealization")),

        "total_price": parse_float_ru(row.get("totalPrice")),
        "discount_percent": parse_int(row.get("discountPercent")),
        "spp": parse_int(row.get("spp")),
        "finished_price": parse_float_ru(row.get("finishedPrice")),
        "price_with_disc": parse_float_ru(row.get("priceWithDisc")),

        "cancel_date": parse_date(row.get("cancelDate")),

        "sticker": row.get("sticker"),
        "g_number": row.get("gNumber"),
    }
