"""
app/normalize/norm_wb_adv.py
Нормализация ответа /adv/v3/fullstats.

Структура ответа WB API:
  [
    {
      "advertId": 123,
      "days": [
        {
          "date": "2026-04-14",
          "apps": [
            {
              "appType": 1,
              "nm": [
                {
                  "nmId": 456,
                  "sum": 123.45,    -- расходы
                  "views": 1000,    -- показы
                  "clicks": 50,     -- клики
                  "atbs": 30,       -- в корзину
                  "orders": 10,     -- заказы (шт)
                  "shks": 8,        -- заказы (физ. шт)
                  "sum_price": 5000 -- сумма заказов (руб)
                }
              ]
            }
          ]
        }
      ]
    }
  ]

normalize_fullstats()
  → плоский список со всеми метриками, агрегированными по (advert_id, nm_id, stat_date)
  → суммирует через все appType

format_for_sheets()
  → только нужные для таблицы столбцы, числа как числа (без апострофов)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any


def normalize_fullstats(raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Разворачивает вложенную структуру в плоский список строк.
    Суммирует все метрики по (advert_id, nm_id, stat_date) через все appType.
    Строки с нулевыми расходами И нулевыми показами отбрасываются.
    """
    # ключ: (advert_id, nm_id, stat_date) → dict метрик
    agg: dict[tuple[int, int, str], dict[str, float]] = {}

    for stat in raw_items:
        advert_id = stat.get("advertId")
        if not advert_id:
            continue
        advert_id = int(advert_id)

        for day in (stat.get("days") or []):
            stat_date = (day.get("date") or "")[:10]
            if not stat_date:
                continue

            for app in (day.get("apps") or []):
                for nm in (app.get("nm") or app.get("nms") or []):
                    nm_id = nm.get("nmId")
                    if not nm_id:
                        continue

                    key = (int(advert_id), int(nm_id), stat_date)
                    if key not in agg:
                        agg[key] = {
                            "spend_rub": 0.0,
                            "views":     0,
                            "clicks":    0,
                            "atbs":      0,
                            "orders":    0,
                            "shks":      0,
                            "sum_price": 0.0,
                        }
                    m = agg[key]
                    m["spend_rub"] += float(nm.get("sum")       or 0)
                    m["views"]     += int(nm.get("views")        or 0)
                    m["clicks"]    += int(nm.get("clicks")       or 0)
                    m["atbs"]      += int(nm.get("atbs")         or 0)
                    m["orders"]    += int(nm.get("orders")       or 0)
                    m["shks"]      += int(nm.get("shks")         or 0)
                    m["sum_price"] += float(nm.get("sum_price")  or 0)

    return [
        {
            "advert_id": advert_id,
            "nm_id":     nm_id,
            "stat_date": stat_date,
            "spend_rub": round(m["spend_rub"], 2),
            "views":     m["views"],
            "clicks":    m["clicks"],
            "atbs":      m["atbs"],
            "orders":    m["orders"],
            "shks":      m["shks"],
            "sum_price": round(m["sum_price"], 2),
        }
        for (advert_id, nm_id, stat_date), m in agg.items()
        if m["spend_rub"] > 0 or m["views"] > 0
    ]


def format_for_sheets(
    norm_rows:  list[dict[str, Any]],
    begin_date: str,
    end_date:   str,
) -> list[list]:
    """
    Агрегирует norm_rows за период и возвращает строки для Google Sheets.

    Столбцы: [advertId, report_date, nmId, spend_rub]
      advertId    — int   (число, не строка → нет апострофа в Sheets)
      report_date — str   "dd.MM.yyyy" (end_date + 1 день, логика оригинального GAS)
      nmId        — int   (число)
      spend_rub   — float (число с двумя знаками, Sheets форматирует сам)

    Передача Python int/float вместо строк исключает апостроф в Google Sheets.
    """
    end_dt          = date.fromisoformat(end_date)
    report_date_str = (end_dt + timedelta(days=1)).strftime("%d.%m.%Y")

    agg: dict[tuple[int, int], float] = {}
    for row in norm_rows:
        if not (begin_date <= row["stat_date"] <= end_date):
            continue
        key = (int(row["advert_id"]), int(row["nm_id"]))
        agg[key] = agg.get(key, 0.0) + float(row["spend_rub"])

    return [
        [
            int(advert_id),         # число → без апострофа
            report_date_str,        # строка-дата
            int(nm_id),             # число → без апострофа
            round(spend, 2),        # float → без апострофа
        ]
        for (advert_id, nm_id), spend in sorted(agg.items())
        if spend > 0
    ]
