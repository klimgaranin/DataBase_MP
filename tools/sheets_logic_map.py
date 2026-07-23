from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
AUDIT_DIR = ROOT / "local" / "audits" / "sheets"
OUTPUT_DIR = ROOT / "local" / "audits" / "logic"

DATE_HEADER_RE = re.compile(r"^=|^\d{4,5}$")
CURRENT_HEADERS = {
    "доступный остаток",
    "остаток",
    "остаток не чз",
    "товары в пути",
    "тек цена",
    "тек wb",
    "тек ozon",
    "тек маржа, %",
    "тек дрр на артикул маржа, %",
    "тек дрр на сцепку маржа, %",
}
PRODUCT_HEADERS = {
    "сцепка",
    "серия",
    "артикул",
    "наименование",
    "арт наш",
    "арт wb",
    "арт вб",
    "арт ozon",
    "арт озон",
}
AD_HEADERS = {
    "дрр",
    "ads прош",
    "ads тек",
    "ads 28к/д",
    "расход на арт",
    "заказы на арт",
    "расход на сцепку",
    "заказы на сцепку",
    "дрр сцепки",
    "затраты, rub",
    "стоимость",
    "затраты",
}
PRODUCTION_HEADERS = {"смп", "осн", "сох", "свх", "тс", "план в минске", "согл заказа", "в произв", "готов", "в пути"}
SUPPLY_HEADERS = {"поставлено с 01.05", "поставлено с 01.06", "склад", "кластер"}
ECONOMY_TOKENS = ("маржа", "разница", "участие в акции")


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _classify_column(sheet: str, header: Any, functions: list[str]) -> str:
    h = _norm(header)
    funcs = {func.upper() for func in functions}
    sheet_key = _norm(sheet)
    if h in PRODUCT_HEADERS:
        return "product_identity"
    if h in PRODUCTION_HEADERS:
        return "production_inventory"
    if h in SUPPLY_HEADERS:
        return "supply_stock_chz"
    if h in CURRENT_HEADERS:
        return "current_marketplace_metrics"
    if h in AD_HEADERS or "дрр" in h or "реклама" in sheet_key:
        return "advertising_metrics"
    if any(token in h for token in ECONOMY_TOKENS):
        return "unit_economy_or_price"
    if DATE_HEADER_RE.search(h) or ("SUMIFS" in funcs and ("аналитика wb" in sheet_key or "аналитика ozon" in sheet_key)):
        return "daily_time_series"
    if "IMPORTRANGE" in funcs:
        return "external_sheet_lookup"
    if "XLOOKUP" in funcs or "VLOOKUP" in funcs:
        return "lookup_enrichment"
    return "other_formula"


def _latest_audit_path() -> Path:
    files = sorted(AUDIT_DIR.glob("analytics_mp_sheets_audit_*.json"))
    if not files:
        raise FileNotFoundError(f"Не найден JSON-аудит в {AUDIT_DIR}")
    return files[-1]


def build_logic_map(audit: dict[str, Any]) -> dict[str, Any]:
    sheets: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()

    for item in audit.get("ranges", []):
        sheet_range = item.get("range", "")
        sheet_name = sheet_range.split("!", 1)[0].strip("'")
        blocks: Counter[str] = Counter()
        columns: list[dict[str, Any]] = []
        for col in item.get("formula_columns", []):
            category = _classify_column(sheet_name, col.get("header"), col.get("top_functions", []))
            blocks[category] += 1
            totals[category] += 1
            columns.append(
                {
                    "column": col.get("column"),
                    "header": col.get("header"),
                    "category": category,
                    "formula_count": col.get("formula_count"),
                    "top_functions": col.get("top_functions", []),
                    "referenced_sheets": col.get("referenced_sheets", []),
                }
            )
        sheets.append(
            {
                "sheet": sheet_name,
                "range": sheet_range,
                "formulas": item.get("formulas", 0),
                "rows_with_values": item.get("rows_with_values", 0),
                "blocks": dict(blocks),
                "columns": columns,
            }
        )

    return {
        "source_audit_created_at": audit.get("created_at"),
        "spreadsheet": audit.get("spreadsheet", {}),
        "category_totals": dict(totals),
        "importrange_sources": audit.get("importrange_sources", []),
        "sheets": sheets,
        "db_mapping": {
            "product_identity": "core.products, core.marketplace_products",
            "external_sheet_lookup": "core справочники плюс импорт внешних Google Sheets",
            "daily_time_series": "analytics.marketplace_product_daily",
            "current_marketplace_metrics": "analytics.marketplace_current",
            "supply_stock_chz": "staging.marketplace_stock_locations, staging.marketplace_stock_summary",
            "production_inventory": "core.production_inventory_snapshot",
            "unit_economy_or_price": "core.product_prices, analytics.marketplace_current",
            "advertising_metrics": "staging.advertising_daily, analytics.marketplace_current",
            "lookup_enrichment": "core справочники и SQL JOIN",
            "other_formula": "требует ручного разбора",
        },
    }


def write_logic_map(logic_map: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = str(logic_map.get("source_audit_created_at", "unknown")).replace(":", "").replace("-", "")[:15]
    json_path = output_dir / f"analytics_mp_logic_map_{stamp}.json"
    md_path = output_dir / f"analytics_mp_logic_map_{stamp}.md"
    json_path.write_text(json.dumps(logic_map, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Карта логики Аналитика МП",
        "",
        f"- Источник аудита: {logic_map.get('source_audit_created_at')}",
        f"- Таблица: {logic_map.get('spreadsheet', {}).get('title', '')}",
        "",
        "## Категории",
    ]
    for category, count in sorted(logic_map["category_totals"].items()):
        lines.append(f"- {category}: {count} колонок")
    lines.extend(["", "## Маппинг в БД"])
    for category, target in logic_map["db_mapping"].items():
        lines.append(f"- {category}: {target}")
    lines.extend(["", "## Листы"])
    for sheet in logic_map["sheets"]:
        lines.append(f"### {sheet['sheet']}")
        lines.append(f"- Формул: {sheet['formulas']}")
        if sheet["blocks"]:
            lines.append("- Блоки: " + ", ".join(f"{k}={v}" for k, v in sorted(sheet["blocks"].items())))
        for col in sheet["columns"][:20]:
            refs = ", ".join(col.get("referenced_sheets", [])[:4]) or "-"
            funcs = ", ".join(col.get("top_functions", [])[:4]) or "-"
            lines.append(
                f"- {col['column']} `{col.get('header', '')}` -> {col['category']} "
                f"({col['formula_count']} формул; {funcs}; refs: {refs})"
            )
        lines.append("")
    if logic_map.get("importrange_sources"):
        lines.extend(["## Внешние источники", ""])
        lines.extend(f"- {source}" for source in logic_map["importrange_sources"])
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build DB-oriented logic map from sheets_audit JSON")
    parser.add_argument("--audit-json", default=str(_latest_audit_path()), help="Path to sheets audit JSON")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Where to write logic map files")
    args = parser.parse_args()

    audit_path = Path(args.audit_json)
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    logic_map = build_logic_map(audit)
    json_path, md_path = write_logic_map(logic_map, Path(args.output_dir))
    print(f"Аудит: {audit_path}")
    print(f"Категорий: {len(logic_map['category_totals'])}")
    for category, count in sorted(logic_map["category_totals"].items()):
        print(f"{category}: {count}")
    print(f"JSON: {json_path}")
    print(f"MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
