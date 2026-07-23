from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import ROOT, get_config


DEFAULT_RANGES = [
    "ПАНЕЛЬ!A1:W120",
    "Аналитика WB!A1:FH120",
    "Аналитика OZON!A1:BT120",
    "Остатки ЧЗ!A1:W120",
    "ИНФО!A1:P120",
    "DATA!A1:AT120",
    "БД!A1:AB120",
    "Stats_Full_WB!A1:L120",
    "Реклама WB!A1:AE120",
    "Реклама OZON!A1:AC120",
]
DEFAULT_OUTPUT_DIR = ROOT / "local" / "audits" / "sheets"
SECRET_MARKERS = ("token", "password", "secret", "api_key", "apikey", "authorization", "private_key")
IMPORTRANGE_RE = re.compile(r"IMPORTRANGE\(([^;,)]+)", re.IGNORECASE)
FORMULA_FUNC_RE = re.compile(r"\b([A-Z][A-Z0-9_.]*)\s*\(", re.IGNORECASE)
SHEET_REF_RE = re.compile(r"(?:'([^']+)'|([A-Za-zА-Яа-яЁё0-9_ ]+))!")


def _build_service(credentials_path: Path):
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Не установлены Google API библиотеки. Выполните: "
            "python -m pip install -r requirements.txt"
        ) from exc

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = service_account.Credentials.from_service_account_file(
        str(credentials_path),
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _redact_cell(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    lowered = value.lower()
    if any(marker in lowered for marker in SECRET_MARKERS):
        return "***REDACTED***"
    return value


def _redact_rows(rows: list[list[Any]], *, max_rows: int = 5, max_cols: int = 16) -> list[list[Any]]:
    return [[_redact_cell(cell) for cell in row[:max_cols]] for row in rows[:max_rows]]


def _count_formulas(rows: list[list[Any]]) -> int:
    return sum(1 for row in rows for cell in row if isinstance(cell, str) and cell.startswith("="))


def _extract_importrange_sources(rows: list[list[Any]]) -> list[str]:
    sources: set[str] = set()
    for row in rows:
        for cell in row:
            if not isinstance(cell, str) or "IMPORTRANGE" not in cell.upper():
                continue
            for match in IMPORTRANGE_RE.findall(cell):
                sources.add(match.strip().strip('"'))
    return sorted(sources)


def _extract_formula_functions(formula: str) -> list[str]:
    return sorted({match.group(1).upper() for match in FORMULA_FUNC_RE.finditer(formula)})


def _extract_sheet_refs(formula: str) -> list[str]:
    refs: set[str] = set()
    for quoted, bare in SHEET_REF_RE.findall(formula):
        ref = (quoted or bare).strip()
        if ref:
            refs.add(ref)
    return sorted(refs)


def _column_letter(index: int) -> str:
    result = ""
    value = index
    while value:
        value, rem = divmod(value - 1, 26)
        result = chr(65 + rem) + result
    return result


def _summarize_columns(rows: list[list[Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    max_cols = max(len(row) for row in rows)
    headers = rows[0]
    summaries: list[dict[str, Any]] = []

    for col_idx in range(max_cols):
        formulas = [
            str(row[col_idx])
            for row in rows[1:]
            if col_idx < len(row) and isinstance(row[col_idx], str) and row[col_idx].startswith("=")
        ]
        if not formulas:
            continue
        functions: Counter[str] = Counter()
        sheet_refs: Counter[str] = Counter()
        for formula in formulas:
            functions.update(_extract_formula_functions(formula))
            sheet_refs.update(_extract_sheet_refs(formula))
        header = headers[col_idx] if col_idx < len(headers) else ""
        summaries.append(
            {
                "column_index": col_idx + 1,
                "column": _column_letter(col_idx + 1),
                "header": _redact_cell(header),
                "formula_count": len(formulas),
                "top_functions": [name for name, _ in functions.most_common(10)],
                "referenced_sheets": [name for name, _ in sheet_refs.most_common(10)],
                "sample_formulas": [_redact_cell(formula) for formula in formulas[:3]],
            }
        )
    return summaries


def _sheet_meta(service, spreadsheet_id: str) -> dict[str, Any]:
    request = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="properties(title,timeZone),sheets(properties(title,gridProperties(rowCount,columnCount),hidden))",
    )
    return request.execute()


def _range_values(service, spreadsheet_id: str, ranges: list[str]) -> list[dict[str, Any]]:
    request = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=ranges,
        valueRenderOption="FORMULA",
        majorDimension="ROWS",
    )
    return request.execute().get("valueRanges", [])


def build_audit(spreadsheet_id: str, credentials_path: Path, ranges: list[str]) -> dict[str, Any]:
    service = _build_service(credentials_path)
    meta = _sheet_meta(service, spreadsheet_id)
    values = _range_values(service, spreadsheet_id, ranges)

    ranges_summary = []
    formula_counter: Counter[str] = Counter()
    importrange_sources: set[str] = set()

    for item in values:
        rows = item.get("values", [])
        formulas = _count_formulas(rows)
        headers = rows[0] if rows else []
        sources = _extract_importrange_sources(rows)
        importrange_sources.update(sources)
        formula_counter[item.get("range", "")] = formulas
        ranges_summary.append(
            {
                "range": item.get("range", ""),
                "rows_with_values": sum(1 for row in rows if any(str(cell).strip() for cell in row)),
                "columns_in_first_row": len(headers),
                "formulas": formulas,
                "headers": _redact_rows([headers], max_rows=1, max_cols=60)[0] if headers else [],
                "sample_rows": _redact_rows(rows[1:], max_rows=5, max_cols=16),
                "importrange_sources": sources,
                "formula_columns": _summarize_columns(rows),
            }
        )

    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet": meta.get("properties", {}),
        "sheets": [
            {
                "title": sheet["properties"]["title"],
                "rows": sheet["properties"].get("gridProperties", {}).get("rowCount"),
                "columns": sheet["properties"].get("gridProperties", {}).get("columnCount"),
                "hidden": bool(sheet["properties"].get("hidden", False)),
            }
            for sheet in meta.get("sheets", [])
        ],
        "ranges": ranges_summary,
        "formula_totals_by_range": dict(formula_counter),
        "importrange_sources": sorted(importrange_sources),
    }


def write_outputs(audit: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = output_dir / f"analytics_mp_sheets_audit_{stamp}.json"
    md_path = output_dir / f"analytics_mp_sheets_audit_{stamp}.md"
    json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Аудит Google Таблицы Аналитика МП",
        "",
        f"- Дата: {audit['created_at']}",
        f"- Таблица: {audit.get('spreadsheet', {}).get('title', '')}",
        f"- Timezone: {audit.get('spreadsheet', {}).get('timeZone', '')}",
        "",
        "## Листы",
    ]
    for sheet in audit["sheets"]:
        hidden = ", hidden" if sheet["hidden"] else ""
        lines.append(f"- {sheet['title']}: {sheet['rows']} строк, {sheet['columns']} колонок{hidden}")

    lines.extend(["", "## Диапазоны"])
    for item in audit["ranges"]:
        lines.append(
            f"- {item['range']}: строк с данными {item['rows_with_values']}, "
            f"формул {item['formulas']}, колонок в первой строке {item['columns_in_first_row']}"
        )
        if item["headers"]:
            lines.append("  Заголовки: " + " | ".join(str(cell) for cell in item["headers"][:24]))
        formula_columns = item.get("formula_columns", [])
        if formula_columns:
            lines.append("  Колонки с формулами:")
            for column in formula_columns[:12]:
                refs = ", ".join(column.get("referenced_sheets", [])[:5]) or "-"
                funcs = ", ".join(column.get("top_functions", [])[:5]) or "-"
                lines.append(
                    "  - "
                    f"{column['column']} ({column.get('header', '')}): "
                    f"{column['formula_count']} формул; функции: {funcs}; ссылки: {refs}"
                )

    lines.extend(["", "## Внешние IMPORTRANGE"])
    if audit["importrange_sources"]:
        lines.extend(f"- {source}" for source in audit["importrange_sources"])
    else:
        lines.append("- не обнаружено в проверенных диапазонах")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only audit of Google Sheet Аналитика МП")
    parser.add_argument("--spreadsheet-id", help="Google Spreadsheet ID")
    parser.add_argument("--credentials", help="Path to service-account JSON")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Where to write audit files")
    parser.add_argument("--range", action="append", dest="ranges", help="Range to inspect; can be repeated")
    parser.add_argument("--no-write", action="store_true", help="Print summary only, do not write local files")
    args = parser.parse_args()

    cfg = get_config()
    spreadsheet_id = args.spreadsheet_id or cfg.analytics_mp_spreadsheet_id
    credentials_path = Path(args.credentials or cfg.google_application_credentials)
    if not credentials_path.is_absolute():
        credentials_path = ROOT / credentials_path
    if not credentials_path.exists():
        raise SystemExit(f"Не найден service account JSON: {credentials_path}")

    audit = build_audit(spreadsheet_id, credentials_path, args.ranges or DEFAULT_RANGES)
    print(f"Таблица: {audit.get('spreadsheet', {}).get('title', '')}")
    print(f"Листов: {len(audit['sheets'])}")
    print(f"Проверенных диапазонов: {len(audit['ranges'])}")
    print(f"IMPORTRANGE источников: {len(audit['importrange_sources'])}")
    for item in audit["ranges"]:
        print(f"{item['range']}: формул={item['formulas']} строк={item['rows_with_values']}")

    if not args.no_write:
        json_path, md_path = write_outputs(audit, Path(args.output_dir))
        print(f"JSON: {json_path}")
        print(f"MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
