from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import struct
import zlib
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "local" / "audits" / "source_files"

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


SECRET_WRAPPED_PATTERNS = [
    re.compile(r'(Authorization\s*=\s*")[^"]+(")', re.IGNORECASE),
    re.compile(r'("?(?:token|api[_-]?key|password|secret)"?\s*[:=]\s*")[^"]+(")', re.IGNORECASE),
]
SECRET_INLINE_PATTERNS = [
    re.compile(r"(Authorization\s*:\s*)[A-Za-z0-9_.=-]{20,}", re.IGNORECASE),
    re.compile(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"),
]


def sanitize_text(text: str) -> str:
    result = text
    for pattern in SECRET_WRAPPED_PATTERNS:
        result = pattern.sub(r"\1***REDACTED***\2", result)
    for pattern in SECRET_INLINE_PATTERNS:
        result = pattern.sub(lambda match: match.group(1) + "***REDACTED***" if match.lastindex else "***REDACTED***", result)
    return result


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_xml(zip_file: zipfile.ZipFile, name: str) -> ET.Element:
    return ET.fromstring(zip_file.read(name))


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _attr(attrs: dict[str, str], name: str) -> str | None:
    for key, value in attrs.items():
        if _local_name(key) == name:
            return value
    return None


def extract_data_mashup_parts(raw_xml: bytes) -> dict[str, str]:
    text = raw_xml.decode("utf-16", errors="replace").lstrip("\ufeff")
    root = ET.fromstring(text)
    payload = (root.text or "").strip()
    if not payload:
        return {}

    package = base64.b64decode(payload)
    parts: dict[str, str] = {}
    pos = 0
    while True:
        header_pos = package.find(b"PK\x03\x04", pos)
        if header_pos < 0:
            break
        header = package[header_pos : header_pos + 30]
        if len(header) < 30:
            break
        (
            _sig,
            _version,
            _flag,
            compression,
            _mod_time,
            _mod_date,
            _crc,
            compressed_size,
            _uncompressed_size,
            name_len,
            extra_len,
        ) = struct.unpack("<IHHHHHIIIHH", header)
        name_start = header_pos + 30
        name_end = name_start + name_len
        payload_start = name_end + extra_len
        payload_end = payload_start + compressed_size
        name = package[name_start:name_end].decode("utf-8", errors="replace")
        compressed = package[payload_start:payload_end]
        if compression == 8:
            content = zlib.decompress(compressed, -15)
        elif compression == 0:
            content = compressed
        else:
            content = b""
        parts[name] = content.decode("utf-8", errors="replace").lstrip("\ufeff")
        pos = payload_end
    return parts


def split_powerquery_section(section_m: str) -> list[dict[str, Any]]:
    pattern = re.compile(r'shared\s+#"(?P<name>[^"]+)"\s*=\s*(?P<body>.*?);(?=\s*shared\s+#"|\s*$)', re.DOTALL)
    queries = []
    for match in pattern.finditer(section_m):
        body = match.group("body").strip()
        sanitized_body = sanitize_text(body)
        queries.append(
            {
                "name": match.group("name"),
                "body": sanitized_body,
                "sources": extract_query_sources(sanitized_body),
                "selected_columns": extract_select_columns(sanitized_body),
                "renamed_columns": extract_rename_pairs(sanitized_body),
                "group_by": extract_group_columns(sanitized_body),
                "filters": extract_filters(sanitized_body),
            }
        )
    return queries


def extract_query_sources(body: str) -> list[str]:
    sources: list[str] = []
    for function_name in ("File.Contents", "Web.Contents", "Excel.CurrentWorkbook"):
        if function_name == "Excel.CurrentWorkbook":
            if function_name in body:
                sources.extend(re.findall(r'Excel\.CurrentWorkbook\(\)\{\[Name="([^"]+)"\]\}\[Content\]', body))
        else:
            sources.extend(re.findall(re.escape(function_name) + r'\("([^"]+)"', body))
    return list(dict.fromkeys(sources))


def extract_select_columns(body: str) -> list[str]:
    columns: list[str] = []
    for match in re.finditer(r"Table\.SelectColumns\([^,]+,\s*\{(?P<cols>.*?)\}\)", body, re.DOTALL):
        columns.extend(re.findall(r'"([^"]+)"', match.group("cols")))
    return list(dict.fromkeys(columns))


def extract_rename_pairs(body: str) -> list[dict[str, str]]:
    pairs: list[dict[str, str]] = []
    for source, target in re.findall(r'\{\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\}', body):
        pairs.append({"from": source, "to": target})
    return pairs


def extract_group_columns(body: str) -> list[str]:
    match = re.search(r"Table\.Group\([^,]+,\s*\{(?P<cols>.*?)\}", body, re.DOTALL)
    if not match:
        return []
    return re.findall(r'"([^"]+)"', match.group("cols"))


def extract_filters(body: str) -> list[str]:
    return [sanitize_text(match.strip()) for match in re.findall(r"Table\.SelectRows\([^,]+,\s*each\s*(.*?)\)", body)]


def parse_workbook(zip_file: zipfile.ZipFile) -> dict[str, Any]:
    ns = {"m": MAIN_NS, "r": REL_NS}
    workbook = _read_xml(zip_file, "xl/workbook.xml")
    sheets = []
    for sheet in workbook.findall("m:sheets/m:sheet", ns):
        sheets.append(
            {
                "name": sheet.attrib.get("name"),
                "sheet_id": sheet.attrib.get("sheetId"),
                "state": sheet.attrib.get("state", "visible"),
                "relationship_id": sheet.attrib.get(f"{{{REL_NS}}}id"),
            }
        )
    defined_names = []
    for defined_name in workbook.findall("m:definedNames/m:definedName", ns):
        defined_names.append(
            {
                "name": defined_name.attrib.get("name"),
                "local_sheet_id": defined_name.attrib.get("localSheetId"),
                "hidden": defined_name.attrib.get("hidden") == "1",
                "ref": defined_name.text or "",
            }
        )
    return {"sheets": sheets, "defined_names": defined_names}


def parse_connections(zip_file: zipfile.ZipFile) -> list[dict[str, Any]]:
    if "xl/connections.xml" not in zip_file.namelist():
        return []
    ns = {"m": MAIN_NS}
    root = _read_xml(zip_file, "xl/connections.xml")
    connections = []
    for connection in root.findall("m:connection", ns):
        db_pr = connection.find("m:dbPr", ns)
        connections.append(
            {
                "id": connection.attrib.get("id"),
                "name": connection.attrib.get("name"),
                "description": connection.attrib.get("description"),
                "type": connection.attrib.get("type"),
                "refresh_on_load": connection.attrib.get("refreshOnLoad") == "1",
                "save_data": connection.attrib.get("saveData") == "1",
                "command": db_pr.attrib.get("command") if db_pr is not None else None,
                "connection": sanitize_text(db_pr.attrib.get("connection", "")) if db_pr is not None else None,
            }
        )
    return connections


def parse_tables(zip_file: zipfile.ZipFile) -> list[dict[str, Any]]:
    ns = {"m": MAIN_NS}
    tables = []
    for name in sorted(n for n in zip_file.namelist() if n.startswith("xl/tables/table") and n.endswith(".xml")):
        root = _read_xml(zip_file, name)
        columns = [
            column.attrib.get("name")
            for column in root.findall("m:tableColumns/m:tableColumn", ns)
            if column.attrib.get("name")
        ]
        tables.append(
            {
                "part": name,
                "name": root.attrib.get("name"),
                "display_name": root.attrib.get("displayName"),
                "ref": root.attrib.get("ref"),
                "columns": columns,
            }
        )
    return tables


def parse_query_tables(zip_file: zipfile.ZipFile) -> list[dict[str, Any]]:
    query_tables = []
    for name in sorted(n for n in zip_file.namelist() if n.startswith("xl/queryTables/queryTable") and n.endswith(".xml")):
        root = _read_xml(zip_file, name)
        query_tables.append(
            {
                "part": name,
                "name": root.attrib.get("name"),
                "connection_id": root.attrib.get("connectionId"),
                "refresh_on_load": root.attrib.get("refreshOnLoad") == "1",
                "background_refresh": root.attrib.get("backgroundRefresh") == "1",
                "adjust_column_width": root.attrib.get("adjustColumnWidth") == "1",
            }
        )
    return query_tables


def extract_vba_modules(path: Path) -> list[dict[str, Any]]:
    try:
        from oletools.olevba import VBA_Parser
    except ModuleNotFoundError:
        return [{"warning": "oletools не установлен; VBA-код не извлечён"}]

    modules = []
    parser = VBA_Parser(str(path))
    try:
        for _filename, _stream_path, vba_filename, code in parser.extract_macros():
            clean_code = sanitize_text(code or "")
            modules.append(
                {
                    "module": vba_filename,
                    "is_empty": not clean_code.strip(),
                    "code": clean_code,
                    "auto_open": "Workbook_Open" in clean_code or "Auto_Open" in clean_code,
                    "refresh_calls": re.findall(r'ThisWorkbook\.Queries\("([^"]+)"\)\.Refresh', clean_code),
                }
            )
    finally:
        parser.close()
    return modules


def infer_db_sources(queries: list[dict[str, Any]], tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def normalize_name(value: str) -> str:
        return re.sub(r"[\s+]+", "_", value).replace("__", "_")

    table_by_name = {table["display_name"]: table for table in tables}
    normalized_table_by_name = {normalize_name(table["display_name"]): table for table in tables if table["display_name"]}
    inferred = []
    for query in queries:
        name = query["name"]
        marketplace = "internal"
        lower = name.lower()
        if "wb" in lower:
            marketplace = "wb"
        elif "ozon" in lower:
            marketplace = "ozon"
        elif "ym" in lower:
            marketplace = "yandex_market"

        subject = "other"
        if "заказы" in lower:
            subject = "orders"
        elif "остатки" in lower:
            subject = "stocks"
        elif "хранение" in lower:
            subject = "storage_costs"
        elif "список заказов" in lower:
            subject = "supply_pipeline"

        output_table = table_by_name.get(name) or normalized_table_by_name.get(normalize_name(name))
        inferred.append(
            {
                "query": name,
                "marketplace": marketplace,
                "subject": subject,
                "sources": query["sources"],
                "output_table": output_table.get("display_name") if output_table else None,
                "output_columns": output_table.get("columns", []) if output_table else [],
                "selected_columns": query["selected_columns"],
                "group_by": query["group_by"],
                "filters": query["filters"],
            }
        )
    return inferred


def build_audit(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as zip_file:
        workbook = parse_workbook(zip_file)
        tables = parse_tables(zip_file)
        query_tables = parse_query_tables(zip_file)
        connections = parse_connections(zip_file)
        mashup_parts = {}
        if "customXml/item1.xml" in zip_file.namelist():
            mashup_parts = extract_data_mashup_parts(zip_file.read("customXml/item1.xml"))
        section_m = mashup_parts.get("Formulas/Section1.m", "")
        queries = split_powerquery_section(section_m)
        has_vba = "xl/vbaProject.bin" in zip_file.namelist()
        vba_sha256 = sha256_bytes(zip_file.read("xl/vbaProject.bin")) if has_vba else None

    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "path": str(path),
        "file_name": path.name,
        "file_size": path.stat().st_size,
        "sha256": sha256_bytes(path.read_bytes()),
        "workbook": workbook,
        "connections": connections,
        "tables": tables,
        "query_tables": query_tables,
        "power_query": {
            "parts": sorted(mashup_parts),
            "queries": queries,
        },
        "vba": {
            "present": has_vba,
            "sha256": vba_sha256,
            "modules": extract_vba_modules(path) if has_vba else [],
        },
        "db_source_candidates": infer_db_sources(queries, tables),
    }


def write_audit(audit: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stem = Path(audit["file_name"]).stem.replace(" ", "_")
    json_path = output_dir / f"{stem}_xlsm_powerquery_audit_{stamp}.json"
    md_path = output_dir / f"{stem}_xlsm_powerquery_audit_{stamp}.md"
    json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Аудит XLSM Power Query/VBA",
        "",
        f"- Файл: {audit['file_name']}",
        f"- Размер: {audit['file_size']} bytes",
        f"- SHA-256: `{audit['sha256']}`",
        f"- Дата: {audit['created_at']}",
        "",
        "## Листы",
    ]
    for sheet in audit["workbook"]["sheets"]:
        lines.append(f"- `{sheet['name']}`: {sheet['state']}")

    lines.extend(["", "## Power Query источники"])
    for source in audit["db_source_candidates"]:
        lines.append(
            f"- `{source['query']}`: marketplace=`{source['marketplace']}`, "
            f"subject=`{source['subject']}`, columns={len(source['output_columns'])}"
        )
        if source["sources"]:
            lines.append("  Источники: " + " | ".join(f"`{item}`" for item in source["sources"]))
        if source["output_columns"]:
            lines.append("  Колонки: " + " | ".join(f"`{item}`" for item in source["output_columns"]))
        if source["filters"]:
            lines.append("  Фильтры: " + " | ".join(f"`{item}`" for item in source["filters"]))

    lines.extend(["", "## Таблицы Excel"])
    for table in audit["tables"]:
        lines.append(f"- `{table['display_name']}` `{table['ref']}`: " + " | ".join(f"`{c}`" for c in table["columns"]))

    lines.extend(["", "## Подключения"])
    for connection in audit["connections"]:
        refresh = "refreshOnLoad" if connection["refresh_on_load"] else "manual"
        lines.append(f"- id={connection['id']} `{connection['name']}`: {refresh}, `{connection['command']}`")

    lines.extend(["", "## VBA"])
    lines.append(f"- VBA present: `{audit['vba']['present']}`")
    if audit["vba"]["sha256"]:
        lines.append(f"- vbaProject.bin SHA-256: `{audit['vba']['sha256']}`")
    for module in audit["vba"]["modules"]:
        if "warning" in module:
            lines.append(f"- {module['warning']}")
            continue
        calls = ", ".join(module["refresh_calls"]) or "нет"
        lines.append(f"- `{module['module']}`: empty={module['is_empty']}, auto_open={module['auto_open']}, refresh={calls}")

    lines.extend(["", "## Вывод для БД"])
    lines.extend(
        [
            "- Excel сейчас выполняет роль ручного ETL: читает CSV/XLSX/API, агрегирует и кладёт результат в листы.",
            "- В БД это нужно заменить единым pipeline: raw source files/API -> typed staging -> analytics views.",
            "- WB/Ozon/Яндекс заказы и остатки лучше хранить раздельно на raw/staging-уровне, а объединять только в analytics-витринах.",
            "- В XLSM обнаруживаются секреты в Power Query; при переносе их нужно держать только в `app/secrets.py`/keyring.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only audit of XLSM Power Query and VBA")
    parser.add_argument("path", help="Path to .xlsm file")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args()

    path = Path(args.path)
    if not path.is_absolute():
        path = ROOT / path
    if not path.exists():
        raise SystemExit(f"Файл не найден: {path}")

    audit = build_audit(path)
    json_path, md_path = write_audit(audit, Path(args.output_dir))
    print(f"Файл: {path}")
    print(f"Power Query запросов: {len(audit['power_query']['queries'])}")
    print(f"Excel tables: {len(audit['tables'])}")
    print(f"VBA modules: {len(audit['vba']['modules'])}")
    print(f"JSON: {json_path}")
    print(f"MD: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
