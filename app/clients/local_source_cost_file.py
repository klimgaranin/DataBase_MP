from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


DEFAULT_COST_FILE_PATH = r"\\tsclient\S\МП\Остатки МП.txt"


def read_source_cost_file(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    with source.open("r", encoding="cp1251", newline="") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))

    if len(rows) < 6:
        raise ValueError(f"Файл себестоимости слишком короткий: {source}")

    warehouse_header = rows[3]
    sub_header = rows[4]
    warehouses = _warehouse_columns(warehouse_header, sub_header)
    data_rows = rows[5:]
    warehouse_rows: list[dict[str, Any]] = []

    for row_number, row in enumerate(data_rows, start=6):
        article = _cell(row, 2)
        if not article:
            continue
        for warehouse in warehouses:
            qty = _cell(row, warehouse["qty_index"])
            unit_cost = _cell(row, warehouse["unit_cost_index"])
            total_cost = _cell(row, warehouse["total_cost_index"])
            if not any(value.strip() for value in (qty, unit_cost, total_cost)):
                continue
            warehouse_rows.append(
                {
                    "row_number": row_number,
                    "code": _cell(row, 0),
                    "product_name": _cell(row, 1),
                    "article": article,
                    "tnved_code": _cell(row, 3),
                    "warehouse_name": warehouse["warehouse_name"],
                    "quantity": qty,
                    "unit_cost_byn": unit_cost,
                    "total_cost_byn": total_cost,
                    "payload": {
                        "source_row": row,
                        "warehouse_columns": warehouse,
                    },
                }
            )

    return {
        "headers": {
            "warehouse_header": warehouse_header,
            "sub_header": sub_header,
        },
        "data_row_count": len(data_rows),
        "warehouse_rows": warehouse_rows,
    }


def _warehouse_columns(warehouse_header: list[str], sub_header: list[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    current_warehouse = ""
    for idx, value in enumerate(warehouse_header):
        if value.strip():
            current_warehouse = value.strip().replace("\n", " ")
        if idx < 7 or not current_warehouse:
            continue
        if idx + 2 >= len(sub_header):
            continue
        first = sub_header[idx].replace("\n", " ").strip().lower()
        second = sub_header[idx + 1].replace("\n", " ").strip().lower()
        third = sub_header[idx + 2].replace("\n", " ").strip().lower()
        if "кол" not in first or "себест" not in second or "себестоимость" not in third:
            continue
        result.append(
            {
                "warehouse_name": current_warehouse,
                "qty_index": idx,
                "unit_cost_index": idx + 1,
                "total_cost_index": idx + 2,
            }
        )
    return result


def _cell(row: list[Any], index: int) -> str:
    if index >= len(row):
        return ""
    return str(row[index] or "").strip()
