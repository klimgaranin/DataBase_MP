from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

from app.clients.source_statistics_excel import read_excel_tables
from app.normalize.norm_source_statistics import (
    normalize_order_daily,
    normalize_ozon_storage,
    normalize_production_inventory,
    normalize_stock_summary,
    normalize_supply_pipeline,
)


class SourceStatisticsExcelTests(unittest.TestCase):
    def test_read_excel_tables_reads_materialized_rows(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "stats.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "DATA"
            sheet.append(["Дата", "Артикул", "Кол-во", "Сумма", "Статус"])
            sheet.append(["2026-07-01", "ART-1", 2, 1000, "delivered"])
            table = Table(displayName="Заказы_OZON", ref="A1:E2")
            table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
            sheet.add_table(table)
            workbook.save(path)
            workbook.close()

            tables = read_excel_tables(path)

        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].name, "Заказы_OZON")
        self.assertEqual(tables[0].rows[0]["Артикул"], "ART-1")


class SourceStatisticsNormalizationTests(unittest.TestCase):
    def test_normalize_order_daily(self) -> None:
        row = {"Дата": "2026-07-01", "Артикул": "ART-1", "Кол-во": "2", "Сумма": "1 234,50", "Статус": "delivered"}
        normalized = normalize_order_daily(row, source_system="ozon")
        self.assertEqual(normalized["fact_date"].isoformat(), "2026-07-01")
        self.assertEqual(normalized["article"], "ART-1")
        self.assertEqual(normalized["orders_qty"], 2)
        self.assertEqual(normalized["revenue"], 1234.5)

    def test_normalize_stock_summary(self) -> None:
        row = {"Артикул": "ART-1", "Остаток, шт": "10", "В пути, шт": "3"}
        normalized = normalize_stock_summary(row, source_system="ozon")
        self.assertEqual(normalized["quantity"], 10)
        self.assertEqual(normalized["in_way_qty"], 3)

    def test_normalize_ozon_storage(self) -> None:
        row = {
            "Артикул": "ART-1",
            "Платно, шт": "2",
            "Платно, л": "3,5",
            "Списано в день, RUB": "10,25",
            "Дней до первой платности": "7",
        }
        normalized = normalize_ozon_storage(row)
        self.assertEqual(normalized["paid_qty"], 2)
        self.assertEqual(normalized["paid_liters"], 3.5)
        self.assertEqual(normalized["daily_writeoff_rub"], 10.25)

    def test_normalize_internal_blocks(self) -> None:
        inventory = normalize_production_inventory({"Артикул": "ART-1", "СМП": "1", "ОСН": "2", "СОХ": "3", "СВХ": "4", "ТС": "5"})
        pipeline = normalize_supply_pipeline({"Артикул": "ART-1", "СОГЛ Заказа": "1", "В ПРОИЗВ": "2", "ГОТОВ": "3", "В ПУТИ": "4", "МИНСК": "01.07.2026"})
        self.assertEqual(inventory["ts_qty"], 5)
        self.assertEqual(pipeline["minsk_date"].isoformat(), "2026-07-01")
        self.assertEqual(pipeline["ready_qty"], 3)


if __name__ == "__main__":
    unittest.main()
