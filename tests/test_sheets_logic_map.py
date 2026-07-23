from __future__ import annotations

import unittest

from tools.sheets_logic_map import _classify_column, build_logic_map


class SheetsLogicMapTests(unittest.TestCase):
    def test_classify_product_identity(self) -> None:
        self.assertEqual(_classify_column("Аналитика WB", "Артикул", []), "product_identity")

    def test_classify_daily_time_series(self) -> None:
        self.assertEqual(_classify_column("Аналитика WB", "=TODAY()-30", ["SUMIFS"]), "daily_time_series")

    def test_classify_advertising_metrics(self) -> None:
        self.assertEqual(_classify_column("Реклама WB", "расход на арт", ["SUMIFS"]), "advertising_metrics")

    def test_build_logic_map(self) -> None:
        audit = {
            "created_at": "2026-07-21T20:00:00",
            "spreadsheet": {"title": "Аналитика МП"},
            "importrange_sources": ["source"],
            "ranges": [
                {
                    "range": "'Аналитика WB'!A1:C3",
                    "formulas": 2,
                    "rows_with_values": 3,
                    "formula_columns": [
                        {
                            "column": "A",
                            "header": "Сцепка",
                            "formula_count": 2,
                            "top_functions": ["XLOOKUP"],
                            "referenced_sheets": [],
                        }
                    ],
                }
            ],
        }
        result = build_logic_map(audit)
        self.assertEqual(result["category_totals"], {"product_identity": 1})
        self.assertEqual(result["db_mapping"]["product_identity"], "core.products, core.marketplace_products")


if __name__ == "__main__":
    unittest.main()
