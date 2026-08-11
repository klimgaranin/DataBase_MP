from __future__ import annotations

import tempfile
import unittest
import csv
from decimal import Decimal
from pathlib import Path

from app.clients.local_source_cost_file import read_source_cost_file
from app.normalize.norm_source_costs import aggregate_source_cost_rows, normalize_source_cost_row
from app.ops.sheets_export import SourceMarketplaceCostSheetRow, build_source_marketplace_cost_sheet_values


class SourceCostsTests(unittest.TestCase):
    def test_read_source_cost_file_with_two_level_headers(self) -> None:
        rows = [
            ["", "Остатки"],
            ["По всем товарам"],
            ["По всем складам"],
            [
                "Код",
                "Товар/Склад",
                "Артикул",
                "Код ТН ВЭД",
                "Количество",
                "Себестоимость\nединицы",
                "Себестоимость",
                "OZON - товар, переданный на склад МП",
                "",
                "",
                "Wildberries- товар, переданный на склад МП",
                "",
                "",
            ],
            [
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "Кол-во",
                "Себест.\nединицы",
                "Себестоимость",
                "Кол-во",
                "Себест.\nединицы",
                "Себестоимость",
            ],
            ["0001", "Товар", "0010031", "", "3.000", "1.23", "3.69", "2.000", "1.10", "2.20", "1.000", "1.37", "1.37"],
        ]
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "СС_общий.txt"
            with path.open("w", encoding="cp1251", newline="") as fh:
                writer = csv.writer(fh, delimiter="\t")
                writer.writerows(rows)

            parsed = read_source_cost_file(path)

        self.assertEqual(parsed["data_row_count"], 1)
        self.assertEqual(len(parsed["warehouse_rows"]), 2)
        normalized = normalize_source_cost_row(parsed["warehouse_rows"][0])
        self.assertEqual(normalized["article"], "10031")
        self.assertEqual(normalized["quantity"], Decimal("2.000"))
        self.assertEqual(normalized["unit_cost_byn"], Decimal("1.10"))

    def test_build_source_marketplace_cost_sheet_values(self) -> None:
        rows = [SourceMarketplaceCostSheetRow(article="10031", unit_cost_byn=Decimal("1.10"))]

        self.assertEqual(build_source_marketplace_cost_sheet_values(rows), [["Артикул", "С/с BYN"], ["10031", 1.1]])

    def test_aggregate_source_cost_rows_by_article_and_warehouse(self) -> None:
        rows = [
            {
                "article": "10264",
                "warehouse_name": "OZON - товар, переданный на склад МП",
                "quantity": Decimal("2"),
                "unit_cost_byn": Decimal("10"),
                "total_cost_byn": Decimal("20"),
                "payload": {"row": 1},
            },
            {
                "article": "10264",
                "warehouse_name": "OZON - товар, переданный на склад МП",
                "quantity": Decimal("3"),
                "unit_cost_byn": Decimal("12"),
                "total_cost_byn": Decimal("36"),
                "payload": {"row": 2},
            },
        ]

        grouped = aggregate_source_cost_rows(rows)

        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0]["quantity"], Decimal("5"))
        self.assertEqual(grouped[0]["total_cost_byn"], Decimal("56"))
        self.assertEqual(grouped[0]["unit_cost_byn"], Decimal("11.2"))
        self.assertEqual(len(grouped[0]["payload"]["rows"]), 2)


if __name__ == "__main__":
    unittest.main()
