from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from app.clients.google_sheets import quote_sheet_name
from app.ops.sheets_export import OzonOrderSheetRow, build_ozon_order_sheet_values


class SheetsExportTests(unittest.TestCase):
    def test_build_ozon_order_sheet_values(self) -> None:
        rows = [
            OzonOrderSheetRow(
                order_date=date(2026, 8, 2),
                article="21045",
                quantity=2,
                amount=Decimal("2700.00"),
                status="awaiting_packaging",
            )
        ]

        self.assertEqual(
            build_ozon_order_sheet_values(rows),
            [
                ["Дата", "Артикул", "Кол-во", "Сумма", "Статус"],
                ["02.08.2026", "21045", 2, 2700, "Ожидает сборки"],
            ],
        )

    def test_quote_sheet_name_escapes_apostrophe(self) -> None:
        self.assertEqual(quote_sheet_name("DATA 2"), "'DATA 2'")
        self.assertEqual(quote_sheet_name("Manager's DATA"), "'Manager''s DATA'")


if __name__ == "__main__":
    unittest.main()
