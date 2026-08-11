from __future__ import annotations

import unittest
from datetime import date

from app.jobs.job_sheets_orders_export import _format_alert_result, _format_alert_warnings
from app.ops.sheets_export import OrderExportResult, SheetSyncResult


def _result(marketplace: str, *, rows: int, added_sheet_rows: int = 0, stale_rows: int = 0) -> OrderExportResult:
    return OrderExportResult(
        marketplace=marketplace,  # type: ignore[arg-type]
        sheet_name="DATA",
        start_cell="A1",
        date_from=date(2026, 6, 1),
        date_to=date(2026, 8, 11),
        rows_count=rows,
        sync=SheetSyncResult(
            mode="replace-order",
            prepared_rows=rows,
            existing_rows=0,
            unchanged_rows=0,
            changed_rows=rows,
            appended_rows=rows,
            stale_rows=stale_rows,
            header_updated=True,
            cleared=True,
            updated_range="DATA!A1:D10",
            updated_cells=rows * 4,
            added_sheet_rows=added_sheet_rows,
        ),
        dry_run=False,
    )


class SheetsOrdersExportJobTests(unittest.TestCase):
    def test_alert_result_is_short(self) -> None:
        text = _format_alert_result(_result("ozon", rows=11287))

        self.assertEqual(text, "➡ Ozon строк: 11287")
        self.assertNotIn("режим", text)
        self.assertNotIn("ячеек", text)

    def test_alert_warnings_show_only_nonstandard_events(self) -> None:
        self.assertEqual(_format_alert_warnings([_result("ozon", rows=10)]), "")

        warning = _format_alert_warnings([_result("ozon", rows=10, added_sheet_rows=3, stale_rows=2)])

        self.assertIn("Строк листа добавлено: 3", warning)
        self.assertIn("Устаревших строк очищено: 2", warning)


if __name__ == "__main__":
    unittest.main()
