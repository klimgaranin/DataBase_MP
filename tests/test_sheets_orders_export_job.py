from __future__ import annotations

import unittest
from datetime import date

from app.ops.sheets_export import OrderExportResult, SheetSyncResult
from app.ops.telegram_alerts import JobAlert, render_job_alert, sheet_rows_metric, sheet_sync_warnings


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
        result = _result("ozon", rows=11287)
        text = render_job_alert(
            JobAlert(
                job_name="Sheets_Orders_Export",
                timestamp="11.08.2026 14:22",
                status="OK",
                metrics=(sheet_rows_metric("Ozon", result),),
            )
        )

        self.assertIn("➡ Ozon: 11287 строк", text)
        self.assertNotIn("режим", text)
        self.assertNotIn("ячеек", text)

    def test_alert_warnings_show_only_nonstandard_events(self) -> None:
        self.assertEqual(sheet_sync_warnings([_result("ozon", rows=10)]), ())

        warning = render_job_alert(
            JobAlert(
                job_name="Sheets_Orders_Export",
                timestamp="11.08.2026 14:22",
                status="OK",
                metrics=(sheet_rows_metric("Ozon", _result("ozon", rows=10)),),
                warnings=sheet_sync_warnings([_result("ozon", rows=10, added_sheet_rows=3, stale_rows=2)]),
            )
        )

        self.assertIn("Строк листа добавлено: 3", warning)
        self.assertIn("Устаревших строк очищено: 2", warning)


if __name__ == "__main__":
    unittest.main()
