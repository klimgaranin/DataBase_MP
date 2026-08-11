from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from app.clients.google_sheets import quote_sheet_name
from app.ops.sheets_export import (
    DEFAULT_ORDERS_SHEET_NAME,
    DEFAULT_OZON_START_CELL,
    DEFAULT_SOURCE_COST_GENERAL_START_CELL,
    DEFAULT_WB_START_CELL,
    OzonPlacementSheetRow,
    OzonOrderSheetRow,
    SourceProductionInventorySheetRow,
    SourceSupplyPipelineSheetRow,
    WB_ORDER_EXPORT_TIME_ZONE,
    build_ozon_placement_sheet_values,
    build_ozon_order_sheet_values,
    build_source_marketplace_cost_sheet_values,
    build_source_production_inventory_sheet_values,
    build_source_supply_pipeline_sheet_values,
    default_orders_date_from,
    plan_sheet_table_sync,
    sync_sheet_table,
    SheetSyncResult,
    PlacementExportResult,
    SourceMarketplaceCostSheetRow,
)
from app.jobs.job_sheets_ozon_placement_export import _fallback_warning


class FakeSheetsClient:
    def __init__(self, existing: list[list[object]], *, row_count: int = 1000) -> None:
        self.existing = existing
        self.row_count = row_count
        self.ensured_rows: list[int] = []
        self.cleared: list[str] = []
        self.updated: list[tuple[str, list[list[object]]]] = []
        self.batch_updated: list[tuple[str, list[list[object]]]] = []

    def get_values(self, *, spreadsheet_id: str, sheet_name: str, a1_range: str) -> list[list[object]]:
        return self.existing

    def clear_values(self, *, spreadsheet_id: str, sheet_name: str, a1_range: str) -> dict[str, object]:
        self.cleared.append(a1_range)
        return {}

    def ensure_sheet_rows(self, *, spreadsheet_id: str, sheet_name: str, min_rows: int) -> int:
        self.ensured_rows.append(min_rows)
        rows_to_add = max(0, min_rows - self.row_count)
        self.row_count += rows_to_add
        return rows_to_add

    def update_values(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        start_cell: str,
        values: list[list[object]],
    ) -> dict[str, object]:
        self.updated.append((start_cell, values))
        return {"updatedRange": f"{sheet_name}!{start_cell}:K{len(values)}", "updatedCells": len(values) * 4}

    def batch_update_values(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        updates: list[tuple[str, list[list[object]]]],
    ) -> dict[str, object]:
        self.batch_updated.extend(updates)
        return {"totalUpdatedCells": sum(len(rows) * 4 for _, rows in updates)}


class SheetsExportTests(unittest.TestCase):
    def test_build_ozon_order_sheet_values(self) -> None:
        rows = [
            OzonOrderSheetRow(
                order_date=date(2026, 8, 2),
                article="21045",
                quantity=2,
                amount=Decimal("2700.00"),
            )
        ]

        self.assertEqual(
            build_ozon_order_sheet_values(rows),
            [
                ["Дата", "Артикул", "Кол-во", "Сумма"],
                ["02.08.2026", "21045", 2, 2700],
            ],
        )

    def test_build_ozon_placement_sheet_values(self) -> None:
        rows = [
            OzonPlacementSheetRow(
                article="21045",
                paid_qty=2,
                paid_liters=Decimal("12.345"),
                daily_writeoff_rub=Decimal("37.50"),
                days_until_first_paid=4,
            )
        ]

        self.assertEqual(
            build_ozon_placement_sheet_values(rows),
            [
                ["Артикул", "Платно, шт", "Платно, л", "Списано в день, RUB", "Дней до первой платности"],
                ["21045", 2, 12.345, 37.5, 4],
            ],
        )

    def test_build_source_file_sheet_values(self) -> None:
        inventory_rows = [
            SourceProductionInventorySheetRow(
                article="21045",
                smp_qty=1,
                osn_qty=2,
                soh_qty=3,
                svh_qty=4,
                ts_qty=5,
            ),
            SourceProductionInventorySheetRow(
                article="10031",
                smp_qty=198,
                osn_qty=4380,
                soh_qty=Decimal("6.7"),
                svh_qty=0,
                ts_qty=0,
            )
        ]
        pipeline_rows = [
            SourceSupplyPipelineSheetRow(
                article="21045",
                approved_order_qty=1,
                in_production_qty=2,
                ready_qty=3,
                in_way_qty=4,
                minsk_date=date(2026, 8, 10),
            )
        ]

        self.assertEqual(
            build_source_production_inventory_sheet_values(inventory_rows),
            [
                ["Артикул", "СМП", "ОСН", "СОХ", "СВХ", "ТС"],
                ["21045", 1, 2, 3, 4, 5],
                ["10031", 198, 4380, 6.7, "", ""],
            ],
        )
        self.assertEqual(
            build_source_supply_pipeline_sheet_values(pipeline_rows),
            [["Артикул", "СОГЛ Заказа", "В ПРОИЗВ", "ГОТОВ", "В ПУТИ", "МИНСК"], ["21045", 1, 2, 3, 4, "10.08.2026"]],
        )

    def test_source_cost_general_export_shape(self) -> None:
        values = build_source_marketplace_cost_sheet_values(
            [
                SourceMarketplaceCostSheetRow(article="21045", unit_cost_byn=Decimal("3.50")),
            ]
        )

        self.assertEqual(DEFAULT_SOURCE_COST_GENERAL_START_CELL, "BK1")
        self.assertEqual(values, [["Артикул", "С/с BYN"], ["21045", 3.5]])

    def test_ozon_placement_fallback_warning_mentions_stale_report(self) -> None:
        result = PlacementExportResult(
            sheet_name="DATA",
            start_cell="K1",
            rows_count=10,
            sync=SheetSyncResult(
                mode="replace",
                prepared_rows=10,
                existing_rows=0,
                unchanged_rows=0,
                changed_rows=10,
                appended_rows=10,
                stale_rows=0,
                header_updated=True,
                cleared=True,
                updated_range="DATA!K1:O11",
                updated_cells=55,
            ),
            dry_run=False,
            report_date=date(2026, 8, 5),
            expected_report_date=date(2026, 8, 6),
        )

        warning = _fallback_warning(result)

        self.assertIn("не сегодняшний отчёт", warning)
        self.assertIn("2026-08-05", warning)
        self.assertIn("2026-08-06", warning)

    def test_quote_sheet_name_escapes_apostrophe(self) -> None:
        self.assertEqual(quote_sheet_name("DATA 2"), "'DATA 2'")
        self.assertEqual(quote_sheet_name("Manager's DATA"), "'Manager''s DATA'")

    def test_default_orders_date_from_is_previous_two_full_months_and_current_month(self) -> None:
        self.assertEqual(default_orders_date_from(date(2026, 8, 2)), date(2026, 6, 1))
        self.assertEqual(default_orders_date_from(date(2026, 1, 15)), date(2025, 11, 1))

    def test_production_order_export_defaults(self) -> None:
        self.assertEqual(DEFAULT_ORDERS_SHEET_NAME, "DATA")
        self.assertEqual(DEFAULT_OZON_START_CELL, "A1")
        self.assertEqual(DEFAULT_WB_START_CELL, "F1")
        self.assertEqual(WB_ORDER_EXPORT_TIME_ZONE, "UTC")

    def test_sync_sheet_table_updates_only_changed_rows(self) -> None:
        client = FakeSheetsClient(
            [
                ["Дата", "Артикул", "Кол-во", "Сумма"],
                ["02.08.2026", "21045", "2", "2700"],
                ["02.08.2026", "14252", "1", "300"],
            ]
        )
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["02.08.2026", "21045", 2, 2700],
            ["02.08.2026", "14252", 2, 600],
            ["02.08.2026", "10569", 1, 280],
        ]

        result = sync_sheet_table(
            client=client,
            spreadsheet_id="spreadsheet",
            sheet_name="DATA 2",
            start_cell="H1",
            values=values,
            mode="upsert",
        )

        self.assertEqual(result.unchanged_rows, 1)
        self.assertEqual(result.changed_rows, 1)
        self.assertEqual(result.appended_rows, 1)
        self.assertEqual(result.stale_rows, 0)
        self.assertEqual(result.added_sheet_rows, 0)
        self.assertEqual(client.cleared, [])
        self.assertEqual(client.batch_updated[0][0], "H3:K3")
        self.assertEqual(client.batch_updated[1][0], "H4:K4")

    def test_sync_sheet_table_adds_missing_sheet_rows_before_write(self) -> None:
        client = FakeSheetsClient(
            [
                ["Дата", "Артикул", "Кол-во", "Сумма"],
            ],
            row_count=2,
        )
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["02.08.2026", "21045", 2, 2700],
            ["02.08.2026", "14252", 2, 600],
            ["02.08.2026", "10569", 1, 280],
        ]

        result = sync_sheet_table(
            client=client,
            spreadsheet_id="spreadsheet",
            sheet_name="DATA 2",
            start_cell="H1",
            values=values,
            mode="upsert",
        )

        self.assertEqual(client.ensured_rows, [4])
        self.assertEqual(result.added_sheet_rows, 2)
        self.assertEqual(client.batch_updated[-1][0], "H2:K4")

    def test_sync_sheet_table_supports_placement_width_and_key(self) -> None:
        headers = ["Артикул", "Платно, шт", "Платно, л", "Списано в день, RUB", "Дней до первой платности"]
        client = FakeSheetsClient(
            [
                headers,
                ["21045", "1", "2", "3", "4"],
            ],
        )
        values = [
            headers,
            ["21045", 2, 12.345, 37.5, 4],
            ["14252", 1, 5, 10, ""],
        ]

        result = sync_sheet_table(
            client=client,
            spreadsheet_id="spreadsheet",
            sheet_name="DATA",
            start_cell="K1",
            values=values,
            mode="upsert",
            headers=headers,
            key_columns=1,
        )

        self.assertEqual(result.changed_rows, 1)
        self.assertEqual(result.appended_rows, 1)
        self.assertEqual(client.batch_updated[0][0], "K2:O2")
        self.assertEqual(client.batch_updated[1][0], "K3:O3")

    def test_sync_sheet_table_replaces_when_stale_rows_exist(self) -> None:
        client = FakeSheetsClient(
            [
                ["Дата", "Артикул", "Кол-во", "Сумма"],
                ["01.05.2026", "OLD", "1", "100"],
            ]
        )
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["01.06.2026", "NEW", 1, 200],
        ]

        result = sync_sheet_table(
            client=client,
            spreadsheet_id="spreadsheet",
            sheet_name="DATA 2",
            start_cell="H1",
            values=values,
            mode="upsert",
        )

        self.assertEqual(result.mode, "replace-stale")
        self.assertEqual(result.stale_rows, 1)
        self.assertEqual(client.cleared, ["H:K"])
        self.assertEqual(client.updated[0][0], "H1")

    def test_sync_sheet_table_replaces_when_row_order_changed(self) -> None:
        client = FakeSheetsClient(
            [
                ["Дата", "Артикул", "Кол-во", "Сумма"],
                ["05.08.2026", "OLD", "1", "100"],
                ["06.08.2026", "NEW", "1", "200"],
            ]
        )
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["06.08.2026", "NEW", 1, 200],
            ["05.08.2026", "OLD", 1, 100],
        ]

        result = sync_sheet_table(
            client=client,
            spreadsheet_id="spreadsheet",
            sheet_name="DATA",
            start_cell="A1",
            values=values,
            mode="upsert",
            replace_on_order_change=True,
        )

        self.assertEqual(result.mode, "replace-order")
        self.assertEqual(client.cleared, ["A:D"])
        self.assertEqual(client.updated[0][1], values)

    def test_plan_sheet_table_sync_does_not_write(self) -> None:
        existing = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["02.08.2026", "21045", "2", "2700"],
        ]
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["02.08.2026", "21045", 2, 2700],
            ["02.08.2026", "10569", 1, 280],
        ]

        plan = plan_sheet_table_sync(existing=existing, start_cell="H1", values=values, mode="upsert")

        self.assertFalse(plan.cleared)
        self.assertEqual(plan.unchanged_rows, 1)
        self.assertEqual(plan.appended_rows, 1)
        self.assertEqual(plan.updated_cells, 4)

    def test_plan_sheet_table_sync_detects_row_order_change(self) -> None:
        existing = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["05.08.2026", "OLD", "1", "100"],
            ["06.08.2026", "NEW", "1", "200"],
        ]
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["06.08.2026", "NEW", 1, 200],
            ["05.08.2026", "OLD", 1, 100],
        ]

        plan = plan_sheet_table_sync(
            existing=existing,
            start_cell="A1",
            values=values,
            mode="upsert",
            replace_on_order_change=True,
        )

        self.assertEqual(plan.mode, "replace-order")
        self.assertTrue(plan.cleared)
        self.assertEqual(plan.updated_cells, 12)

    def test_plan_sheet_table_sync_treats_decimal_comma_as_same_number(self) -> None:
        existing = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["01.06.2026", "10031", "2", "617,5"],
        ]
        values = [
            ["Дата", "Артикул", "Кол-во", "Сумма"],
            ["01.06.2026", "10031", 2, 617.5],
        ]

        plan = plan_sheet_table_sync(existing=existing, start_cell="M1", values=values, mode="upsert")

        self.assertEqual(plan.unchanged_rows, 1)
        self.assertEqual(plan.changed_rows, 0)
        self.assertEqual(plan.updated_cells, 0)


if __name__ == "__main__":
    unittest.main()
