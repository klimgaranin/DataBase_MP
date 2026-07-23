from __future__ import annotations

import unittest

from app.jobs.wb_orders_logic import (
    apply_lookback,
    calc_lookback,
    dedupe_by_srid,
    max_cursor_from_rows,
)


class WbOrdersHelperTests(unittest.TestCase):
    def test_apply_lookback_subtracts_minutes_and_keeps_timezone(self) -> None:
        self.assertEqual(
            apply_lookback("2026-07-21T10:05:00+03:00", 10),
            "2026-07-21T09:55:00+03:00",
        )

    def test_calc_lookback_uses_small_window_for_fresh_cursor(self) -> None:
        self.assertEqual(
            calc_lookback("2026-07-21T10:00:00+03:00", last_dup_pct=40.0, base=10, max_look=30),
            2,
        )

    def test_dedupe_by_srid_keeps_latest_last_change(self) -> None:
        rows = [
            {"srid": "a", "lastChangeDate": "2026-07-21T10:00:00+03:00", "value": 1},
            {"srid": "a", "lastChangeDate": "2026-07-21T10:05:00+03:00", "value": 2},
            {"srid": "b", "lastChangeDate": "2026-07-21T10:01:00+03:00", "value": 3},
        ]

        deduped = sorted(dedupe_by_srid(rows), key=lambda item: item["srid"])

        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]["value"], 2)
        self.assertEqual(deduped[1]["value"], 3)

    def test_max_cursor_from_rows_returns_latest_change(self) -> None:
        rows = [
            {"lastChangeDate": "2026-07-21T10:00:00+03:00"},
            {"lastChangeDate": "2026-07-21T10:07:00+03:00"},
        ]

        self.assertEqual(
            max_cursor_from_rows(rows, "2026-07-21T09:00:00+03:00"),
            "2026-07-21T10:07:00+03:00",
        )


if __name__ == "__main__":
    unittest.main()
