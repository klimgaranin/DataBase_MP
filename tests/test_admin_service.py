from __future__ import annotations

import unittest
from unittest.mock import patch

from app.admin import service
from app.admin.service import (
    get_job_actions,
    get_jobs,
    get_orders_daily_summary,
    get_orders_feed,
    get_overview,
    start_job_batch,
    start_job_action,
)


class AdminServiceTests(unittest.TestCase):
    def test_overview_does_not_expose_secret_values(self) -> None:
        with (
            patch("app.admin.service.secret_status", return_value={"WB_TOKEN": True}),
            patch("app.admin.service.dependency_status", return_value={"requests": True}),
            patch("app.admin.service.get_secret", return_value="postgresql://app@localhost:5432/marketplace"),
            patch("app.admin.service._db_fetch_one", return_value={"database": "marketplace", "username": "app"}),
            patch("app.admin.service.get_jobs", return_value=[]),
        ):
            data = get_overview()

        self.assertEqual(data["secrets"], {"WB_TOKEN": True})
        self.assertNotIn("postgresql://app@localhost:5432/marketplace", str(data["secrets"]))

    def test_orders_feed_rejects_unknown_marketplace(self) -> None:
        with self.assertRaises(ValueError):
            get_orders_feed(marketplace="bad")

    def test_ozon_orders_feed_maps_rows(self) -> None:
        row = {
            "order_key": "123",
            "order_number": "05932939-0033",
            "status": "delivered",
            "order_date": None,
            "warehouse_name": "RFZ",
            "article": "A1",
            "product_name": "Lamp",
            "quantity": 1,
            "price": 100,
            "image_url": "https://example.test/image.jpg",
        }
        with patch("app.admin.service._db_fetch_all", return_value=[row]):
            items = get_orders_feed(marketplace="ozon", limit=10)

        self.assertEqual(items[0]["marketplace"], "Ozon")
        self.assertEqual(items[0]["order_key"], "123")
        self.assertEqual(items[0]["order_group_key"], "05932939-0033")
        self.assertEqual(items[0]["status_label"], "Доставлен")
        self.assertEqual(items[0]["image_url"], "https://example.test/image.jpg")

    def test_orders_daily_summary_maps_marketplace(self) -> None:
        with patch(
            "app.admin.service._db_fetch_one",
            return_value={
                "orders_count": 3,
                "articles_count": 2,
                "quantity": 4,
                "amount": 1200,
                "cancelled_orders_count": 1,
            },
        ):
            summary = get_orders_daily_summary(marketplace="ozon")

        self.assertEqual(summary["marketplace"], "Ozon")
        self.assertEqual(summary["orders_count"], 3)
        self.assertEqual(summary["amount"], 1200)

    def test_job_actions_are_sanitized_for_ui(self) -> None:
        actions = get_job_actions()

        self.assertTrue(any(action["key"] == "ozon_orders" for action in actions))
        self.assertNotIn("script", actions[0])

    def test_start_job_action_rejects_unknown_key(self) -> None:
        with self.assertRaises(ValueError):
            start_job_action("bad")

    def test_start_job_action_uses_allowlisted_script(self) -> None:
        class Proc:
            pid = 123
            def poll(self):
                return None

        with (
            patch("app.admin.service.Path.exists", return_value=True),
            patch("app.admin.service.subprocess.Popen", return_value=Proc()) as popen,
        ):
            result = start_job_action("ozon_orders")

        self.assertEqual(result["key"], "ozon_orders")
        self.assertEqual(result["pid"], 123)
        args = popen.call_args.args[0]
        self.assertIn("run_ozon_orders.cmd", str(args))

    def test_started_job_action_is_visible_in_jobs(self) -> None:
        class Proc:
            pid = 321
            def poll(self):
                return None

        service._ACTION_RUNS.clear()
        self.addCleanup(service._ACTION_RUNS.clear)

        with (
            patch("app.admin.service.Path.exists", return_value=True),
            patch("app.admin.service.subprocess.Popen", return_value=Proc()),
            patch("app.admin.service._db_fetch_all", return_value=[]),
        ):
            start_job_action("source_files")
            rows = get_jobs(since_hours=24, limit=20)

        self.assertEqual(rows[0]["job_name"], "Файлы 1С")
        self.assertEqual(rows[0]["status"], "running")

    def test_start_failed_batch_skips_when_no_failed_jobs(self) -> None:
        with patch("app.admin.service._db_fetch_all", return_value=[]):
            result = start_job_batch("failed")

        self.assertEqual(result["scope"], "failed")
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
