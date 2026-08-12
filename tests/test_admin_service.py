from __future__ import annotations

import unittest
from unittest.mock import patch

from app.admin.service import get_job_actions, get_orders_feed, get_overview, start_job_action


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
            "status": "delivered",
            "order_date": None,
            "warehouse_name": "RFZ",
            "article": "A1",
            "product_name": "Lamp",
            "quantity": 1,
            "price": 100,
        }
        with patch("app.admin.service._db_fetch_all", return_value=[row]):
            items = get_orders_feed(marketplace="ozon", limit=10)

        self.assertEqual(items[0]["marketplace"], "Ozon")
        self.assertEqual(items[0]["order_key"], "123")

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

        with (
            patch("app.admin.service.Path.exists", return_value=True),
            patch("app.admin.service.subprocess.Popen", return_value=Proc()) as popen,
        ):
            result = start_job_action("ozon_orders")

        self.assertEqual(result["key"], "ozon_orders")
        self.assertEqual(result["pid"], 123)
        args = popen.call_args.args[0]
        self.assertIn("run_ozon_orders.cmd", str(args))


if __name__ == "__main__":
    unittest.main()
