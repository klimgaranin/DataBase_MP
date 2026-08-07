from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from app.clients.http_wb_order_feed import WbOrderFeedClient, iter_order_feed
from app.jobs.job_wb_order_feed import _dedupe_by_srid, _period
from app.normalize.norm_wb_order_feed import normalize_wb_order_feed_order


class WbOrderFeedClientTests(unittest.TestCase):
    def test_uses_snapshot_time_for_second_page(self) -> None:
        session = Mock()
        payloads = [
            {"data": {"snapshotTime": "2026-08-07T09:00:00Z", "currency": "RUB", "orders": [{"srid": "1"}, {"srid": "2"}]}},
            {"data": {"snapshotTime": "2026-08-07T09:00:00Z", "currency": "RUB", "orders": [{"srid": "3"}]}},
        ]
        responses = []
        for payload in payloads:
            response = Mock()
            response.status_code = 200
            response.content = json.dumps(payload).encode("utf-8")
            response.json.return_value = payload
            responses.append(response)
        session.post.side_effect = responses
        client = WbOrderFeedClient(token="test-token", session=session, min_request_interval_sec=0)

        pages = list(
            iter_order_feed(
                client,
                start=datetime(2026, 8, 1, tzinfo=timezone.utc),
                end=datetime(2026, 8, 7, tzinfo=timezone.utc),
                limit=2,
            )
        )

        self.assertEqual([len(page[0]) for page in pages], [2, 1])
        first = session.post.call_args_list[0].kwargs["json"]
        second = session.post.call_args_list[1].kwargs["json"]
        self.assertEqual(first["pagination"], {"offset": 0, "limit": 2})
        self.assertEqual(second["pagination"], {"offset": 2, "limit": 2, "snapshotTime": "2026-08-07T09:00:00Z"})
        self.assertEqual(first["timezone"], "UTC")
        self.assertEqual(first["nmIds"], [])

    def test_permanent_http_error_is_not_retried(self) -> None:
        session = Mock()
        response = Mock()
        response.status_code = 403
        response.content = b'{"title":"forbidden"}'
        response.text = '{"title":"forbidden"}'
        response.json.return_value = {"title": "forbidden"}
        session.post.return_value = response
        client = WbOrderFeedClient(token="test-token", session=session, min_request_interval_sec=0, max_attempts=5)

        _, response_log = client.request({"selectedPeriod": {"start": "2026-08-01T00:00:00Z"}})

        self.assertEqual(response_log.response_status, 403)
        self.assertIsNotNone(response_log.error)
        self.assertEqual(session.post.call_count, 1)


class WbOrderFeedNormalizationTests(unittest.TestCase):
    def test_normalizes_all_documented_order_fields(self) -> None:
        source = {
            "srid": "7513432034713632943.1.0",
            "nmId": 47254354,
            "chrtId": 91663228,
            "createdAt": "2026-08-01T10:00:00+03:00",
            "updatedAt": "2026-08-01T12:00:00+03:00",
            "status": "cancel",
            "cancelType": "app",
            "warehouseName": "Электросталь",
            "warehouseRegion": "Центральный",
            "isMp": False,
            "destinationCity": "Минск",
            "destinationDistrict": "Беларусь",
            "sellerPrice": "1234.50",
            "isB2b": True,
        }
        result = normalize_wb_order_feed_order(source, currency="RUB")
        self.assertEqual(result["srid"], source["srid"])
        self.assertEqual(result["nm_id"], 47254354)
        self.assertEqual(result["chrt_id"], 91663228)
        self.assertEqual(result["created_at"].isoformat(), "2026-08-01T07:00:00+00:00")
        self.assertEqual(result["status"], "cancel")
        self.assertEqual(result["cancel_type"], "app")
        self.assertFalse(result["is_mp"])
        self.assertTrue(result["is_b2b"])
        self.assertEqual(result["seller_price"], 1234.5)
        self.assertEqual(result["currency"], "RUB")
        self.assertEqual(result["payload"], source)


class WbOrderFeedPeriodTests(unittest.TestCase):
    def test_default_period_is_rolling_31_days(self) -> None:
        until = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
        since, result_until = _period(until, days=31, since_override=None, until_override=None)
        self.assertEqual(result_until, until)
        self.assertEqual(since, until - timedelta(days=31))

    def test_period_rejects_more_than_31_days(self) -> None:
        until = datetime(2026, 8, 7, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            _period(until, days=31, since_override=until - timedelta(days=32), until_override=until)


class WbOrderFeedDedupeTests(unittest.TestCase):
    def test_keeps_latest_status_for_duplicate_srid(self) -> None:
        older = {"srid": "one", "status": "created", "status_updated_at": datetime(2026, 8, 1, tzinfo=timezone.utc)}
        newer = {"srid": "one", "status": "buyout", "status_updated_at": datetime(2026, 8, 2, tzinfo=timezone.utc)}
        unique, duplicates = _dedupe_by_srid([older, newer, {"srid": "two", "status_updated_at": None}])
        self.assertEqual(duplicates, 1)
        self.assertEqual(len(unique), 2)
        self.assertEqual(next(item for item in unique if item["srid"] == "one")["status"], "buyout")


if __name__ == "__main__":
    unittest.main()
