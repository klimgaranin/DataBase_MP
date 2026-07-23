from __future__ import annotations

import json
import os
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from unittest.mock import Mock

import psycopg2

import app.db as db
from app.clients.http_ozon_seller import OzonSellerClient, iter_fbo_postings
from app.jobs.job_ozon_orders import (
    _apply_lookback,
    _first_run_start,
    _load_job_config,
    _max_cursor_from_postings,
    _resolve_log_file,
)
from app.normalize.norm_ozon_orders import (
    normalize_ozon_fbo_order_items_full,
    normalize_ozon_fbo_posting,
)


class OzonOrdersPeriodTests(unittest.TestCase):
    def test_first_run_start_is_year_start(self) -> None:
        self.assertEqual(_first_run_start(date(2026, 7, 21)).isoformat(), "2026-01-01T00:00:00+00:00")

    def test_first_run_start_allows_env_override(self) -> None:
        old = os.environ.get("OZON_ORDERS_FIRST_RUN_DATE")
        os.environ["OZON_ORDERS_FIRST_RUN_DATE"] = "2025-12-01"
        try:
            self.assertEqual(_first_run_start(date(2026, 7, 21)).isoformat(), "2025-12-01T00:00:00+00:00")
        finally:
            if old is None:
                os.environ.pop("OZON_ORDERS_FIRST_RUN_DATE", None)
            else:
                os.environ["OZON_ORDERS_FIRST_RUN_DATE"] = old

    def test_cursor_helpers(self) -> None:
        cursor = datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc)
        self.assertEqual(_apply_lookback(cursor, 30).isoformat(), "2026-07-21T09:30:00+00:00")
        best = _max_cursor_from_postings(
            [{"in_process_at": "2026-07-21T11:00:00Z"}, {"shipment_date": "2026-07-21T12:00:00Z"}],
            cursor,
        )
        self.assertEqual(best.isoformat(), "2026-07-21T12:00:00+00:00")

    def test_job_config_matches_wb_debug_sleep_pattern(self) -> None:
        old = os.environ.get("DEBUG_SLEEP_AFTER_LOCK_SECONDS")
        os.environ["DEBUG_SLEEP_AFTER_LOCK_SECONDS"] = "999999"
        try:
            self.assertEqual(_load_job_config()["debug_sleep"], 3600)
        finally:
            if old is None:
                os.environ.pop("DEBUG_SLEEP_AFTER_LOCK_SECONDS", None)
            else:
                os.environ["DEBUG_SLEEP_AFTER_LOCK_SECONDS"] = old

    def test_default_log_file_is_absolute_for_task_scheduler(self) -> None:
        self.assertTrue(os.path.isabs(_resolve_log_file("")))


class OzonOrdersNormalizationTests(unittest.TestCase):
    def test_normalize_posting_and_items(self) -> None:
        row = {
            "posting_number": "123-1",
            "order_id": "777",
            "order_number": "777-A",
            "status": "delivered",
            "substatus": "posting_delivered",
            "created_at": "2026-07-01T09:00:00Z",
            "in_process_at": "2026-07-01T10:00:00Z",
            "shipment_date": "2026-07-02T10:00:00Z",
            "analytics_data": {
                "warehouse_id": "100",
                "warehouse_name": "Склад Ozon",
                "is_legal": True,
                "client_delivery_date_begin": "2026-07-03T10:00:00Z",
                "client_delivery_date_end": "2026-07-04T10:00:00Z",
            },
            "financial_data": {
                "cluster_from": "MSK",
                "cluster_to": "SPB",
                "products": [
                    {
                        "payout": 100,
                        "product_id": 10,
                        "old_price": 200,
                        "price": 150,
                        "total_discount_value": 50,
                        "total_discount_percent": 25,
                        "actions": ["promo"],
                        "commission": {"amount": 15, "percent": 10, "currency": "RUB"},
                    }
                ],
            },
            "legal_info": {"company_name": "ООО Ромашка", "inn": "123", "kpp": "456"},
            "cancellation": {"cancel_reason": "reason", "cancellation_initiator": "Ozon", "cancellation_type": "ozon"},
            "external_order": {"is_external": False, "platform_name": ""},
            "additional_data": [{"key": "value"}],
            "products": [
                {
                    "sku": "10",
                    "offer_id": "ART-1",
                    "name": "Product",
                    "quantity": "2",
                    "price": {"amount": "123,45", "currency": "RUB"},
                    "digital_codes": ["A", "B"],
                    "is_marketplace_buyout": True,
                }
            ],
        }
        posting = normalize_ozon_fbo_posting(row)
        full_items = normalize_ozon_fbo_order_items_full(row)
        self.assertEqual(posting["posting_number"], "123-1")
        self.assertEqual(posting["order_id"], 777)
        self.assertEqual(posting["order_number"], "777-A")
        self.assertEqual(posting["substatus"], "posting_delivered")
        self.assertEqual(posting["analytics_warehouse_id"], 100)
        self.assertEqual(posting["analytics_warehouse_name"], "Склад Ozon")
        self.assertTrue(posting["analytics_is_legal"])
        self.assertEqual(posting["financial_cluster_from"], "MSK")
        self.assertEqual(posting["legal_inn"], "123")
        self.assertEqual(posting["cancel_reason"], "reason")
        self.assertEqual(full_items[0]["posting_number"], "123-1")
        self.assertEqual(full_items[0]["line_number"], 1)
        self.assertEqual(full_items[0]["analytics_client_delivery_date_begin"].isoformat(), "2026-07-03T10:00:00+00:00")
        self.assertFalse(full_items[0]["external_order_is_external"])
        self.assertEqual(full_items[0]["additional_data_count"], 1)
        self.assertEqual(full_items[0]["products_count"], 1)
        self.assertEqual(full_items[0]["financial_products_count"], 1)
        self.assertEqual(full_items[0]["product_offer_id"], "ART-1")
        self.assertEqual(full_items[0]["product_price_amount"], 123.45)
        self.assertEqual(full_items[0]["financial_product_id"], 10)
        self.assertEqual(full_items[0]["financial_old_price"], 200)
        self.assertEqual(full_items[0]["financial_price"], 150)
        self.assertEqual(full_items[0]["financial_actions"], ["promo"])
        self.assertEqual(full_items[0]["financial_commission_amount"], 15)
        self.assertEqual(full_items[0]["financial_commission_currency"], "RUB")


class OzonClientTests(unittest.TestCase):
    def test_iter_fbo_postings_uses_cursor_until_empty(self) -> None:
        session = Mock()
        responses = []
        for payload in [
            {"result": {"postings": [{"posting_number": "1"}], "cursor": "next", "has_next": True}},
            {"result": {"postings": [], "cursor": "", "has_next": False}},
        ]:
            response = Mock()
            response.status_code = 200
            response.content = json.dumps(payload).encode("utf-8")
            response.json.return_value = payload
            responses.append(response)
        session.post.side_effect = responses
        client = OzonSellerClient(client_id="client", api_key="key", session=session)

        pages = list(
            iter_fbo_postings(
                client,
                since=datetime(2026, 1, 1, tzinfo=timezone.utc),
                until=datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc),
            )
        )

        self.assertEqual(len(pages), 2)
        self.assertEqual(session.post.call_count, 2)
        first_payload = session.post.call_args_list[0].kwargs["json"]
        second_payload = session.post.call_args_list[1].kwargs["json"]
        self.assertEqual(first_payload["limit"], 100)
        self.assertEqual(first_payload["sort_dir"], "ASC")
        self.assertFalse(first_payload["translit"])
        self.assertEqual(
            first_payload["filter"]["statuses"],
            ["awaiting_packaging", "awaiting_deliver", "delivering", "delivered", "cancelled"],
        )
        self.assertNotIn("posting_numbers", first_payload["filter"])
        self.assertEqual(second_payload["cursor"], "next")
        self.assertTrue(first_payload["with"]["financial_data"])
        self.assertTrue(first_payload["with"]["legal_info"])

    def test_iter_fbo_postings_accepts_top_level_v3_response(self) -> None:
        session = Mock()
        payload = {"postings": [{"posting_number": "1"}], "cursor": "", "has_next": False}
        response = Mock()
        response.status_code = 200
        response.content = json.dumps(payload).encode("utf-8")
        response.json.return_value = payload
        session.post.return_value = response
        client = OzonSellerClient(client_id="client", api_key="key", session=session)

        pages = list(
            iter_fbo_postings(
                client,
                since=datetime(2026, 1, 1, tzinfo=timezone.utc),
                until=datetime(2026, 7, 21, 10, 0, tzinfo=timezone.utc),
            )
        )

        self.assertEqual(pages[0][0], [{"posting_number": "1"}])


class OzonOrdersDbHistoryTests(unittest.TestCase):
    def test_raw_versions_are_inserted_only_on_payload_change(self) -> None:
        kwargs = db._get_connection_kwargs()
        shared_conn = psycopg2.connect(kwargs) if isinstance(kwargs, str) else psycopg2.connect(**kwargs)
        old_connect = db.connect

        @contextmanager
        def shared_rollback_connect():
            yield shared_conn

        db.connect = shared_rollback_connect
        try:
            now = datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc)
            base = {
                "posting_number": "TEST-HISTORY-1",
                "order_id": 1,
                "status": "awaiting_packaging",
                "substatus": "posting_created",
                "in_process_at": now,
                "shipment_date": None,
                "payload": {
                    "posting_number": "TEST-HISTORY-1",
                    "order_id": 1,
                    "status": "awaiting_packaging",
                    "substatus": "posting_created",
                    "products": [],
                },
            }
            changed = {
                **base,
                "status": "delivered",
                "payload": {**base["payload"], "status": "delivered"},
            }

            self.assertEqual(db.upsert_ozon_fbo_postings([base], run_id="test-run-1"), 1)
            self.assertEqual(db.upsert_ozon_fbo_postings([base], run_id="test-run-2"), 0)
            self.assertEqual(db.upsert_ozon_fbo_postings([changed], run_id="test-run-3"), 1)
        finally:
            db.connect = old_connect
            shared_conn.rollback()
            shared_conn.close()


if __name__ == "__main__":
    unittest.main()
