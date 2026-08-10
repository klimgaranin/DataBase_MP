from __future__ import annotations

import json
import unittest
from datetime import date
from unittest.mock import Mock

from app.clients.http_api_erp_tru import ApiErpTruClient, build_product_stats_params
from app.jobs.job_api_erp_tru_product_stats import previous_month_same_day
from app.normalize.norm_api_erp_tru import normalize_product_stat_row
from app.ops.sheets_export import ApiErpTruSalesSheetRow, build_api_erp_tru_sales_sheet_values


class ApiErpTruClientTests(unittest.TestCase):
    def test_build_product_stats_params_uses_full_days(self) -> None:
        params = build_product_stats_params(date_from=date(2026, 7, 10), date_to=date(2026, 8, 10))

        self.assertEqual(params["rel_products_in_order_for_product__order__delivery_DT_from"], "2026-07-10 00:00")
        self.assertEqual(params["rel_products_in_order_for_product__order__delivery_DT_to"], "2026-08-10 23:59")
        self.assertEqual(params["wo_sets"], "false")

    def test_client_reads_list_response(self) -> None:
        session = Mock()
        payload = [{"id": 1, "article": "21045", "sales_count": 2}]
        response = Mock()
        response.status_code = 200
        response.content = json.dumps(payload).encode("utf-8")
        response.json.return_value = payload
        session.get.return_value = response
        client = ApiErpTruClient(token="test-token", session=session)

        rows, log = client.request_product_stats(date_from=date(2026, 8, 9), date_to=date(2026, 8, 10))

        self.assertEqual(rows, payload)
        self.assertEqual(log.response_status, 200)
        headers = session.get.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer test-token")


class ApiErpTruPeriodTests(unittest.TestCase):
    def test_previous_month_same_day(self) -> None:
        self.assertEqual(previous_month_same_day(date(2026, 8, 10)), date(2026, 7, 10))

    def test_previous_month_same_day_clamps_month_end(self) -> None:
        self.assertEqual(previous_month_same_day(date(2026, 3, 31)), date(2026, 2, 28))


class ApiErpTruNormalizeTests(unittest.TestCase):
    def test_normalizes_documented_sample_fields(self) -> None:
        row = {
            "id": 1,
            "article": " 21045 ",
            "name_1s": "Товар",
            "sales_count": "2",
            "sales_sum": "2700.50",
            "avg_price": "1350,25",
            "warehouse_count": None,
        }

        result = normalize_product_stat_row(row)

        self.assertIsNotNone(result)
        self.assertEqual(result["external_id"], 1)
        self.assertEqual(result["article"], "21045")
        self.assertEqual(result["sales_count"], 2)
        self.assertEqual(result["sales_sum"], 2700.50)
        self.assertEqual(result["avg_price"], 1350.25)
        self.assertEqual(result["warehouse_count"], 0)
        self.assertEqual(result["payload"], row)


class ApiErpTruSheetsTests(unittest.TestCase):
    def test_build_sales_sheet_values(self) -> None:
        values = build_api_erp_tru_sales_sheet_values(
            [ApiErpTruSalesSheetRow(article="21045", sales_count=2)]
        )

        self.assertEqual(values, [["Артикул", "Кол-во"], ["21045", 2]])


if __name__ == "__main__":
    unittest.main()
