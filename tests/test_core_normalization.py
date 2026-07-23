from __future__ import annotations

import unittest

from app.normalize.norm_wb_orders import (
    normalize_wb_order,
    parse_bool,
    parse_date,
    parse_float_ru,
    parse_int,
)
from app.normalize.norm_wb_stocks import normalize_wb_stock


class WbOrdersNormalizationTests(unittest.TestCase):
    def test_parse_helpers_handle_common_wb_values(self) -> None:
        self.assertTrue(parse_bool("true"))
        self.assertFalse(parse_bool("0"))
        self.assertIsNone(parse_bool("maybe"))
        self.assertEqual(parse_int("123"), 123)
        self.assertIsNone(parse_int(""))
        self.assertEqual(parse_float_ru("1 234,56"), 1234.56)
        self.assertEqual(parse_date("2026-07-21T12:34:56+03:00").isoformat(), "2026-07-21")

    def test_normalize_wb_order_maps_expected_fields(self) -> None:
        row = {
            "srid": "order-1",
            "isCancel": "false",
            "date": "2026-07-21T10:00:00+03:00",
            "lastChangeDate": "2026-07-21T10:05:00+03:00",
            "warehouseName": "Коледино",
            "supplierArticle": "ART-1",
            "nmId": "123456",
            "barcode": "4600000000000",
            "totalPrice": "1 234,50",
            "discountPercent": "10",
            "priceWithDisc": "1111,05",
            "cancelDate": "2026-07-22",
            "gNumber": "G123",
        }

        normalized = normalize_wb_order(row)

        self.assertEqual(normalized["srid"], "order-1")
        self.assertFalse(normalized["is_cancel"])
        self.assertEqual(normalized["warehouse_name"], "Коледино")
        self.assertEqual(normalized["supplier_article"], "ART-1")
        self.assertEqual(normalized["nm_id"], 123456)
        self.assertEqual(normalized["total_price"], 1234.5)
        self.assertEqual(normalized["discount_percent"], 10)
        self.assertEqual(normalized["price_with_disc"], 1111.05)
        self.assertEqual(normalized["cancel_date"].isoformat(), "2026-07-22")


class WbStocksNormalizationTests(unittest.TestCase):
    def test_normalize_wb_stock_maps_expected_fields(self) -> None:
        row = {
            "nmId": "168149837",
            "chrtId": "279566746",
            "warehouseId": "120762",
            "warehouseName": "Электросталь",
            "regionName": "Центральный",
            "quantity": "39",
            "inWayToClient": "2",
            "inWayFromClient": None,
        }

        normalized = normalize_wb_stock(row)

        self.assertEqual(
            normalized,
            {
                "nm_id": 168149837,
                "chrt_id": 279566746,
                "warehouse_id": 120762,
                "warehouse_name": "Электросталь",
                "region_name": "Центральный",
                "quantity": 39,
                "in_way_to_client": 2,
                "in_way_from_client": None,
            },
        )


if __name__ == "__main__":
    unittest.main()
