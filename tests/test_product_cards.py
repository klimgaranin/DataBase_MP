from __future__ import annotations

import unittest

from app.clients.http_wb_content import WbContentClient, iter_cards_list
from app.normalize.norm_product_cards import normalize_ozon_product_card, normalize_wb_content_card


class ProductCardsTests(unittest.TestCase):
    def test_normalize_wb_content_card_uses_big_photo_first(self) -> None:
        row = {
            "nmID": 123,
            "imtID": 456,
            "vendorCode": "A-1",
            "subjectID": 10,
            "subjectName": "Ленты",
            "brand": "DzivaTek",
            "title": "Светодиодная лента",
            "photos": [
                {"big": "https://img.test/big.webp", "c516x688": "https://img.test/516.webp"},
                {"big": "https://img.test/big-2.webp"},
            ],
            "sizes": [{"techSize": "0"}],
        }

        result = normalize_wb_content_card(row)

        assert result is not None
        self.assertEqual(result["article"], "A-1")
        self.assertEqual(result["marketplace_sku"], 123)
        self.assertEqual(result["primary_image"], "https://img.test/big.webp")
        self.assertEqual(result["images_count"], 2)
        self.assertEqual(result["photos_count"], 2)
        self.assertEqual(result["sizes_count"], 1)

    def test_normalize_ozon_product_card_uses_primary_image(self) -> None:
        row = {
            "id": 1,
            "offer_id": "OZ-1",
            "sku": 987,
            "name": "Лампа",
            "primary_image": "https://img.test/main.jpg",
            "images": ["https://img.test/main.jpg", "https://img.test/second.jpg"],
        }

        result = normalize_ozon_product_card(row)

        assert result is not None
        self.assertEqual(result["marketplace"], "ozon")
        self.assertEqual(result["article"], "OZ-1")
        self.assertEqual(result["product_id"], "1")
        self.assertEqual(result["primary_image"], "https://img.test/main.jpg")
        self.assertEqual(result["images_count"], 2)

    def test_wb_content_iter_cards_uses_cursor(self) -> None:
        class Session:
            calls: list[dict] = []

            def post(self, _url, headers, json, timeout):
                self.calls.append(json)

                class Response:
                    status_code = 200
                    content = b"{}"

                    def __init__(self, payload):
                        self._payload = payload

                    def json(self):
                        return self._payload

                if len(self.calls) == 1:
                    return Response({"cards": [{"nmID": 1}], "cursor": {"total": 1, "nmID": 1, "updatedAt": "2026-08-13T00:00:00Z"}})
                return Response({"cards": [], "cursor": {"total": 0}})

        session = Session()
        client = WbContentClient(token="token", session=session, min_request_interval_sec=0)
        pages = list(iter_cards_list(client, limit=1, max_pages=2))

        self.assertEqual(len(pages), 2)
        self.assertEqual(session.calls[0]["settings"]["cursor"], {"limit": 1})
        self.assertEqual(
            session.calls[1]["settings"]["cursor"],
            {"limit": 1, "nmID": 1, "updatedAt": "2026-08-13T00:00:00Z"},
        )


if __name__ == "__main__":
    unittest.main()
