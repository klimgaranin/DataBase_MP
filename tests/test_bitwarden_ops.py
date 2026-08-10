from __future__ import annotations

import unittest

from app.ops.bitwarden import extract_item_secret


class BitwardenOpsTests(unittest.TestCase):
    def test_extract_item_secret_reads_project_item_password(self) -> None:
        secret = extract_item_secret(
            {
                "name": "DataBase_MP / OZON_API_KEY",
                "login": {"password": "secret-value"},
            }
        )

        self.assertIsNotNone(secret)
        self.assertEqual(secret.name, "OZON_API_KEY")
        self.assertEqual(secret.value, "secret-value")

    def test_extract_item_secret_skips_non_project_items(self) -> None:
        self.assertIsNone(
            extract_item_secret(
                {
                    "name": "Other / OZON_API_KEY",
                    "login": {"password": "secret-value"},
                }
            )
        )

    def test_extract_item_secret_skips_empty_password(self) -> None:
        self.assertIsNone(
            extract_item_secret(
                {
                    "name": "DataBase_MP / OZON_API_KEY",
                    "login": {"password": ""},
                }
            )
        )

    def test_extract_item_secret_reads_secret_named_custom_field(self) -> None:
        secret = extract_item_secret(
            {
                "name": "DataBase_MP / API_ERP_TRU_TOKEN",
                "login": {"password": ""},
                "fields": [{"name": "API_ERP_TRU_TOKEN", "value": "secret-value"}],
            }
        )

        self.assertIsNotNone(secret)
        self.assertEqual(secret.name, "API_ERP_TRU_TOKEN")
        self.assertEqual(secret.value, "secret-value")

    def test_extract_item_secret_reads_single_line_notes(self) -> None:
        secret = extract_item_secret(
            {
                "name": "DataBase_MP / API_ERP_TRU_TOKEN",
                "login": {"password": ""},
                "notes": "secret-value",
            }
        )

        self.assertIsNotNone(secret)
        self.assertEqual(secret.name, "API_ERP_TRU_TOKEN")
        self.assertEqual(secret.value, "secret-value")


if __name__ == "__main__":
    unittest.main()
