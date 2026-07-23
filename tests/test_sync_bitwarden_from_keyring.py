from __future__ import annotations

import unittest

from tools.sync_bitwarden_from_keyring import SecretEntry, build_item


class BitwardenSyncTests(unittest.TestCase):
    def test_build_item_keeps_secret_only_in_password_field(self) -> None:
        item = build_item(SecretEntry(name="OZON_API_KEY", value="secret-value"), "folder-id")

        self.assertEqual(item["name"], "DataBase_MP / OZON_API_KEY")
        self.assertEqual(item["folderId"], "folder-id")
        self.assertEqual(item["login"]["username"], "DataBase_MP")
        self.assertEqual(item["login"]["password"], "secret-value")
        self.assertNotIn("secret-value", item["notes"])


if __name__ == "__main__":
    unittest.main()
