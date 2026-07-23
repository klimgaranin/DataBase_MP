from __future__ import annotations

import unittest

from app.integrations.api_catalog import API_METHODS, methods_by_marketplace, missing_specs


class ApiCatalogTests(unittest.TestCase):
    def test_catalog_has_wb_and_ozon_methods(self) -> None:
        self.assertTrue(methods_by_marketplace("wb"))
        self.assertTrue(methods_by_marketplace("ozon"))

    def test_method_names_are_unique(self) -> None:
        names = [method.name for method in API_METHODS]
        self.assertEqual(len(names), len(set(names)))

    def test_referenced_local_specs_exist(self) -> None:
        self.assertEqual(missing_specs(), [])


if __name__ == "__main__":
    unittest.main()
