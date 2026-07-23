from __future__ import annotations

import os
import unittest

from app.secrets import EnvSecretBackend, secret_status


class SecretBackendTests(unittest.TestCase):
    def test_env_secret_backend_reads_existing_value(self) -> None:
        os.environ["TEST_SECRET_BACKEND_VALUE"] = "secret-value"
        try:
            backend = EnvSecretBackend()
            self.assertEqual(backend.get("TEST_SECRET_BACKEND_VALUE"), "secret-value")
            self.assertTrue(backend.exists("TEST_SECRET_BACKEND_VALUE"))
        finally:
            os.environ.pop("TEST_SECRET_BACKEND_VALUE", None)

    def test_secret_status_never_returns_values(self) -> None:
        os.environ["TEST_SECRET_STATUS_VALUE"] = "secret-value"
        try:
            self.assertEqual(secret_status(["TEST_SECRET_STATUS_VALUE"]), {"TEST_SECRET_STATUS_VALUE": True})
        finally:
            os.environ.pop("TEST_SECRET_STATUS_VALUE", None)


if __name__ == "__main__":
    unittest.main()
