from __future__ import annotations

import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.secrets import EnvSecretBackend, KeyringSecretBackend, secret_status


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

    def test_keyring_backend_prefers_keyring_value(self) -> None:
        fake_keyring = SimpleNamespace(
            get_password=lambda service, name: "from-keyring" if (service, name) == ("DataBase_MP", "TEST_SECRET") else None,
            set_password=lambda service, name, value: None,
            delete_password=lambda service, name: None,
        )
        with patch.dict(sys.modules, {"keyring": fake_keyring}):
            backend = KeyringSecretBackend(service_name="DataBase_MP", fallback=EnvSecretBackend())
            self.assertEqual(backend.get("TEST_SECRET"), "from-keyring")

    def test_keyring_backend_falls_back_to_env(self) -> None:
        os.environ["TEST_SECRET_FALLBACK"] = "from-env"
        fake_keyring = SimpleNamespace(
            get_password=lambda service, name: None,
            set_password=lambda service, name, value: None,
            delete_password=lambda service, name: None,
        )
        try:
            with patch.dict(sys.modules, {"keyring": fake_keyring}):
                backend = KeyringSecretBackend(service_name="DataBase_MP", fallback=EnvSecretBackend())
                self.assertEqual(backend.get("TEST_SECRET_FALLBACK"), "from-env")
        finally:
            os.environ.pop("TEST_SECRET_FALLBACK", None)


if __name__ == "__main__":
    unittest.main()
