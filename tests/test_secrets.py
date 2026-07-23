from __future__ import annotations

import os
import sys
import tempfile
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.ops.secrets import clean_env_file, pull_from_bitwarden
from app.secrets import EnvSecretBackend, KeyringSecretBackend, get_secret, secret_status, split_pg_dsn_password


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

    def test_wb_token_aliases_fall_back_to_common_token(self) -> None:
        os.environ["WB_TOKEN"] = "common-wb-token"
        old_backend = os.environ.get("APP_SECRET_BACKEND")
        os.environ["APP_SECRET_BACKEND"] = "env"
        os.environ.pop("WB_TOKEN_CONTENT", None)
        try:
            self.assertEqual(get_secret("WB_TOKEN_CONTENT"), "common-wb-token")
            self.assertEqual(secret_status(["WB_TOKEN_CONTENT"]), {"WB_TOKEN_CONTENT": True})
        finally:
            os.environ.pop("WB_TOKEN", None)
            if old_backend is None:
                os.environ.pop("APP_SECRET_BACKEND", None)
            else:
                os.environ["APP_SECRET_BACKEND"] = old_backend

    def test_split_pg_dsn_password_for_url_dsn(self) -> None:
        sanitized, password = split_pg_dsn_password("postgresql://app:secret@localhost:5432/marketplace")
        self.assertEqual(sanitized, "postgresql://app@localhost:5432/marketplace")
        self.assertEqual(password, "secret")

    def test_split_pg_dsn_password_for_libpq_dsn(self) -> None:
        sanitized, password = split_pg_dsn_password("hostaddr=127.0.0.1 port=5432 dbname=marketplace user=app password='secret'")
        self.assertEqual(sanitized, "hostaddr=127.0.0.1 port=5432 dbname=marketplace user=app")
        self.assertEqual(password, "secret")

    def test_clean_env_file_removes_secret_values_and_keeps_sanitized_pg_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env"
            path.write_text(
                "\n".join(
                    [
                        "APP_SECRET_BACKEND=env",
                        "PG_DSN=postgresql://app:secret@localhost:5432/marketplace",
                        "POSTGRES_PASSWORD=secret",
                        "WB_TOKEN=secret",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with redirect_stdout(StringIO()):
                self.assertEqual(clean_env_file(path=str(path), backend="env"), 0)
            text = path.read_text(encoding="utf-8")
            self.assertIn("APP_SECRET_BACKEND=env", text)
            self.assertIn("PG_DSN=postgresql://app@localhost:5432/marketplace", text)
            self.assertIn("# POSTGRES_PASSWORD хранится в secret backend", text)
            self.assertIn("# WB_TOKEN хранится в secret backend", text)
            self.assertNotIn("=secret", text)

    def test_pull_from_bitwarden_updates_keyring_without_printing_values(self) -> None:
        saved: dict[str, str] = {}
        fake_backend = SimpleNamespace(
            get=lambda name: saved.get(name),
            exists=lambda name: name in saved,
            set=lambda name, value: saved.__setitem__(name, value),
            delete=lambda name: False,
        )

        with (
            patch("app.ops.secrets.get_keyring_backend", return_value=fake_backend),
            patch("app.ops.secrets.keyring_secret_status", return_value={"OZON_API_KEY": False}),
            patch("app.ops.bitwarden.load_bitwarden_secrets", return_value={"OZON_API_KEY": "secret-value"}),
            redirect_stdout(StringIO()) as output,
        ):
            self.assertEqual(pull_from_bitwarden(["OZON_API_KEY"]), 0)

        self.assertEqual(saved, {"OZON_API_KEY": "secret-value"})
        self.assertNotIn("secret-value", output.getvalue())


if __name__ == "__main__":
    unittest.main()
