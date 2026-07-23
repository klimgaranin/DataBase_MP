from __future__ import annotations

import unittest

from tools.health_check import _safe_dsn_summary


class HealthCheckTests(unittest.TestCase):
    def test_safe_dsn_summary_handles_url_without_password(self) -> None:
        self.assertEqual(
            _safe_dsn_summary("postgresql://app:secret@localhost:5432/marketplace"),
            "postgresql://localhost:5432/marketplace",
        )

    def test_safe_dsn_summary_handles_libpq_without_password(self) -> None:
        self.assertEqual(
            _safe_dsn_summary("hostaddr=127.0.0.1 port=5432 dbname=marketplace user=app password='secret'"),
            "libpq://127.0.0.1:5432/marketplace",
        )

    def test_safe_dsn_summary_tolerates_unbalanced_password_quote(self) -> None:
        summary = _safe_dsn_summary("hostaddr=127.0.0.1 dbname=marketplace password='secret")
        self.assertEqual(summary, "libpq://127.0.0.1:default/marketplace")


if __name__ == "__main__":
    unittest.main()
