from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from app.cli import build_parser, main
from app.ops.migrations import migration_version, selected_migrations


class OpsCliTests(unittest.TestCase):
    def test_cli_parser_accepts_health_command(self) -> None:
        args = build_parser().parse_args(["health", "--skip-db", "--log-lines", "0"])
        self.assertEqual(args.command, "health")
        self.assertTrue(args.skip_db)
        self.assertEqual(args.log_lines, 0)

    def test_cli_parser_accepts_audit_command(self) -> None:
        args = build_parser().parse_args(["audit"])
        self.assertEqual(args.command, "audit")

    def test_cli_parser_accepts_checks_command(self) -> None:
        args = build_parser().parse_args(["checks", "audit"])
        self.assertEqual(args.command, "checks")
        self.assertEqual(args.names, ["audit"])

    def test_cli_parser_accepts_bitwarden_push_command(self) -> None:
        args = build_parser().parse_args(["bitwarden", "push-from-keyring", "--dry-run", "WB_TOKEN"])
        self.assertEqual(args.command, "bitwarden")
        self.assertEqual(args.bitwarden_command, "push-from-keyring")
        self.assertTrue(args.dry_run)
        self.assertEqual(args.names, ["WB_TOKEN"])

    def test_health_command_can_skip_db(self) -> None:
        self.assertEqual(main(["health", "--skip-db", "--log-lines", "0"]), 0)

    def test_migration_version_sort_key(self) -> None:
        self.assertEqual(migration_version(selected_migrations(from_version=21, to_version=21)[0]), 21)

    def test_runtime_error_is_printed_without_traceback(self) -> None:
        with (
            patch("app.ops.secrets.pull_from_bitwarden", side_effect=RuntimeError("Bitwarden locked")),
            redirect_stdout(StringIO()) as output,
        ):
            self.assertEqual(main(["secrets", "pull-from-bitwarden", "OZON_API_KEY"]), 1)

        text = output.getvalue()
        self.assertIn("Ошибка: Bitwarden locked", text)
        self.assertNotIn("Traceback", text)


if __name__ == "__main__":
    unittest.main()
