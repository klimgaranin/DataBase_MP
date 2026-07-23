from __future__ import annotations

import unittest

from app.cli import build_parser, main
from app.ops.migrations import migration_version, selected_migrations


class OpsCliTests(unittest.TestCase):
    def test_cli_parser_accepts_health_command(self) -> None:
        args = build_parser().parse_args(["health", "--skip-db", "--log-lines", "0"])
        self.assertEqual(args.command, "health")
        self.assertTrue(args.skip_db)
        self.assertEqual(args.log_lines, 0)

    def test_health_command_can_skip_db(self) -> None:
        self.assertEqual(main(["health", "--skip-db", "--log-lines", "0"]), 0)

    def test_migration_version_sort_key(self) -> None:
        self.assertEqual(migration_version(selected_migrations(from_version=21, to_version=21)[0]), 21)


if __name__ == "__main__":
    unittest.main()

