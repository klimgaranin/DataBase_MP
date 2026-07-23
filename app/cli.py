from __future__ import annotations

import argparse
from typing import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataBase_MP operational CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    health_parser = subparsers.add_parser("health", help="проверить окружение, зависимости и БД")
    health_parser.add_argument("--log-lines", type=int, default=5, help="сколько последних строк логов показать")
    health_parser.add_argument("--skip-db", action="store_true", help="не подключаться к PostgreSQL")

    migrate_parser = subparsers.add_parser("migrate", help="применить SQL-миграции")
    migrate_parser.add_argument("--from-version", type=int, default=1)
    migrate_parser.add_argument("--to-version", type=int)

    jobs_parser = subparsers.add_parser("jobs-status", help="показать последние запуски jobs")
    jobs_parser.add_argument("--limit", type=int, default=10, help="сколько последних запусков показать")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "health":
        from app.ops.health import main as health_main

        forwarded = ["--log-lines", str(args.log_lines)]
        if args.skip_db:
            forwarded.append("--skip-db")
        return health_main(forwarded)

    if args.command == "migrate":
        from app.ops.migrations import apply_migrations

        return apply_migrations(from_version=args.from_version, to_version=args.to_version)

    if args.command == "jobs-status":
        from app.ops.jobs_status import print_jobs_status

        return print_jobs_status(limit=max(1, args.limit))

    return 2


if __name__ == "__main__":
    raise SystemExit(main())

