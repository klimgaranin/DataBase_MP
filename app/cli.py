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

    secrets_parser = subparsers.add_parser("secrets", help="управление секретами keyring")
    secrets_subparsers = secrets_parser.add_subparsers(dest="secrets_command", required=True)

    secrets_status = secrets_subparsers.add_parser("status", help="показать, какие секреты заданы")
    secrets_status.add_argument("names", nargs="*")
    secrets_status.add_argument("--backend", choices=("active", "keyring"), default="active")

    secrets_set = secrets_subparsers.add_parser("set", help="сохранить секрет в keyring")
    secrets_set.add_argument("name")
    secrets_set.add_argument("--value")

    secrets_delete = secrets_subparsers.add_parser("delete", help="удалить секрет из keyring")
    secrets_delete.add_argument("name")

    secrets_migrate = secrets_subparsers.add_parser("migrate-from-env", help="перенести секреты из env в keyring")
    secrets_migrate.add_argument("names", nargs="*")
    secrets_migrate.add_argument("--overwrite", action="store_true")

    secrets_pull_bw = secrets_subparsers.add_parser("pull-from-bitwarden", help="подтянуть секреты из Bitwarden в keyring")
    secrets_pull_bw.add_argument("names", nargs="*")
    secrets_pull_bw.add_argument("--folder", default="DataBase_MP")
    secrets_pull_bw.add_argument("--no-overwrite", action="store_true")

    secrets_pg = secrets_subparsers.add_parser("normalize-postgres", help="разделить PG_DSN и POSTGRES_PASSWORD")
    secrets_pg.add_argument("--overwrite-password", action="store_true")

    secrets_clean_env = secrets_subparsers.add_parser("clean-env", help="очистить .env от секретов")
    secrets_clean_env.add_argument("--path", default=".env")
    secrets_clean_env.add_argument("--backend", choices=("env", "keyring"), default="keyring")

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

    if args.command == "secrets":
        from app.ops.secrets import (
            clean_env_file,
            delete_secret,
            migrate_from_env,
            normalize_postgres_secrets,
            print_secrets_status,
            pull_from_bitwarden,
            set_secret,
        )

        if args.secrets_command == "status":
            return print_secrets_status(args.names, backend=args.backend)
        if args.secrets_command == "set":
            return set_secret(args.name, args.value)
        if args.secrets_command == "delete":
            return delete_secret(args.name)
        if args.secrets_command == "migrate-from-env":
            return migrate_from_env(args.names, overwrite=args.overwrite)
        if args.secrets_command == "pull-from-bitwarden":
            return pull_from_bitwarden(args.names, folder=args.folder, overwrite=not args.no_overwrite)
        if args.secrets_command == "normalize-postgres":
            return normalize_postgres_secrets(overwrite_password=args.overwrite_password)
        if args.secrets_command == "clean-env":
            return clean_env_file(path=args.path, backend=args.backend)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
