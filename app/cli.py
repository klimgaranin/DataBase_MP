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

    subparsers.add_parser("audit", help="проверить целостность проекта")

    checks_parser = subparsers.add_parser("checks", help="запустить проектные проверки")
    checks_parser.add_argument(
        "names",
        nargs="*",
        help="какие проверки запустить; по умолчанию все",
    )

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

    bitwarden_parser = subparsers.add_parser("bitwarden", help="операции Bitwarden")
    bitwarden_subparsers = bitwarden_parser.add_subparsers(dest="bitwarden_command", required=True)
    bw_push = bitwarden_subparsers.add_parser("push-from-keyring", help="создать/обновить записи Bitwarden из keyring")
    bw_push.add_argument("names", nargs="*")
    bw_push.add_argument("--folder", default="DataBase_MP")
    bw_push.add_argument("--dry-run", action="store_true")

    ozon_parser = subparsers.add_parser("ozon", help="ручные Ozon операции")
    ozon_subparsers = ozon_parser.add_subparsers(dest="ozon_command", required=True)
    ozon_placement = ozon_subparsers.add_parser("placement-report", help="ручные отчёты Ozon placement")
    ozon_placement_subparsers = ozon_placement.add_subparsers(dest="ozon_placement_command", required=True)
    ozon_placement_supplies = ozon_placement_subparsers.add_parser(
        "by-supplies",
        help="скачать тестовый отчёт стоимости размещения по поставкам",
    )
    ozon_placement_supplies.add_argument("--date-from")
    ozon_placement_supplies.add_argument("--date-to")
    ozon_placement_supplies.add_argument("--output-dir")
    ozon_placement_supplies.add_argument("--poll-attempts", type=int)
    ozon_placement_supplies.add_argument("--poll-sleep-seconds", type=int)

    sheets_parser = subparsers.add_parser("sheets", help="экспорт данных в Google Sheets")
    sheets_subparsers = sheets_parser.add_subparsers(dest="sheets_command", required=True)
    sheets_ozon_orders = sheets_subparsers.add_parser(
        "ozon-orders",
        help="выгрузить Ozon FBO заказы в DATA",
    )
    sheets_ozon_orders.add_argument("--spreadsheet-id")
    sheets_ozon_orders.add_argument("--sheet-name", default="DATA")
    sheets_ozon_orders.add_argument("--start-cell", default="A1")
    sheets_ozon_orders.add_argument("--date-from", help="YYYY-MM-DD")
    sheets_ozon_orders.add_argument("--date-to", help="YYYY-MM-DD")
    sheets_ozon_orders.add_argument("--limit", type=int)
    sheets_ozon_orders.add_argument("--mode", choices=("upsert", "replace"), default="upsert")
    sheets_ozon_orders.add_argument("--dry-run", action="store_true")

    sheets_wb_orders = sheets_subparsers.add_parser(
        "wb-orders",
        help="выгрузить WB заказы в DATA",
    )
    sheets_wb_orders.add_argument("--spreadsheet-id")
    sheets_wb_orders.add_argument("--sheet-name", default="DATA")
    sheets_wb_orders.add_argument("--start-cell", default="F1")
    sheets_wb_orders.add_argument("--date-from", help="YYYY-MM-DD")
    sheets_wb_orders.add_argument("--date-to", help="YYYY-MM-DD")
    sheets_wb_orders.add_argument("--limit", type=int)
    sheets_wb_orders.add_argument("--mode", choices=("upsert", "replace"), default="upsert")
    sheets_wb_orders.add_argument("--dry-run", action="store_true")

    sheets_ozon_placement = sheets_subparsers.add_parser(
        "ozon-placement",
        help="выгрузить Ozon платное хранение в DATA",
    )
    sheets_ozon_placement.add_argument("--spreadsheet-id")
    sheets_ozon_placement.add_argument("--sheet-name", default="DATA")
    sheets_ozon_placement.add_argument("--start-cell", default="K1")
    sheets_ozon_placement.add_argument("--limit", type=int)
    sheets_ozon_placement.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_ozon_placement.add_argument("--dry-run", action="store_true")

    sheets_api_erp_tru_sales = sheets_subparsers.add_parser(
        "api-erp-tru-sales",
        help="выгрузить ERP/TRU продажи в DATA",
    )
    sheets_api_erp_tru_sales.add_argument("--spreadsheet-id")
    sheets_api_erp_tru_sales.add_argument("--sheet-name", default="DATA")
    sheets_api_erp_tru_sales.add_argument("--start-cell", default="AE1")
    sheets_api_erp_tru_sales.add_argument("--limit", type=int)
    sheets_api_erp_tru_sales.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_api_erp_tru_sales.add_argument("--dry-run", action="store_true")

    sheets_source_inventory = sheets_subparsers.add_parser(
        "source-production-inventory",
        help="выгрузить внутренние остатки МП в DATA",
    )
    sheets_source_inventory.add_argument("--spreadsheet-id")
    sheets_source_inventory.add_argument("--sheet-name", default="DATA")
    sheets_source_inventory.add_argument("--start-cell", default="Q1")
    sheets_source_inventory.add_argument("--limit", type=int)
    sheets_source_inventory.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_inventory.add_argument("--dry-run", action="store_true")

    sheets_source_pipeline = sheets_subparsers.add_parser(
        "source-supply-pipeline",
        help="выгрузить список заказов в DATA",
    )
    sheets_source_pipeline.add_argument("--spreadsheet-id")
    sheets_source_pipeline.add_argument("--sheet-name", default="DATA")
    sheets_source_pipeline.add_argument("--start-cell", default="X1")
    sheets_source_pipeline.add_argument("--limit", type=int)
    sheets_source_pipeline.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_pipeline.add_argument("--dry-run", action="store_true")

    sheets_source_specs = sheets_subparsers.add_parser(
        "source-supply-order-specs",
        help="выгрузить LOT и даты производства в DATA 2",
    )
    sheets_source_specs.add_argument("--spreadsheet-id")
    sheets_source_specs.add_argument("--sheet-name", default="DATA 2")
    sheets_source_specs.add_argument("--start-cell", default="H1")
    sheets_source_specs.add_argument("--limit", type=int)
    sheets_source_specs.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_specs.add_argument("--dry-run", action="store_true")

    sheets_source_cost_ozon = sheets_subparsers.add_parser(
        "source-cost-ozon",
        help="выгрузить себестоимость Ozon из 1С в DATA",
    )
    sheets_source_cost_ozon.add_argument("--spreadsheet-id")
    sheets_source_cost_ozon.add_argument("--sheet-name", default="DATA")
    sheets_source_cost_ozon.add_argument("--start-cell", default="AX1")
    sheets_source_cost_ozon.add_argument("--limit", type=int)
    sheets_source_cost_ozon.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_cost_ozon.add_argument("--dry-run", action="store_true")

    sheets_source_cost_wb = sheets_subparsers.add_parser(
        "source-cost-wb",
        help="выгрузить себестоимость WB из 1С в DATA",
    )
    sheets_source_cost_wb.add_argument("--spreadsheet-id")
    sheets_source_cost_wb.add_argument("--sheet-name", default="DATA")
    sheets_source_cost_wb.add_argument("--start-cell", default="BB1")
    sheets_source_cost_wb.add_argument("--limit", type=int)
    sheets_source_cost_wb.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_cost_wb.add_argument("--dry-run", action="store_true")

    sheets_source_cost_general = sheets_subparsers.add_parser(
        "source-cost-general",
        help="выгрузить общую себестоимость 1С в DATA",
    )
    sheets_source_cost_general.add_argument("--spreadsheet-id")
    sheets_source_cost_general.add_argument("--sheet-name", default="DATA")
    sheets_source_cost_general.add_argument("--start-cell", default="BK1")
    sheets_source_cost_general.add_argument("--limit", type=int)
    sheets_source_cost_general.add_argument("--mode", choices=("upsert", "replace"), default="replace")
    sheets_source_cost_general.add_argument("--dry-run", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
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

        if args.command == "audit":
            from app.ops.tool_checks import run_project_audit

            return run_project_audit()

        if args.command == "checks":
            from app.ops.tool_checks import run_checks

            return run_checks(args.names)

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

        if args.command == "bitwarden":
            from tools.sync_bitwarden_from_keyring import main as sync_bitwarden_main

            forwarded = []
            if args.folder:
                forwarded.extend(["--folder", args.folder])
            if args.dry_run:
                forwarded.append("--dry-run")
            forwarded.extend(args.names)
            if args.bitwarden_command == "push-from-keyring":
                return sync_bitwarden_main(forwarded)

        if args.command == "ozon":
            from app.ops.ozon_placement_reports import main as ozon_placement_reports_main

            if args.ozon_command == "placement-report":
                forwarded = [args.ozon_placement_command]
                for attr, option in [
                    ("date_from", "--date-from"),
                    ("date_to", "--date-to"),
                    ("output_dir", "--output-dir"),
                    ("poll_attempts", "--poll-attempts"),
                    ("poll_sleep_seconds", "--poll-sleep-seconds"),
                ]:
                    value = getattr(args, attr, None)
                    if value is not None:
                        forwarded.extend([option, str(value)])
                return ozon_placement_reports_main(forwarded)

        if args.command == "sheets":
            from app.ops.sheets_export import main as sheets_export_main

            forwarded = [args.sheets_command]
            for attr, option in [
                ("spreadsheet_id", "--spreadsheet-id"),
                ("sheet_name", "--sheet-name"),
                ("start_cell", "--start-cell"),
                ("date_from", "--date-from"),
                ("date_to", "--date-to"),
            ]:
                value = getattr(args, attr, None)
                if value:
                    forwarded.extend([option, value])
            if args.limit is not None:
                forwarded.extend(["--limit", str(args.limit)])
            if args.mode:
                forwarded.extend(["--mode", args.mode])
            if args.dry_run:
                forwarded.append("--dry-run")
            return sheets_export_main(forwarded)
    except RuntimeError as exc:
        print(f"Ошибка: {exc}")
        return 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
