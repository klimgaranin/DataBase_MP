from __future__ import annotations

import argparse
import getpass
from typing import Sequence

from app.config import get_config, load_project_env
from app.secrets import (
    SENSITIVE_SECRET_NAMES,
    get_keyring_backend,
    keyring_secret_status,
    secret_status,
)


def _selected_names(raw_names: Sequence[str] | None) -> list[str]:
    if raw_names:
        return list(dict.fromkeys(raw_names))
    return list(SENSITIVE_SECRET_NAMES)


def print_secrets_status(names: Sequence[str] | None = None, *, backend: str = "active") -> int:
    selected = _selected_names(names)
    cfg = get_config()
    if backend == "keyring":
        status = keyring_secret_status(selected)
        label = f"keyring service={cfg.secret_service_name}"
    else:
        status = secret_status(selected)
        label = f"active backend={cfg.secret_backend}, service={cfg.secret_service_name}"

    print("Статус секретов")
    print("--------------")
    print(label)
    for name in selected:
        print(f"{name}: {'задан' if status.get(name) else 'не задан'}")
    return 0


def set_secret(name: str, value: str | None = None) -> int:
    if value is None:
        first = getpass.getpass(f"{name}: ")
        second = getpass.getpass(f"{name} ещё раз: ")
        if first != second:
            print("Значения не совпали, секрет не сохранён")
            return 1
        value = first
    value = value.strip()
    if not value:
        print("Пустое значение не сохранено")
        return 1
    get_keyring_backend().set(name, value)
    print(f"{name}: сохранён в keyring")
    return 0


def delete_secret(name: str) -> int:
    deleted = get_keyring_backend().delete(name)
    print(f"{name}: {'удалён из keyring' if deleted else 'не найден в keyring'}")
    return 0


def migrate_from_env(names: Sequence[str] | None = None, *, overwrite: bool = False) -> int:
    load_project_env()
    from app.secrets import EnvSecretBackend

    selected = _selected_names(names)
    env_backend = EnvSecretBackend()
    keyring_backend = get_keyring_backend()
    current = keyring_secret_status(selected)

    migrated = 0
    skipped_existing = 0
    missing = 0
    for name in selected:
        value = env_backend.get(name)
        if not value:
            missing += 1
            print(f"{name}: нет значения в .env/окружении")
            continue
        if current.get(name) and not overwrite:
            skipped_existing += 1
            print(f"{name}: уже есть в keyring, пропущен")
            continue
        keyring_backend.set(name, value)
        migrated += 1
        print(f"{name}: перенесён в keyring")

    print(
        "Итог: "
        f"перенесено={migrated}, уже было={skipped_existing}, "
        f"не найдено в env={missing}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage DataBase_MP secrets")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="показать, какие секреты заданы")
    status_parser.add_argument("names", nargs="*", help="имена секретов, по умолчанию ключевые секреты проекта")
    status_parser.add_argument(
        "--backend",
        choices=("active", "keyring"),
        default="active",
        help="active учитывает текущий APP_SECRET_BACKEND и fallback, keyring проверяет только keyring",
    )

    set_parser = subparsers.add_parser("set", help="сохранить секрет в keyring")
    set_parser.add_argument("name")
    set_parser.add_argument("--value", help="значение секрета; лучше не использовать в интерактивной консоли")

    delete_parser = subparsers.add_parser("delete", help="удалить секрет из keyring")
    delete_parser.add_argument("name")

    migrate_parser = subparsers.add_parser("migrate-from-env", help="перенести секреты из .env/окружения в keyring")
    migrate_parser.add_argument("names", nargs="*", help="имена секретов, по умолчанию ключевые секреты проекта")
    migrate_parser.add_argument("--overwrite", action="store_true", help="перезаписать уже существующие секреты")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "status":
        return print_secrets_status(args.names, backend=args.backend)
    if args.command == "set":
        return set_secret(args.name, args.value)
    if args.command == "delete":
        return delete_secret(args.name)
    if args.command == "migrate-from-env":
        return migrate_from_env(args.names, overwrite=args.overwrite)
    return 2

