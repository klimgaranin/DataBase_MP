from __future__ import annotations

import argparse
import getpass
from typing import Sequence

from app.config import get_config, load_project_env
from app.secrets import (
    SENSITIVE_SECRET_NAMES,
    get_keyring_backend,
    get_secret,
    keyring_secret_status,
    secret_status,
    split_pg_dsn_password,
)


ENV_SECRET_NAMES = [
    name for name in SENSITIVE_SECRET_NAMES if name != "PG_DSN"
]


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


def normalize_postgres_secrets(*, overwrite_password: bool = False) -> int:
    keyring_backend = get_keyring_backend()
    dsn = get_secret("PG_DSN") or ""
    sanitized_dsn, password_from_dsn = split_pg_dsn_password(dsn)
    if not sanitized_dsn:
        print("PG_DSN: не задан")
        return 1

    if password_from_dsn:
        current_password = get_secret("POSTGRES_PASSWORD")
        if overwrite_password or not current_password:
            keyring_backend.set("POSTGRES_PASSWORD", password_from_dsn)
            print("POSTGRES_PASSWORD: сохранён из PG_DSN")
        else:
            print("POSTGRES_PASSWORD: уже задан, не перезаписан")
    else:
        print("PG_DSN: пароль внутри DSN не найден")

    keyring_backend.set("PG_DSN", sanitized_dsn)
    print("PG_DSN: сохранён без пароля")
    return 0


def clean_env_file(*, path: str = ".env", backend: str = "keyring") -> int:
    from pathlib import Path

    from app.ops.health import PROJECT_ROOT

    env_path = Path(path)
    if not env_path.is_absolute():
        env_path = PROJECT_ROOT / env_path
    if not env_path.exists():
        print(f"{env_path}: файл не найден")
        return 1

    lines = env_path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()
    output: list[str] = []
    seen_backend = False
    changed = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("APP_SECRET_BACKEND="):
            output.append(f"APP_SECRET_BACKEND={backend}")
            seen_backend = True
            changed = True
            continue
        if stripped.startswith("PG_DSN="):
            raw_dsn = stripped.split("=", 1)[1]
            sanitized_dsn, password = split_pg_dsn_password(raw_dsn)
            output.append(f"PG_DSN={sanitized_dsn}")
            if password:
                changed = True
            continue

        secret_name = next((name for name in ENV_SECRET_NAMES if stripped.startswith(name + "=")), None)
        if secret_name:
            output.append(f"# {secret_name} хранится в secret backend")
            changed = True
            continue

        output.append(line)

    if not seen_backend:
        output.insert(0, f"APP_SECRET_BACKEND={backend}")
        changed = True

    env_path.write_text("\n".join(output) + "\n", encoding="utf-8")
    print(f"{env_path}: очищен, изменён={changed}")
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

    normalize_pg = subparsers.add_parser("normalize-postgres", help="разделить PG_DSN и POSTGRES_PASSWORD в keyring")
    normalize_pg.add_argument("--overwrite-password", action="store_true", help="перезаписать POSTGRES_PASSWORD паролем из PG_DSN")

    clean_env = subparsers.add_parser("clean-env", help="очистить .env от секретов и оставить PG_DSN без пароля")
    clean_env.add_argument("--path", default=".env")
    clean_env.add_argument("--backend", choices=("env", "keyring"), default="keyring")

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
    if args.command == "normalize-postgres":
        return normalize_postgres_secrets(overwrite_password=args.overwrite_password)
    if args.command == "clean-env":
        return clean_env_file(path=args.path, backend=args.backend)
    return 2
