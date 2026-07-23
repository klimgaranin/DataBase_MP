"""
Создать/обновить записи Bitwarden из Windows Credential Manager.

Скрипт не печатает значения секретов. Перед запуском Bitwarden CLI должен быть
залогинен и разблокирован пользователем.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.secrets import SENSITIVE_SECRET_NAMES, get_keyring_backend
from app.ops.bitwarden import (
    DEFAULT_BITWARDEN_FOLDER,
    DEFAULT_ITEM_PREFIX,
    encode_bw_object,
    ensure_folder,
    load_json,
    require_unlocked,
    run_bw,
)


SKIP_ALIASES = {
    "WB_TOKEN_CONTENT",
    "WB_ANALYTICS_TOKEN",
    "WB_SUPPLIES_TOKEN",
}


@dataclass(frozen=True)
class SecretEntry:
    name: str
    value: str


def load_keyring_entries(names: Iterable[str]) -> list[SecretEntry]:
    backend = get_keyring_backend()
    entries: list[SecretEntry] = []
    for name in names:
        if name in SKIP_ALIASES:
            continue
        value = backend.get(name)
        if value:
            entries.append(SecretEntry(name=name, value=value))
    return entries


def build_item(entry: SecretEntry, folder_id: str, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    item = dict(existing or {})
    item.update(
        {
            "type": 1,
            "name": f"{DEFAULT_ITEM_PREFIX}{entry.name}",
            "folderId": folder_id,
            "notes": f"DataBase_MP runtime secret. Source: Windows Credential Manager keyring. Name: {entry.name}",
            "login": {
                "uris": [],
                "username": "DataBase_MP",
                "password": entry.value,
                "totp": None,
            },
            "favorite": False,
        }
    )
    return item


def list_existing_items() -> dict[str, dict[str, Any]]:
    items = load_json(run_bw(["list", "items"])) or []
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            result[item["name"]] = item
    return result


def sync_entries(entries: list[SecretEntry], *, folder_name: str, dry_run: bool) -> int:
    require_unlocked()
    folder_id = ensure_folder(folder_name)
    existing = list_existing_items()

    created = 0
    updated = 0
    for entry in entries:
        item_name = f"DataBase_MP / {entry.name}"
        old_item = existing.get(item_name)
        if dry_run:
            print(f"{item_name}: {'будет обновлён' if old_item else 'будет создан'}")
            continue

        item_payload = build_item(entry, folder_id, old_item)
        encoded = encode_bw_object(item_payload)
        if old_item and old_item.get("id"):
            run_bw(["edit", "item", str(old_item["id"]), encoded])
            updated += 1
        else:
            run_bw(["create", "item", encoded])
            created += 1

    if not dry_run:
        run_bw(["sync"])
    print(f"Bitwarden: создано {created}, обновлено {updated}, пропущено {len(SKIP_ALIASES)} alias")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Синхронизировать keyring секреты DataBase_MP в Bitwarden")
    parser.add_argument("--folder", default=DEFAULT_BITWARDEN_FOLDER)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("names", nargs="*", help="Секреты для переноса; по умолчанию все основные")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    names = args.names or SENSITIVE_SECRET_NAMES
    entries = load_keyring_entries(names)
    if not entries:
        print("Нет секретов для переноса из keyring")
        return 0
    return sync_entries(entries, folder_name=args.folder, dry_run=args.dry_run)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        raise SystemExit(1)
