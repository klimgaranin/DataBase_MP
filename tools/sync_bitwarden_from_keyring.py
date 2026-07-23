"""
Создать/обновить записи Bitwarden из Windows Credential Manager.

Скрипт не печатает значения секретов. Перед запуском Bitwarden CLI должен быть
залогинен и разблокирован пользователем.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Iterable

from app.secrets import SENSITIVE_SECRET_NAMES, get_keyring_backend


DEFAULT_FOLDER_NAME = "DataBase_MP"
SKIP_ALIASES = {
    "WB_TOKEN_CONTENT",
    "WB_ANALYTICS_TOKEN",
    "WB_SUPPLIES_TOKEN",
}


@dataclass(frozen=True)
class SecretEntry:
    name: str
    value: str


def _run_bw(args: list[str], *, input_text: str | None = None) -> str:
    completed = subprocess.run(
        ["bw", *args],
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(message or f"bw {' '.join(args)} завершился с кодом {completed.returncode}")
    return completed.stdout.strip()


def _load_json(text: str) -> Any:
    return json.loads(text) if text else None


def get_bw_status() -> str:
    status = _load_json(_run_bw(["status"]))
    if not isinstance(status, dict):
        raise RuntimeError("bw status вернул неожиданный ответ")
    return str(status.get("status") or "")


def require_unlocked() -> None:
    status = get_bw_status()
    if status == "unlocked":
        return
    if status == "locked":
        raise RuntimeError(
            "Bitwarden CLI залогинен, но заблокирован. Выполните в PowerShell: "
            "bw unlock --raw, затем setx BW_SESSION \"полученная_строка\" и откройте новое окно PowerShell."
        )
    raise RuntimeError(
        "Bitwarden CLI не залогинен. Выполните в PowerShell: bw login, затем bw unlock --raw."
    )


def encode_bw_object(payload: dict[str, Any]) -> str:
    return _run_bw(["encode"], input_text=json.dumps(payload, ensure_ascii=False))


def ensure_folder(folder_name: str) -> str:
    folders = _load_json(_run_bw(["list", "folders"])) or []
    for folder in folders:
        if isinstance(folder, dict) and folder.get("name") == folder_name:
            return str(folder["id"])

    template = _load_json(_run_bw(["get", "template", "folder"]))
    if not isinstance(template, dict):
        raise RuntimeError("bw get template folder вернул неожиданный ответ")
    template["name"] = folder_name
    created = _load_json(_run_bw(["create", "folder", encode_bw_object(template)]))
    if not isinstance(created, dict) or not created.get("id"):
        raise RuntimeError("Bitwarden не вернул id созданной папки")
    return str(created["id"])


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
            "name": f"DataBase_MP / {entry.name}",
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
    items = _load_json(_run_bw(["list", "items"])) or []
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
            _run_bw(["edit", "item", str(old_item["id"]), encoded])
            updated += 1
        else:
            _run_bw(["create", "item", encoded])
            created += 1

    if not dry_run:
        _run_bw(["sync"])
    print(f"Bitwarden: создано {created}, обновлено {updated}, пропущено {len(SKIP_ALIASES)} alias")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Синхронизировать keyring секреты DataBase_MP в Bitwarden")
    parser.add_argument("--folder", default=DEFAULT_FOLDER_NAME)
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
