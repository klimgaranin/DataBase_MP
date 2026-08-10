from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any


DEFAULT_BITWARDEN_FOLDER = "DataBase_MP"
DEFAULT_ITEM_PREFIX = "DataBase_MP / "
SECRET_FIELD_NAMES = {"token", "value", "password", "secret", "api_key", "api-key", "api key"}


@dataclass(frozen=True)
class BitwardenItemSecret:
    name: str
    value: str


def run_bw(args: list[str], *, input_text: str | None = None) -> str:
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


def load_json(text: str) -> Any:
    return json.loads(text) if text else None


def get_bw_status() -> str:
    status = load_json(run_bw(["status"]))
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
            "$env:BW_SESSION = $(bw unlock --raw)"
        )
    raise RuntimeError("Bitwarden CLI не залогинен. Выполните в PowerShell: bw login")


def encode_bw_object(payload: dict[str, Any]) -> str:
    return run_bw(["encode"], input_text=json.dumps(payload, ensure_ascii=False))


def ensure_folder(folder_name: str) -> str:
    folders = load_json(run_bw(["list", "folders"])) or []
    for folder in folders:
        if isinstance(folder, dict) and folder.get("name") == folder_name:
            return str(folder["id"])

    template = load_json(run_bw(["get", "template", "folder"]))
    if not isinstance(template, dict):
        raise RuntimeError("bw get template folder вернул неожиданный ответ")
    template["name"] = folder_name
    created = load_json(run_bw(["create", "folder", encode_bw_object(template)]))
    if not isinstance(created, dict) or not created.get("id"):
        raise RuntimeError("Bitwarden не вернул id созданной папки")
    return str(created["id"])


def extract_item_secret(item: dict[str, Any], *, prefix: str = DEFAULT_ITEM_PREFIX) -> BitwardenItemSecret | None:
    item_name = item.get("name")
    if not isinstance(item_name, str) or not item_name.startswith(prefix):
        return None

    secret_name = item_name[len(prefix) :].strip()
    if not secret_name:
        return None

    value = _extract_secret_value(item, secret_name)
    if not value:
        return None

    return BitwardenItemSecret(name=secret_name, value=value)


def _extract_secret_value(item: dict[str, Any], secret_name: str) -> str | None:
    login = item.get("login")
    if isinstance(login, dict):
        value = login.get("password")
        if isinstance(value, str) and value.strip():
            return value.strip()

    fields = item.get("fields")
    if isinstance(fields, list):
        expected_names = {secret_name.lower(), *SECRET_FIELD_NAMES}
        for field in fields:
            if not isinstance(field, dict):
                continue
            field_name = str(field.get("name") or "").strip().lower()
            value = field.get("value")
            if field_name in expected_names and isinstance(value, str) and value.strip():
                return value.strip()

    notes = item.get("notes")
    if isinstance(notes, str):
        value = notes.strip()
        if value and "\n" not in value and "\r" not in value:
            return value
    return None


def list_folder_items(folder_id: str) -> list[dict[str, Any]]:
    items = load_json(run_bw(["list", "items", "--folderid", folder_id])) or []
    return [item for item in items if isinstance(item, dict)]


def load_bitwarden_secrets(*, folder_name: str = DEFAULT_BITWARDEN_FOLDER) -> dict[str, str]:
    require_unlocked()
    folder_id = ensure_folder(folder_name)
    result: dict[str, str] = {}
    for item in list_folder_items(folder_id):
        secret = extract_item_secret(item)
        if secret:
            result[secret.name] = secret.value
    return result
