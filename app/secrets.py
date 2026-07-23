from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote, unquote, urlparse, urlunparse

from app.config import DEFAULT_SECRET_SERVICE, get_config, load_project_env


SENSITIVE_SECRET_NAMES = [
    "PG_DSN",
    "POSTGRES_PASSWORD",
    "WB_TOKEN",
    "WB_TOKEN_CONTENT",
    "WB_ANALYTICS_TOKEN",
    "WB_SUPPLIES_TOKEN",
    "OZON_CLIENT_ID",
    "OZON_API_KEY",
    "TG_BOT_TOKEN",
    "TG_CHAT_ID",
    "API_SERVER_TOKEN",
]

SECRET_ALIASES = {
    "WB_TOKEN_CONTENT": ("WB_TOKEN",),
    "WB_ANALYTICS_TOKEN": ("WB_TOKEN",),
    "WB_SUPPLIES_TOKEN": ("WB_TOKEN",),
}


class SecretBackend(Protocol):
    def get(self, name: str) -> str | None:
        ...

    def exists(self, name: str) -> bool:
        ...

    def set(self, name: str, value: str) -> None:
        ...

    def delete(self, name: str) -> bool:
        ...


@dataclass(frozen=True)
class EnvSecretBackend:
    def get(self, name: str) -> str | None:
        load_project_env()
        value = os.getenv(name)
        return value if value else None

    def exists(self, name: str) -> bool:
        return self.get(name) is not None

    def set(self, name: str, value: str) -> None:
        raise RuntimeError("EnvSecretBackend не умеет безопасно записывать секреты")

    def delete(self, name: str) -> bool:
        raise RuntimeError("EnvSecretBackend не умеет безопасно удалять секреты")


@dataclass(frozen=True)
class KeyringSecretBackend:
    service_name: str = DEFAULT_SECRET_SERVICE
    fallback: SecretBackend | None = None

    def get(self, name: str) -> str | None:
        try:
            import keyring
        except ModuleNotFoundError:
            return self.fallback.get(name) if self.fallback else None

        try:
            value = keyring.get_password(self.service_name, name)
        except Exception:
            value = None
        if value:
            return value
        return self.fallback.get(name) if self.fallback else None

    def exists(self, name: str) -> bool:
        return self.get(name) is not None

    def set(self, name: str, value: str) -> None:
        try:
            import keyring
        except ModuleNotFoundError as exc:
            raise RuntimeError("keyring не установлен") from exc
        keyring.set_password(self.service_name, name, value)

    def delete(self, name: str) -> bool:
        try:
            import keyring
        except ModuleNotFoundError as exc:
            raise RuntimeError("keyring не установлен") from exc
        try:
            keyring.delete_password(self.service_name, name)
        except Exception:
            return False
        return True


def get_secret_backend() -> SecretBackend:
    cfg = get_config()
    env_backend = EnvSecretBackend()
    if cfg.secret_backend == "keyring":
        return KeyringSecretBackend(service_name=cfg.secret_service_name, fallback=env_backend)
    return env_backend


def get_secret(name: str, *, required: bool = False) -> str | None:
    value = get_secret_backend().get(name)
    if not value:
        for alias in SECRET_ALIASES.get(name, ()):
            value = get_secret_backend().get(alias)
            if value:
                break
    if required and not value:
        raise RuntimeError(f"Секрет {name} не задан")
    return value


def secret_status(names: list[str]) -> dict[str, bool]:
    return {name: get_secret(name) is not None for name in names}


def get_keyring_backend() -> KeyringSecretBackend:
    cfg = get_config()
    return KeyringSecretBackend(service_name=cfg.secret_service_name, fallback=None)


def keyring_secret_status(names: list[str]) -> dict[str, bool]:
    backend = get_keyring_backend()
    return {name: backend.exists(name) for name in names}


def split_pg_dsn_password(dsn: str) -> tuple[str, str | None]:
    dsn = (dsn or "").strip()
    if not dsn:
        return "", None

    parsed = urlparse(dsn)
    if parsed.scheme:
        password = unquote(parsed.password or "") or None
        host = parsed.hostname or ""
        netloc = host
        if parsed.username:
            netloc = quote(unquote(parsed.username), safe="") + ("@" + host if host else "@")
        if parsed.port:
            netloc += f":{parsed.port}"
        sanitized = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
        return sanitized, password

    values = _parse_libpq_dsn(dsn)
    password = values.pop("password", "") or None
    if not values:
        return dsn, password
    ordered_keys = ["host", "hostaddr", "port", "dbname", "user", "sslmode"]
    rendered: list[str] = []
    for key in ordered_keys:
        if key in values:
            rendered.append(f"{key}={_quote_libpq_value(values.pop(key))}")
    for key in sorted(values):
        rendered.append(f"{key}={_quote_libpq_value(values[key])}")
    return " ".join(rendered), password


def _parse_libpq_dsn(dsn: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)=", dsn))
    result: dict[str, str] = {}
    for idx, match in enumerate(matches):
        key = match.group(1)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(dsn)
        value = dsn[start:end].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        result[key] = value
    return result


def _quote_libpq_value(value: str) -> str:
    if not value:
        return "''"
    if re.search(r"\s|'|\\\\", value):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return value
