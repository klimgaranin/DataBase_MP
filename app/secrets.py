from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

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
    if required and not value:
        raise RuntimeError(f"Секрет {name} не задан")
    return value


def secret_status(names: list[str]) -> dict[str, bool]:
    backend = get_secret_backend()
    return {name: backend.exists(name) for name in names}


def get_keyring_backend() -> KeyringSecretBackend:
    cfg = get_config()
    return KeyringSecretBackend(service_name=cfg.secret_service_name, fallback=None)


def keyring_secret_status(names: list[str]) -> dict[str, bool]:
    backend = get_keyring_backend()
    return {name: backend.exists(name) for name in names}
