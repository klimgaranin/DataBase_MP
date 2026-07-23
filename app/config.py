from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SECRET_SERVICE = "DataBase_MP"


def load_project_env() -> None:
    env_path = ROOT / ".env"
    load_dotenv(dotenv_path=env_path if env_path.exists() else None, override=False)


@dataclass(frozen=True)
class AppConfig:
    env: str
    secret_backend: str
    secret_service_name: str
    analytics_mp_spreadsheet_id: str
    google_application_credentials: str


def get_config() -> AppConfig:
    load_project_env()
    return AppConfig(
        env=os.getenv("APP_ENV", "local").strip() or "local",
        secret_backend=os.getenv("APP_SECRET_BACKEND", "env").strip().lower() or "env",
        secret_service_name=os.getenv("APP_SECRET_SERVICE_NAME", DEFAULT_SECRET_SERVICE).strip()
        or DEFAULT_SECRET_SERVICE,
        analytics_mp_spreadsheet_id=os.getenv(
            "GOOGLE_SHEETS_ANALYTICS_MP_SPREADSHEET_ID",
            "1j34jji_YhWFoPh7CwFR_mFltqDw_Qm6Ey-ee-uczWP8",
        ).strip(),
        google_application_credentials=os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "secrets/google-service-account.json",
        ).strip(),
    )
