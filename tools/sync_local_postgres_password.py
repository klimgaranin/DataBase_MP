from __future__ import annotations

import subprocess
from pathlib import Path
import shlex
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parent.parent


def _read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = ROOT / ".env"
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _quote_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _dsn_parts(dsn: str) -> dict[str, str]:
    parsed = urlparse(dsn)
    if parsed.scheme:
        return {
            "user": parsed.username or "app",
            "password": unquote(parsed.password or ""),
            "database": parsed.path.lstrip("/") or "marketplace",
        }
    values: dict[str, str] = {}
    try:
        parts = shlex.split(dsn)
    except ValueError:
        parts = dsn.split()
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return {
        "user": values.get("user", "app"),
        "password": values.get("password", ""),
        "database": values.get("dbname", "marketplace"),
    }


def main() -> int:
    env = _read_env()
    parts = _dsn_parts(env.get("PG_DSN", ""))
    user = parts["user"]
    password = parts["password"] or env.get("POSTGRES_PASSWORD", "")
    database = parts["database"]
    if not password:
        raise SystemExit("PG_DSN/POSTGRES_PASSWORD не содержат пароль")

    sql = f"ALTER USER {user} WITH PASSWORD {_quote_sql_literal(password)};\n"
    result = subprocess.run(
        ["docker", "exec", "-i", "infra-db-1", "psql", "-U", "app", "-d", database, "-v", "ON_ERROR_STOP=1"],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        print("FAIL: пароль локального PostgreSQL не синхронизирован")
        if result.stderr:
            print(result.stderr.strip())
        return result.returncode
    print(f"OK: пароль локального PostgreSQL синхронизирован для user={user}, database={database}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
