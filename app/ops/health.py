from __future__ import annotations

import argparse
import importlib.util
import os
import re
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CORE_TABLES = [
    "job_cursors",
    "job_runs",
    "wb_orders_raw_dedup",
    "wb_orders_norm",
    "wb_stocks_raw",
    "wb_stocks_snap",
]
CORE_ENV_KEYS = [
    "PG_DSN",
    "WB_TOKEN",
    "POSTGRES_PASSWORD",
    "TG_BOT_TOKEN",
    "TG_CHAT_ID",
    "WB_ORDERS_LOG_FILE",
    "WB_STOCKS_LOG_FILE",
]
CORE_MODULES = [
    "requests",
    "dotenv",
    "psycopg2",
]


def load_health_env() -> dict[str, str]:
    env_path = PROJECT_ROOT / ".env"
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        load_dotenv = None
    if load_dotenv is not None:
        load_dotenv(env_path if env_path.exists() else None)

    values: dict[str, str] = {}
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    for key, value in os.environ.items():
        values.setdefault(key, value)
    return values


def mask_secret(text: str, env: dict[str, str]) -> str:
    masked = text
    for key, value in env.items():
        if not value or len(value) < 5:
            continue
        if any(mark in key.upper() for mark in ("TOKEN", "PASSWORD", "SECRET", "DSN")):
            masked = masked.replace(value, "***MASKED***")
    return masked


def safe_dsn_summary(dsn: str) -> str:
    if not dsn:
        return "не задан"
    parsed = urlparse(dsn)
    if not parsed.scheme:
        if "=" in dsn:
            parts = re.findall(r"(\w+)=('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|\S*)", dsn)
            values: dict[str, str] = {}
            for key, value in parts:
                values[key] = value.strip().strip("'").strip('"')
            host = values.get("host") or values.get("hostaddr") or "unknown-host"
            port = values.get("port") or "default"
            db_name = values.get("dbname") or "unknown-db"
            return f"libpq://{host}:{port}/{db_name}"
        return "задан, формат не распознан"
    host = parsed.hostname or "unknown-host"
    port = parsed.port or "default"
    db_name = parsed.path.lstrip("/") or "unknown-db"
    return f"{parsed.scheme}://{host}:{port}/{db_name}"


def dependency_status() -> dict[str, bool]:
    return {name: importlib.util.find_spec(name) is not None for name in CORE_MODULES}


def _print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _log_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _tail(path: Path, lines: int) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return [f"не удалось прочитать: {exc}"]
    return content[-lines:]


def _print_logs(env: dict[str, str], *, log_lines: int) -> None:
    _print_section("Логи")
    for key in ("WB_ORDERS_LOG_FILE", "WB_STOCKS_LOG_FILE"):
        path = _log_path(env.get(key))
        if path is None:
            print(f"{key}: не задан")
            continue
        if not path.exists():
            print(f"{key}: файл не найден ({path})")
            continue
        print(f"{key}: {path} ({path.stat().st_size} bytes)")
        for line in _tail(path, log_lines):
            print("  " + mask_secret(line, env))


def check_db(env: dict[str, str], *, log_lines: int) -> None:
    from app.secrets import get_secret

    dsn = get_secret("PG_DSN") or ""
    print(f"PG_DSN: {safe_dsn_summary(dsn)}")
    if not dsn:
        print("DB: SKIP, PG_DSN не задан")
        return
    try:
        import psycopg2  # noqa: F401
    except ModuleNotFoundError:
        print("DB: SKIP, psycopg2 не установлен в текущем Python")
        return

    try:
        from app.db import connect

        conn_ctx = connect()
        conn = conn_ctx.__enter__()
    except Exception as exc:
        print(f"DB: FAIL, не удалось подключиться: {mask_secret(repr(exc), env)}")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, NOW()")
            db_name, db_user, now = cur.fetchone()
            print(f"DB: OK, database={db_name}, user={db_user}, now={now}")

            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
                """
            )
            tables = {row[0] for row in cur.fetchall()}
            missing = [name for name in CORE_TABLES if name not in tables]
            print(f"Core tables: {'OK' if not missing else 'MISSING ' + ', '.join(missing)}")

            if tables:
                cur.execute(
                    """
                    SELECT relname, n_live_tup
                    FROM pg_stat_user_tables
                    WHERE relname = ANY(%s)
                    ORDER BY relname
                    """,
                    (CORE_TABLES,),
                )
                for relname, n_live_tup in cur.fetchall():
                    print(f"  {relname}: approx_rows={n_live_tup}")

            if "job_runs" in tables:
                cur.execute(
                    """
                    SELECT job_name, started_at, finished_at, status,
                           api_rows, raw_new, norm_upserted, duplicates,
                           ROUND(dup_pct::numeric, 2), LEFT(COALESCE(error, ''), 160)
                    FROM job_runs
                    ORDER BY id DESC
                    LIMIT 10
                    """
                )
                rows = cur.fetchall()
                print("Latest job_runs:")
                if not rows:
                    print("  нет записей")
                for row in rows:
                    job_name, started_at, _finished_at, status, api_rows, raw_new, norm_upserted, duplicates, dup_pct, error = row
                    suffix = f", error={error}" if error else ""
                    print(
                        "  "
                        f"{job_name} | {status} | start={started_at} | "
                        f"api={api_rows} raw={raw_new} norm={norm_upserted} "
                        f"dups={duplicates} dup_pct={dup_pct}{suffix}"
                    )

            if "job_cursors" in tables:
                cur.execute(
                    """
                    SELECT job_name, cursor_val, updated_at
                    FROM job_cursors
                    ORDER BY job_name
                    """
                )
                rows = cur.fetchall()
                print("Job cursors:")
                if not rows:
                    print("  нет записей")
                for job_name, cursor_val, updated_at in rows:
                    print(f"  {job_name}: {cursor_val} updated_at={updated_at}")
    finally:
        conn_ctx.__exit__(None, None, None)

    if log_lines > 0:
        _print_logs(env, log_lines=log_lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataBase_MP core health check")
    parser.add_argument("--log-lines", type=int, default=5, help="сколько последних строк логов показать")
    parser.add_argument("--skip-db", action="store_true", help="не подключаться к PostgreSQL")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = load_health_env()
    from app.secrets import SENSITIVE_SECRET_NAMES, secret_status

    active_secret_status = secret_status(SENSITIVE_SECRET_NAMES)

    print("DataBase_MP health check")
    print(f"Root: {PROJECT_ROOT}")

    _print_section("Окружение")
    for key in CORE_ENV_KEYS:
        is_set = active_secret_status.get(key, False) if key in SENSITIVE_SECRET_NAMES else bool(env.get(key))
        print(f"{key}: {'задан' if is_set else 'не задан'}")

    _print_section("Зависимости")
    deps = dependency_status()
    for name, ok in deps.items():
        print(f"{name}: {'OK' if ok else 'MISSING'}")

    if args.skip_db:
        _print_section("База данных")
        print("DB: SKIP по параметру --skip-db")
        if args.log_lines > 0:
            _print_logs(env, log_lines=args.log_lines)
        return 0

    _print_section("База данных")
    check_db(env, log_lines=max(0, args.log_lines))
    return 0
