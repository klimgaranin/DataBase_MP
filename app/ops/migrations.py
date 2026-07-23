from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"


def migration_version(path: Path) -> int:
    prefix = path.name.split("__", 1)[0].lstrip("V")
    return int(prefix)


def selected_migrations(*, from_version: int, to_version: int | None) -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("V*__*.sql"), key=migration_version)
    selected = [path for path in files if migration_version(path) >= from_version]
    if to_version is not None:
        selected = [path for path in selected if migration_version(path) <= to_version]
    return selected


def apply_migrations(*, from_version: int, to_version: int | None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    from app.db import connect

    selected = selected_migrations(from_version=from_version, to_version=to_version)
    if not selected:
        print("Нет миграций для применения")
        return 0

    with connect() as conn:
        with conn.cursor() as cur:
            for path in selected:
                sql = path.read_text(encoding="utf-8")
                cur.execute(sql)
                print(f"OK: применена {path.name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply DataBase_MP SQL migrations")
    parser.add_argument("--from-version", type=int, default=1)
    parser.add_argument("--to-version", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return apply_migrations(from_version=args.from_version, to_version=args.to_version)

