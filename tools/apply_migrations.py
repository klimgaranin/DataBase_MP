from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
MIGRATIONS_DIR = ROOT / "migrations"


def _version(path: Path) -> int:
    prefix = path.name.split("__", 1)[0].lstrip("V")
    return int(prefix)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply DataBase_MP SQL migrations")
    parser.add_argument("--from-version", type=int, default=1)
    parser.add_argument("--to-version", type=int)
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    from app.db import connect

    files = sorted(MIGRATIONS_DIR.glob("V*__*.sql"), key=_version)
    selected = [path for path in files if _version(path) >= args.from_version]
    if args.to_version is not None:
        selected = [path for path in selected if _version(path) <= args.to_version]
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


if __name__ == "__main__":
    raise SystemExit(main())
