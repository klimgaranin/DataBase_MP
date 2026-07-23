from __future__ import annotations

import argparse
from typing import Sequence

from dotenv import load_dotenv

from app.ops.health import PROJECT_ROOT, load_health_env, mask_secret


def print_jobs_status(*, limit: int) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    env = load_health_env()

    try:
        from app.db import connect
    except Exception as exc:
        print(f"Не удалось загрузить DB-модуль: {exc!r}")
        return 1

    try:
        conn_ctx = connect()
        conn = conn_ctx.__enter__()
    except Exception as exc:
        print(f"DB: FAIL, не удалось подключиться: {mask_secret(repr(exc), env)}")
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_name, started_at, finished_at, status,
                       api_rows, raw_new, norm_upserted, duplicates,
                       ROUND(dup_pct::numeric, 2), LEFT(COALESCE(error, ''), 240)
                FROM job_runs
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn_ctx.__exit__(None, None, None)

    print("Последние запуски jobs")
    print("----------------------")
    if not rows:
        print("Запусков пока нет")
        return 0
    for row in rows:
        job_name, started_at, finished_at, status, api_rows, raw_new, norm_upserted, duplicates, dup_pct, error = row
        print(
            f"{job_name} | {status} | start={started_at} | finish={finished_at} | "
            f"api={api_rows} raw={raw_new} norm={norm_upserted} dups={duplicates} dup_pct={dup_pct}"
        )
        if error:
            print(f"  ошибка: {error}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show latest DataBase_MP job runs")
    parser.add_argument("--limit", type=int, default=10, help="сколько последних запусков показать")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return print_jobs_status(limit=max(1, args.limit))
