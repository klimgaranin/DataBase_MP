import os
import re
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


def get_dsn() -> str:
    """Builds a psycopg2 DSN string from env vars.

    Priority:
      1) PG_DSN (full DSN string)
      2) PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD
    """
    raw = os.environ.get("PG_DSN", "").strip()
    if raw:
        # Support URL or key=val style. psycopg2 accepts both.
        return raw

    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DB", "marketplace")
    user = os.environ.get("PG_USER", "app")
    password = os.environ.get("PG_PASSWORD", "")

    def q(v: str) -> str:
        # Quote if contains spaces or special chars; escape quotes
        if re.search(r"\s|['\"\\]", v):
            v = v.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{v}"'
        return v

    parts = [
        f"host={q(host)}",
        f"port={q(port)}",
        f"dbname={q(db)}",
        f"user={q(user)}",
    ]
    if password:
        parts.append(f"password={q(password)}")
    return " ".join(parts)


def _connect():
    """Единая точка подключения к Postgres.

    Зачем:
    - Чтобы не дублировать psycopg2.connect(...) по всему проекту.
    - Чтобы гарантированно был connect_timeout (на Windows + Docker это критично).
    """
    return psycopg2.connect(get_dsn(), connect_timeout=10)


def create_extension_if_needed() -> None:
    with _connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")


def exec_sql(sql: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def load_migrations() -> None:
    """Creates basic schema if missing."""
    create_extension_if_needed()

    sql = """
    CREATE TABLE IF NOT EXISTS meta_job_state (
      job_name TEXT PRIMARY KEY,
      cursor_iso TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS wb_orders_raw (
      id BIGSERIAL PRIMARY KEY,
      srid TEXT NOT NULL,
      last_change_ts TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL,
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Raw versions deduplicated by (srid, last_change_ts)
    CREATE TABLE IF NOT EXISTS wb_orders_raw_dedup (
      id BIGSERIAL PRIMARY KEY,
      srid TEXT NOT NULL,
      last_change_ts TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL,
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (srid, last_change_ts)
    );

    -- Normalized table
    CREATE TABLE IF NOT EXISTS wb_orders_norm (
      srid TEXT PRIMARY KEY,
      order_id BIGINT NULL,
      nm_id BIGINT NULL,
      supplier_article TEXT NULL,
      warehouse_name TEXT NULL,
      date DATE NULL,
      date_ts TIMESTAMPTZ NULL,
      last_change_ts TIMESTAMPTZ NOT NULL,
      price_with_disc NUMERIC NULL,
      is_cancel BOOLEAN NOT NULL DEFAULT FALSE,
      cancel_dt DATE NULL,
      cancel_ts TIMESTAMPTZ NULL,
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_srid ON wb_orders_raw (srid);
    CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_last_change_ts ON wb_orders_raw (last_change_ts);

    CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_dedup_srid ON wb_orders_raw_dedup (srid);
    CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_dedup_last_change_ts ON wb_orders_raw_dedup (last_change_ts);

    CREATE INDEX IF NOT EXISTS idx_wb_orders_norm_last_change_ts ON wb_orders_norm (last_change_ts);
    """
    exec_sql(sql)


def get_job_cursor(job_name: str) -> Optional[str]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cursor_iso FROM meta_job_state WHERE job_name=%s", (job_name,))
            row = cur.fetchone()
            if not row:
                return None
            return str(row[0])


def set_job_cursor(job_name: str, cursor_iso: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta_job_state (job_name, cursor_iso)
                VALUES (%s, %s)
                ON CONFLICT (job_name) DO UPDATE
                  SET cursor_iso=EXCLUDED.cursor_iso,
                      updated_at=NOW()
                """,
                (job_name, cursor_iso),
            )


def insert_wb_orders_raw(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    values: List[Tuple[str, str, Any]] = []
    for r in rows:
        srid = str(r.get("srid") or "").strip()
        last_change = r.get("lastChangeDate") or r.get("lastChangeTs") or r.get("last_change_ts")
        if not srid or not last_change:
            continue
        values.append((srid, str(last_change), r))

    if not values:
        return 0

    with _connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO wb_orders_raw (srid, last_change_ts, payload)
                VALUES %s
                """,
                values,
                page_size=1000,
            )
        return len(values)


def insert_wb_orders_raw_dedup(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    values: List[Tuple[str, str, Any]] = []
    for r in rows:
        srid = str(r.get("srid") or "").strip()
        last_change = r.get("lastChangeDate") or r.get("lastChangeTs") or r.get("last_change_ts")
        if not srid or not last_change:
            continue
        values.append((srid, str(last_change), r))

    if not values:
        return 0

    with _connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO wb_orders_raw_dedup (srid, last_change_ts, payload)
                VALUES %s
                ON CONFLICT (srid, last_change_ts) DO NOTHING
                """,
                values,
                page_size=1000,
            )
        return len(values)


def upsert_wb_orders_norm(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    cols = [
        "srid",
        "order_id",
        "nm_id",
        "supplier_article",
        "warehouse_name",
        "date",
        "date_ts",
        "last_change_ts",
        "price_with_disc",
        "is_cancel",
        "cancel_dt",
        "cancel_ts",
    ]

    values: List[Tuple[Any, ...]] = []
    for r in rows:
        values.append(tuple(r.get(c) for c in cols))

    with _connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO wb_orders_norm ({", ".join(cols)})
                VALUES %s
                ON CONFLICT (srid) DO UPDATE SET
                  order_id=EXCLUDED.order_id,
                  nm_id=EXCLUDED.nm_id,
                  supplier_article=EXCLUDED.supplier_article,
                  warehouse_name=EXCLUDED.warehouse_name,
                  date=EXCLUDED.date,
                  date_ts=EXCLUDED.date_ts,
                  last_change_ts=EXCLUDED.last_change_ts,
                  price_with_disc=EXCLUDED.price_with_disc,
                  is_cancel=EXCLUDED.is_cancel,
                  cancel_dt=EXCLUDED.cancel_dt,
                  cancel_ts=EXCLUDED.cancel_ts,
                  updated_at=NOW()
                """,
                values,
                page_size=1000,
            )
        return len(values)


def cleanup_wb_orders_raw_dedup(days: int = 30) -> int:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM wb_orders_raw_dedup WHERE inserted_at < NOW() - (%s || ' days')::interval",
                (days,),
            )
            return cur.rowcount


# Advisory lock: one process at a time
_LOCK_CONNS: Dict[int, Any] = {}


def acquire_job_lock(lock_key: int) -> bool:
    # Keep connection open while holding the lock
    conn = _connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        ok = bool(cur.fetchone()[0])
        if ok:
            _LOCK_CONNS[lock_key] = conn
            return True
    conn.close()
    return False


def release_job_lock(lock_key: int) -> None:
    conn = _LOCK_CONNS.pop(lock_key, None)
    if not conn:
        return
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
    finally:
        conn.close()