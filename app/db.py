"""
db.py — все DB-функции проекта DataBase_MP.
Зависимости: psycopg2-binary, python-dotenv.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Generator, Optional
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


def _load_env() -> None:
    for candidate in [
        Path(__file__).parent / ".env",
        Path(__file__).parent.parent / ".env",
    ]:
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            return
    load_dotenv(override=False)


_load_env()


def _get_dsn() -> str:
    dsn = os.getenv("PG_DSN", "")
    if not dsn:
        raise RuntimeError("PG_DSN не задан в .env")
    return dsn


@contextmanager
def connect() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(_get_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_WB_ORDERS_RAW_DEDUP = """
CREATE TABLE IF NOT EXISTS wb_orders_raw_dedup (
    id             BIGSERIAL   PRIMARY KEY,
    srid           TEXT        NOT NULL,
    last_change_ts TIMESTAMPTZ NOT NULL,
    payload        JSONB       NOT NULL,
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (srid, last_change_ts)
);
"""

_DDL_WB_ORDERS_NORM = """
CREATE TABLE IF NOT EXISTS wb_orders_norm (
    srid               TEXT PRIMARY KEY,
    is_cancel          BOOLEAN,
    date_ts            TIMESTAMPTZ,
    last_change_ts     TIMESTAMPTZ,
    warehouse_name     TEXT,
    warehouse_type     TEXT,
    country_name       TEXT,
    oblast_okrug_name  TEXT,
    region_name        TEXT,
    supplier_article   TEXT,
    nm_id              BIGINT,
    barcode            TEXT,
    category           TEXT,
    subject            TEXT,
    brand              TEXT,
    tech_size          TEXT,
    income_id          BIGINT,
    is_supply          BOOLEAN,
    is_realization     BOOLEAN,
    total_price        NUMERIC(12,2),
    discount_percent   INT,
    spp                INT,
    finished_price     NUMERIC(12,2),
    price_with_disc    NUMERIC(12,2),
    cancel_date        DATE,
    sticker            TEXT,
    g_number           TEXT,
    inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_CURSORS = """
CREATE TABLE IF NOT EXISTS job_cursors (
    job_name   TEXT PRIMARY KEY,
    cursor_val TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_RUNS = """
CREATE TABLE IF NOT EXISTS job_runs (
    id           BIGSERIAL   PRIMARY KEY,
    job_name     TEXT        NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL,
    status       TEXT        NOT NULL,
    api_rows     INT         NOT NULL DEFAULT 0,
    raw_new      INT         NOT NULL DEFAULT 0,
    norm_upserted INT        NOT NULL DEFAULT 0,
    duplicates   INT         NOT NULL DEFAULT 0,
    dup_pct      NUMERIC(6,2) NOT NULL DEFAULT 0,
    cursor_old   TEXT,
    cursor_used  TEXT,
    cursor_new   TEXT,
    error        TEXT
);
"""

_DDL_WB_STOCKS_RAW = """
CREATE TABLE IF NOT EXISTS wb_stocks_raw (
    id           BIGSERIAL   PRIMARY KEY,
    nm_id        BIGINT      NOT NULL,
    chrt_id      BIGINT      NOT NULL,
    warehouse_id BIGINT      NOT NULL,
    payload      JSONB       NOT NULL,
    snapped_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_nm_id   ON wb_stocks_raw (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_chrt_id ON wb_stocks_raw (chrt_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_snapped ON wb_stocks_raw (snapped_at);
"""

_DDL_WB_STOCKS_SNAP = """
CREATE TABLE IF NOT EXISTS wb_stocks_snap (
    nm_id              BIGINT      NOT NULL,
    chrt_id            BIGINT      NOT NULL,
    warehouse_id       BIGINT      NOT NULL,
    warehouse_name     TEXT,
    region_name        TEXT,
    quantity           INT,
    in_way_to_client   INT,
    in_way_from_client INT,
    snapped_at         TIMESTAMPTZ NOT NULL,
    inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nm_id, chrt_id, warehouse_id)
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_nm_id   ON wb_stocks_snap (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_snapped ON wb_stocks_snap (snapped_at);
"""


# ── Advisory locks ────────────────────────────────────────────────────────────

def try_advisory_lock(lock_id: int) -> bool:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            return bool(cur.fetchone()[0])


def advisory_unlock(lock_id: int) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))


# ── Job cursors ───────────────────────────────────────────────────────────────

def get_job_cursor(job_name: str) -> Optional[str]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_CURSORS)
            cur.execute("SELECT cursor_val FROM job_cursors WHERE job_name = %s", (job_name,))
            row = cur.fetchone()
            return row[0] if row else None


def set_job_cursor(job_name: str, cursor_val: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_CURSORS)
            cur.execute(
                """
                INSERT INTO job_cursors (job_name, cursor_val, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (job_name) DO UPDATE
                SET cursor_val = EXCLUDED.cursor_val,
                    updated_at = NOW()
                """,
                (job_name, cursor_val),
            )


# ── WB Orders ─────────────────────────────────────────────────────────────────

def insert_wb_orders_raw_dedup(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    records = []
    for r in rows:
        srid = r.get("srid") or r.get("sr_id") or ""
        lcd  = r.get("lastChangeDate") or r.get("last_change_date") or None
        records.append((srid, lcd, json.dumps(r, ensure_ascii=False)))
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_ORDERS_RAW_DEDUP)
            execute_values(
                cur,
                """
                INSERT INTO wb_orders_raw_dedup (srid, last_change_ts, payload)
                VALUES %s
                ON CONFLICT (srid, last_change_ts) DO NOTHING
                """,
                records,
                fetch=False,
            )
            return cur.rowcount if cur.rowcount >= 0 else 0


def cleanup_wb_orders_raw_dedup(retention_days: int) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM wb_orders_raw_dedup WHERE inserted_at < NOW() - INTERVAL '1 day' * %s",
                (retention_days,),
            )
            return cur.rowcount


def upsert_wb_orders_norm(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = [
        "srid", "is_cancel", "date_ts", "last_change_ts",
        "warehouse_name", "warehouse_type", "country_name",
        "oblast_okrug_name", "region_name",
        "supplier_article", "nm_id", "barcode",
        "category", "subject", "brand", "tech_size",
        "income_id", "is_supply", "is_realization",
        "total_price", "discount_percent", "spp",
        "finished_price", "price_with_disc",
        "cancel_date", "sticker", "g_number",
    ]

    def _row_tuple(r: dict[str, Any]) -> tuple:
        return tuple(r.get(c) for c in cols)

    update_cols = [c for c in cols if c != "srid"]
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    update_set += ", updated_at = NOW()"

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_ORDERS_NORM)
            execute_values(
                cur,
                f"""
                INSERT INTO wb_orders_norm ({', '.join(cols)})
                VALUES %s
                ON CONFLICT (srid) DO UPDATE SET {update_set}
                """,
                [_row_tuple(r) for r in rows],
            )
            return cur.rowcount


# ── WB Stocks ─────────────────────────────────────────────────────────────────

def insert_wb_stocks_raw(rows: list[dict[str, Any]], snapped_at: str) -> int:
    if not rows:
        return 0
    values = []
    for r in rows:
        nm_id        = r.get("nmId")        or r.get("nm_id")
        chrt_id      = r.get("chrtId")      or r.get("chrt_id")
        warehouse_id = r.get("warehouseId") or r.get("warehouse_id")
        if nm_id and chrt_id and warehouse_id:
            values.append((nm_id, chrt_id, warehouse_id, json.dumps(r, ensure_ascii=False), snapped_at))
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_RAW)
            execute_values(
                cur,
                """
                INSERT INTO wb_stocks_raw (nm_id, chrt_id, warehouse_id, payload, snapped_at)
                VALUES %s
                """,
                values,
                page_size=1000,
            )
            return len(values)


def upsert_wb_stocks_snap(rows: list[dict[str, Any]], snapped_at: str) -> int:
    if not rows:
        return 0
    cols = (
        "nm_id", "chrt_id", "warehouse_id",
        "warehouse_name", "region_name",
        "quantity", "in_way_to_client", "in_way_from_client",
        "snapped_at",
    )
    values = []
    for r in rows:
        if not (r.get("nm_id") and r.get("chrt_id") and r.get("warehouse_id")):
            continue
        values.append(tuple([r.get(c) for c in cols[:-1]] + [snapped_at]))
    if not values:
        return 0
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in cols
        if c not in ("nm_id", "chrt_id", "warehouse_id")
    ) + ", updated_at = NOW()"
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_SNAP)
            execute_values(
                cur,
                f"""
                INSERT INTO wb_stocks_snap ({', '.join(cols)}) VALUES %s
                ON CONFLICT (nm_id, chrt_id, warehouse_id) DO UPDATE SET {update_set}
                """,
                values,
                page_size=1000,
            )
            return len(values)


def cleanup_wb_stocks_raw(retention_days: int = 30) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_RAW)
            cur.execute(
                "DELETE FROM wb_stocks_raw WHERE snapped_at < NOW() - INTERVAL '1 day' * %s",
                (retention_days,),
            )
            return cur.rowcount


# ── Job runs ──────────────────────────────────────────────────────────────────

def insert_job_run(
    *,
    job_name:        str,
    started_at_iso:  str,
    finished_at_iso: str,
    status:          str,
    api_rows:        int           = 0,
    raw_new_versions: int          = 0,
    norm_upserted:   int           = 0,
    duplicates:      int           = 0,
    dup_pct:         float         = 0.0,
    cursor_old:      Optional[str] = None,
    cursor_used:     Optional[str] = None,
    cursor_new:      Optional[str] = None,
    error:           Optional[str] = None,
) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_RUNS)
            cur.execute(
                """
                INSERT INTO job_runs
                    (job_name, started_at, finished_at, status,
                     api_rows, raw_new, norm_upserted, duplicates, dup_pct,
                     cursor_old, cursor_used, cursor_new, error)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    job_name, started_at_iso, finished_at_iso, status,
                    api_rows, raw_new_versions, norm_upserted, duplicates, dup_pct,
                    cursor_old, cursor_used, cursor_new, error,
                ),
            )


def get_last_dup_pct(job_name: str) -> Optional[float]:
    with connect() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT dup_pct FROM job_runs
                    WHERE job_name = %s AND status = 'ok'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (job_name,),
                )
                row = cur.fetchone()
                return float(row[0]) if row else None
            except Exception:
                conn.rollback()
                return None


# ── WB Adv Campaigns (V8) ─────────────────────────────────────────────────────

def upsert_wb_adv_campaigns(items: list[dict]) -> int:
    """UPSERT кампаний из /adv/v1/promotion/count."""
    if not items:
        return 0
    records = [
        (
            r["advert_id"],
            r["status"],
            r.get("type"),
            r.get("change_time"),
            json.dumps(r.get("payload", {}), ensure_ascii=False),
        )
        for r in items
        if r.get("advert_id")
    ]
    if not records:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO wb_adv_campaigns
                    (advert_id, status, type, change_time, payload, fetched_at, updated_at)
                VALUES %s
                ON CONFLICT (advert_id) DO UPDATE SET
                    status      = EXCLUDED.status,
                    type        = EXCLUDED.type,
                    change_time = EXCLUDED.change_time,
                    payload     = EXCLUDED.payload,
                    fetched_at  = NOW(),
                    updated_at  = NOW()
                """,
                records,
                template="(%s, %s, %s, %s, %s::jsonb, NOW(), NOW())",
            )
            return cur.rowcount if cur.rowcount >= 0 else len(records)


def get_adv_campaign_ids(statuses: list[int] | None = None) -> list[int]:
    """
    Список advert_id из wb_adv_campaigns.
    Сортировка: change_time DESC NULLS LAST — свежие РК идут в первые батчи.
    """
    if statuses is None:
        statuses = [7, 9, 11]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT advert_id
                FROM   wb_adv_campaigns
                WHERE  status = ANY(%s)
                ORDER  BY change_time DESC NULLS LAST, advert_id
                """,
                (statuses,),
            )
            return [row[0] for row in cur.fetchall()]


# ── WB Adv Fullstats (V8 + V9) ───────────────────────────────────────────────

def atomic_save_fullstats(
    raw_items:  list[dict],
    begin_date: str,
    end_date:   str,
    norm_rows:  list[dict],
) -> tuple[int, int]:
    """
    Атомарно в одной транзакции:
      1. TRUNCATE wb_adv_fullstats_raw, wb_adv_fullstats_norm
      2. INSERT raw  — сырые ответы API
      3. INSERT norm — все метрики (spend, views, clicks, atbs, orders, shks, sum_price)

    Возвращает (raw_count, norm_count).
    При ошибке — ROLLBACK, старые данные в БД остаются целы.
    """
    raw_records = [
        (
            int(r["advertId"]),
            begin_date,
            end_date,
            json.dumps(r, ensure_ascii=False),
        )
        for r in raw_items
        if r.get("advertId")
    ]

    norm_records = [
        (
            r["advert_id"],
            r["nm_id"],
            r["stat_date"],
            r["spend_rub"],
            r["views"],
            r["clicks"],
            r["atbs"],
            r["orders"],
            r["shks"],
            r["sum_price"],
        )
        for r in norm_rows
        if r.get("advert_id") and r.get("nm_id") and r.get("stat_date")
    ]

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE wb_adv_fullstats_raw, wb_adv_fullstats_norm")

            if raw_records:
                execute_values(
                    cur,
                    """
                    INSERT INTO wb_adv_fullstats_raw
                        (advert_id, begin_date, end_date, payload, fetched_at)
                    VALUES %s
                    """,
                    raw_records,
                    template="(%s, %s, %s, %s::jsonb, NOW())",
                    page_size=500,
                )

            if norm_records:
                execute_values(
                    cur,
                    """
                    INSERT INTO wb_adv_fullstats_norm
                        (advert_id, nm_id, stat_date,
                         spend_rub, views, clicks, atbs, orders, shks, sum_price,
                         updated_at)
                    VALUES %s
                    """,
                    norm_records,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                    page_size=1000,
                )

    return len(raw_records), len(norm_records)
