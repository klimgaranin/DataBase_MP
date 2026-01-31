from __future__ import annotations

import os
import re
import json
from datetime import datetime
from typing import Dict, Optional, Any, Iterable, List, Tuple

import psycopg2
from psycopg2.extras import execute_values


# ---------------------------------------------------------------------
# DSN
# ---------------------------------------------------------------------

def get_dsn() -> str:
    """
    ЕДИНАЯ точка получения DSN.
    Поддерживаем PG_DSN и старый PGDSN (на переходный период).
    """
    dsn = (os.getenv("PG_DSN") or "").strip()
    if dsn:
        return dsn

    dsn = (os.getenv("PGDSN") or "").strip()
    if dsn:
        return dsn

    return "postgresql://app:app_password@localhost:5432/marketplace"


# ---------------------------------------------------------------------
# Safety helpers (защита от SQL-инъекций через имена таблиц)
# ---------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\.]*$")


def _assert_safe_identifier(name: str) -> str:
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


# ---------------------------------------------------------------------
# WB time parsing
# WB: если TZ не указан — считаем Московское время (UTC+3)
# ---------------------------------------------------------------------

def _ensure_wb_tz(iso_str: str) -> str:
    s = (iso_str or "").strip()
    if not s:
        return s

    if s.endswith("Z"):
        return s  # UTC

    # уже есть +03:00 / -05:00 (RFC3339)
    if len(s) >= 6 and (s[-6] in ["+", "-"]) and s[-3] == ":":
        return s

    # нет TZ -> считаем как MSK (UTC+3)
    return s + "+03:00"


def _parse_wb_dt(iso_str: str) -> Optional[datetime]:
    s = _ensure_wb_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ---------------------------------------------------------------------
# Cursor fetch helpers (чтобы Pylance НЕ ругался никогда)
# ---------------------------------------------------------------------

def _fetchone_bool(cur) -> bool:
    """
    Безопасно достаёт bool из SELECT ...;
    Никогда не делает row[0] без проверки row is None.
    """
    row = cur.fetchone()
    if row is None:
        return False
    # row обычно tuple длины 1
    try:
        return bool(row[0])
    except Exception:
        return False


def _fetchone_scalar(cur) -> Optional[Any]:
    """
    Возвращает первое поле первой строки или None.
    """
    row = cur.fetchone()
    if row is None:
        return None
    try:
        return row[0]
    except Exception:
        return None


# ---------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------

def insert_raw(table: str, payload: dict) -> None:
    """
    Универсальная вставка raw-json (в таблицу с колонкой payload jsonb).
    """
    table = _assert_safe_identifier(table)
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"insert into {table} (payload) values (%s::jsonb)",
                [json.dumps(payload, ensure_ascii=False)],
            )


def _chunks(lst: List[Tuple[Any, ...]], size: int) -> Iterable[List[Tuple[Any, ...]]]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def insert_wb_orders_raw_dedup(rows: List[dict], page_size: int = 1000) -> int:
    """
    Возвращает сколько реально вставилось новых версий (dedup).
    Dedup: primary key (srid, last_change_ts) + ON CONFLICT DO NOTHING.
    """
    if not rows:
        return 0

    values: List[Tuple[str, datetime, str]] = []
    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue

        lch = r.get("lastChangeDate") or r.get("date")
        dt = _parse_wb_dt(str(lch)) if lch else None
        if not dt:
            continue

        values.append((str(srid), dt, json.dumps(r, ensure_ascii=False)))

    if not values:
        return 0

    sql = """
    insert into wb_orders_raw_dedup (srid, last_change_ts, payload)
    values %s
    on conflict (srid, last_change_ts) do nothing
    """

    inserted_total = 0
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            for page in _chunks(values, page_size):
                execute_values(cur, sql, page, page_size=len(page))
                # rowcount у psycopg2 для INSERT обычно корректный
                inserted_total += int(cur.rowcount)

    return inserted_total


def cleanup_wb_orders_raw_dedup(days: int = 14) -> int:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from wb_orders_raw_dedup where loaded_at < now() - (%s || ' days')::interval;",
                [days],
            )
            return int(cur.rowcount)


def upsert_wb_orders_norm(rows: List[dict]) -> None:
    if not rows:
        return

    cols = [
        "srid", "is_cancel", "date_ts", "last_change_ts",
        "warehouse_name", "warehouse_type", "country_name", "oblast_okrug_name", "region_name",
        "supplier_article", "nm_id", "barcode", "category", "subject", "brand", "tech_size",
        "income_id", "is_supply", "is_realization", "total_price", "discount_percent", "spp",
        "finished_price", "price_with_disc", "cancel_date", "sticker", "g_number"
    ]

    values = [[r.get(c) for c in cols] for r in rows]

    sql = f"""
    insert into wb_orders_norm ({",".join(cols)})
    values %s
    on conflict (srid) do update set
      is_cancel=excluded.is_cancel,
      date_ts=excluded.date_ts,
      last_change_ts=excluded.last_change_ts,
      warehouse_name=excluded.warehouse_name,
      warehouse_type=excluded.warehouse_type,
      country_name=excluded.country_name,
      oblast_okrug_name=excluded.oblast_okrug_name,
      region_name=excluded.region_name,
      supplier_article=excluded.supplier_article,
      nm_id=excluded.nm_id,
      barcode=excluded.barcode,
      category=excluded.category,
      subject=excluded.subject,
      brand=excluded.brand,
      tech_size=excluded.tech_size,
      income_id=excluded.income_id,
      is_supply=excluded.is_supply,
      is_realization=excluded.is_realization,
      total_price=excluded.total_price,
      discount_percent=excluded.discount_percent,
      spp=excluded.spp,
      finished_price=excluded.finished_price,
      price_with_disc=excluded.price_with_disc,
      cancel_date=excluded.cancel_date,
      sticker=excluded.sticker,
      g_number=excluded.g_number,
      loaded_at=now()
    """

    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)


def get_job_cursor(job_name: str) -> Optional[str]:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select cursor from job_state where job_name=%s", [job_name])
            val = _fetchone_scalar(cur)
            return str(val) if val is not None else None


def set_job_cursor(job_name: str, cursor: str) -> None:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into job_state (job_name, cursor, updated_at)
                values (%s, %s, now())
                on conflict (job_name) do update set
                  cursor=excluded.cursor,
                  updated_at=now()
                """,
                [job_name, cursor],
            )


def insert_job_run(
    job_name: str,
    started_at_iso: str,
    finished_at_iso: str,
    status: str,
    api_rows: int,
    raw_new_versions: int,
    norm_upserted: int,
    duplicates: int,
    dup_pct: float,
    cursor_old: Optional[str],
    cursor_used: Optional[str],
    cursor_new: Optional[str],
    error: Optional[str],
) -> None:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into job_runs (
                  job_name, started_at, finished_at, status,
                  api_rows, raw_new_versions, norm_upserted,
                  duplicates, dup_pct,
                  cursor_old, cursor_used, cursor_new, error
                )
                values (%s, %s::timestamptz, %s::timestamptz, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s)
                """,
                [
                    job_name, started_at_iso, finished_at_iso, status,
                    int(api_rows), int(raw_new_versions), int(norm_upserted),
                    int(duplicates), float(dup_pct),
                    cursor_old, cursor_used, cursor_new, error
                ],
            )


# ---------------------------------------------------------------------
# Advisory lock (КРИТИЧЕСКИ ВАЖНО)
# Lock сессионный -> держим ОТКРЫТОЕ соединение до конца job
# ---------------------------------------------------------------------

_LOCK_CONNS: Dict[int, psycopg2.extensions.connection] = {}


def try_advisory_lock(lock_id: int) -> bool:
    """
    Берём pg_try_advisory_lock(lock_id) и ДЕРЖИМ соединение открытым.
    Если соединение закрыть — lock пропадает.
    """
    if lock_id in _LOCK_CONNS:
        return True

    dsn = get_dsn()
    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(%s);", [lock_id])
            ok = _fetchone_bool(cur)

        if ok:
            _LOCK_CONNS[lock_id] = conn
            return True

        conn.close()
        return False

    except Exception:
        conn.close()
        raise


def advisory_unlock(lock_id: int) -> None:
    """
    Освобождаем lock тем же соединением, на котором брали.
    Если соединения нет — best-effort unlock (не валим job).
    """
    conn = _LOCK_CONNS.pop(lock_id, None)

    if conn is None:
        # best-effort
        try:
            dsn = get_dsn()
            tmp = psycopg2.connect(dsn)
            tmp.autocommit = True
            try:
                with tmp.cursor() as cur:
                    cur.execute("select pg_advisory_unlock(%s);", [lock_id])
                    _ = _fetchone_bool(cur)
            finally:
                tmp.close()
        except Exception:
            pass
        return

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_unlock(%s);", [lock_id])
            _ = _fetchone_bool(cur)
    finally:
        conn.close()
