# Project review bundle

Root: C:\Програмирование\Проекты\DataBase_MP
Generated: 2026-01-30 20:58:22
MaxFileBytes: 524288

## File list

- app/clients/wb_statistics.py
- app/db.py
- app/jobs_cleanup_raw.py
- app/jobs_wb_orders_backfill.py
- app/jobs_wb_orders_raw.py
- app/jobs_wb_orders_raw_norm.py
- app/main.py
- app/requirements.txt
- app/wb_normalize.py
- infra/docker-compose.yml
- infra/docker-compose.yml.txt
- migrations/V1__init_raw.sql
- migrations/V2__wb_orders_norm.sql
- migrations/V3__raw_retention_index.sql
- migrations/V4__job_state.sql
- migrations/V5__wb_orders_raw_dedup.sql
- migrations/V6__job_runs.sql
- migrations/V7__job_runs_dups.sql
- requirements.txt
- run_hidden.vbs
- run_wb_orders.cmd
- run_wb_orders_hidden.ps1
- tools/make_review_bundle.ps1
- логи для чата.txt
- логи после двух дней.txt

## Files content

---
### app/clients/wb_statistics.py

```python
import os
import time
from collections.abc import Iterator, Mapping
from typing import Any, Tuple, cast

import requests

BASE_URL = "https://statistics-api.wildberries.ru"

MAX_ROWS_PER_RESPONSE = 80000
DEFAULT_SLEEP_SEC = 61

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_READ_TIMEOUT_SEC = 60
DEFAULT_MAX_RETRIES = 5


def _get_token() -> str:
    token = (
        os.getenv("WB_TOKEN", "").strip()
        or os.getenv("WBTOKEN", "").strip()
        or os.getenv("WB_API_KEY", "").strip()
    )
    if not token:
        raise RuntimeError("WB_TOKEN не задан. Добавь WB_TOKEN=... в .env")
    return token


def _headers() -> dict[str, str]:
    return {"Authorization": _get_token()}


def _timeouts() -> tuple[int, int]:
    connect = int(os.getenv("WB_CONNECT_TIMEOUT_SEC", str(DEFAULT_CONNECT_TIMEOUT_SEC)))
    read = int(os.getenv("WB_READ_TIMEOUT_SEC", str(DEFAULT_READ_TIMEOUT_SEC)))
    return connect, read


def _max_retries() -> int:
    try:
        v = int(os.getenv("WB_HTTP_RETRIES", str(DEFAULT_MAX_RETRIES)))
        return max(0, min(v, 20))
    except Exception:
        return DEFAULT_MAX_RETRIES


def _parse_int_header(headers: Mapping[str, str], name: str) -> int | None:
    """
    headers у requests.Response — CaseInsensitiveDict (Mapping), не обычный dict.
    Поэтому тип — Mapping[str, str], чтобы Pylance не ругался. [web:236]
    """
    v = headers.get(name)
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _wait_seconds_for_429(headers: Mapping[str, str]) -> int:
    """
    Сначала пробуем X-Ratelimit-Retry, потом Retry-After, потом X-Ratelimit-Reset.
    """
    retry = _parse_int_header(headers, "X-Ratelimit-Retry")
    if retry is None:
        retry = _parse_int_header(headers, "Retry-After")

    reset = _parse_int_header(headers, "X-Ratelimit-Reset")

    candidates = [x for x in (retry, reset) if isinstance(x, int) and x > 0]
    return max(candidates) if candidates else 60


def _request_json(session: requests.Session, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    timeout = _timeouts()
    retries = _max_retries()

    for attempt in range(retries + 1):
        try:
            resp = session.get(
                url,
                headers=_headers(),
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as e:
            if attempt >= retries:
                raise
            wait = min(2 ** attempt, 30)
            print(f"[WB] network error, retry in {wait}s: {repr(e)}")
            time.sleep(wait)
            continue

        # resp.headers всегда есть и это Mapping (CaseInsensitiveDict), поэтому НЕ делаем `or {}`. [web:232]
        resp_headers = cast(Mapping[str, str], resp.headers)

        if resp.status_code == 429:
            wait = _wait_seconds_for_429(resp_headers)
            if attempt >= retries:
                resp.raise_for_status()
            print(f"[WB] 429 rate limit, wait {wait}s then retry")
            time.sleep(wait)
            continue

        if 500 <= resp.status_code <= 599:
            if attempt >= retries:
                resp.raise_for_status()
            wait = min(2 ** attempt, 60)
            print(f"[WB] {resp.status_code} server error, retry in {wait}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(f"WB unexpected response type: {type(data)}")

        # поджимаем типы для Pylance: ожидаем list[dict[str, Any]]
        return cast(list[dict[str, Any]], data)

    raise RuntimeError("WB request failed after retries")


def fetch_orders_page(
    date_from_iso: str,
    flag: int = 0,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    sess = session or requests.Session()
    url = f"{BASE_URL}/api/v1/supplier/orders"
    params: dict[str, Any] = {"dateFrom": date_from_iso, "flag": flag}
    return _request_json(sess, url, params)


def iter_orders(
    date_from_iso: str,
    flag: int = 0,
    sleep_sec: int = DEFAULT_SLEEP_SEC,
    session: requests.Session | None = None,
) -> Iterator[list[dict[str, Any]]]:
    sess = session or requests.Session()

    cursor = date_from_iso
    page = 1

    while True:
        print(f"[WB] page={page} dateFrom={cursor}")

        chunk = fetch_orders_page(cursor, flag=flag, session=sess)
        print(f"[WB] page={page} rows={len(chunk)}")

        yield chunk

        if not chunk:
            break

        last = chunk[-1]
        new_cursor = last.get("lastChangeDate") or last.get("date")

        if not new_cursor:
            print("[WB] stop: last record has no lastChangeDate/date")
            break

        if len(chunk) < MAX_ROWS_PER_RESPONSE:
            break

        if str(new_cursor) == str(cursor):
            print(f"[WB] stop: cursor not advancing (new_cursor={new_cursor})")
            break

        cursor = str(new_cursor)
        page += 1

        time.sleep(int(sleep_sec))


def fetch_orders_incremental(
    date_from_iso: str,
    flag: int = 0,
    sleep_sec: int = DEFAULT_SLEEP_SEC,
) -> Tuple[list[dict[str, Any]], str]:
    all_rows: list[dict[str, Any]] = []
    last_cursor_seen = date_from_iso

    with requests.Session() as sess:
        for chunk in iter_orders(date_from_iso, flag=flag, sleep_sec=sleep_sec, session=sess):
            if not chunk:
                break

            all_rows.extend(chunk)

            last = chunk[-1]
            c = last.get("lastChangeDate") or last.get("date")
            if c:
                last_cursor_seen = str(c)

    return all_rows, last_cursor_seen
```

---
### app/db.py

```python
import os
import json
import psycopg2
from psycopg2.extras import execute_values


def get_dsn() -> str:
    return os.getenv("PG_DSN", "postgresql://app:app_password@localhost:5432/marketplace")


def insert_raw(table: str, payload: dict) -> None:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"insert into {table} (payload) values (%s::jsonb)",
                [json.dumps(payload, ensure_ascii=False)]
            )


def _chunks(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def insert_wb_orders_raw_dedup(rows: list[dict], page_size: int = 1000) -> int:
    """
    Возвращает сколько РЕАЛЬНО вставилось новых версий (dedup).
    Dedup: primary key (srid, last_change_ts) + ON CONFLICT DO NOTHING. [web:314]
    """
    if not rows:
        return 0

    values = []
    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue
        lch = r.get("lastChangeDate") or r.get("date")
        if not lch:
            continue
        values.append((str(srid), str(lch), json.dumps(r, ensure_ascii=False)))

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
                inserted_total += cur.rowcount

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


def upsert_wb_orders_norm(rows: list[dict]) -> None:
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


def get_job_cursor(job_name: str) -> str | None:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select cursor from job_state where job_name=%s", [job_name])
            row = cur.fetchone()
            return row[0] if row else None


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
    cursor_old: str | None,
    cursor_used: str | None,
    cursor_new: str | None,
    error: str | None,
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


def try_advisory_lock(lock_id: int) -> bool:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(%s);", [lock_id])
            return bool(cur.fetchone()[0])


def advisory_unlock(lock_id: int) -> None:
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_unlock(%s);", [lock_id])
```

---
### app/jobs_cleanup_raw.py

```python
from dotenv import load_dotenv
import psycopg2
from db import get_dsn

def cleanup_wb_orders_raw(days: int = 14):
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from wb_orders_raw where loaded_at < now() - (%s || ' days')::interval;",
                [days],
            )
            print(f"[CLEANUP] deleted_rows={cur.rowcount}")

if __name__ == "__main__":
    load_dotenv()
    cleanup_wb_orders_raw(14)
```

---
### app/jobs_wb_orders_backfill.py

```python
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from clients.wb_statistics import fetch_orders_page
from db import (
    insert_wb_orders_raw_dedup,
    cleanup_wb_orders_raw_dedup,
    upsert_wb_orders_norm,
    set_job_cursor,
    try_advisory_lock,
    advisory_unlock,
)
from wb_normalize import normalize_wb_order

JOB_NAME_MAIN = "wb_orders"
JOB_NAME_BACKFILL = "wb_orders_backfill"
LOCK_ID = 4242001

MAX_ROWS_PER_RESPONSE = 80000  # условный лимит при flag=0 [web:197]
SLEEP_SEC = 61                 # 1 запрос в минуту [web:197]


def ensure_tz(iso_str: str) -> str:
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    if len(s) >= 6 and (s[-6] in ["+", "-"]) and s[-3] == ":":
        return s
    return s + "+03:00"


def add_months(dt: datetime, months: int) -> datetime:
    y = dt.year
    m = dt.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return dt.replace(year=y, month=m)


def calc_start_cursor_iso() -> str:
    tz = timezone(timedelta(hours=3))
    now = datetime.now(tz)
    start = add_months(now, -2).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


def run_backfill_full():
    load_dotenv()

    cursor = calc_start_cursor_iso()
    print(f"[PIZ] start_cursor={cursor}")

    total_rows = 0
    total_norm = 0
    pages = 0

    import time

    while True:
        pages += 1
        print(f"[PIZ] page={pages} dateFrom={cursor}")

        chunk = fetch_orders_page(cursor, flag=0)
        print(f"[PIZ] page={pages} rows={len(chunk)}")

        if not chunk:
            print("[PIZ] done: got empty []")
            break

        insert_wb_orders_raw_dedup(chunk)
        total_rows += len(chunk)

        norm_rows = [normalize_wb_order(r) for r in chunk if r.get("srid")]
        upsert_wb_orders_norm(norm_rows)
        total_norm += len(norm_rows)

        last = chunk[-1]
        new_cursor = last.get("lastChangeDate") or last.get("date")
        if not new_cursor:
            print("[PIZ] stop: last record has no lastChangeDate/date")
            break

        new_cursor = ensure_tz(new_cursor)

        print(f"[PIZ] totals: raw_seen={total_rows} norm_upserted={total_norm} new_cursor={new_cursor}")

        set_job_cursor(JOB_NAME_BACKFILL, new_cursor)
        set_job_cursor(JOB_NAME_MAIN, new_cursor)

        # Финальная страница: МЕНЬШЕ лимита => можно завершать без лишнего запроса. [web:197]
        if len(chunk) < MAX_ROWS_PER_RESPONSE:
            print("[PIZ] stop: rows < 80000 (final page likely reached)")
            break

        # Иначе строк >= лимита: возможно есть продолжение
        if new_cursor == cursor:
            print("[PIZ] stop: cursor not advancing")
            break

        cursor = new_cursor
        time.sleep(SLEEP_SEC)

    deleted = cleanup_wb_orders_raw_dedup(14)
    print(f"[PIZ] cleanup_raw_dedup_deleted={deleted}")


if __name__ == "__main__":
    load_dotenv()

    if not try_advisory_lock(LOCK_ID):
        print("[PIZ] skip: lock busy (hourly job or previous run is active)")
        raise SystemExit(0)

    try:
        run_backfill_full()
    finally:
        advisory_unlock(LOCK_ID)
```

---
### app/jobs_wb_orders_raw.py

```python
from dotenv import load_dotenv
from clients.wb_statistics import iter_orders
from db import insert_raw

def run(date_from_iso: str):
    load_dotenv()

    total = 0
    for chunk in iter_orders(date_from_iso, flag=0):
        for row in chunk:
            insert_raw("wb_orders_raw", row)
        total += len(chunk)
        print(f"[JOB] inserted_total={total}")

    print(f"[JOB] done. total_inserted={total}")

if __name__ == "__main__":
    # Для теста бери небольшую дату, чтобы не ждать очень долго
    run("2026-01-16T00:00:00+03:00")
```

---
### app/jobs_wb_orders_raw_norm.py

```python
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv

from clients.wb_statistics import iter_orders
from db import (
    insert_wb_orders_raw_dedup,
    cleanup_wb_orders_raw_dedup,
    upsert_wb_orders_norm,
    get_job_cursor,
    set_job_cursor,
    insert_job_run,
    try_advisory_lock,
    advisory_unlock,
)
from wb_normalize import normalize_wb_order

JOB_NAME = "wb_orders"
LOCK_ID = 4242001


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_tz(iso_str: str) -> str:
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # already has +03:00 / -05:00
    if len(s) >= 6 and (s[-6] in ["+", "-"]) and s[-3] == ":":
        return s
    return s + "+03:00"


def parse_iso_dt(iso_str: str) -> datetime | None:
    s = ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def get_pg_dsn() -> str:
    # В проекте исторически используется PGDSN (см. db.py), но поддержим и PG_DSN.
    dsn = (os.getenv("PGDSN") or "").strip()
    if dsn:
        return dsn
    dsn = (os.getenv("PG_DSN") or "").strip()
    if dsn:
        return dsn
    return "postgresql://app:apppassword@localhost:5432/marketplace"


def get_last_dup_pct(job_name: str) -> float | None:
    """
    Берём duppct (в процентах) из последнего jobruns по startedat.
    Если БД недоступна или таблица/колонка ещё не создана — вернём None (job не падает).
    """
    dsn = get_pg_dsn()
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select duppct
                    from jobruns
                    where jobname = %s
                    order by startedat desc
                    limit 1
                    """,
                    (job_name,),
                )
                row = cur.fetchone()
                if not row or row[0] is None:
                    return None
                return float(row[0])
    except Exception:
        return None


def calc_lookback_minutes(cursor_old_iso: str, last_dup_pct: float | None) -> int:
    """
    Авто-настройка окна догрузки (lookback), чтобы:
    - не потерять “догоняющие” изменения WB;
    - держать дубли под контролем.

    Настройки:
    - WB_LOOKBACK_MINUTES: базовое окно (дефолт 10)
    - WB_LOOKBACK_MAX_MINUTES: верхняя граница авто-увеличения (дефолт max(base, 20))
    """
    base = int(os.getenv("WB_LOOKBACK_MINUTES", "10"))
    base = max(0, min(base, 120))

    max_auto = int(os.getenv("WB_LOOKBACK_MAX_MINUTES", str(max(base, 20))))
    max_auto = max(0, min(max_auto, 240))

    dt_old = parse_iso_dt(cursor_old_iso)
    if not dt_old:
        return base

    now_same_tz = datetime.now(dt_old.tzinfo or timezone.utc)
    gap_min = max(int((now_same_tz - dt_old).total_seconds() // 60), 0)

    # 1) Базовая логика по gap: чем ближе к real-time, тем меньше окно
    if gap_min <= 20:
        look = min(base, 2)
    elif gap_min <= 60:
        look = min(base, 5)
    elif gap_min <= 360:
        look = min(base, 10)
    else:
        look = min(max_auto, max(base, 15))

    # 2) Подстройка по прошлому duppct (в процентах)
    if last_dup_pct is not None:
        if last_dup_pct >= 30.0:
            look = min(look, 2)
        elif last_dup_pct >= 15.0:
            look = min(look, 5)
        elif last_dup_pct <= 3.0:
            look = min(max_auto, look + 5)
        elif last_dup_pct <= 7.0:
            look = min(max_auto, look + 2)

    return max(0, int(look))


def apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = parse_iso_dt(cursor_old_iso)
    if not dt:
        return ensure_tz(cursor_old_iso)
    dt2 = dt - timedelta(minutes=max(0, minutes))
    return dt2.replace(microsecond=0).isoformat()


def max_cursor_from_rows(rows: list[dict], fallback: str) -> str:
    best_dt: datetime | None = None
    best_raw: str | None = None

    for r in rows:
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = parse_iso_dt(str(c))
        if not dt:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_raw = str(c)

    return ensure_tz(best_raw) if best_raw else ensure_tz(fallback)


def dedupe_for_norm(rows: list[dict]) -> list[dict]:
    """
    В рамках одной страницы оставляем по srid самую свежую версию.
    Это уменьшает лишние upsert'ы в NORM, не меняя смысл данных.
    """
    best_by_srid: dict[str, tuple[datetime, dict]] = {}

    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue

        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue

        dt = parse_iso_dt(str(c))
        if not dt:
            continue

        key = str(srid)
        prev = best_by_srid.get(key)
        if prev is None or dt > prev[0]:
            best_by_srid[key] = (dt, r)

    return [pair[1] for pair in best_by_srid.values()]


if __name__ == "__main__":
    load_dotenv()

    if not try_advisory_lock(LOCK_ID):
        print("[JOB] skip: previous run still active (lock busy)")
        raise SystemExit(0)

    started_at = now_utc_iso()

    cursor_old: str | None = None
    cursor_used: str | None = None
    cursor_new: str | None = None

    api_rows = 0
    raw_new_versions = 0
    norm_upserted = 0

    status = "ok"
    error: str | None = None

    try:
        cursor_old = get_job_cursor(JOB_NAME)

        # Первый запуск: 3 дня назад (MSK)
        if not cursor_old:
            tz_msk = timezone(timedelta(hours=3))
            cursor_old = (datetime.now(tz_msk) - timedelta(days=3)).replace(microsecond=0).isoformat()

        last_dup_pct = get_last_dup_pct(JOB_NAME)
        lookback_min = calc_lookback_minutes(cursor_old, last_dup_pct=last_dup_pct)
        cursor_used = apply_lookback(cursor_old, minutes=lookback_min)

        print(f"[JOB] cursor_old={cursor_old} lookback_min={lookback_min} cursor_used={cursor_used}")

        cursor_max_seen = cursor_old

        with requests.Session() as sess:
            for chunk in iter_orders(cursor_used, flag=0, session=sess):
                if not chunk:
                    break

                api_rows += len(chunk)

                # RAW dedup
                raw_new_versions += insert_wb_orders_raw_dedup(chunk)

                # NORM upsert (схлопываем по srid в рамках страницы)
                compact = dedupe_for_norm(chunk)
                norm_rows = [normalize_wb_order(r) for r in compact]
                upsert_wb_orders_norm(norm_rows)
                norm_upserted += len(norm_rows)

                # max cursor по данным
                cursor_max_seen = max_cursor_from_rows(chunk, fallback=cursor_max_seen)

        cursor_new = ensure_tz(cursor_max_seen)

        # курсор не откатываем назад
        dt_old = parse_iso_dt(cursor_old)
        dt_new = parse_iso_dt(cursor_new)
        if dt_old and dt_new and dt_new < dt_old:
            print(f"[JOB] cursor_new would go backwards, keep cursor_old (cursor_new={cursor_new})")
            cursor_new = cursor_old

        duplicates = max(api_rows - raw_new_versions, 0)
        dup_pct = (duplicates / api_rows) if api_rows else 0.0

        print(
            f"[REPORT] api_rows={api_rows} raw_new_versions={raw_new_versions} "
            f"duplicates={duplicates} dup_pct={dup_pct:.2%} "
            f"norm_upserted={norm_upserted} cursor_old={cursor_old} "
            f"cursor_used={cursor_used} cursor_new={cursor_new}"
        )

        if cursor_new:
            set_job_cursor(JOB_NAME, cursor_new)

        deleted = cleanup_wb_orders_raw_dedup(14)
        print(f"[JOB] cleanup_raw_dedup_deleted={deleted}")

    except Exception as e:
        status = "fail"
        error = repr(e)
        print(f"[JOB] ERROR: {error}")

    finally:
        finished_at = now_utc_iso()
        try:
            duplicates = max(api_rows - raw_new_versions, 0)
            dup_pct = (duplicates / api_rows) if api_rows else 0.0

            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=api_rows,
                raw_new_versions=raw_new_versions,
                norm_upserted=norm_upserted,
                duplicates=duplicates,
                dup_pct=dup_pct * 100.0,
                cursor_old=cursor_old,
                cursor_used=cursor_used,
                cursor_new=cursor_new,
                error=error,
            )
        finally:
            advisory_unlock(LOCK_ID)
```

---
### app/main.py

```python
from dotenv import load_dotenv
from db import insert_raw

def main():
    load_dotenv()

    fake_order = {
        "order_id": "TEST-1",
        "created_at": "2026-01-17T02:35:00+03:00",
        "items": [{"sku": "SKU-123", "qty": 1, "price": 1000}],
    }

    insert_raw("wb_orders_raw", fake_order)
    print("OK: inserted 1 row into wb_orders_raw")

if __name__ == "__main__":
    main()
```

---
### app/requirements.txt

```txt
psycopg2-binary==2.9.9
python-dotenv==1.0.1
requests==2.32.3
```

---
### app/wb_normalize.py

```python
from __future__ import annotations
from datetime import datetime, date
from typing import Any, Optional

def parse_bool(v: Any) -> Optional[bool]:
    if v is None: return None
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"): return True
    if s in ("false", "0", "no"): return False
    return None

def parse_int(v: Any) -> Optional[int]:
    if v is None or v == "": return None
    try: return int(v)
    except: return None

def parse_float_ru(v: Any) -> Optional[float]:
    if v is None or v == "": return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try: return float(s)
    except: return None

def parse_date(v: Any) -> Optional[date]:
    if v is None or v == "": return None
    s = str(v).strip()
    # WB часто отдаёт ISO; но на вход может попасть "YYYY-MM-DD ..."
    s = s.replace(" ", "T")
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except:
        return None

def parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "": return None
    s = str(v).strip().replace(" ", "T")
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except:
        return None

def normalize_wb_order(row: dict) -> dict:
    return {
        "srid": str(row.get("srid") or ""),

        "is_cancel": parse_bool(row.get("isCancel")),
        "date_ts": parse_dt(row.get("date")),
        "last_change_ts": parse_dt(row.get("lastChangeDate")),

        "warehouse_name": row.get("warehouseName"),
        "warehouse_type": row.get("warehouseType"),
        "country_name": row.get("countryName"),
        "oblast_okrug_name": row.get("oblastOkrugName"),
        "region_name": row.get("regionName"),

        "supplier_article": row.get("supplierArticle"),
        "nm_id": parse_int(row.get("nmId")),
        "barcode": row.get("barcode"),

        "category": row.get("category"),
        "subject": row.get("subject"),
        "brand": row.get("brand"),
        "tech_size": row.get("techSize"),

        "income_id": parse_int(row.get("incomeID")),
        "is_supply": parse_bool(row.get("isSupply")),
        "is_realization": parse_bool(row.get("isRealization")),

        "total_price": parse_float_ru(row.get("totalPrice")),
        "discount_percent": parse_int(row.get("discountPercent")),
        "spp": parse_int(row.get("spp")),
        "finished_price": parse_float_ru(row.get("finishedPrice")),
        "price_with_disc": parse_float_ru(row.get("priceWithDisc")),

        "cancel_date": parse_date(row.get("cancelDate")),

        "sticker": row.get("sticker"),
        "g_number": row.get("gNumber"),
    }
```

---
### infra/docker-compose.yml

```yaml
services:
  db:
    image: postgres:16
    restart: unless-stopped
    environment:
      POSTGRES_DB: marketplace
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app_password
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

volumes:
  pg_data:
```

---
### infra/docker-compose.yml.txt

```txt
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_DB: marketplace
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app_password
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

volumes:
  pg_data:
```

---
### migrations/V1__init_raw.sql

```sql
create table if not exists wb_orders_raw (
  id bigserial primary key,
  loaded_at timestamptz not null default now(),
  payload jsonb not null
);

create index if not exists idx_wb_orders_raw_loaded_at
  on wb_orders_raw (loaded_at);

create table if not exists ozon_orders_raw (
  id bigserial primary key,
  loaded_at timestamptz not null default now(),
  payload jsonb not null
);

create index if not exists idx_ozon_orders_raw_loaded_at
  on ozon_orders_raw (loaded_at);
```

---
### migrations/V2__wb_orders_norm.sql

```sql
create table if not exists wb_orders_norm (
  srid text primary key,

  is_cancel boolean,
  date_ts timestamptz,
  last_change_ts timestamptz,

  warehouse_name text,
  warehouse_type text,
  country_name text,
  oblast_okrug_name text,
  region_name text,

  supplier_article text,
  nm_id bigint,
  barcode text,

  category text,
  subject text,
  brand text,
  tech_size text,

  income_id bigint,
  is_supply boolean,
  is_realization boolean,

  total_price numeric(12,2),
  discount_percent int,
  spp int,
  finished_price numeric(12,2),
  price_with_disc numeric(12,2),

  cancel_date date,

  sticker text,
  g_number text,

  loaded_at timestamptz not null default now()
);

create index if not exists idx_wb_orders_norm_last_change_ts
  on wb_orders_norm (last_change_ts);

create index if not exists idx_wb_orders_norm_date_ts
  on wb_orders_norm (date_ts);

create index if not exists idx_wb_orders_norm_supplier_article
  on wb_orders_norm (supplier_article);
```

---
### migrations/V3__raw_retention_index.sql

```sql
create index if not exists idx_wb_orders_raw_loaded_at
  on wb_orders_raw (loaded_at);
```

---
### migrations/V4__job_state.sql

```sql
create table if not exists job_state (
  job_name text primary key,
  cursor text,
  updated_at timestamptz not null default now()
);
```

---
### migrations/V5__wb_orders_raw_dedup.sql

```sql
create table if not exists wb_orders_raw_dedup (
  srid text not null,
  last_change_ts timestamptz not null,
  payload jsonb not null,
  loaded_at timestamptz not null default now(),
  primary key (srid, last_change_ts)
);

create index if not exists idx_wb_orders_raw_dedup_loaded_at
  on wb_orders_raw_dedup (loaded_at);

create index if not exists idx_wb_orders_raw_dedup_last_change_ts
  on wb_orders_raw_dedup (last_change_ts);
```

---
### migrations/V6__job_runs.sql

```sql
create table if not exists job_runs (
  id bigserial primary key,
  job_name text not null,
  started_at timestamptz not null,
  finished_at timestamptz not null,
  status text not null,              -- ok / fail
  api_rows int not null default 0,
  raw_new_versions int not null default 0,
  norm_upserted int not null default 0,
  cursor_old text,
  cursor_used text,
  cursor_new text,
  error text
);

create index if not exists idx_job_runs_job_time
  on job_runs (job_name, started_at desc);
```

---
### migrations/V7__job_runs_dups.sql

```sql
alter table job_runs
  add column if not exists duplicates int not null default 0;

alter table job_runs
  add column if not exists dup_pct numeric(6,2) not null default 0;
```

---
### requirements.txt

SKIPPED (binary file detected: NUL byte)

---
### run_hidden.vbs

SKIPPED (binary file detected: NUL byte)

---
### run_wb_orders.cmd

```bat
@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM WB Orders runner (portable)
REM - ROOT вычисляется от расположения этого .cmd (работает из Планировщика)
REM - поднимает Postgres через docker compose
REM - ждёт готовность db через psql внутри контейнера
REM - запускает Python job в .venv
REM - всё пишет в logs_wb_orders.txt
REM ============================================================

REM Для нормальной UTF-8 печати из Python в лог (без кракозябр)
chcp 65001 >nul
set "PYTHONUTF8=1"

REM ROOT = папка, где лежит этот .cmd
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "LOG=%ROOT%\logs_wb_orders.txt"
set "COMPOSE_FILE=%ROOT%\infra\docker-compose.yml"
set "PY=%ROOT%\.venv\Scripts\python.exe"
set "JOB=%ROOT%\app\jobs_wb_orders_raw_norm.py"

echo ==== %date% %time% START ====>> "%LOG%"
echo ROOT="%ROOT%">> "%LOG%"

REM 0) Валидация файлов, чтобы ошибка была понятной
if not exist "%COMPOSE_FILE%" (
  echo [ERR] docker-compose file not found: "%COMPOSE_FILE%">> "%LOG%"
  echo ==== %date% %time% END FAIL: missing docker-compose ====>> "%LOG%"
  exit /b 2
)

if not exist "%PY%" (
  echo [ERR] venv python not found: "%PY%">> "%LOG%"
  echo Hint: create venv and install requirements in app\requirements.txt>> "%LOG%"
  echo ==== %date% %time% END FAIL: missing venv ====>> "%LOG%"
  exit /b 3
)

if not exist "%JOB%" (
  echo [ERR] job not found: "%JOB%">> "%LOG%"
  echo ==== %date% %time% END FAIL: missing job ====>> "%LOG%"
  exit /b 4
)

REM 1) Поднять docker compose (если Docker Engine не поднят, команда может упасть)
docker compose -f "%COMPOSE_FILE%" up -d >> "%LOG%" 2>&1

REM 2) Подождать готовность БД (до ~120 секунд)
set /a tries=24

:WAIT_DOCKER
docker compose -f "%COMPOSE_FILE%" ps >> "%LOG%" 2>&1
docker compose -f "%COMPOSE_FILE%" exec -T db psql -U app -d marketplace -P pager=off -c "select 1;" >> "%LOG%" 2>&1

if %errorlevel%==0 goto DO_JOB

set /a tries-=1
if %tries% LEQ 0 goto FAIL_DOCKER

timeout /t 5 /nobreak >nul
goto WAIT_DOCKER

:DO_JOB
cd /d "%ROOT%"

REM 3) Запуск основного джоба
"%PY%" "%JOB%" >> "%LOG%" 2>&1
set "JOB_EXIT=%errorlevel%"

if not "%JOB_EXIT%"=="0" (
  echo ==== %date% %time% END FAIL: job exit=%JOB_EXIT% ====>> "%LOG%"
  exit /b %JOB_EXIT%
)

echo ==== %date% %time% END OK ====>> "%LOG%"
exit /b 0

:FAIL_DOCKER
echo ==== %date% %time% END FAIL: docker/db not ready ====>> "%LOG%"
exit /b 1
```

---
### run_wb_orders_hidden.ps1

```powershell
﻿# C:\marketplace-etl\run_wb_orders_hidden.ps1
$ErrorActionPreference = "Stop"

$notifyLog = "C:\marketplace-etl\notify_wb_orders.log"
$cmd = "C:\marketplace-etl\run_wb_orders.cmd"

function Write-NotifyLog {
  param([string]$Message)
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -Path $notifyLog -Encoding UTF8 -Value "$ts $Message"
}

function Show-Toast {
  param(
    [string]$Title,
    [string]$Text
  )

  try {
    # Пример создания Toast через Windows Runtime типы [web:557][web:562]
    $xml = @"
<toast>
  <visual>
    <binding template="ToastGeneric">
      <text>$Title</text>
      <text>$Text</text>
    </binding>
  </visual>
</toast>
"@

    $doc = [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime]::New()
    $doc.LoadXml($xml)

    # AppId для PowerShell, чтобы CreateToastNotifier мог показать уведомление [web:557][web:560]
    $appId = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'

    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime]::CreateToastNotifier($appId).Show($doc)
  }
  catch {
    # Если Toast по какой-то причине не показался — это не должно ломать задачу
    Write-NotifyLog ("TOAST_FAIL: " + $_.Exception.Message)
  }
}

$sw = [System.Diagnostics.Stopwatch]::StartNew()

Write-NotifyLog "START"
Show-Toast -Title "WB Orders" -Text "Старт синхронизации"

try {
  & cmd.exe /c "`"$cmd`""
  $exit = $LASTEXITCODE

  $sw.Stop()

  if ($exit -eq 0) {
    Write-NotifyLog ("END OK in {0:n0}s" -f $sw.Elapsed.TotalSeconds)
    Show-Toast -Title "WB Orders" -Text ("Готово OK за {0:n0} сек." -f $sw.Elapsed.TotalSeconds)
    exit 0
  } else {
    Write-NotifyLog ("END FAIL exit={0} in {1:n0}s" -f $exit, $sw.Elapsed.TotalSeconds)
    Show-Toast -Title "WB Orders" -Text ("Ошибка. Код {0}. {1:n0} сек." -f $exit, $sw.Elapsed.TotalSeconds)
    exit $exit
  }
}
catch {
  $sw.Stop()
  Write-NotifyLog ("END EXCEPTION in {0:n0}s: {1}" -f $sw.Elapsed.TotalSeconds, $_.Exception.Message)
  Show-Toast -Title "WB Orders" -Text ("Исключение: " + $_.Exception.Message)
  exit 1
}
```

---
### tools/make_review_bundle.ps1

```powershell
param(
  [string]$Root = "",
  [string]$OutDir = "_review",
  [int64]$MaxFileBytes = 512KB
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Dir([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Set-TextUtf8NoBom([string]$path, [string]$text) {
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::WriteAllText($path, $text, $utf8NoBom)
}

function Add-TextUtf8NoBom([string]$path, [string]$text) {
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::AppendAllText($path, $text, $utf8NoBom)
}

function Get-LangTag([string]$path) {
  switch ([IO.Path]::GetExtension($path).ToLowerInvariant()) {
    ".py"   { "python" }
    ".ps1"  { "powershell" }
    ".cmd"  { "bat" }
    ".bat"  { "bat" }
    ".vbs"  { "vbscript" }
    ".yml"  { "yaml" }
    ".yaml" { "yaml" }
    ".sql"  { "sql" }
    ".md"   { "markdown" }
    ".txt"  { "txt" }
    ".json" { "json" }
    default { "" }
  }
}

function Test-HasNulByte([byte[]]$bytes) {
  foreach ($b in $bytes) { if ($b -eq 0) { return $true } }
  return $false
}

function ConvertFrom-BytesSmart([byte[]]$bytes) {
  $utf8Strict = [System.Text.UTF8Encoding]::new($false, $true)
  try {
    return $utf8Strict.GetString($bytes)
  } catch {
    $cp1251 = [System.Text.Encoding]::GetEncoding(1251)
    return $cp1251.GetString($bytes)
  }
}

function ConvertTo-RelNorm([string]$rel) {
  return ($rel -replace "\\", "/").TrimStart("/")
}

# Root берём от местоположения скрипта, чтобы не зависеть от кириллицы в литералах
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
} else {
  $Root = (Resolve-Path $Root).Path
}

$excludeDirs = @(
  ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache",
  "node_modules", ".idea", ".vscode",
  "pgdata", "data", "logs", $OutDir,
  "migrations/__pycache__", "tools/__pycache__"
) | ForEach-Object { ConvertTo-RelNorm $_ }

$excludePathMasks = @(
  ".env", ".env.*",

  "*.pyc", "*.pyo", "*.pyd",

  "*.log", "*.stderr.txt",
  "logs_*.txt", "log*.txt", "notify_*.txt", "notify_*.log",
  "*для чата*.txt", "*после двух дней*.txt",

  "backup_*.sql", "*.dump", "*.bak",

  "*.zip", "*.7z", "*.rar",
  "*.png", "*.jpg", "*.jpeg", "*.gif", "*.ico",
  "*.pdf", "*.mp4", "*.mov",

  "PROJECT_REVIEW.md", "CHECKSUMS.sha256"
)

$allowExt = @(
  ".py", ".ps1", ".cmd", ".bat", ".vbs",
  ".yml", ".yaml", ".json",
  ".sql",
  ".md",
  ".txt"
)

function Get-RelPath([string]$fullPath) {
  $rel = $fullPath.Substring($Root.Length).TrimStart("\", "/")
  return ConvertTo-RelNorm $rel
}

function Test-ExcludedRelPath([string]$relNorm) {
  foreach ($d in $excludeDirs) {
    if ($relNorm -ieq $d -or $relNorm.StartsWith($d + "/")) { return $true }
  }
  return $false
}

function Test-ExcludedByMask([string]$relNorm) {
  $name = [IO.Path]::GetFileName($relNorm)
  foreach ($m in $excludePathMasks) {
    if ($name -like $m) { return $true }
    if ($relNorm -like $m) { return $true }
  }
  return $false
}

function Test-AllowedExt([string]$fullPath) {
  $ext = [IO.Path]::GetExtension($fullPath).ToLowerInvariant()
  return $allowExt -contains $ext
}

if (-not (Test-Path -LiteralPath $Root)) {
  throw "Root folder not found: $Root"
}

$absOutDir = Join-Path $Root $OutDir
New-Dir $absOutDir

$outBundle = Join-Path $absOutDir "PROJECT_REVIEW.md"
$outSums   = Join-Path $absOutDir "CHECKSUMS.sha256"

$files = Get-ChildItem -Path $Root -Recurse -File -Force | ForEach-Object {
  $relNorm = Get-RelPath $_.FullName
  [PSCustomObject]@{
    FullName = $_.FullName
    RelNorm  = $relNorm
    Length   = $_.Length
  }
} | Where-Object {
  -not (Test-ExcludedRelPath $_.RelNorm)
} | Where-Object {
  -not (Test-ExcludedByMask $_.RelNorm)
} | Where-Object {
  Test-AllowedExt $_.FullName
} | Sort-Object RelNorm

$fence = '```'

$header =
"# Project review bundle`n`n" +
"Root: $Root`n" +
"Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n" +
"MaxFileBytes: $MaxFileBytes`n`n" +
"## File list`n`n"

Set-TextUtf8NoBom $outBundle $header

foreach ($f in $files) {
  Add-TextUtf8NoBom $outBundle ("- " + $f.RelNorm + "`n")
}

Add-TextUtf8NoBom $outBundle "`n## Files content`n"

Set-TextUtf8NoBom $outSums "# SHA256 checksums (relative_path  sha256)`n"
foreach ($f in $files) {
  $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToLowerInvariant()
  Add-TextUtf8NoBom $outSums ("{0}  {1}`n" -f $f.RelNorm, $hash)
}

foreach ($f in $files) {
  $lang = Get-LangTag $f.FullName

  Add-TextUtf8NoBom $outBundle ("`n---`n### " + $f.RelNorm + "`n`n")

  if ($f.Length -gt $MaxFileBytes) {
    Add-TextUtf8NoBom $outBundle ("SKIPPED (too large): " + $f.Length + " bytes (limit=" + $MaxFileBytes + ")`n")
    continue
  }

  $bytes = [System.IO.File]::ReadAllBytes($f.FullName)
  if (Test-HasNulByte $bytes) {
    Add-TextUtf8NoBom $outBundle "SKIPPED (binary file detected: NUL byte)`n"
    continue
  }

  Add-TextUtf8NoBom $outBundle ($fence + $lang + "`n")
  $text = ConvertFrom-BytesSmart $bytes
  $text = ($text -replace "`r`n", "`n").TrimEnd()
  Add-TextUtf8NoBom $outBundle ($text + "`n")
  Add-TextUtf8NoBom $outBundle ($fence + "`n")
}

Write-Host "OK generated:"
Write-Host $outBundle
Write-Host $outSums
Write-Host ("Files: " + $files.Count)
```

---
### логи для чата.txt

```txt
PS C:\Users\klimg> schtasks /Query /TN "WB_Orders_Hourly" /V /FO LIST

Папка: \
Имя узла:                                            KLIMGARANIN
Имя задачи:                                          \WB_Orders_Hourly
Время следующего запуска:                            18.01.2026 21:05:00
Состояние:                                           Готово
Режим входа в систему:                               Только интерактивный
Время прошлого запуска:                              18.01.2026 20:05:01
Прошлый результат:                                   1
Автор:                                               KLIMGARANIN\klimg
Задача для выполнения:                               C:\marketplace-etl\run_wb_orders.cmd
Рабочая папка:                                       Н/Д
Примечание:                                          Н/Д
Состояние назначенной задачи:                        Включено
Время простоя:                                       Отключено
Управление электропитанием:                          Останавливать при питании от батареи, Не запускать при питании от батареи
Запуск от имени:                                     klimg
Удалить задачу, если она не перенесена:              Отключено
Остановить задачу, если она выполняется X ч и X мин: 72:00:00
Расписание:                                          Планирование данных в этом формате недоступно.
Тип расписания:                                      Однократно, Ежечасно
Время начала:                                        00:05:00
Дата начала:                                         17.01.2026
Дата окончания:                                      N/A
дн.:                                                 N/A
мес.:                                                N/A
Повторять: каждые:                                   1 ч, 0 мин
Повторять: до: время:                                Нет
Повторять: в течение: длительность:                  Отключено
Повторять: остановить, если выполняется:             Отключено
PS C:\Users\klimg> cd C:\marketplace-etl\infra
PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select status, count(*) as cnt from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by status order by status;"
error during connect: Get "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/v1.51/containers/json?filters=%7B%22label%22%3A%7B%22com.docker.compose.config-hash%22%3Atrue%2C%22com.docker.compose.project%3Dinfra%22%3Atrue%2C%22com.docker.compose.service%3Ddb%22%3Atrue%7D%7D": open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified.
PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select status, count(*) as cnt from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by status order by status;"
service "db" is not running
PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select status, count(*) as cnt from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by status order by status;"
 status | cnt
--------+-----
 ok     |   4
(1 row)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select id, started_at, status, api_rows, raw_new_versions, duplicates, dup_pct, cursor_old, cursor_new from job_runs where job_name='wb_orders' order by id desc limit 20;"
 id |       started_at       | status | api_rows | raw_new_versions | duplicates | dup_pct |        cursor_old         |        cursor_new
----+------------------------+--------+----------+------------------+------------+---------+---------------------------+---------------------------
  4 | 2026-01-17 12:05:04+00 | ok     |      324 |              155 |        169 |   52.16 | 2026-01-17T13:59:25+03:00 | 2026-01-17T14:59:19+03:00
  3 | 2026-01-17 11:05:05+00 | ok     |      252 |              102 |        150 |   59.52 | 2026-01-17T13:21:40+03:00 | 2026-01-17T13:59:25+03:00
  2 | 2026-01-17 10:26:14+00 | ok     |      181 |                5 |        176 |   97.24 | 2026-01-17T13:17:19+03:00 | 2026-01-17T13:21:40+03:00
  1 | 2026-01-17 10:22:31+00 | ok     |      542 |              449 |          0 |    0.00 | 2026-01-17T02:47:24+03:00 | 2026-01-17T13:17:19+03:00
(4 rows)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select date_trunc('hour', started_at) as hour_utc, count(*) runs, sum(api_rows) api_rows, sum(raw_new_versions) raw_new_versions from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by 1 order by 1;"
        hour_utc        | runs | api_rows | raw_new_versions
------------------------+------+----------+------------------
 2026-01-17 10:00:00+00 |    2 |      723 |              454
 2026-01-17 11:00:00+00 |    1 |      252 |              102
 2026-01-17 12:00:00+00 |    1 |      324 |              155
(3 rows)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select id, started_at, cursor_old, cursor_new from job_runs where job_name='wb_orders' order by id desc limit 30;"
 id |       started_at       |        cursor_old         |        cursor_new
----+------------------------+---------------------------+---------------------------
  4 | 2026-01-17 12:05:04+00 | 2026-01-17T13:59:25+03:00 | 2026-01-17T14:59:19+03:00
  3 | 2026-01-17 11:05:05+00 | 2026-01-17T13:21:40+03:00 | 2026-01-17T13:59:25+03:00
  2 | 2026-01-17 10:26:14+00 | 2026-01-17T13:17:19+03:00 | 2026-01-17T13:21:40+03:00
  1 | 2026-01-17 10:22:31+00 | 2026-01-17T02:47:24+03:00 | 2026-01-17T13:17:19+03:00
(4 rows)

PS C:\marketplace-etl\infra> cd C:\marketplace-etl\infra
PS C:\marketplace-etl\infra> docker compose up -d
[+] up 1/1
 ✔ Container infra-db-1 Running                                                                                 0.0s
PS C:\marketplace-etl\infra> docker compose ps
NAME         IMAGE         COMMAND                  SERVICE   CREATED        STATUS          PORTS
infra-db-1   postgres:16   "docker-entrypoint.s…"   db        43 hours ago   Up 28 minutes   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp
PS C:\marketplace-etl\infra> schtasks /Run /TN "WB_Orders_Hourly"
УСПЕХ. Попытка выполнить запланированную задачу "WB_Orders_Hourly".
PS C:\marketplace-etl\infra> schtasks /Query /TN "WB_Orders_Hourly" /V /FO LIST

Папка: \
Имя узла:                                            KLIMGARANIN
Имя задачи:                                          \WB_Orders_Hourly
Время следующего запуска:                            18.01.2026 21:05:00
Состояние:                                           Готово
Режим входа в систему:                               Только интерактивный
Время прошлого запуска:                              18.01.2026 20:47:46
Прошлый результат:                                   0
Автор:                                               KLIMGARANIN\klimg
Задача для выполнения:                               C:\marketplace-etl\run_wb_orders.cmd
Рабочая папка:                                       Н/Д
Примечание:                                          Н/Д
Состояние назначенной задачи:                        Включено
Время простоя:                                       Отключено
Управление электропитанием:                          Останавливать при питании от батареи, Не запускать при питании от батареи
Запуск от имени:                                     klimg
Удалить задачу, если она не перенесена:              Отключено
Остановить задачу, если она выполняется X ч и X мин: 72:00:00
Расписание:                                          Планирование данных в этом формате недоступно.
Тип расписания:                                      Однократно, Ежечасно
Время начала:                                        00:05:00
Дата начала:                                         17.01.2026
Дата окончания:                                      N/A
дн.:                                                 N/A
мес.:                                                N/A
Повторять: каждые:                                   1 ч, 0 мин
Повторять: до: время:                                Нет
Повторять: в течение: длительность:                  Отключено
Повторять: остановить, если выполняется:             Отключено
PS C:\marketplace-etl\infra> cd C:\marketplace-etl\infra
PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select id, started_at, status, api_rows, raw_new_versions, duplicates, dup_pct, cursor_old, cursor_new, error from job_runs where job_name='wb_orders' order by id desc limit 10;"
 id |       started_at       | status | api_rows | raw_new_versions | duplicates | dup_pct |        cursor_old         |        cursor_new         | error
----+------------------------+--------+----------+------------------+------------+---------+---------------------------+---------------------------+-------
  5 | 2026-01-18 17:47:49+00 | ok     |     2649 |             2463 |        186 |    7.02 | 2026-01-17T14:59:19+03:00 | 2026-01-18T20:43:17+03:00 |
  4 | 2026-01-17 12:05:04+00 | ok     |      324 |              155 |        169 |   52.16 | 2026-01-17T13:59:25+03:00 | 2026-01-17T14:59:19+03:00 |
  3 | 2026-01-17 11:05:05+00 | ok     |      252 |              102 |        150 |   59.52 | 2026-01-17T13:21:40+03:00 | 2026-01-17T13:59:25+03:00 |
  2 | 2026-01-17 10:26:14+00 | ok     |      181 |                5 |        176 |   97.24 | 2026-01-17T13:17:19+03:00 | 2026-01-17T13:21:40+03:00 |
  1 | 2026-01-17 10:22:31+00 | ok     |      542 |              449 |          0 |    0.00 | 2026-01-17T02:47:24+03:00 | 2026-01-17T13:17:19+03:00 |
(5 rows)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select status, count(*) cnt from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by status order by status;"
 status | cnt
--------+-----
 ok     |   5
(1 row)
```

---
### логи после двух дней.txt

```txt
PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select status, count(*) as cnt from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by status order by status;"
 status | cnt
--------+-----
 ok     |   4
(1 row)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select id, started_at, status, api_rows, raw_new_versions, duplicates, dup_pct, cursor_old, cursor_new from job_runs where job_name='wb_orders' order by id desc limit 20;"
 id |       started_at       | status | api_rows | raw_new_versions | duplicates | dup_pct |        cursor_old         |        cursor_new
----+------------------------+--------+----------+------------------+------------+---------+---------------------------+---------------------------
  4 | 2026-01-17 12:05:04+00 | ok     |      324 |              155 |        169 |   52.16 | 2026-01-17T13:59:25+03:00 | 2026-01-17T14:59:19+03:00
  3 | 2026-01-17 11:05:05+00 | ok     |      252 |              102 |        150 |   59.52 | 2026-01-17T13:21:40+03:00 | 2026-01-17T13:59:25+03:00
  2 | 2026-01-17 10:26:14+00 | ok     |      181 |                5 |        176 |   97.24 | 2026-01-17T13:17:19+03:00 | 2026-01-17T13:21:40+03:00
  1 | 2026-01-17 10:22:31+00 | ok     |      542 |              449 |          0 |    0.00 | 2026-01-17T02:47:24+03:00 | 2026-01-17T13:17:19+03:00
(4 rows)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select date_trunc('hour', started_at) as hour_utc, count(*) runs, sum(api_rows) api_rows, sum(raw_new_versions) raw_new_versions from job_runs where job_name='wb_orders' and started_at >= now() - interval '2 days' group by 1 order by 1;"
        hour_utc        | runs | api_rows | raw_new_versions
------------------------+------+----------+------------------
 2026-01-17 10:00:00+00 |    2 |      723 |              454
 2026-01-17 11:00:00+00 |    1 |      252 |              102
 2026-01-17 12:00:00+00 |    1 |      324 |              155
(3 rows)

PS C:\marketplace-etl\infra> docker compose exec db psql -U app -d marketplace -P pager=off -c "select id, started_at, cursor_old, cursor_new from job_runs where job_name='wb_orders' order by id desc limit 30;"
 id |       started_at       |        cursor_old         |        cursor_new
----+------------------------+---------------------------+---------------------------
  4 | 2026-01-17 12:05:04+00 | 2026-01-17T13:59:25+03:00 | 2026-01-17T14:59:19+03:00
  3 | 2026-01-17 11:05:05+00 | 2026-01-17T13:21:40+03:00 | 2026-01-17T13:59:25+03:00
  2 | 2026-01-17 10:26:14+00 | 2026-01-17T13:17:19+03:00 | 2026-01-17T13:21:40+03:00
  1 | 2026-01-17 10:22:31+00 | 2026-01-17T02:47:24+03:00 | 2026-01-17T13:17:19+03:00
(4 rows)
```
