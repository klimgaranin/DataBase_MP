# Project review bundle

Root: C:\Програмирование\Проекты\DataBase_MP
Generated: 2026-04-21 02:11:58

## File list

- .gitignore
- app\clients\__init__.py
- app\clients\http_wb_statistics.py
- app\clients\http_wb_stocks.py
- app\db.py
- app\jobs\__init__.py
- app\jobs\job_wb_orders_backfill.py
- app\jobs\job_wb_orders.py
- app\jobs\job_wb_stocks.py
- app\normalize\__init__.py
- app\normalize\norm_wb_orders.py
- app\normalize\norm_wb_stocks.py
- app\utils.py
- infra\docker-compose.yml
- infra\docker-compose.yml.txt
- README.md
- requirements.txt
- scripts\run_wb_orders.cmd
- scripts\run_wb_orders.md
- scripts\run_wb_stocks.cmd
- scripts\run_wb_stocks.md
- tools\export_review.md
- tools\export_review.ps1

## Files content


---
### .gitignore

```
__pycache__/
*.py[cod]
*$py.class

.venv/
venv/
ENV/
env/
.ENV/
.env_*/

.env
.env.*
!.env.example

*.log
logs/
*.pid
*.pid.lock

.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
htmlcov/
.cache/

build/
dist/
*.egg-info/
pip-wheel-metadata/

.idea/
.vscode/*
!.vscode/settings.json
!.vscode/extensions.json

.DS_Store
Thumbs.db
Desktop.ini

**/pgdata/
**/postgres-data/
**/postgres_data/
**/db-data/
**/db_data/
**/.docker/
```

---
### app\clients\__init__.py

```python

```

---
### app\clients\http_wb_statistics.py

```python
from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)

# WB Statistics API (Orders)
DEFAULT_ORDERS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"


@dataclass(frozen=True)
class WbOrdersClientConfig:
    token: str
    orders_url: str = DEFAULT_ORDERS_URL

    # Пагинация:
    # - strict_pagination=True: догружаем ТОЛЬКО если ответ опасно близко к лимиту (по порогу)
    # - force_pagination=True: всегда пытаемся догрузить (для отладки)
    strict_pagination: bool = True
    force_pagination: bool = False
    pagination_threshold: int = 79_000  # близко к лимиту 80k

    # Ограничение WB: 1 запрос/мин на аккаунт (оставляем запас)
    min_request_interval_sec: float = 61.0

    # HTTP
    http_timeout_sec: float = 30.0
    http_max_retries: int = 6
    http_backoff_base_sec: float = 1.5
    http_backoff_max_sec: float = 30.0

    # Защита от бесконечной пагинации
    max_pages: int = 50


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, min_v: int, max_v: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        val = int(raw)
    except Exception:
        return default
    return max(min_v, min(max_v, val))


def _env_float(name: str, default: float, min_v: float, max_v: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        val = float(raw)
    except Exception:
        return default
    return max(min_v, min(max_v, val))


def load_config() -> WbOrdersClientConfig:
    # Ожидаем, что токен лежит в WB_TOKEN
    token = (os.getenv("WB_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("WB_TOKEN не задан. Добавь WB_TOKEN в .env (токен Statistics API).")

    return WbOrdersClientConfig(
        token=token,
        orders_url=(os.getenv("WB_ORDERS_URL") or DEFAULT_ORDERS_URL).strip(),
        strict_pagination=_env_bool("WB_STRICT_PAGINATION", True),
        force_pagination=_env_bool("WB_FORCE_PAGINATION", False),
        pagination_threshold=_env_int("WB_PAGINATION_THRESHOLD", 79_000, 1_000, 80_000),
        min_request_interval_sec=_env_float("WB_MIN_REQUEST_INTERVAL_SEC", 61.0, 0.0, 600.0),
        http_timeout_sec=_env_float("WB_HTTP_TIMEOUT_SEC", 30.0, 1.0, 300.0),
        http_max_retries=_env_int("WB_HTTP_MAX_RETRIES", 6, 1, 20),
        http_backoff_base_sec=_env_float("WB_HTTP_BACKOFF_BASE_SEC", 1.5, 0.1, 60.0),
        http_backoff_max_sec=_env_float("WB_HTTP_BACKOFF_MAX_SEC", 30.0, 1.0, 600.0),
        max_pages=_env_int("WB_MAX_PAGES", 50, 1, 500),
    )


def _ensure_msk_tz(iso_str: str) -> str:
    """
    Если TZ не указан — WB трактует dateFrom как МСК.
    Чтобы избежать сюрпризов, добавляем +03:00, если TZ нет.
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # уже есть +HH:MM / -HH:MM
    if len(s) >= 6 and (s[-6] in {"+", "-"}) and s[-3] == ":":
        return s
    return s + "+03:00"


def _sleep_for_rate_limit(last_ts: Optional[float], min_interval: float) -> float:
    """
    Спим перед запросом, если не выдержан интервал.
    Возвращаем timestamp момента выполнения запроса.
    """
    now = time.time()
    if last_ts is None or min_interval <= 0:
        return now

    elapsed = now - last_ts
    wait = min_interval - elapsed
    if wait > 0:
        log.info("[WB] лимит 1 запрос/мин: сплю %.1f сек", wait)
        time.sleep(wait)
    return time.time()


def _request_json_list(
    session: requests.Session,
    cfg: WbOrdersClientConfig,
    params: Dict[str, Any],
    last_request_ts: Optional[float],
) -> Tuple[List[Dict[str, Any]], float]:
    """
    Делает один запрос и возвращает (список строк, timestamp запроса).
    """
    # Лимит применяем перед первым запросом страницы (а не перед каждым retry)
    request_ts = _sleep_for_rate_limit(last_request_ts, cfg.min_request_interval_sec)

    headers = {"Authorization": cfg.token}
    url = cfg.orders_url

    for attempt in range(1, cfg.http_max_retries + 1):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=cfg.http_timeout_sec)
        except requests.RequestException as e:
            wait = min(cfg.http_backoff_max_sec, cfg.http_backoff_base_sec * (2 ** (attempt - 1)))
            wait += random.uniform(0.0, 1.0)
            log.warning(
                "[WB] ошибка сети (%s/%s): %s. Жду %.1f сек и повторяю.",
                attempt,
                cfg.http_max_retries,
                str(e),
                wait,
            )
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError(f"WB вернул неожиданный JSON: {type(data)} (ожидали list).")
            rows: List[Dict[str, Any]] = [x for x in data if isinstance(x, dict)]
            return rows, request_ts

        if resp.status_code == 401:
            raise RuntimeError("WB: 401 Unauthorized. Проверь WB_TOKEN (Statistics API).")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = cfg.min_request_interval_sec
            if retry_after:
                try:
                    wait = max(wait, float(retry_after))
                except Exception:
                    pass
            log.warning("[WB] 429 Too Many Requests. Жду %.1f сек и повторяю.", wait)
            time.sleep(wait)
            continue

        if 500 <= resp.status_code <= 599:
            wait = min(cfg.http_backoff_max_sec, cfg.http_backoff_base_sec * (2 ** (attempt - 1)))
            wait += random.uniform(0.0, 1.0)
            log.warning(
                "[WB] серверная ошибка %s (%s/%s). Жду %.1f сек и повторяю.",
                resp.status_code,
                attempt,
                cfg.http_max_retries,
                wait,
            )
            time.sleep(wait)
            continue

        body_preview = (resp.text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"WB: HTTP {resp.status_code}. Ответ: {body_preview}")

    raise RuntimeError("WB: превышено число попыток запроса (network/5xx/429).")


def iter_orders(
    date_from_iso: str,
    flag: int = 0,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Загружает заказы WB, начиная с date_from_iso.

    Пагинация:
      - если WB_FORCE_PAGINATION=1 -> всегда пробуем догружать
      - иначе если WB_STRICT_PAGINATION=1 и len(rows) >= WB_PAGINATION_THRESHOLD -> догружаем

    Важно: при flag=0 WB использует lastChangeDate >= dateFrom (включительно),
    поэтому дубликаты на границе — нормальны (их должна убирать БД).
    """
    cfg = load_config()

    if flag not in (0, 1):
        raise ValueError("flag должен быть 0 или 1")

    params: Dict[str, Any] = {"dateFrom": _ensure_msk_tz(date_from_iso)}
    if flag != 0:
        params["flag"] = str(flag)

    own_session = session is None
    sess = session or requests.Session()

    all_rows: List[Dict[str, Any]] = []
    page = 1
    last_request_ts: Optional[float] = None

    try:
        while True:
            log.info("[WB] страница=%s dateFrom=%s", page, params["dateFrom"])

            rows, last_request_ts = _request_json_list(sess, cfg, params, last_request_ts)
            log.info("[WB] страница=%s строк=%s", page, len(rows))

            if not rows:
                break

            all_rows.extend(rows)

            need_next = False
            if cfg.force_pagination:
                need_next = True
                log.info("[WB] пагинация: принудительно (WB_FORCE_PAGINATION=1)")
            elif cfg.strict_pagination and len(rows) >= cfg.pagination_threshold:
                need_next = True
                log.info(
                    "[WB] пагинация: строк=%s >= порога=%s (риск лимита 80k)",
                    len(rows),
                    cfg.pagination_threshold,
                )

            if not need_next:
                break

            last_change = rows[-1].get("lastChangeDate")
            if not last_change:
                log.warning("[WB] пагинация: нет lastChangeDate в последней строке — стоп.")
                break

            next_date_from = _ensure_msk_tz(str(last_change))

            # Из-за >= WB может вернуть ту же последнюю строку ещё раз.
            # Если курсор не двигается — останавливаемся, чтобы не зациклиться.
            if next_date_from == str(params["dateFrom"]):
                log.warning(
                    "[WB] пагинация: курсор не двигается (lastChangeDate >= dateFrom). "
                    "Останавливаюсь, чтобы не зациклиться. Дубликаты уберёт БД, lookback защитит от пропусков."
                )
                break

            params["dateFrom"] = next_date_from
            page += 1

            if page > cfg.max_pages:
                log.warning("[WB] пагинация: достигнут лимит страниц WB_MAX_PAGES=%s — стоп.", cfg.max_pages)
                break

        return all_rows
    finally:
        if own_session:
            sess.close()

```

---
### app\clients\http_wb_stocks.py

```python
"""
clients/wb_stocks.py
WB Analytics API — Ostatok na skladah WB
POST /api/analytics/v1/stocks-report/wb-warehouses
Host: https://seller-analytics-api.wildberries.ru
Limit: 3 req/min, interval 20 sec, burst 1.
Data updated every 30 min.
"""
from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger("wb_stocks")

# VAZNO: pravilnyj host po dokumentacii WB API
API_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses"
PAGE_LIMIT = 250_000
MIN_REQUEST_INTERVAL = 21.0  # sec: 3 req/min -> 20 sec + margin


@dataclass(frozen=True)
class WbStocksConfig:
    token: str
    api_url: str = API_URL
    page_limit: int = PAGE_LIMIT
    min_request_interval: float = MIN_REQUEST_INTERVAL
    http_timeout_sec: float = 60.0
    http_max_retries: int = 6
    http_backoff_base: float = 2.0
    http_backoff_max: float = 30.0


def load_config() -> WbStocksConfig:
    token = (os.getenv("WB_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            "WB_TOKEN ne zadan v .env -- nuzhen token kategorii Analitika."
        )
    return WbStocksConfig(token=token)


def _post_json(
    session: requests.Session,
    cfg: WbStocksConfig,
    body: Dict[str, Any],
    last_request_ts: Optional[float],
) -> Tuple[Dict[str, Any], float]:
    """POST request with rate-limit and retry."""
    now = time.time()
    if last_request_ts is not None:
        wait = cfg.min_request_interval - (now - last_request_ts)
        if wait > 0:
            log.info("WB stocks: rate-limit, zhdyu %.1f sec.", wait)
            time.sleep(wait)
    ts = time.time()

    headers = {
        "Authorization": cfg.token,
        "Content-Type": "application/json",
    }

    for attempt in range(1, cfg.http_max_retries + 1):
        try:
            resp = session.post(
                cfg.api_url,
                json=body,
                headers=headers,
                timeout=cfg.http_timeout_sec,
            )
        except requests.RequestException as e:
            wait = min(cfg.http_backoff_max, cfg.http_backoff_base ** attempt) + random.uniform(0, 1)
            log.warning(
                "WB stocks: network error %s (attempt %d/%d), wait %.1f sec.",
                e, attempt, cfg.http_max_retries, wait,
            )
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json(), ts

        if resp.status_code == 401:
            raise RuntimeError(
                "WB stocks: 401 Unauthorized -- prover WB_TOKEN (nuzhna kategoriya Analitika)."
            )

        if resp.status_code == 403:
            raise RuntimeError(
                "WB stocks: 403 Forbidden -- token ne imeet dostupa k metodu /analytics/v1/stocks-report/wb-warehouses."
                " Prover chto token sozdavan s kategoriej Analitika."
            )

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", cfg.min_request_interval))
            log.warning("WB stocks: 429 Too Many Requests, wait %.1f sec.", retry_after)
            time.sleep(retry_after)
            continue

        if 500 <= resp.status_code <= 599:
            wait = min(cfg.http_backoff_max, cfg.http_backoff_base ** attempt) + random.uniform(0, 1)
            log.warning(
                "WB stocks: HTTP %d (attempt %d/%d), wait %.1f sec.",
                resp.status_code, attempt, cfg.http_max_retries, wait,
            )
            time.sleep(wait)
            continue

        raise RuntimeError(
            f"WB stocks: HTTP {resp.status_code}: {resp.text[:300]}"
        )

    raise RuntimeError("WB stocks: max retries exceeded (network/5xx).")


def fetch_all_stocks(
    nm_ids: Optional[List[int]] = None,
    chrt_ids: Optional[List[int]] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Returns all stock rows (pagination by offset).
    nm_ids / chrt_ids = None means all seller products.
    """
    cfg = load_config()
    own_session = session is None
    sess = session or requests.Session()
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    last_ts: Optional[float] = None

    try:
        while True:
            body: Dict[str, Any] = {
                "limit": cfg.page_limit,
                "offset": offset,
            }
            if nm_ids:
                body["nmIds"] = nm_ids
            if chrt_ids:
                body["chrtIds"] = chrt_ids

            log.info("WB stocks: request offset=%d limit=%d", offset, cfg.page_limit)
            data, last_ts = _post_json(sess, cfg, body, last_ts)

            items: List[Dict[str, Any]] = (
                data.get("data", {}).get("items") or []
            )
            log.info("WB stocks: received %d rows", len(items))

            if not items:
                break

            all_rows.extend(items)

            if len(items) < cfg.page_limit:
                break

            offset += len(items)

    finally:
        if own_session:
            sess.close()

    return all_rows

```

---
### app\db.py

```python
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


_DDL_WB_ORDERS_RAW_DEDUP = """
CREATE TABLE IF NOT EXISTS wb_orders_raw_dedup (
    id BIGSERIAL PRIMARY KEY,
    srid TEXT NOT NULL,
    last_change_ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (srid, last_change_ts)
);
"""

_DDL_WB_ORDERS_NORM = """
CREATE TABLE IF NOT EXISTS wb_orders_norm (
    srid TEXT PRIMARY KEY,
    is_cancel BOOLEAN,
    date_ts TIMESTAMPTZ,
    last_change_ts TIMESTAMPTZ,
    warehouse_name TEXT,
    warehouse_type TEXT,
    country_name TEXT,
    oblast_okrug_name TEXT,
    region_name TEXT,
    supplier_article TEXT,
    nm_id BIGINT,
    barcode TEXT,
    category TEXT,
    subject TEXT,
    brand TEXT,
    tech_size TEXT,
    income_id BIGINT,
    is_supply BOOLEAN,
    is_realization BOOLEAN,
    total_price NUMERIC(12,2),
    discount_percent INT,
    spp INT,
    finished_price NUMERIC(12,2),
    price_with_disc NUMERIC(12,2),
    cancel_date DATE,
    sticker TEXT,
    g_number TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_CURSORS = """
CREATE TABLE IF NOT EXISTS job_cursors (
    job_name TEXT PRIMARY KEY,
    cursor_val TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_RUNS = """
CREATE TABLE IF NOT EXISTS job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    api_rows INT NOT NULL DEFAULT 0,
    raw_new INT NOT NULL DEFAULT 0,
    norm_upserted INT NOT NULL DEFAULT 0,
    duplicates INT NOT NULL DEFAULT 0,
    dup_pct NUMERIC(6,2) NOT NULL DEFAULT 0,
    cursor_old TEXT,
    cursor_used TEXT,
    cursor_new TEXT,
    error TEXT
);
"""

_DDL_WB_STOCKS_RAW = """
CREATE TABLE IF NOT EXISTS wb_stocks_raw (
    id BIGSERIAL PRIMARY KEY,
    nm_id BIGINT NOT NULL,
    chrt_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    snapped_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_nm_id ON wb_stocks_raw (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_chrt_id ON wb_stocks_raw (chrt_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_snapped ON wb_stocks_raw (snapped_at);
"""

_DDL_WB_STOCKS_SNAP = """
CREATE TABLE IF NOT EXISTS wb_stocks_snap (
    nm_id BIGINT NOT NULL,
    chrt_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    warehouse_name TEXT,
    region_name TEXT,
    quantity INT,
    in_way_to_client INT,
    in_way_from_client INT,
    snapped_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nm_id, chrt_id, warehouse_id)
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_nm_id ON wb_stocks_snap (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_snapped ON wb_stocks_snap (snapped_at);
"""


def try_advisory_lock(lock_id: int) -> bool:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            return bool(cur.fetchone()[0])


def advisory_unlock(lock_id: int) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))


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


def insert_wb_orders_raw_dedup(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    records = []
    for r in rows:
        srid = r.get("srid") or r.get("sr_id") or ""
        lcd = r.get("lastChangeDate") or r.get("last_change_date") or None
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
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
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


def insert_wb_stocks_raw(rows: list[dict[str, Any]], snapped_at: str) -> int:
    if not rows:
        return 0
    values = []
    for r in rows:
        nm_id = r.get("nmId") or r.get("nm_id")
        chrt_id = r.get("chrtId") or r.get("chrt_id")
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


def insert_job_run(
    *,
    job_name: str,
    started_at_iso: str,
    finished_at_iso: str,
    status: str,
    api_rows: int = 0,
    raw_new_versions: int = 0,
    norm_upserted: int = 0,
    duplicates: int = 0,
    dup_pct: float = 0.0,
    cursor_old: Optional[str] = None,
    cursor_used: Optional[str] = None,
    cursor_new: Optional[str] = None,
    error: Optional[str] = None,
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
```

---
### app\jobs\__init__.py

```python


```

---
### app\jobs\job_wb_orders_backfill.py

```python
"""
app/jobs/job_wb_orders_backfill.py
Ручной бэкфилл заказов WB: загружает ВСЮ историю с указанной даты постранично.
Каждая страница сразу пишется в БД — память не копится.
После каждой страницы курсор сохраняется — можно возобновить после обрыва.

Запуск:
    python app/jobs/job_wb_orders_backfill.py 2026-01-01
"""
from __future__ import annotations

import sys
import time
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

_THIS = Path(__file__).resolve()

def _add_sys_path() -> None:
    # jobs/ → app/ → DataBase_MP/
    for p in (_THIS.parent.parent, _THIS.parent.parent.parent):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)

_add_sys_path()

for _c in (
    _THIS.parent.parent.parent / ".env",   # DataBase_MP/.env
    _THIS.parent.parent / ".env",          # app/.env (запасной)
):
    if _c.exists():
        load_dotenv(dotenv_path=_c)
        break
else:
    load_dotenv()

try:
    from app.clients.http_wb_statistics import iter_orders          # type: ignore
    from app.db import (                                             # type: ignore
        insert_wb_orders_raw_dedup, cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm, set_job_cursor,
        try_advisory_lock, advisory_unlock,
    )
    from app.normalize.norm_wb_orders import normalize_wb_order     # type: ignore
except Exception:
    from clients.http_wb_statistics import iter_orders              # type: ignore
    from db import (                                                 # type: ignore
        insert_wb_orders_raw_dedup, cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm, set_job_cursor,
        try_advisory_lock, advisory_unlock,
    )
    from normalize.norm_wb_orders import normalize_wb_order         # type: ignore

JOB_NAME_MAIN     = "wb_orders"
JOB_NAME_BACKFILL = "wb_orders_backfill"
LOCK_ID           = 4_242_001

SLEEP_SEC  = 62        # лимит WB: 1 запрос/мин
THRESHOLD  = 79_000    # если строк >= порога — есть следующая страница

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("backfill")


def _ensure_tz(s: str) -> str:
    s = (s or "").strip()
    if not s or s.endswith("Z"):
        return s
    if len(s) >= 6 and s[-6] in "+-" and s[-3] == ":":
        return s
    return s + "+03:00"


def run_backfill(date_from: str = "2026-01-01T00:00:00+03:00") -> None:
    cursor = _ensure_tz(date_from)
    log.info("Бэкфилл старт: dateFrom=%s", cursor)

    total_api  = 0
    total_norm = 0
    page       = 0

    import requests
    with requests.Session() as sess:
        while True:
            page += 1
            log.info("Страница=%s dateFrom=%s", page, cursor)

            chunk = iter_orders(cursor, flag=0, session=sess)
            log.info("Страница=%s строк=%s", page, len(chunk))

            if not chunk:
                log.info("Пустой ответ — бэкфилл завершён")
                break

            insert_wb_orders_raw_dedup(chunk)
            total_api += len(chunk)

            norm_rows = [
                nr
                for r in chunk
                if r.get("srid")
                for nr in [normalize_wb_order(r)]
                if nr is not None
            ]
            upsert_wb_orders_norm(norm_rows)
            total_norm += len(norm_rows)

            last_change = chunk[-1].get("lastChangeDate") or chunk[-1].get("date")
            if not last_change:
                log.warning("Нет lastChangeDate в последней строке — стоп")
                break

            new_cursor = _ensure_tz(str(last_change))

            log.info(
                "Итого: api=%s norm=%s | курсор_старый=%s курсор_новый=%s",
                total_api, total_norm, cursor, new_cursor,
            )

            # Сохраняем оба курсора — чтобы основной джоб не откатился назад
            set_job_cursor(JOB_NAME_BACKFILL, new_cursor)
            set_job_cursor(JOB_NAME_MAIN,     new_cursor)

            if len(chunk) < THRESHOLD:
                log.info("Строк=%s < порога=%s — финальная страница, стоп", len(chunk), THRESHOLD)
                break

            if new_cursor == cursor:
                log.warning("Курсор не сдвинулся — стоп (защита от зацикливания)")
                break

            cursor = new_cursor
            log.info("Жду %s сек (лимит WB 1 запрос/мин)...", SLEEP_SEC)
            time.sleep(SLEEP_SEC)

    deleted = cleanup_wb_orders_raw_dedup(14)
    log.info("Очистка raw_dedup: удалено=%s", deleted)
    log.info("Бэкфилл завершён: api_rows=%s norm_upserted=%s pages=%s", total_api, total_norm, page)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_arg = sys.argv[1].strip()
        if "T" not in date_arg:
            date_arg = date_arg + "T00:00:00+03:00"
        start_date = date_arg
    else:
        start_date = "2026-01-01T00:00:00+03:00"

    if not try_advisory_lock(LOCK_ID):
        log.warning("Пропуск: лок занят (основной джоб работает)")
        raise SystemExit(0)

    try:
        run_backfill(date_from=start_date)
    finally:
        advisory_unlock(LOCK_ID)

```

---
### app\jobs\job_wb_orders.py

```python
"""
app/jobs/job_wb_orders.py
ETL-джоб: WB заказы (raw_dedup + norm).
Advisory lock: 4242001.
Расписание (Task Scheduler): каждые N минут.
"""
from __future__ import annotations

import os
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

_THIS_FILE = Path(__file__).resolve()

def _add_sys_path() -> None:
    # jobs/ → app/ → DataBase_MP/
    for p in (_THIS_FILE.parent.parent, _THIS_FILE.parent.parent.parent):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)

def _find_env_path() -> Optional[Path]:
    for c in (
        _THIS_FILE.parent.parent.parent / ".env",   # DataBase_MP/.env
        _THIS_FILE.parent.parent / ".env",          # app/.env (запасной)
    ):
        if c.exists():
            return c
    return None

_add_sys_path()
_env_path = _find_env_path()
if _env_path is not None:
    load_dotenv(dotenv_path=_env_path)
else:
    load_dotenv()

try:
    from app.clients.http_wb_statistics import iter_orders          # type: ignore
    from app.db import (                                             # type: ignore
        insert_wb_orders_raw_dedup,
        cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm,
        get_job_cursor,
        set_job_cursor,
        insert_job_run,
        try_advisory_lock,
        advisory_unlock,
    )
    from app.normalize.norm_wb_orders import normalize_wb_order     # type: ignore
except Exception:
    from clients.http_wb_statistics import iter_orders              # type: ignore
    from db import (                                                 # type: ignore
        insert_wb_orders_raw_dedup,
        cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm,
        get_job_cursor,
        set_job_cursor,
        insert_job_run,
        try_advisory_lock,
        advisory_unlock,
    )
    from normalize.norm_wb_orders import normalize_wb_order         # type: ignore

JOB_NAME = "wb_orders"
LOCK_ID  = 4_242_001

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID",   "").strip()


@dataclass(frozen=True)
class JobConfig:
    log_level: str
    log_file: Optional[str]
    first_run_days_back: int
    lookback_base_minutes: int
    lookback_max_minutes: int
    raw_dedup_retention_days: int
    debug_sleep_after_lock_seconds: int

    @staticmethod
    def from_env() -> "JobConfig":
        log_level              = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
        log_file               = (os.getenv("LOG_FILE") or "").strip() or None
        first_run_days_back    = int(os.getenv("WB_FIRST_RUN_DAYS_BACK", "3"))
        lookback_base_minutes  = int(os.getenv("WB_LOOKBACK_MINUTES", "10"))
        lookback_max_minutes   = int(os.getenv("WB_LOOKBACK_MAX_MINUTES",
                                               str(max(lookback_base_minutes, 20))))
        raw_dedup_retention_days         = int(os.getenv("WB_RAW_DEDUP_RETENTION_DAYS", "14"))
        debug_sleep_after_lock_seconds   = int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0"))

        first_run_days_back              = max(1,   min(first_run_days_back, 30))
        lookback_base_minutes            = max(0,   min(lookback_base_minutes, 120))
        lookback_max_minutes             = max(0,   min(lookback_max_minutes, 240))
        raw_dedup_retention_days         = max(1,   min(raw_dedup_retention_days, 365))
        debug_sleep_after_lock_seconds   = max(0,   min(debug_sleep_after_lock_seconds, 3600))

        return JobConfig(
            log_level=log_level,
            log_file=log_file,
            first_run_days_back=first_run_days_back,
            lookback_base_minutes=lookback_base_minutes,
            lookback_max_minutes=lookback_max_minutes,
            raw_dedup_retention_days=raw_dedup_retention_days,
            debug_sleep_after_lock_seconds=debug_sleep_after_lock_seconds,
        )


def setup_logging(cfg: JobConfig) -> logging.Logger:
    level = getattr(logging, cfg.log_level, logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if cfg.log_file:
        handlers.append(logging.FileHandler(cfg.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )
    return logging.getLogger(JOB_NAME)


def _ensure_tz(iso_str: str) -> str:
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    if len(s) >= 6 and (s[-6] in ("+", "-")) and (s[-3] == ":"):
        return s
    return s + "+03:00"


def _parse_iso_dt(iso_str: str) -> Optional[datetime]:
    s = _ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_msk_label() -> str:
    msk = datetime.now(timezone(timedelta(hours=3)))
    return msk.strftime("%d.%m.%Y %H:%M")


def _first_run_cursor(days_back: int) -> str:
    tz_msk = timezone(timedelta(hours=3))
    return (datetime.now(tz_msk) - timedelta(days=days_back)).replace(microsecond=0).isoformat()


def _tg_send(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logging.getLogger(JOB_NAME).warning("TG send failed: %s", e)


def _calc_lookback_minutes(
    cfg: JobConfig,
    cursor_old_iso: str,
    last_dup_pct: Optional[float],
) -> int:
    base     = cfg.lookback_base_minutes
    max_auto = cfg.lookback_max_minutes
    dt_old   = _parse_iso_dt(cursor_old_iso)
    if dt_old is None:
        return base
    now_same_tz = datetime.now(dt_old.tzinfo or timezone.utc)
    gap_min = max(int((now_same_tz - dt_old).total_seconds() // 60), 0)
    if gap_min <= 20:
        look = min(base, 2)
    elif gap_min <= 60:
        look = min(base, 5)
    elif gap_min <= 360:
        look = min(base, 10)
    else:
        look = min(max_auto, max(base, 15))
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


def _apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = _parse_iso_dt(cursor_old_iso)
    if dt is None:
        return _ensure_tz(cursor_old_iso)
    return (dt - timedelta(minutes=max(0, minutes))).replace(microsecond=0).isoformat()


def _max_cursor_from_rows(rows: list[dict[str, Any]], fallback: str) -> str:
    best_dt: Optional[datetime] = None
    best_raw: Optional[str]     = None
    for r in rows:
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = _parse_iso_dt(str(c))
        if dt is None:
            continue
        if best_dt is None or dt > best_dt:
            best_dt  = dt
            best_raw = str(c)
    return _ensure_tz(best_raw) if best_raw else _ensure_tz(fallback)


def _dedupe_page_by_srid(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = _parse_iso_dt(str(c))
        if dt is None:
            continue
        key  = str(srid)
        prev = best.get(key)
        if prev is None or dt > prev[0]:
            best[key] = (dt, r)
    return [pair[1] for pair in best.values()]


def main() -> int:
    cfg = JobConfig.from_env()
    log = setup_logging(cfg)

    if not try_advisory_lock(LOCK_ID):
        log.info("[ЗАДАЧА] пропуск: предыдущий запуск ещё выполняется (лок занят)")
        return 0

    if cfg.debug_sleep_after_lock_seconds > 0:
        log.info("[ЗАДАЧА] отладочная пауза после лока: %s сек.", cfg.debug_sleep_after_lock_seconds)
        time.sleep(cfg.debug_sleep_after_lock_seconds)

    started_at  = _now_iso_utc()
    status      = "ok"
    error: Optional[str] = None
    api_rows    = 0
    raw_new     = 0
    norm_upserted = 0
    duplicates  = 0
    dup_pct     = 0.0
    cursor_old  = ""
    cursor_used = ""
    cursor_new  = ""

    try:
        from app.db import get_last_dup_pct  # type: ignore
    except Exception:
        try:
            from db import get_last_dup_pct  # type: ignore
        except Exception:
            get_last_dup_pct = lambda job: None  # noqa: E731

    try:
        cursor_old   = get_job_cursor(JOB_NAME) or ""
        last_dup_pct = get_last_dup_pct(JOB_NAME)

        if not cursor_old:
            cursor_used = _first_run_cursor(cfg.first_run_days_back)
            log.info("[ЗАДАЧА] первый запуск, cursor_used=%s", cursor_used)
        else:
            look_min    = _calc_lookback_minutes(cfg, cursor_old, last_dup_pct)
            cursor_used = _apply_lookback(cursor_old, look_min)
            log.info(
                "[ЗАДАЧА] cursor_old=%s look_min=%d cursor_used=%s last_dup_pct=%s",
                cursor_old, look_min, cursor_used, last_dup_pct,
            )

        try:
            rows_raw = iter_orders(cursor_used, flag=0)
        except Exception as e:
            raise RuntimeError(f"WB API error: {e}") from e

        api_rows = len(rows_raw)
        log.info("[ЗАДАЧА] API вернул строк: %d", api_rows)

        if rows_raw:
            rows_deduped = _dedupe_page_by_srid(rows_raw)
            raw_new   = insert_wb_orders_raw_dedup(rows_deduped)
            duplicates = len(rows_deduped) - raw_new
            dup_pct = round(duplicates / api_rows * 100, 2) if api_rows else 0.0
            norm_rows = [
                nr
                for r in rows_deduped
                if r.get("srid")
                for nr in [normalize_wb_order(r)]
                if nr is not None
            ]
            norm_upserted = upsert_wb_orders_norm(norm_rows)

            cursor_new = _max_cursor_from_rows(rows_raw, cursor_used)
            set_job_cursor(JOB_NAME, cursor_new)

            log.info(
                "[ЗАДАЧА] raw_new=%d duplicates=%d dup_pct=%.1f%% norm_upserted=%d cursor_new=%s",
                raw_new, duplicates, dup_pct, norm_upserted, cursor_new,
            )
        else:
            cursor_new = cursor_used
            log.info("[ЗАДАЧА] API вернул 0 строк, курсор не сдвигаем")

        deleted = cleanup_wb_orders_raw_dedup(cfg.raw_dedup_retention_days)
        log.info("[ЗАДАЧА] cleanup raw_dedup: удалено=%d строк", deleted)

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("[ЗАДАЧА] ОШИБКА — %s", error)
        return 2

    finally:
        finished_at = _now_iso_utc()
        try:
            insert_job_run(
                job_name          = JOB_NAME,
                started_at_iso    = started_at,
                finished_at_iso   = finished_at,
                status            = status,
                api_rows          = api_rows,
                raw_new_versions  = raw_new,
                norm_upserted     = norm_upserted,
                duplicates        = duplicates,
                dup_pct           = dup_pct,
                cursor_old        = cursor_old  or None,
                cursor_used       = cursor_used or None,
                cursor_new        = cursor_new  or None,
                error             = error,
            )
        except Exception as je:
            log.warning("[ЗАДАЧА] insert_job_run failed: %s", je)

        ts = _now_msk_label()
        if status == "ok":
            msg = (
                f"\u2705 WB_Orders_Sync | {ts} | OK\n\n"
                f"\u27a1 WB Api вернул строк: {api_rows}\n\n"
                f"\U0001f504 Обновлено строк: {norm_upserted}"
            )
        else:
            short_err = (error or "unknown")[:200]
            msg = (
                f"\u274C WB_Orders_Sync | {ts} | FAIL\n"
                f"{short_err}"
            )
        _tg_send(msg)
        advisory_unlock(LOCK_ID)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

```

---
### app\jobs\job_wb_stocks.py

```python
"""
app/jobs/job_wb_stocks.py
Джоб: получить текущие остатки с WB и сохранить в PostgreSQL.
Advisory lock: 4242002 (не пересекается с wb_orders 4242001).
Расписание (Task Scheduler): каждые 30 минут.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

_THIS = Path(__file__).resolve()

def _add_sys_path() -> None:
    # jobs/ → app/ → DataBase_MP/
    for p in (_THIS.parent.parent, _THIS.parent.parent.parent):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)

_add_sys_path()

_env = next(
    (c for c in (
        _THIS.parent.parent.parent / ".env",   # DataBase_MP/.env
        _THIS.parent.parent / ".env",          # app/.env (запасной)
    ) if c.exists()),
    None,
)
if _env:
    load_dotenv(dotenv_path=_env)
else:
    load_dotenv()

try:
    from app.db import (                                             # type: ignore
        try_advisory_lock, advisory_unlock, insert_job_run,
        insert_wb_stocks_raw, upsert_wb_stocks_snap, cleanup_wb_stocks_raw,
    )
    from app.clients.http_wb_stocks import fetch_all_stocks         # type: ignore
    from app.normalize.norm_wb_stocks import normalize_wb_stock     # type: ignore
except Exception:
    from db import (                                                 # type: ignore
        try_advisory_lock, advisory_unlock, insert_job_run,
        insert_wb_stocks_raw, upsert_wb_stocks_snap, cleanup_wb_stocks_raw,
    )
    from clients.http_wb_stocks import fetch_all_stocks             # type: ignore
    from normalize.norm_wb_stocks import normalize_wb_stock         # type: ignore

JOB_NAME           = "wb_stocks"
LOCK_ID            = 4_242_002
RAW_RETENTION_DAYS = int(os.getenv("WB_STOCKS_RAW_RETENTION_DAYS", "30"))
DEBUG_SLEEP        = int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0"))

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID",   "").strip()


def _setup_logging() -> logging.Logger:
    level    = getattr(logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO)
    log_file = (os.getenv("WB_STOCKS_LOG_FILE") or "").strip() or None
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )
    return logging.getLogger(JOB_NAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_msk_label() -> str:
    msk = datetime.now(timezone(timedelta(hours=3)))
    return msk.strftime("%d.%m.%Y %H:%M")


def _tg_send(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logging.getLogger(JOB_NAME).warning("TG send failed: %s", e)


def main() -> int:
    log = _setup_logging()

    if not try_advisory_lock(LOCK_ID):
        log.info("WB stocks: lock занят, выходим.")
        return 0

    if DEBUG_SLEEP > 0:
        log.info("WB stocks: DEBUG_SLEEP %d сек.", DEBUG_SLEEP)
        time.sleep(DEBUG_SLEEP)

    snapped_at    = _now_iso()
    api_rows      = 0
    raw_inserted  = 0
    snap_upserted = 0
    status        = "ok"
    error: Optional[str] = None
    started_at    = snapped_at

    try:
        log.info("WB stocks: старт, snapped_at=%s", snapped_at)
        with requests.Session() as sess:
            raw_items = fetch_all_stocks(session=sess)

        api_rows = len(raw_items)
        log.info("WB stocks: получено %d строк из API", api_rows)

        if not raw_items:
            log.warning("WB stocks: API вернул 0 строк.")
            return 0

        norm_rows     = [normalize_wb_stock(r) for r in raw_items]
        raw_inserted  = insert_wb_stocks_raw(raw_items, snapped_at)
        snap_upserted = upsert_wb_stocks_snap(norm_rows, snapped_at)

        log.info(
            "WB stocks: raw_inserted=%d snap_upserted=%d snapped_at=%s",
            raw_inserted, snap_upserted, snapped_at,
        )

        deleted = cleanup_wb_stocks_raw(RAW_RETENTION_DAYS)
        log.info("WB stocks: cleanup raw > %d дней, удалено %d строк", RAW_RETENTION_DAYS, deleted)

        return 0

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("WB stocks: ОШИБКА — %s", error)
        return 2

    finally:
        finished_at = _now_iso()
        try:
            insert_job_run(
                job_name          = JOB_NAME,
                started_at_iso    = started_at,
                finished_at_iso   = finished_at,
                status            = status,
                api_rows          = api_rows,
                raw_new_versions  = raw_inserted,
                norm_upserted     = snap_upserted,
                duplicates        = 0,
                dup_pct           = 0.0,
                cursor_old        = None,
                cursor_used       = None,
                cursor_new        = None,
                error             = error,
            )
        except Exception as je:
            log.warning("WB stocks: insert_job_run failed: %s", je)

        ts = _now_msk_label()
        if status == "ok":
            msg = (
                f"\u2705 WB_Stocks_Sync | {ts} | OK\n\n"
                f"\u27a1 WB Api вернул строк: {api_rows}\n\n"
                f"\U0001f504 Обновлено строк: {snap_upserted}"
            )
        else:
            short_err = (error or "unknown")[:200]
            msg = (
                f"\u274C WB_Stocks_Sync | {ts} | FAIL\n"
                f"{short_err}"
            )
        _tg_send(msg)
        advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())

```

---
### app\normalize\__init__.py

```python


```

---
### app\normalize\norm_wb_orders.py

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
### app\normalize\norm_wb_stocks.py

```python
"""
app/wb_stocks_db.py
Нормализация строк WB Stocks API.

Реальная структура ответа /analytics/v1/stocks-report/wb-warehouses:
{
    "nmId": 168149837,
    "chrtId": 279566746,
    "warehouseId": 120762,
    "warehouseName": "Электросталь",
    "regionName": "Центральный",
    "quantity": 39,
    "inWayToClient": 0,
    "inWayFromClient": 0
}
"""
from __future__ import annotations
from typing import Any, Dict


def normalize_wb_stock(row: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует одну строку из WB Stocks API в snake_case для БД."""
    def _int(v: Any) -> int | None:
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    return {
        "nm_id":              _int(row.get("nmId")),
        "chrt_id":            _int(row.get("chrtId")),
        "warehouse_id":       _int(row.get("warehouseId")),
        "warehouse_name":     row.get("warehouseName") or None,
        "region_name":        row.get("regionName") or None,
        "quantity":           _int(row.get("quantity")),
        "in_way_to_client":   _int(row.get("inWayToClient")),
        "in_way_from_client": _int(row.get("inWayFromClient")),
    }
```

---
### app\utils.py

```python
"""
app/utils.py
──────────────────────────────────────────────────────────────────────────────
Общие утилиты для всех ETL-джобов проекта DataBase_MP.

Что здесь:
  - Поиск и загрузка .env
  - Добавление нужных путей в sys.path (чтобы импорты работали из любой папки)
  - Настройка логирования (setup_logging)
  - Вспомогательные функции времени: now_iso_utc(), now_msk_label()
  - Работа с ISO-датами: ensure_tz(), parse_iso_dt(), first_run_cursor()
  - Telegram-алерт: tg_send()

Все функции — без side-эффектов при импорте.
TG_BOT_TOKEN и TG_CHAT_ID читаются из окружения один раз при вызове tg_send(),
чтобы .env успел загрузиться в джобе до первого обращения.
──────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

# ─────────────────────────────────────────────────────────────────────────────
# sys.path + .env
# ─────────────────────────────────────────────────────────────────────────────

def setup_sys_path(file: str | Path) -> None:
    """
    Добавляет папку файла и папку выше неё в sys.path.

    Зачем: джобы могут запускаться из разных директорий (Task Scheduler,
    VS Code, командная строка). Без этого Python не найдёт модули app/db.py,
    app/clients/ и т.д.

    Использование — первая строка в джобе, ДО любых from app.xxx import:
        from app.utils import setup_sys_path, load_env
        setup_sys_path(__file__)
        load_env(__file__)
    """
    p = Path(file).resolve()
    for candidate in (p.parent, p.parent.parent):
        s = str(candidate)
        if s not in sys.path:
            sys.path.insert(0, s)


def load_env(file: str | Path) -> None:
    """
    Ищет .env рядом с файлом или на уровень выше и загружает его.
    Если .env не найден — вызывает load_dotenv() без аргументов
    (он ищет в текущей рабочей директории).

    Зачем: в Task Scheduler рабочая папка может быть любой,
    поэтому ищем .env относительно самого скрипта.
    """
    from dotenv import load_dotenv  # импорт здесь — чтобы не падать если не установлен

    p = Path(file).resolve()
    for candidate in (p.parent / ".env", p.parent.parent / ".env"):
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
            return
    load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Логирование
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(
    job_name: str,
    *,
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Настраивает корневой логгер и возвращает логгер с именем job_name.

    log_level — строка: 'DEBUG', 'INFO', 'WARNING', 'ERROR'.
                Если None — берётся из переменной окружения LOG_LEVEL,
                а если и её нет — используется INFO.

    log_file  — путь к файлу для записи логов.
                Если None — пишем только в консоль.

    Формат: "2026-04-20 23:10:05 | INFO | wb_orders | текст"
    """
    level_str = (log_level or os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, level_str, logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    return logging.getLogger(job_name)


# ─────────────────────────────────────────────────────────────────────────────
# Время
# ─────────────────────────────────────────────────────────────────────────────

def now_iso_utc() -> str:
    """
    Текущее время UTC в формате ISO 8601 без микросекунд.
    Пример: '2026-04-20T20:10:05+00:00'

    Используется как started_at / finished_at при записи job_runs.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def now_msk_label() -> str:
    """
    Текущее московское время в читаемом формате для Telegram-алертов.
    Пример: '20.04.2026 23:10'
    """
    msk = datetime.now(timezone(timedelta(hours=3)))
    return msk.strftime("%d.%m.%Y %H:%M")


# ─────────────────────────────────────────────────────────────────────────────
# Работа с ISO-датами
# ─────────────────────────────────────────────────────────────────────────────

def ensure_tz(iso_str: str) -> str:
    """
    Гарантирует наличие часового пояса в ISO-строке.

    Зачем: WB API иногда возвращает даты без timezone-суффикса,
    например '2026-04-20T10:00:00'. Мы считаем такие даты московскими (+03:00).

    Примеры:
        '2026-04-20T10:00:00'        → '2026-04-20T10:00:00+03:00'
        '2026-04-20T10:00:00Z'       → без изменений
        '2026-04-20T10:00:00+03:00'  → без изменений
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # Проверяем суффикс вида +HH:MM или -HH:MM
    if len(s) >= 6 and s[-6] in ("+", "-") and s[-3] == ":":
        return s
    return s + "+03:00"


def parse_iso_dt(iso_str: str) -> Optional[datetime]:
    """
    Парсит ISO-строку в datetime с timezone.
    При ошибке возвращает None (не бросает исключений).

    Зачем: безопасный парсинг при сравнении курсоров,
    когда формат даты из API может быть нестандартным.
    """
    s = ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def first_run_cursor(days_back: int) -> str:
    """
    Возвращает ISO-строку: текущий момент минус days_back дней (МСК).

    Используется при первом запуске джоба, когда в job_cursors ещё
    нет записи — нужно с чего-то начать выгрузку из API.

    Пример: first_run_cursor(3) → '2026-04-17T23:10:05+03:00'
    """
    tz_msk = timezone(timedelta(hours=3))
    return (
        datetime.now(tz_msk) - timedelta(days=max(1, days_back))
    ).replace(microsecond=0).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────────────────────

def tg_send(text: str, *, logger: Optional[logging.Logger] = None) -> None:
    """
    Отправляет сообщение в Telegram-бот. Никогда не бросает исключений.

    Токен и chat_id берутся из переменных окружения:
        TG_BOT_TOKEN — токен бота (от @BotFather)
        TG_CHAT_ID   — id чата или канала, куда слать сообщения

    Если переменные не заданы — функция молча выходит.

    parse_mode='HTML' — можно использовать <b>жирный</b>, <i>курсив</i> и т.д.

    logger — опционально: если передать логгер джоба, предупреждение
             об ошибке отправки попадёт в лог именно этого джоба.
    """
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    _log = logger or logging.getLogger(__name__)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        _log.warning("tg_send failed: %s", e)
```

---
### infra\docker-compose.yml

```yaml
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: marketplace
      POSTGRES_USER: app
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}
      POSTGRES_INITDB_ARGS: "--auth-host=scram-sha-256"
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d marketplace"]
      interval: 5s
      timeout: 3s
      retries: 20
```

---
### infra\docker-compose.yml.txt

```
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: marketplace
      POSTGRES_USER: app
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}
      POSTGRES_INITDB_ARGS: "--auth-host=scram-sha-256"
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d marketplace"]
      interval: 5s
      timeout: 3s
      retries: 20
```

---
### README.md

```markdown
# DataBase_MP — Автоматизация загрузки данных маркетплейсов

Проект автоматически собирает заказы с Wildberries (и в будущем Ozon) в локальную PostgreSQL-базу через Docker. Данные обновляются каждый час через Планировщик задач Windows.

---

## Стек

| Компонент | Версия |
|---|---|
| Python | 3.11+ |
| PostgreSQL | 16 (Docker) |
| psycopg2-binary | последняя |
| requests | последняя |
| python-dotenv | последняя |
| Docker Desktop | последняя |

---

## Структура проекта

```
DataBase_MP/
├── app/
│   ├── clients/
│   │   └── wb_statistics.py          # HTTP-клиент WB Statistics API
│   ├── db.py                         # Все функции работы с БД
│   ├── wb_normalize.py               # Нормализация сырых данных WB
│   ├── jobs_wb_orders_raw_norm.py    # Инкрементальный джоб (каждый час)
│   ├── jobs_wb_orders_backfill.py    # Разовый бэкфилл исторических данных
│   └── requirements.txt
├── infra/
│   ├── docker-compose.yml            # PostgreSQL контейнер
│   └── init/                         # SQL-скрипты инициализации БД
├── logs/
│   └── wb_orders.txt                 # Логи запусков
├── run_wb_orders.cmd                 # Точка входа для планировщика
├── run_wb_orders.ps1                 # PowerShell-обёртка
├── .env                              # Секреты (не в git!)
├── .env.example                      # Шаблон переменных
└── README.md
```

---

## Быстрый старт

### 1. Клонировать и настроить окружение

```powershell
cd C:\Програмирование\Проекты
# скопировать .env.example → .env и заполнить значения
copy DataBase_MP\.env.example DataBase_MP\.env
```

Открыть `.env` и заполнить:

```env
WB_TOKEN=ваш_токен_statistics_api
PG_DSN=postgresql://app:ваш_пароль@localhost:5432/marketplace
POSTGRES_PASSWORD=ваш_пароль
```

> **Где взять WB_TOKEN:** Личный кабинет WB → Настройки → Доступ к API → Статистика

### 2. Поднять базу данных

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
docker compose -f infra\docker-compose.yml up -d
```

Проверить что контейнер запущен:

```powershell
docker ps
# должна быть строка: infra-db-1   postgres:16   Up
```

### 3. Установить зависимости Python

```powershell
pip install -r app\requirements.txt
```

### 4. Загрузить исторические данные (бэкфилл — один раз)

```powershell
# Загрузит все данные начиная с указанной даты
python app\jobs_wb_orders_backfill.py 2026-01-01
```

Ожидаемое время: 2–10 минут в зависимости от объёма данных.
WB-лимит: 1 запрос в минуту → каждые 80 000 строк = +62 сек.

### 5. Добавить в Планировщик задач Windows (автообновление каждый час)

```powershell
# Создать задачу (выполнить один раз от имени администратора)
$action  = New-ScheduledTaskAction -Execute "powershell.exe" `
           -Argument "-NonInteractive -File `"C:\Програмирование\Проекты\DataBase_MP\run_wb_orders.ps1`""
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 60) -Once -At "00:00"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync" `
    -Action $action -Trigger $trigger -Settings $settings
```

---

## Как работает инкрементальный джоб

```
Каждый час (Планировщик Windows)
  └── run_wb_orders.cmd
        └── run_wb_orders.ps1
              └── jobs_wb_orders_raw_norm.py
                    ├── Читает курсор из job_cursors (последняя дата изменения)
                    ├── Запрашивает WB API: lastChangeDate >= cursor - lookback
                    ├── INSERT → wb_orders_raw_dedup (сырые, дедупликация)
                    ├── UPSERT → wb_orders_norm (по srid, обновляет статусы)
                    ├── Обновляет курсор в job_cursors
                    └── Пишет результат в job_runs
```

**Lookback** — намеренный откат курсора на 2–15 минут назад чтобы не пропустить заказы с запоздавшим обновлением статуса на стороне WB.

---

## Таблицы БД

| Таблица | Назначение |
|---|---|
| `wb_orders_norm` | Нормализованные заказы WB (основная, 27 полей) |
| `wb_orders_raw_dedup` | Сырые JSON-версии каждого изменения (хранятся 14 дней) |
| `job_cursors` | Текущий курсор каждого джоба |
| `job_runs` | История всех запусков с метриками |

### Ключевые поля `wb_orders_norm`

| Поле | Тип | Описание |
|---|---|---|
| `srid` | TEXT PK | Уникальный ID заказа WB |
| `is_cancel` | BOOLEAN | Статус отмены |
| `date_ts` | TIMESTAMPTZ | Дата создания заказа |
| `last_change_ts` | TIMESTAMPTZ | Дата последнего изменения |
| `brand` | TEXT | Бренд товара |
| `price_with_disc` | NUMERIC | Цена со скидкой (выручка) |
| `cancel_date` | DATE | Дата отмены (если отменён) |

---

## Мониторинг

### Проверить последние запуски

```sql
SELECT job_name, started_at, api_rows, norm_upserted, dup_pct, status
FROM job_runs
ORDER BY id DESC
LIMIT 10;
```

### Проверить текущий курсор

```sql
SELECT * FROM job_cursors;
```

### Статистика по месяцам

```sql
SELECT
    DATE_TRUNC('month', date_ts)    AS month,
    COUNT(*)                         AS orders,
    SUM(price_with_disc)             AS revenue,
    SUM(CASE WHEN is_cancel THEN 1 ELSE 0 END) AS cancels
FROM wb_orders_norm
GROUP BY 1
ORDER BY 1;
```

---

## Управление планировщиком

```powershell
# Запустить вручную
Start-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# Приостановить (например перед бэкфиллом)
Disable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# Возобновить
Enable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# Проверить статус последнего запуска
Get-ScheduledTaskInfo -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync" |
    Select LastRunTime, LastTaskResult, NextRunTime
# LastTaskResult = 0 → успех
```

---

## Повторный бэкфилл (если нужно перезалить данные)

```powershell
# 1. Остановить планировщик
Disable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# 2. Запустить бэкфилл с нужной даты
python app\jobs_wb_orders_backfill.py 2026-01-01

# 3. Возобновить планировщик
Enable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"
```

---

## Переменные окружения (.env)

| Переменная | Обязательна | По умолчанию | Описание |
|---|---|---|---|
| `WB_TOKEN` | ✅ | — | Токен WB Statistics API |
| `PG_DSN` | ✅ | — | DSN подключения к PostgreSQL |
| `POSTGRES_PASSWORD` | ✅ | — | Пароль для Docker-контейнера |
| `LOG_LEVEL` | ❌ | `INFO` | Уровень логов (DEBUG/INFO/WARNING) |
| `WB_FIRST_RUN_DAYS_BACK` | ❌ | `3` | Глубина первого запуска (дней) |
| `WB_LOOKBACK_MINUTES` | ❌ | `10` | Откат курсора назад (минут) |
| `WB_FORCE_PAGINATION` | ❌ | `false` | Принудительная пагинация |
| `WB_RAW_DEDUP_RETENTION_DAYS` | ❌ | `14` | Хранение сырых данных (дней) |

---

## Подключение в DBeaver

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `marketplace`
- **User:** `app`
- **Password:** значение `POSTGRES_PASSWORD` из `.env`

---

## Дорожная карта

- [x] WB заказы — инкрементальная загрузка
- [x] WB заказы — бэкфилл исторических данных
- [x] Advisory lock — защита от двойного запуска
- [x] Мониторинг через `job_runs`
- [ ] Ozon заказы
- [ ] WB продажи (`/api/v1/supplier/sales`)
- [ ] Дашборд аналитики
- [ ] Алерты при `LastTaskResult != 0`

```

---
### requirements.txt

_Skipped: binary-NUL_


---
### scripts\run_wb_orders.cmd

```bat
@echo off
:: Запуск ETL-джоба WB Orders
:: Расположение: DataBase_MP\scripts\run_wb_orders.cmd
:: Task Scheduler: Action → Start a program
::   Program:  cmd.exe
::   Args:     /c "C:\Програмирование\Проекты\DataBase_MP\scripts\run_wb_orders.cmd"

setlocal
set ROOT=C:\Програмирование\Проекты\DataBase_MP
set VENV=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs\job_wb_orders.py
set LOG=%ROOT%\logs\wb_orders.log

%VENV% "%JOB%" >> "%LOG%" 2>&1
endlocal

```

---
### scripts\run_wb_orders.md

```markdown
@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
set PYTHONUTF8=1

set ROOT=%~dp0
if "%ROOT:~-1%"=="\" set ROOT=%ROOT:~0,-1%

set LOG=%ROOT%\logs\wb_orders.txt
set COMPOSEFILE=%ROOT%\infra\docker-compose.yml
set ENVFILE=%ROOT%\.env
set PY=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs_wb_orders_raw_norm.py
set CONTAINER=infra-db-1

if not exist "%ROOT%\logs\" mkdir "%ROOT%\logs\"

echo %date% %time% START >> "%LOG%"
echo ROOT=%ROOT% >> "%LOG%"

if not exist "%COMPOSEFILE%" (
    echo ERR: no compose file: %COMPOSEFILE% >> "%LOG%"
    echo %date% %time% END FAIL missing compose >> "%LOG%"
    exit /b 2
)
if not exist "%PY%" (
    echo ERR: no venv python: %PY% >> "%LOG%"
    echo %date% %time% END FAIL missing venv >> "%LOG%"
    exit /b 3
)
if not exist "%JOB%" (
    echo ERR: no job file: %JOB% >> "%LOG%"
    echo %date% %time% END FAIL missing job >> "%LOG%"
    exit /b 4
)

echo %date% %time% docker compose up -d >> "%LOG%"
docker compose --env-file "%ENVFILE%" -f "%COMPOSEFILE%" up -d >> "%LOG%" 2>&1

set /a tries=24
:WAITDOCKER
docker exec -e PGPASSWORD=%DB_PASS% %CONTAINER% psql -U app -d marketplace --no-password -c "select 1" >> "%LOG%" 2>&1
if !errorlevel!==0 goto DOJOB
set /a tries=!tries!-1
if !tries! LEQ 0 goto FAILDOCKER
timeout /t 5 /nobreak >nul
goto WAITDOCKER

:DOJOB
echo %date% %time% DB ready >> "%LOG%"
cd /d "%ROOT%"
"%PY%" "%JOB%" >> "%LOG%" 2>&1
set JOBEXIT=!errorlevel!
if not !JOBEXIT!==0 (
    echo %date% %time% END FAIL job exit=!JOBEXIT! >> "%LOG%"
    exit /b !JOBEXIT!
)
echo %date% %time% END OK >> "%LOG%"
exit /b 0

:FAILDOCKER
echo %date% %time% END FAIL db not ready after 120s >> "%LOG%"
exit /b 1

```

---
### scripts\run_wb_stocks.cmd

```bat
@echo off
:: Запуск ETL-джоба WB Stocks
:: Расположение: DataBase_MP\scripts\run_wb_stocks.cmd
:: Task Scheduler: Action → Start a program
::   Program:  cmd.exe
::   Args:     /c "C:\Програмирование\Проекты\DataBase_MP\scripts\run_wb_stocks.cmd"

setlocal
set ROOT=C:\Програмирование\Проекты\DataBase_MP
set VENV=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs\job_wb_stocks.py
set LOG=%ROOT%\logs\wb_stocks.log

%VENV% "%JOB%" >> "%LOG%" 2>&1
endlocal

```

---
### scripts\run_wb_stocks.md

```markdown
@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
set PYTHONUTF8=1

set ROOT=%~dp0
if "%ROOT:~-1%"=="\" set ROOT=%ROOT:~0,-1%

set LOG=%ROOT%\logs\wb_stocks.txt
set COMPOSEFILE=%ROOT%\infra\docker-compose.yml
set ENVFILE=%ROOT%\.env
set PY=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\wb_stocks_job.py
set CONTAINER=infra-db-1

if not exist "%ROOT%\logs\" mkdir "%ROOT%\logs\"

echo %date% %time% START >> "%LOG%"
echo ROOT=%ROOT% >> "%LOG%"

if not exist "%COMPOSEFILE%" (
    echo ERR: no compose file: %COMPOSEFILE% >> "%LOG%"
    echo %date% %time% END FAIL missing compose >> "%LOG%"
    exit /b 2
)
if not exist "%PY%" (
    echo ERR: no venv python: %PY% >> "%LOG%"
    echo %date% %time% END FAIL missing venv >> "%LOG%"
    exit /b 3
)
if not exist "%JOB%" (
    echo ERR: no job file: %JOB% >> "%LOG%"
    echo %date% %time% END FAIL missing job >> "%LOG%"
    exit /b 4
)

echo %date% %time% docker compose up -d >> "%LOG%"
docker compose --env-file "%ENVFILE%" -f "%COMPOSEFILE%" up -d >> "%LOG%" 2>&1

set /a tries=24
:WAITDOCKER
docker exec -e PGPASSWORD=%DB_PASS% %CONTAINER% psql -U app -d marketplace --no-password -c "select 1" >> "%LOG%" 2>&1
if !errorlevel!==0 goto DOJOB
set /a tries=!tries!-1
if !tries! LEQ 0 goto FAILDOCKER
timeout /t 5 /nobreak >nul
goto WAITDOCKER

:DOJOB
echo %date% %time% DB ready >> "%LOG%"
cd /d "%ROOT%"
"%PY%" "%JOB%" >> "%LOG%" 2>&1
set JOBEXIT=!errorlevel!
if not !JOBEXIT!==0 (
    echo %date% %time% END FAIL job exit=!JOBEXIT! >> "%LOG%"
    exit /b !JOBEXIT!
)
echo %date% %time% END OK >> "%LOG%"
exit /b 0

:FAILDOCKER
echo %date% %time% END FAIL db not ready after 120s >> "%LOG%"
exit /b 1
```

---
### tools\export_review.md

```markdown
$ErrorActionPreference = "Stop"

$root = (Get-Location).Path
$out  = Join-Path $root "PROJECT_REVIEW.md"

$excludeDirs = @(
  ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache",
  "node_modules", ".idea", ".vscode",
  "infra\pg_data", "infra\pgdata", "infra\data", "pg_data", "data",
  "logs"
)

$allowExt   = @(".py",".ps1",".bat",".cmd",".vbs",".yml",".yaml",".json",".md",".txt",".ini",".cfg",".toml",".dockerignore",".gitignore")
$allowNames = @("Dockerfile","docker-compose.yml","docker-compose.yaml","requirements.txt","README.md")

$excludeFileMasks = @(
  ".env", ".env.*",
  "PROJECT_REVIEW.md", "export_review_v2.ps1",
  "*.log","*.zip","*.7z","*.rar",
  "*.exe","*.dll","*.pdb",
  "*.png","*.jpg","*.jpeg","*.gif","*.ico",
  "*.pdf","*.mp4","*.mov",
  "*.db","*.sqlite","*.bak",
  "*.sql"
)

function Is-ExcludedPath([string]$fullPath) {
  $rel = $fullPath.Substring($root.Length).TrimStart('\')
  foreach ($d in $excludeDirs) {
    if ($rel -like "$d\*" -or $rel -eq $d) { return $true }
  }
  return $false
}

function Is-AllowedFile([System.IO.FileInfo]$f) {
  $name = $f.Name
  foreach ($mask in $excludeFileMasks) { if ($name -like $mask) { return $false } }
  if ($allowNames -contains $name) { return $true }
  $ext = [IO.Path]::GetExtension($name).ToLowerInvariant()
  return ($allowExt -contains $ext)
}

function Get-LangByExt([string]$path) {
  switch ([IO.Path]::GetExtension($path).ToLowerInvariant()) {
    ".py"   { "python" }
    ".ps1"  { "powershell" }
    ".bat"  { "bat" }
    ".cmd"  { "bat" }
    ".vbs"  { "vbscript" }
    ".json" { "json" }
    ".yml"  { "yaml" }
    ".yaml" { "yaml" }
    ".md"   { "markdown" }
    default { "" }
  }
}

function Read-TextSmart([string]$path) {
  $bytes = [System.IO.File]::ReadAllBytes($path)
  if ($bytes.Length -eq 0) { return @{ Ok=$true; Text="" } }
  $sample = $bytes[0..([Math]::Min($bytes.Length-1,4095))]
  if ($sample -contains 0) { return @{ Ok=$false; Reason="binary-NUL"; Text=$null } }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::Unicode).GetString($bytes) }
  }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::BigEndianUnicode).GetString($bytes) }
  }
  try {
    $utf8 = New-Object System.Text.UTF8Encoding($false,$true)
    return @{ Ok=$true; Text=$utf8.GetString($bytes) }
  } catch {
    $cp1251 = [System.Text.Encoding]::GetEncoding(1251)
    return @{ Ok=$true; Text=$cp1251.GetString($bytes) }
  }
}

@(
  "# Project review bundle"
  ""
  "Root: $root"
  "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  ""
  "## File list"
  ""
) | Set-Content -Path $out -Encoding UTF8

$files = Get-ChildItem -Path $root -Recurse -File -Force -ErrorAction SilentlyContinue |
  Where-Object { -not (Is-ExcludedPath $_.FullName) } |
  Where-Object { Is-AllowedFile $_ } |
  Sort-Object FullName

$files |
  ForEach-Object { "- " + $_.FullName.Substring($root.Length).TrimStart('\') } |
  Add-Content -Path $out -Encoding UTF8

Add-Content -Path $out -Encoding UTF8 -Value "`n## Files content`n"

foreach ($f in $files) {
  $rel  = $f.FullName.Substring($root.Length).TrimStart('\')
  $lang = Get-LangByExt $f.FullName
  Add-Content -Path $out -Encoding UTF8 -Value "`n---`n### $rel`n"
  $r = Read-TextSmart $f.FullName
  if (-not $r.Ok) {
    Add-Content -Path $out -Encoding UTF8 -Value "_Skipped: $($r.Reason)_`n"
    continue
  }
  Add-Content -Path $out -Encoding UTF8 -Value ('```' + $lang)
  Add-Content -Path $out -Encoding UTF8 -Value $r.Text
  Add-Content -Path $out -Encoding UTF8 -Value '```'
}

Write-Host "OK: files=$($files.Count) out=$out"

```

---
### tools\export_review.ps1

```powershell
$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$out = Join-Path $PSScriptRoot "PROJECT_REVIEW.md"

$excludeDirs = @(
  ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache",
  "node_modules", ".idea", ".vscode",
  "infra\pg_data", "infra\pgdata", "infra\data", "pg_data", "data",
  "logs"
)

$allowExt   = @(".py",".ps1",".bat",".cmd",".vbs",".yml",".yaml",".json",".md",".txt",".ini",".cfg",".toml",".dockerignore",".gitignore")
$allowNames = @("Dockerfile","docker-compose.yml","docker-compose.yaml","requirements.txt","README.md")

$excludeFileMasks = @(
  ".env", ".env.*",
  "PROJECT_REVIEW.md", "export_review_v2.ps1",
  "*.log","*.zip","*.7z","*.rar",
  "*.exe","*.dll","*.pdb",
  "*.png","*.jpg","*.jpeg","*.gif","*.ico",
  "*.pdf","*.mp4","*.mov",
  "*.db","*.sqlite","*.bak",
  "*.sql"
)

function Is-ExcludedPath([string]$fullPath) {
  $rel = $fullPath.Substring($root.Length).TrimStart('\')
  foreach ($d in $excludeDirs) {
    if ($rel -like "$d\*" -or $rel -eq $d) { return $true }
  }
  return $false
}

function Is-AllowedFile([System.IO.FileInfo]$f) {
  $name = $f.Name
  foreach ($mask in $excludeFileMasks) { if ($name -like $mask) { return $false } }
  if ($allowNames -contains $name) { return $true }
  $ext = [IO.Path]::GetExtension($name).ToLowerInvariant()
  return ($allowExt -contains $ext)
}

function Get-LangByExt([string]$path) {
  switch ([IO.Path]::GetExtension($path).ToLowerInvariant()) {
    ".py"   { "python" }
    ".ps1"  { "powershell" }
    ".bat"  { "bat" }
    ".cmd"  { "bat" }
    ".vbs"  { "vbscript" }
    ".json" { "json" }
    ".yml"  { "yaml" }
    ".yaml" { "yaml" }
    ".md"   { "markdown" }
    default { "" }
  }
}

function Read-TextSmart([string]$path) {
  $bytes = [System.IO.File]::ReadAllBytes($path)
  if ($bytes.Length -eq 0) { return @{ Ok=$true; Text="" } }
  $sample = $bytes[0..([Math]::Min($bytes.Length-1,4095))]
  if ($sample -contains 0) { return @{ Ok=$false; Reason="binary-NUL"; Text=$null } }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::Unicode).GetString($bytes) }
  }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::BigEndianUnicode).GetString($bytes) }
  }
  try {
    $utf8 = New-Object System.Text.UTF8Encoding($false,$true)
    return @{ Ok=$true; Text=$utf8.GetString($bytes) }
  } catch {
    $cp1251 = [System.Text.Encoding]::GetEncoding(1251)
    return @{ Ok=$true; Text=$cp1251.GetString($bytes) }
  }
}

@(
  "# Project review bundle"
  ""
  "Root: $root"
  "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  ""
  "## File list"
  ""
) | Set-Content -Path $out -Encoding UTF8

$files = Get-ChildItem -Path $root -Recurse -File -Force -ErrorAction SilentlyContinue |
  Where-Object { -not (Is-ExcludedPath $_.FullName) } |
  Where-Object { Is-AllowedFile $_ } |
  Sort-Object FullName

$files |
  ForEach-Object { "- " + $_.FullName.Substring($root.Length).TrimStart('\') } |
  Add-Content -Path $out -Encoding UTF8

Add-Content -Path $out -Encoding UTF8 -Value "`n## Files content`n"

foreach ($f in $files) {
  $rel  = $f.FullName.Substring($root.Length).TrimStart('\')
  $lang = Get-LangByExt $f.FullName
  Add-Content -Path $out -Encoding UTF8 -Value "`n---`n### $rel`n"
  $r = Read-TextSmart $f.FullName
  if (-not $r.Ok) {
    Add-Content -Path $out -Encoding UTF8 -Value "_Skipped: $($r.Reason)_`n"
    continue
  }
  Add-Content -Path $out -Encoding UTF8 -Value ('```' + $lang)
  Add-Content -Path $out -Encoding UTF8 -Value $r.Text
  Add-Content -Path $out -Encoding UTF8 -Value '```'
}

Write-Host "OK: files=$($files.Count) out=$out"

```
