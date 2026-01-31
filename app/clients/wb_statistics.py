from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Optional, cast

import requests

BASE_URL = "https://statistics-api.wildberries.ru"
MAX_ROWS_PER_RESPONSE = 80000  # условный лимит ответа WB для flag=0

# WB: лимит "1 запрос в минуту" для этих отчётов
DEFAULT_MIN_INTERVAL_SEC = 61

DEFAULT_CONNECT_TIMEOUT_SEC = 10
DEFAULT_READ_TIMEOUT_SEC = 60
DEFAULT_MAX_RETRIES = 5
DEFAULT_MAX_BACKOFF_SEC = 60

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

def _get_token() -> str:
    token = (
        os.getenv("WB_TOKEN", "").strip()
        or os.getenv("WBTOKEN", "").strip()
        or os.getenv("WB_API_KEY", "").strip()
    )
    if not token:
        raise RuntimeError("WB_TOKEN не задан. Добавь WB_TOKEN=... в .env")
    return token


def _auth_header_value(token: str) -> str:
    """
    По умолчанию используем как HeaderApiKey: Authorization: <token>.
    Если нужно Bearer-схема, включи WB_AUTH_BEARER=1 или положи в WB_TOKEN уже 'Bearer ...'.
    """
    t = token.strip()
    if not t:
        return t

    if t.lower().startswith("bearer "):
        return t

    bearer = (os.getenv("WB_AUTH_BEARER", "0").strip() == "1")
    return f"Bearer {t}" if bearer else t


def _headers() -> dict[str, str]:
    return {"Authorization": _auth_header_value(_get_token())}


def _timeouts() -> tuple[float, float]:
    connect = float(os.getenv("WB_CONNECT_TIMEOUT_SEC", str(DEFAULT_CONNECT_TIMEOUT_SEC)))
    read = float(os.getenv("WB_READ_TIMEOUT_SEC", str(DEFAULT_READ_TIMEOUT_SEC)))
    # safety: не даём нулевые/отрицательные значения
    connect = connect if connect > 0 else float(DEFAULT_CONNECT_TIMEOUT_SEC)
    read = read if read > 0 else float(DEFAULT_READ_TIMEOUT_SEC)
    return connect, read


def _max_retries() -> int:
    try:
        v = int(os.getenv("WB_HTTP_RETRIES", str(DEFAULT_MAX_RETRIES)))
        return max(0, min(v, 20))
    except Exception:
        return DEFAULT_MAX_RETRIES


def _min_interval_sec(explicit_sleep_sec: Optional[int] = None) -> int:
    """
    Минимальный интервал между запросами (rate limit).
    Можно переопределить:
      - WB_MIN_INTERVAL_SEC
      - параметром sleep_sec в iter_orders
    """
    base = int(os.getenv("WB_MIN_INTERVAL_SEC", str(DEFAULT_MIN_INTERVAL_SEC)))
    base = max(0, min(base, 600))

    if explicit_sleep_sec is None:
        return base

    try:
        s = int(explicit_sleep_sec)
        s = max(0, min(s, 600))
        return max(base, s)
    except Exception:
        return base


def _strict_pagination_default() -> bool:
    return os.getenv("WB_STRICT_PAGINATION", "0").strip() == "1"


def _max_pages_default() -> int:
    try:
        v = int(os.getenv("WB_MAX_PAGES", "5000"))
        return max(1, min(v, 100000))
    except Exception:
        return 5000


# ---------------------------------------------------------------------
# Rate limiter (чтобы не ловить 429 и соблюдать 1 req/min)
# ---------------------------------------------------------------------

@dataclass
class _RateLimiter:
    next_allowed_at: float = 0.0  # time.monotonic()

    def wait_turn(self, min_interval: int) -> None:
        now = time.monotonic()
        if now < self.next_allowed_at:
            time.sleep(self.next_allowed_at - now)
        # резервируем слот на следующий запрос
        self.next_allowed_at = max(self.next_allowed_at, time.monotonic()) + float(min_interval)

    def penalize(self, seconds: int) -> None:
        if seconds <= 0:
            return
        self.next_allowed_at = max(self.next_allowed_at, time.monotonic() + float(seconds))


_GLOBAL_RL = _RateLimiter()


# ---------------------------------------------------------------------
# 429 helpers
# ---------------------------------------------------------------------

def _parse_int_header(headers: Mapping[str, str], name: str) -> Optional[int]:
    v = headers.get(name)
    if v is None:
        return None
    try:
        x = int(str(v).strip())
        return x if x > 0 else None
    except Exception:
        return None


def _wait_seconds_for_429(headers: Mapping[str, str]) -> int:
    """
    WB рекомендует использовать:
      - X-Ratelimit-Retry (секунд до следующей попытки)
      - X-Ratelimit-Reset (секунд до восстановления лимита)
    Иногда встречается Retry-After.
    Берём максимально безопасную задержку.
    """
    retry = _parse_int_header(headers, "X-Ratelimit-Retry")
    if retry is None:
        retry = _parse_int_header(headers, "Retry-After")

    reset = _parse_int_header(headers, "X-Ratelimit-Reset")

    candidates = [x for x in (retry, reset) if isinstance(x, int) and x > 0]
    return max(candidates) if candidates else 60


# ---------------------------------------------------------------------
# HTTP request core
# ---------------------------------------------------------------------

def _request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    min_interval: int,
    rate_limiter: _RateLimiter,
) -> list[dict[str, Any]]:
    timeout = _timeouts()
    retries = _max_retries()

    for attempt in range(retries + 1):
        # соблюдаем rate limit перед ЛЮБОЙ попыткой (включая ретраи)
        rate_limiter.wait_turn(min_interval)

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
            wait = min(int((2 ** attempt)), DEFAULT_MAX_BACKOFF_SEC)
            wait += random.randint(0, 3)  # jitter
            log.warning("WB network error, retry in %ss: %r", wait, e)
            time.sleep(wait)
            continue

        resp_headers = cast(Mapping[str, str], resp.headers)

        # 429: ждём как велит WB по заголовкам
        if resp.status_code == 429:
            wait = _wait_seconds_for_429(resp_headers)
            rate_limiter.penalize(wait)
            if attempt >= retries:
                resp.raise_for_status()
            log.warning("WB 429 rate limit, wait %ss then retry", wait)
            continue

        # 5xx: ретраим
        if 500 <= resp.status_code <= 599:
            if attempt >= retries:
                resp.raise_for_status()
            wait = min(int((2 ** attempt)), DEFAULT_MAX_BACKOFF_SEC)
            wait += random.randint(0, 3)
            log.warning("WB %s server error, retry in %ss", resp.status_code, wait)
            time.sleep(wait)
            continue

        # 401/403/400/422 и т.п. — НЕ ретраим: это либо токен/права, либо неправильные параметры
        if resp.status_code in (400, 401, 403, 422):
            resp.raise_for_status()

        resp.raise_for_status()

        try:
            data = resp.json()
        except ValueError as e:
            if attempt >= retries:
                raise RuntimeError(f"WB invalid JSON response: {repr(e)}") from e
            wait = min(int((2 ** attempt)), DEFAULT_MAX_BACKOFF_SEC)
            wait += random.randint(0, 3)
            log.warning("WB invalid JSON, retry in %ss: %r", wait, e)
            time.sleep(wait)
            continue

        if not isinstance(data, list):
            raise RuntimeError(f"WB unexpected response type: {type(data)}")

        return cast(list[dict[str, Any]], data)

    raise RuntimeError("WB request failed after retries")


# ---------------------------------------------------------------------
# Public API: Orders
# ---------------------------------------------------------------------

def fetch_orders_page(
    date_from_iso: str,
    flag: int = 0,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/api/v1/supplier/orders"
    params: dict[str, Any] = {"dateFrom": date_from_iso, "flag": flag}

    if session is None:
        with requests.Session() as sess:
            return _request_json(sess, url, params, min_interval=_min_interval_sec(), rate_limiter=_GLOBAL_RL)

    return _request_json(session, url, params, min_interval=_min_interval_sec(), rate_limiter=_GLOBAL_RL)


def iter_orders(
    date_from_iso: str,
    flag: int = 0,
    sleep_sec: int = DEFAULT_MIN_INTERVAL_SEC,
    session: requests.Session | None = None,
    *,
    strict: Optional[bool] = None,
    max_pages: Optional[int] = None,
) -> Iterator[list[dict[str, Any]]]:
    """
    Итерируем страницы отчёта Orders.

    Быстрый режим (по умолчанию):
      - если пришло < 80000 строк, считаем что WB отдал всё за этот диапазон, выходим.
      - это критично для скорости, потому что лимит 1 запрос/мин.

    Строгий режим (strict=True или WB_STRICT_PAGINATION=1):
      - продолжаем ходить по страницам до пустого массива [] (или пока курсор не перестанет двигаться).
      - использовать для backfill, когда важнее полнота, чем скорость.
    """
    own_session = session is None
    sess = session or requests.Session()

    if strict is None:
        strict = _strict_pagination_default()
    max_pages_eff = max_pages if isinstance(max_pages, int) else _max_pages_default()
    max_pages_eff = max(1, min(max_pages_eff, 100000))

    cursor = date_from_iso
    page = 1

    min_interval = _min_interval_sec(sleep_sec)
    rl = _GLOBAL_RL

    try:
        while True:
            log.info("[WB] page=%s dateFrom=%s", page, cursor)

            chunk = _request_json(
                sess,
                f"{BASE_URL}/api/v1/supplier/orders",
                {"dateFrom": cursor, "flag": flag},
                min_interval=min_interval,
                rate_limiter=rl,
            )

            log.info("[WB] page=%s rows=%s", page, len(chunk))
            yield chunk

            if not chunk:
                break

            last = chunk[-1]
            new_cursor_any = last.get("lastChangeDate") or last.get("date")
            if not new_cursor_any:
                log.warning("[WB] stop: last record has no lastChangeDate/date")
                break

            new_cursor = str(new_cursor_any)

            # защита от бесконечного цикла
            if new_cursor == str(cursor):
                log.warning("[WB] stop: cursor not advancing (new_cursor=%s)", new_cursor)
                break

            # быстрый выход: если меньше лимита — обычно больше данных нет
            if (not strict) and (len(chunk) < MAX_ROWS_PER_RESPONSE):
                break

            cursor = new_cursor
            page += 1

            if page > max_pages_eff:
                log.warning("[WB] stop: max_pages reached (%s)", max_pages_eff)
                break

    finally:
        if own_session:
            sess.close()


def fetch_orders_incremental(
    date_from_iso: str,
    flag: int = 0,
    sleep_sec: int = DEFAULT_MIN_INTERVAL_SEC,
    *,
    strict: Optional[bool] = None,
) -> tuple[list[dict[str, Any]], str]:
    all_rows: list[dict[str, Any]] = []
    last_cursor_seen = date_from_iso

    with requests.Session() as sess:
        for chunk in iter_orders(
            date_from_iso,
            flag=flag,
            sleep_sec=sleep_sec,
            session=sess,
            strict=strict,
        ):
            if not chunk:
                break

            all_rows.extend(chunk)

            last = chunk[-1]
            c = last.get("lastChangeDate") or last.get("date")
            if c:
                last_cursor_seen = str(c)

    return all_rows, last_cursor_seen
