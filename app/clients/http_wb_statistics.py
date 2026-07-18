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
