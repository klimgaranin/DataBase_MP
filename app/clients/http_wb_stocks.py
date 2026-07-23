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
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.secrets import get_secret

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
    token = (get_secret("WB_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            "WB_TOKEN ne zadan -- nuzhen token kategorii Analitika."
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
