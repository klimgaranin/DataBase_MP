"""HTTP-клиент WB Analytics API: Лента заказов."""
from __future__ import annotations

import hashlib
import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

import requests

from app.secrets import get_secret


DEFAULT_BASE_URL = "https://seller-analytics-api.wildberries.ru"
DEFAULT_PATH = "/api/analytics/v1/order-feed"


@dataclass(frozen=True)
class WbOrderFeedResponseLog:
    method_name: str
    http_method: str
    url: str
    request_payload: dict[str, Any]
    response_status: int | None
    response_payload: dict[str, Any] | None
    duration_ms: int
    attempt: int
    error: str | None = None


class WbOrderFeedClient:
    """Транспорт и rate-limit для POST /api/analytics/v1/order-feed."""

    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        timeout_sec: int = 60,
        max_attempts: int = 5,
        min_request_interval_sec: float = 61.0,
    ) -> None:
        self.token = token if token is not None else get_secret("WB_ANALYTICS_TOKEN")
        if not self.token:
            raise RuntimeError("WB_ANALYTICS_TOKEN (или WB_TOKEN) не задан")
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout_sec = max(1, timeout_sec)
        self.max_attempts = max(1, max_attempts)
        self.min_request_interval_sec = max(0.0, min_request_interval_sec)
        self._last_request_at: float | None = None

    def _wait_for_limit(self) -> None:
        if self._last_request_at is None or self.min_request_interval_sec <= 0:
            return
        remaining = self.min_request_interval_sec - (time.monotonic() - self._last_request_at)
        if remaining > 0:
            time.sleep(remaining)

    def request(self, payload: dict[str, Any]) -> tuple[dict[str, Any], WbOrderFeedResponseLog]:
        url = self.base_url + DEFAULT_PATH
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            self._wait_for_limit()
            started = time.monotonic()
            status: int | None = None
            response_payload: dict[str, Any] | None = None
            try:
                response = self.session.post(
                    url,
                    headers={"Authorization": self.token, "Content-Type": "application/json"},
                    json=payload,
                    timeout=self.timeout_sec,
                )
                self._last_request_at = time.monotonic()
                status = response.status_code
                response_payload = response.json() if response.content else {}
                duration_ms = int((time.monotonic() - started) * 1000)
                response_log = WbOrderFeedResponseLog(
                    method_name="wb_order_feed",
                    http_method="POST",
                    url=url,
                    request_payload=payload,
                    response_status=status,
                    response_payload=response_payload,
                    duration_ms=duration_ms,
                    attempt=attempt,
                )
                if 200 <= status < 300:
                    return response_payload, response_log
                if status not in {408, 429, 500, 502, 503, 504}:
                    return {}, WbOrderFeedResponseLog(
                        method_name="wb_order_feed",
                        http_method="POST",
                        url=url,
                        request_payload=payload,
                        response_status=status,
                        response_payload=response_payload,
                        duration_ms=duration_ms,
                        attempt=attempt,
                        error=f"WB Order Feed HTTP {status}: {response.text[:500]}",
                    )
                retry_after = response.headers.get("Retry-After")
            except Exception as exc:
                last_error = exc
                duration_ms = int((time.monotonic() - started) * 1000)
                if attempt >= self.max_attempts:
                    return {}, WbOrderFeedResponseLog(
                        method_name="wb_order_feed",
                        http_method="POST",
                        url=url,
                        request_payload=payload,
                        response_status=status,
                        response_payload=response_payload,
                        duration_ms=duration_ms,
                        attempt=attempt,
                        error=repr(exc),
                    )
                retry_after = None

            if attempt < self.max_attempts:
                try:
                    retry_seconds = float(retry_after or "0")
                except ValueError:
                    retry_seconds = 0.0
                # Лимит метода — один запрос в минуту. Повтор раньше минуты только ухудшит ситуацию.
                time.sleep(max(self.min_request_interval_sec, retry_seconds, min(2 ** attempt, 30) + random.random()))

        raise RuntimeError(f"WB Order Feed failed: {last_error}")


def iter_order_feed(
    client: WbOrderFeedClient,
    *,
    start: datetime,
    end: datetime,
    timezone_name: str = "UTC",
    limit: int = 10_000,
    max_pages: int = 100,
) -> Iterator[tuple[list[dict[str, Any]], str | None, WbOrderFeedResponseLog]]:
    """Итерирует стабильный снимок WB: первый запрос задаёт snapshotTime для остальных."""
    page_limit = max(1, min(limit, 10_000))
    snapshot_time: str | None = None
    offset = 0

    for _page in range(1, max(1, max_pages) + 1):
        pagination: dict[str, Any] = {"offset": offset, "limit": page_limit}
        if snapshot_time:
            pagination["snapshotTime"] = snapshot_time
        payload = {
            "selectedPeriod": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "timezone": timezone_name,
            "nmIds": [],
            "subjectIds": [],
            "brandNames": [],
            "tagIds": [],
            "pagination": pagination,
        }
        response, response_log = client.request(payload)
        if response_log.error:
            raise RuntimeError(response_log.error)
        data = response.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("WB Order Feed: в успешном ответе отсутствует объект data")
        orders = data.get("orders") or []
        if not isinstance(orders, list) or any(not isinstance(row, dict) for row in orders):
            raise RuntimeError("WB Order Feed: data.orders должен быть массивом объектов")
        if snapshot_time is None:
            raw_snapshot = data.get("snapshotTime")
            snapshot_time = str(raw_snapshot) if raw_snapshot else None
        currency = data.get("currency")
        yield orders, str(currency) if currency else None, response_log
        if not orders or len(orders) < page_limit:
            return
        if not snapshot_time:
            raise RuntimeError("WB Order Feed: для следующей страницы WB не вернул snapshotTime")
        offset += len(orders)

    raise RuntimeError(f"WB Order Feed: превышено число страниц ({max_pages})")


def response_sha256(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
