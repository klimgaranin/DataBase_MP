"""HTTP-клиент WB Content API: карточки товаров."""
from __future__ import annotations

import hashlib
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Iterator

import requests

from app.secrets import get_secret


DEFAULT_BASE_URL = "https://content-api.wildberries.ru"
DEFAULT_PATH = "/content/v2/get/cards/list"


@dataclass(frozen=True)
class WbContentResponseLog:
    method_name: str
    http_method: str
    url: str
    request_payload: dict[str, Any]
    response_status: int | None
    response_payload: dict[str, Any] | None
    duration_ms: int
    attempt: int
    error: str | None = None


class WbContentClient:
    """Транспорт WB Content API для POST /content/v2/get/cards/list."""

    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        timeout_sec: int = 60,
        max_attempts: int = 5,
        min_request_interval_sec: float = 0.7,
    ) -> None:
        self.token = token if token is not None else get_secret("WB_TOKEN_CONTENT")
        if not self.token:
            raise RuntimeError("WB_TOKEN_CONTENT (или WB_TOKEN) не задан")
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

    def request(self, payload: dict[str, Any]) -> tuple[dict[str, Any], WbContentResponseLog]:
        url = self.base_url + DEFAULT_PATH
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            self._wait_for_limit()
            started = time.monotonic()
            status: int | None = None
            response_payload: dict[str, Any] | None = None
            retry_after: str | None = None
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
                response_log = WbContentResponseLog(
                    method_name="wb_content_cards_list",
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
                    return {}, WbContentResponseLog(
                        method_name="wb_content_cards_list",
                        http_method="POST",
                        url=url,
                        request_payload=payload,
                        response_status=status,
                        response_payload=response_payload,
                        duration_ms=duration_ms,
                        attempt=attempt,
                        error=f"WB Content API HTTP {status}: {response.text[:500]}",
                    )
                retry_after = response.headers.get("Retry-After")
            except Exception as exc:
                last_error = exc
                duration_ms = int((time.monotonic() - started) * 1000)
                if attempt >= self.max_attempts:
                    return {}, WbContentResponseLog(
                        method_name="wb_content_cards_list",
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
                time.sleep(max(retry_seconds, min(2 ** attempt, 30) + random.random()))

        raise RuntimeError(f"WB Content API failed: {last_error}")


def iter_cards_list(
    client: WbContentClient,
    *,
    limit: int = 100,
    max_pages: int = 1000,
    with_photo: int = -1,
) -> Iterator[tuple[list[dict[str, Any]], WbContentResponseLog]]:
    page_limit = max(1, min(limit, 100))
    cursor: dict[str, Any] = {"limit": page_limit}

    for _page in range(1, max(1, max_pages) + 1):
        payload = {
            "settings": {
                "sort": {"ascending": True},
                "filter": {"withPhoto": with_photo},
                "cursor": cursor,
            }
        }
        response, response_log = client.request(payload)
        if response_log.error:
            raise RuntimeError(response_log.error)
        cards = response.get("cards") or []
        if not isinstance(cards, list) or any(not isinstance(row, dict) for row in cards):
            raise RuntimeError("WB Content API: cards должен быть массивом объектов")
        yield cards, response_log

        next_cursor = response.get("cursor") or {}
        total = int(next_cursor.get("total") or len(cards) or 0)
        nm_id = next_cursor.get("nmID") or next_cursor.get("nmId")
        updated_at = next_cursor.get("updatedAt")
        if not cards or total < page_limit or not nm_id or not updated_at:
            return
        cursor = {"limit": page_limit, "nmID": nm_id, "updatedAt": updated_at}

    raise RuntimeError(f"WB Content API: превышено число страниц ({max_pages})")


def response_sha256(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
