from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import requests

from app.secrets import get_secret


API_ERP_TRU_BASE_URL = "https://li1430-252.members.linode.com"
PRODUCT_STATS_PATH = "/api/v1/product/stat_list/"


@dataclass(frozen=True)
class ApiErpTruResponseLog:
    method_name: str
    http_method: str
    url: str
    request_payload: dict[str, Any] | None
    response_status: int | None
    response_payload: Any
    duration_ms: int
    attempt: int
    error: str | None = None


class ApiErpTruClient:
    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = API_ERP_TRU_BASE_URL,
        session: requests.Session | None = None,
        timeout: int = 60,
        max_attempts: int = 5,
    ) -> None:
        self.token = token if token is not None else get_secret("API_ERP_TRU_TOKEN")
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_attempts = max(1, max_attempts)
        if not self.token:
            raise RuntimeError("API_ERP_TRU_TOKEN не задан")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json, text/plain, */*",
        }

    def request_product_stats(self, *, date_from: date, date_to: date, wo_sets: bool = False) -> tuple[list[dict[str, Any]], ApiErpTruResponseLog]:
        url = self.base_url + PRODUCT_STATS_PATH
        params = build_product_stats_params(date_from=date_from, date_to=date_to, wo_sets=wo_sets)
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            status: int | None = None
            response_payload: Any = None
            retry_after: str | None = None
            try:
                response = self.session.get(url, headers=self._headers(), params=params, timeout=self.timeout)
                status = response.status_code
                retry_after = response.headers.get("Retry-After")
                response_payload = response.json() if response.content else []
                duration_ms = int((time.monotonic() - started) * 1000)
                log = ApiErpTruResponseLog(
                    method_name="api_erp_tru_product_stats",
                    http_method="GET",
                    url=url,
                    request_payload=params,
                    response_status=status,
                    response_payload=response_payload,
                    duration_ms=duration_ms,
                    attempt=attempt,
                )
                if status < 400:
                    if not isinstance(response_payload, list):
                        raise RuntimeError(f"ERP/TRU product stats вернул не список: {type(response_payload).__name__}")
                    return [item for item in response_payload if isinstance(item, dict)], log
                if status not in {408, 429, 500, 502, 503, 504} or attempt >= self.max_attempts:
                    raise RuntimeError(f"ERP/TRU API HTTP {status}: {response.text[:500]}")
            except Exception as exc:
                last_error = exc
                duration_ms = int((time.monotonic() - started) * 1000)
                if attempt >= self.max_attempts:
                    return [], ApiErpTruResponseLog(
                        method_name="api_erp_tru_product_stats",
                        http_method="GET",
                        url=url,
                        request_payload=params,
                        response_status=status,
                        response_payload=response_payload,
                        duration_ms=duration_ms,
                        attempt=attempt,
                        error=repr(exc),
                    )
            sleep_seconds = _retry_sleep_seconds(attempt=attempt, retry_after=retry_after)
            time.sleep(sleep_seconds)
        raise RuntimeError(f"ERP/TRU API failed: {last_error}")


def build_product_stats_params(*, date_from: date, date_to: date, wo_sets: bool = False) -> dict[str, str]:
    return {
        "rel_products_in_order_for_product__order__delivery_DT_from": f"{date_from.isoformat()} 00:00",
        "rel_products_in_order_for_product__order__delivery_DT_to": f"{date_to.isoformat()} 23:59",
        "wo_sets": "true" if wo_sets else "false",
    }


def _retry_sleep_seconds(*, attempt: int, retry_after: str | None) -> int:
    if retry_after:
        try:
            return max(1, min(int(retry_after), 120))
        except ValueError:
            pass
    return min(2 ** attempt, 30)


def response_sha256(payload: Any) -> str:
    import hashlib
    import json

    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
