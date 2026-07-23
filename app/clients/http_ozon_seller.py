from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import requests

from app.secrets import get_secret


OZON_BASE_URL = "https://api-seller.ozon.ru"
OZON_FBO_POSTING_STATUSES = (
    "awaiting_packaging",
    "awaiting_deliver",
    "delivering",
    "delivered",
    "cancelled",
)


@dataclass(frozen=True)
class OzonResponseLog:
    method_name: str
    http_method: str
    url: str
    request_payload: dict[str, Any] | None
    response_status: int | None
    response_payload: dict[str, Any] | None
    duration_ms: int
    attempt: int
    error: str | None = None


class OzonSellerClient:
    def __init__(
        self,
        *,
        client_id: str | None = None,
        api_key: str | None = None,
        base_url: str = OZON_BASE_URL,
        session: requests.Session | None = None,
        timeout: int = 60,
        max_attempts: int = 10,
    ) -> None:
        self.client_id = client_id if client_id is not None else get_secret("OZON_CLIENT_ID")
        self.api_key = api_key if api_key is not None else get_secret("OZON_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_attempts = max(1, max_attempts)
        if not self.client_id:
            raise RuntimeError("OZON_CLIENT_ID не задан")
        if not self.api_key:
            raise RuntimeError("OZON_API_KEY не задан")

    def _headers(self) -> dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def request(self, method_name: str, path: str, payload: dict[str, Any] | None = None) -> tuple[dict[str, Any], OzonResponseLog]:
        url = self.base_url + path
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            status: int | None = None
            response_payload: dict[str, Any] | None = None
            retry_after: str | None = None
            try:
                response = self.session.post(url, headers=self._headers(), json=payload or {}, timeout=self.timeout)
                status = response.status_code
                retry_after = response.headers.get("Retry-After")
                response_payload = response.json() if response.content else {}
                duration_ms = int((time.monotonic() - started) * 1000)
                log = OzonResponseLog(
                    method_name=method_name,
                    http_method="POST",
                    url=url,
                    request_payload=payload,
                    response_status=status,
                    response_payload=response_payload,
                    duration_ms=duration_ms,
                    attempt=attempt,
                )
                if status < 400:
                    return response_payload, log
                if status not in {408, 429, 500, 502, 503, 504} or attempt >= self.max_attempts:
                    raise RuntimeError(f"Ozon API {method_name} HTTP {status}: {response.text[:500]}")
            except Exception as exc:
                last_error = exc
                duration_ms = int((time.monotonic() - started) * 1000)
                if attempt >= self.max_attempts:
                    return {}, OzonResponseLog(
                        method_name=method_name,
                        http_method="POST",
                        url=url,
                        request_payload=payload,
                        response_status=status,
                        response_payload=response_payload,
                        duration_ms=duration_ms,
                        attempt=attempt,
                        error=repr(exc),
                    )
            if status == 429:
                try:
                    sleep_seconds = int(retry_after or "0")
                except ValueError:
                    sleep_seconds = 0
                time.sleep(max(sleep_seconds, min(10 * attempt, 60)))
            else:
                time.sleep(min(2 ** attempt, 30))
        raise RuntimeError(f"Ozon API {method_name} failed: {last_error}")


def iter_fbo_postings(
    client: OzonSellerClient,
    *,
    since: datetime,
    until: datetime,
    limit: int = 100,
) -> Iterable[tuple[list[dict[str, Any]], OzonResponseLog]]:
    cursor = ""
    while True:
        payload = {
            "cursor": cursor,
            "filter": {
                "since": since.isoformat().replace("+00:00", "Z"),
                "statuses": list(OZON_FBO_POSTING_STATUSES),
                "to": until.isoformat().replace("+00:00", "Z"),
            },
            "limit": max(1, min(limit, 100)),
            "sort_dir": "ASC",
            "translit": False,
            "with": {"analytics_data": True, "financial_data": True, "legal_info": True},
        }
        data, response_log = client.request("ozon_posting_fbo_list", "/v3/posting/fbo/list", payload)
        if response_log.error:
            raise RuntimeError(response_log.error)
        result = data.get("result") or data
        postings = result.get("postings") or []
        yield postings, response_log
        next_cursor = result.get("cursor") or ""
        has_next = bool(result.get("has_next"))
        if not postings or not has_next or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def iter_product_list(
    client: OzonSellerClient,
    *,
    visibility: str = "ALL",
    limit: int = 1000,
) -> Iterable[tuple[list[dict[str, Any]], OzonResponseLog]]:
    last_id = ""
    while True:
        payload = {"filter": {"visibility": visibility}, "last_id": last_id, "limit": max(1, min(limit, 1000))}
        data, response_log = client.request("ozon_product_list", "/v3/product/list", payload)
        if response_log.error:
            raise RuntimeError(response_log.error)
        result = data.get("result") or {}
        items = result.get("items") or []
        yield items, response_log
        next_last_id = result.get("last_id") or ""
        if not items or not next_last_id or next_last_id == last_id:
            break
        last_id = next_last_id


def fetch_product_info_list(
    client: OzonSellerClient,
    *,
    product_ids: list[int] | None = None,
    offer_ids: list[str] | None = None,
    skus: list[int] | None = None,
) -> tuple[list[dict[str, Any]], OzonResponseLog]:
    payload: dict[str, Any] = {}
    if product_ids:
        payload["product_id"] = product_ids
    if offer_ids:
        payload["offer_id"] = offer_ids
    if skus:
        payload["sku"] = skus
    data, response_log = client.request("ozon_product_info_list", "/v3/product/info/list", payload)
    if response_log.error:
        raise RuntimeError(response_log.error)
    return data.get("items") or [], response_log


def fetch_analytics_stocks(client: OzonSellerClient, *, skus: list[int]) -> tuple[list[dict[str, Any]], OzonResponseLog]:
    payload = {"skus": [str(sku) for sku in skus]}
    data, response_log = client.request("ozon_analytics_stocks", "/v1/analytics/stocks", payload)
    if response_log.error:
        raise RuntimeError(response_log.error)
    return data.get("items") or [], response_log


def fetch_analytics_stocks_batch(
    client: OzonSellerClient,
    *,
    sku_chunks: list[list[int]],
    max_workers: int = 20,
) -> tuple[list[dict[str, Any]], list[OzonResponseLog]]:
    pending = [
        {"index": index, "payload": {"skus": [str(sku) for sku in chunk]}, "attempt": 1}
        for index, chunk in enumerate(sku_chunks)
    ]
    results: list[list[dict[str, Any]] | None] = [None] * len(pending)
    logs: list[OzonResponseLog] = []

    while pending:
        retry: list[dict[str, Any]] = []
        retry_sleep = 0
        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(pending)))) as executor:
            futures = {executor.submit(_post_analytics_stocks_once, client, item): item for item in pending}
            for future in as_completed(futures):
                item = futures[future]
                response_items, response_log, retry_after = future.result()
                logs.append(response_log)
                status = response_log.response_status or 0
                if response_log.error is None and 200 <= status < 300:
                    results[item["index"]] = response_items
                    continue
                can_retry = status == 429 or status >= 500
                if can_retry and item["attempt"] < client.max_attempts:
                    retry.append({"index": item["index"], "payload": item["payload"], "attempt": item["attempt"] + 1})
                    if status == 429:
                        retry_sleep = max(retry_sleep, retry_after + 1 if retry_after > 0 else min(item["attempt"] * 30, 180))
                    else:
                        retry_sleep = max(retry_sleep, retry_after + 1 if retry_after > 0 else item["attempt"] * 3)
                    continue
                raise RuntimeError(response_log.error or f"Ozon API ozon_analytics_stocks HTTP {status}")
        if retry:
            time.sleep(max(1, retry_sleep))
        pending = retry

    combined: list[dict[str, Any]] = []
    for rows in results:
        combined.extend(rows or [])
    return combined, logs


def _post_analytics_stocks_once(
    client: OzonSellerClient,
    item: dict[str, Any],
) -> tuple[list[dict[str, Any]], OzonResponseLog, int]:
    url = client.base_url + "/v1/analytics/stocks"
    started = time.monotonic()
    status: int | None = None
    response_payload: dict[str, Any] | None = None
    retry_after = 0
    try:
        response = client.session.post(url, headers=client._headers(), json=item["payload"], timeout=client.timeout)
        status = response.status_code
        try:
            retry_after = int(response.headers.get("Retry-After") or "0")
        except ValueError:
            retry_after = 0
        response_payload = response.json() if response.content else {}
        duration_ms = int((time.monotonic() - started) * 1000)
        error = None if status < 400 else f"Ozon API ozon_analytics_stocks HTTP {status}: {response.text[:500]}"
        return (
            (response_payload.get("items") or []) if status < 400 else [],
            OzonResponseLog(
                method_name="ozon_analytics_stocks",
                http_method="POST",
                url=url,
                request_payload=item["payload"],
                response_status=status,
                response_payload=response_payload,
                duration_ms=duration_ms,
                attempt=item["attempt"],
                error=error,
            ),
            retry_after,
        )
    except Exception as exc:
        duration_ms = int((time.monotonic() - started) * 1000)
        return (
            [],
            OzonResponseLog(
                method_name="ozon_analytics_stocks",
                http_method="POST",
                url=url,
                request_payload=item["payload"],
                response_status=status,
                response_payload=response_payload,
                duration_ms=duration_ms,
                attempt=item["attempt"],
                error=repr(exc),
            ),
            retry_after,
        )


def create_placement_by_products_report(
    client: OzonSellerClient,
    *,
    date_from: date,
    date_to: date,
) -> tuple[str, OzonResponseLog]:
    payload = {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()}
    data, response_log = client.request("ozon_placement_by_products_create", "/v1/report/placement/by-products/create", payload)
    if response_log.error:
        raise RuntimeError(response_log.error)
    return str(data.get("code") or ""), response_log


def fetch_report_info(client: OzonSellerClient, *, code: str) -> tuple[dict[str, Any], OzonResponseLog]:
    data, response_log = client.request("ozon_report_info", "/v1/report/info", {"code": code})
    if response_log.error:
        raise RuntimeError(response_log.error)
    return data.get("result") or {}, response_log


def download_report_file(url: str, *, timeout: int = 120) -> bytes:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def chunked(values: list[Any], size: int) -> Iterable[list[Any]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def response_sha256(payload: dict[str, Any] | None) -> str:
    raw = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    import hashlib

    return hashlib.sha256(raw).hexdigest()
