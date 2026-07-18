"""
app/clients/http_wb_adv.py
HTTP-клиент для WB Advertising API.

Методы:
  fetch_campaigns()         — GET /adv/v1/promotion/count
  fetch_fullstats_batched() — GET /adv/v3/fullstats батчами по 50, sleep(21s)
  calc_eta()                — расчёт кол-ва батчей и времени ожидания

Токен: WB_TOKEN_CONTENT из окружения.
Rate limit /adv/v3/fullstats: 3 запроса/мин, интервал 21 сек между батчами.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable

import requests

_ADV_BASE            = "https://advert-api.wildberries.ru"
_FULLSTATS_BATCH     = 50
_FULLSTATS_SLEEP_SEC = 21   # интервал между батчами (rate-limit WB)
_MAX_ATTEMPTS        = 3    # попыток на батч

log = logging.getLogger(__name__)


def _headers() -> dict[str, str]:
    token = os.getenv("WB_TOKEN_CONTENT", "").strip()
    if not token:
        raise RuntimeError("WB_TOKEN_CONTENT не задан в .env")
    return {"Authorization": token}


# ─── /adv/v1/promotion/count ─────────────────────────────────────────────────

def fetch_campaigns(session: requests.Session) -> list[dict[str, Any]]:
    """
    Возвращает плоский список кампаний со всеми полями.
    Структура ответа WB:
      {adverts: [{status, type, advert_list: [{advertId, changeTime}]}]}
    """
    url  = f"{_ADV_BASE}/adv/v1/promotion/count"
    resp = session.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()

    items: list[dict[str, Any]] = []
    for group in (resp.json().get("adverts") or []):
        status   = group.get("status")
        adv_type = group.get("type")
        for ad in (group.get("advert_list") or []):
            advert_id = ad.get("advertId")
            if not advert_id:
                continue
            items.append({
                "advert_id":   int(advert_id),
                "status":      status,
                "type":        adv_type,
                "change_time": ad.get("changeTime"),
                "payload":     ad,
            })

    log.info("fetch_campaigns: %d кампаний получено", len(items))
    return items


# ─── /adv/v3/fullstats ───────────────────────────────────────────────────────

def fetch_fullstats_batched(
    advert_ids: list[int],
    begin_date: str,
    end_date:   str,
    session:    requests.Session,
    on_batch:   Callable[[int, int], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Загружает fullstats батчами по 50 ID с соблюдением rate-limit.

    advert_ids — список ID кампаний
    begin_date / end_date — строки YYYY-MM-DD
    on_batch(batch_idx, total) — опциональный прогресс-callback
    """
    url     = f"{_ADV_BASE}/adv/v3/fullstats"
    batches = [
        advert_ids[i : i + _FULLSTATS_BATCH]
        for i in range(0, len(advert_ids), _FULLSTATS_BATCH)
    ]
    total   = len(batches)
    results: list[dict[str, Any]] = []

    for idx, batch in enumerate(batches):
        # sleep перед каждым батчем кроме первого
        if idx > 0:
            log.info("fullstats: sleep %ds (батч %d/%d)", _FULLSTATS_SLEEP_SEC, idx + 1, total)
            time.sleep(_FULLSTATS_SLEEP_SEC)

        params = {
            "ids":       ",".join(str(i) for i in batch),
            "beginDate": begin_date,
            "endDate":   end_date,
        }

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                resp = session.get(url, headers=_headers(), params=params, timeout=30)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("X-Ratelimit-Retry", _FULLSTATS_SLEEP_SEC))
                    log.warning(
                        "fullstats: 429, ждём %ds (батч %d/%d, попытка %d)",
                        retry_after, idx + 1, total, attempt,
                    )
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    results.extend(data)
                    log.info("fullstats: батч %d/%d — %d элементов", idx + 1, total, len(data))
                break

            except requests.RequestException as e:
                log.warning("fullstats: батч %d/%d попытка %d ошибка: %s", idx + 1, total, attempt, e)
                if attempt == _MAX_ATTEMPTS:
                    log.error("fullstats: батч %d/%d — все %d попытки исчерпаны", idx + 1, total, _MAX_ATTEMPTS)
                else:
                    time.sleep(5)

        if on_batch:
            on_batch(idx + 1, total)

    log.info("fullstats: итого получено %d элементов за %d батчей", len(results), total)
    return results


# ─── Расчёт ETA ──────────────────────────────────────────────────────────────

def calc_eta(advert_ids: list[int]) -> tuple[int, int]:
    """
    Возвращает (batches, eta_seconds).

    Формула: первый батч сразу, между остальными sleep(21s).
    Плюс ~2 сек на каждый HTTP-запрос.
    """
    batches = max(1, (len(advert_ids) + _FULLSTATS_BATCH - 1) // _FULLSTATS_BATCH)
    eta_sec = (batches - 1) * _FULLSTATS_SLEEP_SEC + batches * 2
    return batches, eta_sec
