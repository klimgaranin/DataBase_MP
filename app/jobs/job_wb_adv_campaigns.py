"""
app/jobs/job_wb_adv_campaigns.py
ETL: обновляет список рекламных кампаний WB из /adv/v1/promotion/count.

Advisory lock : 4242003
Task Scheduler: \DB_MP\WB_Adv_Campaigns_Sync — раз в сутки (01:00)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import requests

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_iso_utc, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

from app.db import (
    try_advisory_lock, advisory_unlock, insert_job_run,
    upsert_wb_adv_campaigns,
)
from app.clients.http_wb_adv import fetch_campaigns

JOB_NAME = "wb_adv_campaigns"
LOCK_ID  = 4_242_003


def main() -> int:
    log = setup_logging(
        JOB_NAME,
        log_file=(os.getenv("WB_ADV_CAMPAIGNS_LOG_FILE") or "").strip() or None,
    )

    if not try_advisory_lock(LOCK_ID):
        log.info("[%s] lock занят, выходим", JOB_NAME)
        return 0

    started_at          = now_iso_utc()
    status              = "ok"
    error: Optional[str] = None
    api_rows            = 0
    upserted            = 0

    try:
        with requests.Session() as sess:
            items = fetch_campaigns(sess)

        api_rows = len(items)
        upserted = upsert_wb_adv_campaigns(items)
        log.info("[%s] api_rows=%d upserted=%d", JOB_NAME, api_rows, upserted)

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("[%s] ОШИБКА: %s", JOB_NAME, error)
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            insert_job_run(
                job_name         = JOB_NAME,
                started_at_iso   = started_at,
                finished_at_iso  = finished_at,
                status           = status,
                api_rows         = api_rows,
                raw_new_versions = 0,
                norm_upserted    = upserted,
                duplicates       = 0,
                dup_pct          = 0.0,
                error            = error,
            )
        except Exception as je:
            log.warning("[%s] insert_job_run failed: %s", JOB_NAME, je)

        ts  = now_msk_label()
        msg = (
            f"✅ WB_Adv_Campaigns | {ts} | OK\n\n"
            f"➡ Кампаний в API: {api_rows}\n"
            f"🔄 Обновлено в БД: {upserted}"
            if status == "ok" else
            f"❌ WB_Adv_Campaigns | {ts} | FAIL\n{(error or '')[:200]}"
        )
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
