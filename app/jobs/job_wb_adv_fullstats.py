"""
app/jobs/job_wb_adv_fullstats.py  —  v2
ETL: получает fullstats рекламных кампаний и записывает в PostgreSQL + Google Sheets.

Изменения v2:
  • truncate + insert_raw + insert_norm → atomic_save_fullstats() (одна транзакция)
  • norm хранит все метрики: spend, views, clicks, atbs, orders, shks, sum_price
  • в Sheets передаются числа (int/float), апострофов нет
  • кампании берутся в порядке change_time DESC (свежие — первые батчи)
"""
from __future__ import annotations

import argparse
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
    get_adv_campaign_ids,
    atomic_save_fullstats,
)
from app.clients.http_wb_adv import fetch_fullstats_batched
from app.normalize.norm_wb_adv import normalize_fullstats, format_for_sheets

JOB_NAME = "wb_adv_fullstats"
LOCK_ID  = 4_242_004


def _gs_client():
    import gspread
    sa_file = os.getenv("GSPREAD_SA_FILE", "secrets/service_account.json")
    return gspread.service_account(filename=sa_file)


def _write_status(sheet_id: str, sheet_name: str, msg: str) -> None:
    import gspread
    try:
        ss = _gs_client().open_by_key(sheet_id)
        try:
            ws = ss.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = ss.add_worksheet(title=sheet_name, rows=10000, cols=10)
        ws.update([[msg]], "A1")
    except Exception:
        pass

def _write_results(sheet_id: str, sheet_name: str, rows: list[list], status_msg: str) -> None:
    import gspread
    ss = _gs_client().open_by_key(sheet_id)
    try:
        ws = ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows=10000, cols=10)
    ws.clear()
    ws.update([[status_msg]], "A1")
    ws.update([["ID кампании", "Дата", "Артикул WB", "Затраты, RUB"]], "B1")

    if rows:
        ws.update(rows, "B2")

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--begin",      required=True)
    parser.add_argument("--end",        required=True)
    parser.add_argument("--sheet_id",   required=True)
    parser.add_argument("--sheet_name", default="Stats_Full_WB")
    args = parser.parse_args()

    log = setup_logging(
        JOB_NAME,
        log_file=(os.getenv("WB_ADV_FULLSTATS_LOG_FILE") or "").strip() or None,
    )

    if not try_advisory_lock(LOCK_ID):
        log.info("[%s] lock занят — задача уже выполняется", JOB_NAME)
        return 0

    started_at            = now_iso_utc()
    status                = "ok"
    error: Optional[str]  = None
    api_rows              = 0
    norm_count            = 0
    sheet_rows_count      = 0

    _write_status(args.sheet_id, args.sheet_name,
                  f"🔄 {now_msk_label()} | Выполняется… | {args.begin} – {args.end}")

    try:
        advert_ids = get_adv_campaign_ids(statuses=[7, 9, 11])
        log.info("[%s] кампаний: %d (порядок change_time DESC)", JOB_NAME, len(advert_ids))

        if not advert_ids:
            _write_status(args.sheet_id, args.sheet_name,
                          f"⚠️ {now_msk_label()} | Нет активных кампаний в БД")
            return 0

        with requests.Session() as sess:
            raw_items = fetch_fullstats_batched(
                advert_ids=advert_ids, begin_date=args.begin,
                end_date=args.end, session=sess,
            )

        api_rows   = len(raw_items)
        norm_rows  = normalize_fullstats(raw_items)
        norm_count = len(norm_rows)
        log.info("[%s] api_rows=%d norm_rows=%d", JOB_NAME, api_rows, norm_count)

        # Атомарно: TRUNCATE + INSERT raw + INSERT norm в одной транзакции
        raw_saved, norm_saved = atomic_save_fullstats(
            raw_items=raw_items, begin_date=args.begin,
            end_date=args.end, norm_rows=norm_rows,
        )
        log.info("[%s] БД: raw=%d norm=%d", JOB_NAME, raw_saved, norm_saved)

        sheet_rows       = format_for_sheets(norm_rows, args.begin, args.end)
        sheet_rows_count = len(sheet_rows)
        _write_results(
            sheet_id=args.sheet_id, sheet_name=args.sheet_name,
            rows=sheet_rows,
            status_msg=f"✅ {now_msk_label()} | {args.begin} – {args.end} | {sheet_rows_count} строк",
        )
        log.info("[%s] Sheets: %d строк", JOB_NAME, sheet_rows_count)

    except Exception as e:
        status = "fail"
        error  = repr(e)
        log.exception("[%s] ОШИБКА: %s", JOB_NAME, error)
        _write_status(args.sheet_id, args.sheet_name,
                      f"❌ {now_msk_label()} | ОШИБКА | {error[:120]}")
        return 2

    finally:
        finished_at = now_iso_utc()
        try:
            insert_job_run(
                job_name=JOB_NAME, started_at_iso=started_at,
                finished_at_iso=finished_at, status=status,
                api_rows=api_rows, raw_new_versions=api_rows,
                norm_upserted=norm_count, duplicates=0, dup_pct=0.0, error=error,
            )
        except Exception as je:
            log.warning("[%s] insert_job_run failed: %s", JOB_NAME, je)

        ts  = now_msk_label()
        msg = (
            f"✅ WB_Adv_Fullstats | {ts} | OK\n\n"
            f"📅 {args.begin} – {args.end}\n"
            f"➡ API rows: {api_rows} | Norm: {norm_count} | Sheets: {sheet_rows_count}"
            if status == "ok" else
            f"❌ WB_Adv_Fullstats | {ts} | FAIL\n{(error or '')[:200]}"
        )
        tg_send(msg, logger=log)
        advisory_unlock(LOCK_ID)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
