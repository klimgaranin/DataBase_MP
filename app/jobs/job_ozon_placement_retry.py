"""
app/jobs/job_ozon_placement_retry.py
Умный повтор Ozon placement, если утренний отчёт был пустым и Sheets взял старую дату.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parent.parent))
sys.path.insert(0, str(_THIS.parent.parent.parent))

from app.utils import setup_sys_path, load_env, setup_logging, tg_send, now_msk_label

setup_sys_path(__file__)
load_env(__file__)

JOB_NAME = "ozon_placement_retry"
ALERT_NAME = "Ozon_Placement_Retry"


def should_retry_placement(*, report_date: date | None, expected_date: date) -> bool:
    return report_date is None or report_date < expected_date


def main() -> int:
    from app.ops.sheets_export import default_placement_expected_date, fetch_ozon_placement_report_selection

    log = setup_logging(JOB_NAME)
    try:
        expected_date = default_placement_expected_date()
        selection = fetch_ozon_placement_report_selection()
        current_report_date = selection["report_date"] if selection else None

        if not should_retry_placement(report_date=current_report_date, expected_date=expected_date):
            log.info(
                "Повтор Ozon placement: не нужен, актуальный отчёт уже есть, дата=%s, строк=%s",
                current_report_date,
                selection["rows_count"] if selection else 0,
            )
            return 0

        log.warning(
            "Повтор Ozon placement: нужен, текущая дата отчёта=%s, ожидаемая дата=%s",
            current_report_date,
            expected_date,
        )

        from app.jobs import job_ozon_placement, job_sheets_ozon_placement_export

        placement_code = job_ozon_placement.main()
        if placement_code != 0:
            tg_send(
                f"❌ {ALERT_NAME} | {now_msk_label()} | FAIL\n"
                f"Повторная загрузка Ozon placement завершилась кодом {placement_code}",
                logger=log,
            )
            return placement_code

        sheets_code = job_sheets_ozon_placement_export.main()
        if sheets_code != 0:
            tg_send(
                f"❌ {ALERT_NAME} | {now_msk_label()} | FAIL\n"
                f"Отчёт загрузился, но повторная выгрузка в Sheets завершилась кодом {sheets_code}",
                logger=log,
            )
            return sheets_code

        refreshed = fetch_ozon_placement_report_selection()
        refreshed_date = refreshed["report_date"] if refreshed else None
        msg = (
            f"✅ {ALERT_NAME} | {now_msk_label()} | OK\n\n"
            f"➡ Ожидали отчёт: {expected_date}\n"
            f"➡ Был отчёт: {current_report_date or 'нет'}\n"
            f"➡ После ретрая: {refreshed_date or 'нет'}"
        )
        tg_send(msg, logger=log)
        return 0

    except Exception as exc:
        log.exception("Повтор Ozon placement: ОШИБКА - %r", exc)
        tg_send(
            f"❌ {ALERT_NAME} | {now_msk_label()} | FAIL\n"
            f"{repr(exc)[:200]}",
            logger=log,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
