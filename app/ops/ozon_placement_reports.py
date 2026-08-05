from __future__ import annotations

import argparse
import hashlib
import re
import time
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from app.clients.http_ozon_seller import (
    OzonSellerClient,
    create_placement_by_supplies_report,
    download_report_file,
    fetch_report_info,
)
from app.jobs.job_ozon_placement import default_placement_report_date
from app.normalize.norm_ozon_placement import parse_placement_xlsx


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "local" / "reports" / "ozon_placement"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _safe_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "report"


def _headers_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    payload = rows[0].get("payload") or {}
    return [str(key) for key in payload.keys()]


def download_placement_by_supplies_report(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    poll_attempts: int = 20,
    poll_sleep_seconds: int = 30,
) -> dict[str, Any]:
    report_date = default_placement_report_date()
    actual_date_from = date_from or report_date
    actual_date_to = date_to or report_date
    if (actual_date_to - actual_date_from).days > 30:
        raise RuntimeError("Ozon placement by supplies: максимальный период отчёта 31 день")

    client = OzonSellerClient()
    code, _ = create_placement_by_supplies_report(client, date_from=actual_date_from, date_to=actual_date_to)
    if not code:
        raise RuntimeError("Ozon placement by supplies: API не вернул код отчёта")

    info: dict[str, Any] = {}
    for attempt in range(1, max(1, poll_attempts) + 1):
        info, _ = fetch_report_info(client, code=code)
        status = info.get("status")
        print(f"Проверка готовности отчёта по поставкам: попытка {attempt}, статус={status}")
        if status == "success":
            break
        if status == "failed":
            raise RuntimeError(f"Ozon placement by supplies: отчёт завершился ошибкой: {info.get('error')}")
        time.sleep(max(5, poll_sleep_seconds))
    else:
        raise RuntimeError("Ozon placement by supplies: отчёт не был готов за отведённое время")

    file_url = str(info.get("file") or "")
    if not file_url:
        raise RuntimeError("Ozon placement by supplies: отчёт готов, но ссылка на файл отсутствует")

    content = download_report_file(file_url)
    digest = _sha256(content)
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        f"ozon_placement_by_supplies_{actual_date_from.isoformat()}_"
        f"{actual_date_to.isoformat()}_{_safe_part(code)}.xlsx"
    )
    path = output_dir / filename
    path.write_bytes(content)

    rows = parse_placement_xlsx(content)
    return {
        "code": code,
        "date_from": actual_date_from.isoformat(),
        "date_to": actual_date_to.isoformat(),
        "path": str(path),
        "sha256": digest,
        "rows": len(rows),
        "headers": _headers_from_rows(rows),
        "status": info.get("status"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ozon placement diagnostic reports")
    subparsers = parser.add_subparsers(dest="command", required=True)

    supplies = subparsers.add_parser("by-supplies", help="скачать отчёт стоимости размещения по поставкам")
    supplies.add_argument("--date-from", help="YYYY-MM-DD; по умолчанию текущая дата Europe/Minsk")
    supplies.add_argument("--date-to", help="YYYY-MM-DD; по умолчанию текущая дата Europe/Minsk")
    supplies.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    supplies.add_argument("--poll-attempts", type=int, default=20)
    supplies.add_argument("--poll-sleep-seconds", type=int, default=30)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "by-supplies":
        result = download_placement_by_supplies_report(
            date_from=date.fromisoformat(args.date_from) if args.date_from else None,
            date_to=date.fromisoformat(args.date_to) if args.date_to else None,
            output_dir=Path(args.output_dir),
            poll_attempts=args.poll_attempts,
            poll_sleep_seconds=args.poll_sleep_seconds,
        )
        print(f"Отчёт Ozon по поставкам скачан: {result['path']}")
        print(f"Период: {result['date_from']}..{result['date_to']}")
        print(f"Код отчёта: {result['code']}")
        print(f"Строк: {result['rows']}")
        print(f"SHA-256: {result['sha256']}")
        print("Колонки:")
        for header in result["headers"]:
            print(f"- {header}")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
