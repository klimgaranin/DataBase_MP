from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
REQUIRED_CORE_FILES = [
    "requirements.txt",
    "README.md",
    "infra/docker-compose.yml",
    "app/config.py",
    "app/db.py",
    "app/secrets.py",
    "app/utils.py",
    "app/clients/http_wb_statistics.py",
    "app/clients/http_wb_stocks.py",
    "app/clients/http_ozon_seller.py",
    "app/clients/local_source_files.py",
    "app/clients/source_statistics_excel.py",
    "app/integrations/api_catalog.py",
    "app/cli.py",
    "app/jobs/job_wb_orders.py",
    "app/jobs/job_wb_stocks.py",
    "app/jobs/job_ozon_orders.py",
    "app/jobs/job_ozon_placement.py",
    "app/jobs/job_ozon_stocks.py",
    "app/jobs/job_source_statistics.py",
    "app/jobs/wb_orders_logic.py",
    "app/normalize/norm_ozon_orders.py",
    "app/normalize/norm_ozon_placement.py",
    "app/normalize/norm_ozon_stocks.py",
    "app/normalize/norm_source_statistics.py",
    "app/normalize/norm_wb_orders.py",
    "app/normalize/norm_wb_stocks.py",
    "app/ops/health.py",
    "app/ops/jobs_status.py",
    "app/ops/migrations.py",
    "app/ops/secrets.py",
    "migrations/V11__source_statistics_ingestion.sql",
    "migrations/V12__ozon_api_orders.sql",
    "migrations/V13__ozon_stocks_and_placement.sql",
    "migrations/V14__ozon_stock_details_and_fbo_payload.sql",
    "scripts/run_ozon_orders.cmd",
    "scripts/run_ozon_placement.cmd",
    "scripts/run_ozon_stocks.cmd",
    "scripts/run_hidden.vbs",
    "scripts/run_source_statistics.cmd",
    "scripts/run_wb_orders.cmd",
    "scripts/run_wb_stocks.cmd",
    "tools/sheets_audit.py",
    "tools/sheets_logic_map.py",
    "tools/source_file_audit.py",
    "tools/xlsm_powerquery_audit.py",
]
ARCHIVE_PATH = "archive/wb-legal-export-20260718"


@dataclass
class Finding:
    level: str
    code: str
    message: str


def _run(cmd: list[str]) -> tuple[int, str]:
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def _is_utf8(path: Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return True


def audit() -> list[Finding]:
    findings: list[Finding] = []

    for rel_path in REQUIRED_CORE_FILES:
        if not (ROOT / rel_path).exists():
            findings.append(Finding("FAIL", "missing_core_file", f"Не найден обязательный файл: {rel_path}"))

    if not (ROOT / ARCHIVE_PATH).exists():
        findings.append(Finding("WARN", "missing_archive", f"Архив WB legal export не найден: {ARCHIVE_PATH}"))

    req_path = ROOT / "requirements.txt"
    if req_path.exists():
        raw = req_path.read_bytes()
        if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff") or raw.startswith(b"\xef\xbb\xbf"):
            findings.append(Finding("FAIL", "requirements_bom", "requirements.txt содержит BOM/UTF marker"))
        if not _is_utf8(req_path):
            findings.append(Finding("FAIL", "requirements_encoding", "requirements.txt не читается как UTF-8"))

    for rel_path in (
        "scripts/run_wb_orders.cmd",
        "scripts/run_wb_stocks.cmd",
        "scripts/run_source_statistics.cmd",
        "scripts/run_ozon_orders.cmd",
        "scripts/run_ozon_stocks.cmd",
        "scripts/run_ozon_placement.cmd",
    ):
        path = ROOT / rel_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if ".venv\\Scripts\\python.exe" not in text:
            findings.append(Finding("WARN", "cmd_no_venv", f"{rel_path} не использует .venv Python"))
        if "%~dp0.." not in text:
            findings.append(Finding("WARN", "cmd_no_project_root", f"{rel_path} не вычисляет ROOT от script path"))

    code, output = _run([sys.executable, "-m", "compileall", "app", "tools"])
    if code != 0:
        findings.append(Finding("FAIL", "compileall", output[-1000:]))

    tests_dir = ROOT / "tests"
    if not any(tests_dir.glob("test_*.py")):
        findings.append(Finding("WARN", "no_tests", "В tests/ нет test_*.py"))
    else:
        code, output = _run([sys.executable, "-m", "unittest", "discover", "-s", "tests"])
        if code != 0:
            findings.append(Finding("FAIL", "unit_tests", output[-1000:]))

    return findings


def main() -> int:
    findings = audit()
    if not findings:
        print("OK: базовая проверка взрослости проекта без замечаний")
        return 0
    print(json.dumps([finding.__dict__ for finding in findings], ensure_ascii=False, indent=2))
    return 1 if any(finding.level == "FAIL" for finding in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
