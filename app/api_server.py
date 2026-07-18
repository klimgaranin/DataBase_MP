"""
app/api_server.py
FastAPI — HTTP-триггер для ETL-джобов проекта DataBase_MP.

Запуск:
  python -m uvicorn app.api_server:app --host 0.0.0.0 --port 8080 --workers 1

  workers=1 обязательно: используется in-memory состояние (_active_job).

Эндпоинты:
  POST /api/v1/drr/start   — запустить ETL fullstats, вернуть 202 + ETA
  GET  /api/v1/health      — проверка доступности сервера

Безопасность:
  Authorization: Bearer <API_SERVER_TOKEN>
  Токен задаётся в .env: API_SERVER_TOKEN=...

Если задача уже выполняется → 409 Conflict.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator

# ── bootstrap .env ────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))

from app.utils import load_env, setup_logging
load_env(__file__)

# ── logger ────────────────────────────────────────────────────────────────────
log = setup_logging(
    "api_server",
    log_file=(os.getenv("API_SERVER_LOG_FILE") or "").strip() or None,
)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="DataBase_MP API",
    version="1.0.0",
    docs_url=None,   # отключаем Swagger в продакшне
    redoc_url=None,
)

# ── Auth ──────────────────────────────────────────────────────────────────────
_bearer_scheme = HTTPBearer()

def _verify_token(creds: HTTPAuthorizationCredentials = Depends(_bearer_scheme)) -> str:
    expected = os.getenv("API_SERVER_TOKEN", "").strip()
    if not expected:
        raise RuntimeError("API_SERVER_TOKEN не задан в .env")
    if creds.credentials != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return creds.credentials


# ── In-memory job tracker ─────────────────────────────────────────────────────
_job_lock: threading.Lock = threading.Lock()
_active_job: dict         = {}   # {"job_id": str, "started_at": str} или {}


def _watch_and_clear(proc: subprocess.Popen, job_id: str) -> None:
    """Daemon-поток: ждёт завершения subprocess, затем очищает _active_job."""
    proc.wait()
    with _job_lock:
        if _active_job.get("job_id") == job_id:
            _active_job.clear()
    log.info("api_server: job %s завершён (exit_code=%s)", job_id, proc.returncode)


# ── Schemas ───────────────────────────────────────────────────────────────────
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

class DrrStartRequest(BaseModel):
    begin:      str
    end:        str
    sheet_id:   str
    sheet_name: str = "Stats_Full_WB"

    @field_validator("begin", "end")
    @classmethod
    def _check_date(cls, v: str) -> str:
        if not _DATE_RE.match(v):
            raise ValueError("Формат даты: YYYY-MM-DD")
        return v

    @field_validator("end")
    @classmethod
    def _check_range(cls, v: str, info) -> str:
        begin = info.data.get("begin")
        if begin and v < begin:
            raise ValueError("end не может быть раньше begin")
        from datetime import date
        diff = (date.fromisoformat(v) - date.fromisoformat(begin)).days + 1 if begin else 0
        if diff > 31:
            raise ValueError("/adv/v3/fullstats: максимальный период 31 день")
        return v


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/health")
def health():
    """Проверка доступности. Без авторизации — для мониторинга."""
    with _job_lock:
        running = bool(_active_job)
        job_id  = _active_job.get("job_id")
    return {"status": "ok", "job_running": running, "job_id": job_id}


@app.post("/api/v1/drr/start", status_code=202)
def drr_start(
    body:  DrrStartRequest,
    _auth: str = Depends(_verify_token),
):
    """
    Запускает job_wb_adv_fullstats.py в отдельном процессе.
    Возвращает 202 с расчётом батчей и ETA.
    Если задача уже выполняется → 409.
    """
    with _job_lock:
        if _active_job:
            raise HTTPException(
                status_code=409,
                detail={
                    "error":      "Задача уже выполняется",
                    "job_id":     _active_job.get("job_id"),
                    "started_at": _active_job.get("started_at"),
                },
            )

    # Считаем ETA из БД (без блокировки advisory lock)
    try:
        from app.db import get_adv_campaign_ids
        from app.clients.http_wb_adv import calc_eta
        advert_ids      = get_adv_campaign_ids(statuses=[7, 9, 11])
        batches, eta_sec = calc_eta(advert_ids)
    except Exception as e:
        log.warning("api_server: не удалось посчитать ETA: %s", e)
        advert_ids = []
        batches    = 0
        eta_sec    = 0

    if batches == 0:
        raise HTTPException(
            status_code=422,
            detail="Нет активных кампаний в wb_adv_campaigns (статусы 7/9/11). "
                   "Запустите сначала job_wb_adv_campaigns.",
        )

    job_id = str(uuid.uuid4())

    with _job_lock:
        _active_job["job_id"]     = job_id
        _active_job["started_at"] = datetime.now().isoformat()

    # Путь к джобу — всегда рядом с api_server.py
    job_path = Path(__file__).parent / "jobs" / "job_wb_adv_fullstats.py"

    cmd = [
        sys.executable, str(job_path),
        "--begin",      body.begin,
        "--end",        body.end,
        "--sheet_id",   body.sheet_id,
        "--sheet_name", body.sheet_name,
    ]

    proc = subprocess.Popen(
        cmd,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )

    threading.Thread(
        target=_watch_and_clear,
        args=(proc, job_id),
        daemon=True,
    ).start()

    log.info(
        "api_server: job %s запущен | %s – %s | %d кампаний | %d батчей | ETA ~%ds",
        job_id, body.begin, body.end, len(advert_ids), batches, eta_sec,
    )

    return {
        "job_id":       job_id,
        "batches":      batches,
        "campaigns":    len(advert_ids),
        "eta_sec":      eta_sec,
        "begin":        body.begin,
        "end":          body.end,
        "sheet_id":     body.sheet_id,
        "sheet_name":   body.sheet_name,
    }


# ── Dev run ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.api_server:app",
        host    = os.getenv("API_SERVER_HOST", "0.0.0.0"),
        port    = int(os.getenv("API_SERVER_PORT", "8080")),
        workers = 1,
        reload  = False,
    )
