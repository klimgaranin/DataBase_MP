from __future__ import annotations

import os
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv


# ============================================================================
# Загрузка .env + стабильные импорты (чтобы не зависеть от "где запущено")
# ============================================================================

_THIS_FILE = Path(__file__).resolve()


def _add_sys_path() -> None:
    """
    Делает импорты устойчивыми при запуске:
    - из корня проекта
    - из подпапки app\
    - из планировщика задач (рабочий каталог может быть другим)
    """
    # папка файла (если файл лежит в app\ — это и будет app)
    p1 = str(_THIS_FILE.parent)
    # папка уровнем выше (если файл лежит в app\ — это корень проекта)
    p2 = str(_THIS_FILE.parent.parent)

    # Вставляем в начало, чтобы наши локальные модули имели приоритет.
    if p1 not in sys.path:
        sys.path.insert(0, p1)
    if p2 not in sys.path:
        sys.path.insert(0, p2)


def _find_env_path() -> Optional[Path]:
    """
    Ищем .env рядом или уровнем выше.
    Это убирает классическую проблему: планировщик запускает "не там" и .env не подхватывается.
    """
    candidates = [
        _THIS_FILE.parent / ".env",
        _THIS_FILE.parent.parent / ".env",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


_add_sys_path()
_env_path = _find_env_path()
if _env_path is not None:
    load_dotenv(dotenv_path=_env_path)
else:
    # fallback: стандартное поведение dotenv (ищет в текущей директории)
    load_dotenv()


# ============================================================================
# Импорты проекта (поддержка обеих структур: корень или app/)
# ============================================================================

# Сначала пробуем "как пакет app", потом "как плоский проект".
try:
    # Вариант, если у тебя структура app\...
    from app.clients.wb_statistics import iter_orders  # type: ignore
    from app.db import (  # type: ignore
        insert_wb_orders_raw_dedup,
        cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm,
        get_job_cursor,
        set_job_cursor,
        insert_job_run,
        try_advisory_lock,
        advisory_unlock,
    )
    from app.wb_normalize import normalize_wb_order  # type: ignore
except Exception:
    # Вариант, если у тебя всё лежит в корне (clients\, db.py, wb_normalize.py)
    from clients.wb_statistics import iter_orders  # type: ignore
    from db import (  # type: ignore
        insert_wb_orders_raw_dedup,
        cleanup_wb_orders_raw_dedup,
        upsert_wb_orders_norm,
        get_job_cursor,
        set_job_cursor,
        insert_job_run,
        try_advisory_lock,
        advisory_unlock,
    )
    from wb_normalize import normalize_wb_order  # type: ignore


# ============================================================================
# Конфиг
# ============================================================================

JOB_NAME = "wb_orders"
LOCK_ID = 4_242_001


@dataclass(frozen=True)
class JobConfig:
    # Логирование
    log_level: str
    log_file: Optional[str]

    # Курсор / догрузка
    first_run_days_back: int
    lookback_base_minutes: int
    lookback_max_minutes: int

    # Очистка RAW-таблицы дедупа
    raw_dedup_retention_days: int

    # Отладка
    debug_sleep_after_lock_seconds: int

    @staticmethod
    def from_env() -> "JobConfig":
        # Логи
        log_level = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
        log_file = (os.getenv("LOG_FILE") or "").strip() or None

        # Первичный запуск
        first_run_days_back = int(os.getenv("WB_FIRST_RUN_DAYS_BACK", "3"))

        # Окно догрузки
        lookback_base_minutes = int(os.getenv("WB_LOOKBACK_MINUTES", "10"))
        lookback_max_minutes = int(
            os.getenv("WB_LOOKBACK_MAX_MINUTES", str(max(lookback_base_minutes, 20)))
        )

        # RAW дедуп: хранение
        raw_dedup_retention_days = int(os.getenv("WB_RAW_DEDUP_RETENTION_DAYS", "14"))

        # Отладка лока
        debug_sleep_after_lock_seconds = int(os.getenv("DEBUG_SLEEP_AFTER_LOCK_SECONDS", "0"))

        # Нормализуем границы (чтобы новичок не сломал вводом ерунды)
        first_run_days_back = max(1, min(first_run_days_back, 30))
        lookback_base_minutes = max(0, min(lookback_base_minutes, 120))
        lookback_max_minutes = max(0, min(lookback_max_minutes, 240))
        raw_dedup_retention_days = max(1, min(raw_dedup_retention_days, 365))
        debug_sleep_after_lock_seconds = max(0, min(debug_sleep_after_lock_seconds, 3600))

        return JobConfig(
            log_level=log_level,
            log_file=log_file,
            first_run_days_back=first_run_days_back,
            lookback_base_minutes=lookback_base_minutes,
            lookback_max_minutes=lookback_max_minutes,
            raw_dedup_retention_days=raw_dedup_retention_days,
            debug_sleep_after_lock_seconds=debug_sleep_after_lock_seconds,
        )


# ============================================================================
# Логирование (полностью по-русски)
# ============================================================================

def setup_logging(cfg: JobConfig) -> logging.Logger:
    level = getattr(logging, cfg.log_level, logging.INFO)

    handlers: list[logging.Handler] = []
    console = logging.StreamHandler()
    handlers.append(console)

    if cfg.log_file:
        # файл логов (UTF-8)
        file_handler = logging.FileHandler(cfg.log_file, encoding="utf-8")
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(JOB_NAME)


# ============================================================================
# Время / ISO helpers
# ============================================================================

def _ensure_tz(iso_str: str) -> str:
    """
    WB: если TZ не указан — считается МСК (UTC+3).
    Здесь мы жёстко дописываем +03:00, если TZ отсутствует.
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # уже есть +03:00 / -05:00
    if len(s) >= 6 and (s[-6] in ("+", "-")) and (s[-3] == ":"):
        return s
    return s + "+03:00"


def _parse_iso_dt(iso_str: str) -> Optional[datetime]:
    s = _ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _first_run_cursor(days_back: int) -> str:
    tz_msk = timezone(timedelta(hours=3))
    return (datetime.now(tz_msk) - timedelta(days=days_back)).replace(microsecond=0).isoformat()


# ============================================================================
# Логика lookback (чтобы не терять "догоняющие" обновления WB)
# ============================================================================

def _calc_lookback_minutes(
    cfg: JobConfig,
    cursor_old_iso: str,
    last_dup_pct: Optional[float],
) -> int:
    """
    Логика простая и предсказуемая:
    - когда мы близко к real-time — окно маленькое
    - когда отставание большое — окно больше
    - если в прошлый раз было слишком много дублей — окно уменьшаем
    """
    base = cfg.lookback_base_minutes
    max_auto = cfg.lookback_max_minutes

    dt_old = _parse_iso_dt(cursor_old_iso)
    if dt_old is None:
        return base

    now_same_tz = datetime.now(dt_old.tzinfo or timezone.utc)
    gap_min = max(int((now_same_tz - dt_old).total_seconds() // 60), 0)

    # базовая шкала по отставанию
    if gap_min <= 20:
        look = min(base, 2)
    elif gap_min <= 60:
        look = min(base, 5)
    elif gap_min <= 360:
        look = min(base, 10)
    else:
        look = min(max_auto, max(base, 15))

    # коррекция по дублям
    if last_dup_pct is not None:
        if last_dup_pct >= 30.0:
            look = min(look, 2)
        elif last_dup_pct >= 15.0:
            look = min(look, 5)
        elif last_dup_pct <= 3.0:
            look = min(max_auto, look + 5)
        elif last_dup_pct <= 7.0:
            look = min(max_auto, look + 2)

    return max(0, int(look))


def _apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = _parse_iso_dt(cursor_old_iso)
    if dt is None:
        return _ensure_tz(cursor_old_iso)
    dt2 = dt - timedelta(minutes=max(0, minutes))
    return dt2.replace(microsecond=0).isoformat()


# ============================================================================
# Работа с данными / курсором
# ============================================================================

def _max_cursor_from_rows(rows: list[dict[str, Any]], fallback: str) -> str:
    best_dt: Optional[datetime] = None
    best_raw: Optional[str] = None

    for r in rows:
        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue
        dt = _parse_iso_dt(str(c))
        if dt is None:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_raw = str(c)

    return _ensure_tz(best_raw) if best_raw else _ensure_tz(fallback)


def _dedupe_page_by_srid(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    В рамках одной страницы оставляем по srid самую свежую версию.
    Это уменьшает лишние upsert в NORM, смысл данных не меняется.
    """
    best: dict[str, tuple[datetime, dict[str, Any]]] = {}

    for r in rows:
        srid = r.get("srid")
        if not srid:
            continue

        c = r.get("lastChangeDate") or r.get("date")
        if not c:
            continue

        dt = _parse_iso_dt(str(c))
        if dt is None:
            continue

        key = str(srid)
        prev = best.get(key)
        if prev is None or dt > prev[0]:
            best[key] = (dt, r)

    return [pair[1] for pair in best.values()]


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    cfg = JobConfig.from_env()
    log = setup_logging(cfg)

    # 1) Лок (чтобы две копии не долбили БД одновременно)
    if not try_advisory_lock(LOCK_ID):
        log.info("[ЗАДАЧА] пропуск: предыдущий запуск ещё выполняется (лок занят)")
        return 0

    # 2) Отладочная пауза (чтобы руками проверить лок)
    if cfg.debug_sleep_after_lock_seconds > 0:
        log.info(
            "[ЗАДАЧА] отладочная пауза после лока: %s сек.",
            cfg.debug_sleep_after_lock_seconds,
        )
        time.sleep(cfg.debug_sleep_after_lock_seconds)

    started_at = _now_iso_utc()

    cursor_old: Optional[str] = None
    cursor_used: Optional[str] = None
    cursor_new: Optional[str] = None

    api_rows = 0
    raw_new_versions = 0
    norm_upserted = 0

    status = "ok"
    error: Optional[str] = None

    try:
        # 3) Курсор
        cursor_old = get_job_cursor(JOB_NAME)
        if not cursor_old:
            cursor_old = _first_run_cursor(cfg.first_run_days_back)

        # 4) Lookback
        # last_dup_pct будем читать уже из job_runs (функция живёт в db.py)
        # но если таблицы нет — db.py должен вернуть None, и задача не падает.
        try:
            from db import get_last_dup_pct  # type: ignore
        except Exception:
            get_last_dup_pct = None  # type: ignore

        last_dup_pct: Optional[float] = None
        if get_last_dup_pct is not None:
            last_dup_pct = get_last_dup_pct(JOB_NAME)  # type: ignore

        lookback_min = _calc_lookback_minutes(cfg, cursor_old, last_dup_pct=last_dup_pct)
        cursor_used = _apply_lookback(cursor_old, minutes=lookback_min)

        log.info(
            "[ЗАДАЧА] курсор_старый=%s | догрузка=%s мин. | курсор_используем=%s",
            cursor_old,
            lookback_min,
            cursor_used,
        )

        cursor_max_seen = cursor_old

        # 5) Запросы к WB
        with requests.Session() as sess:
            for chunk in iter_orders(_ensure_tz(cursor_used), flag=0, session=sess):
                if not chunk:
                    break

                api_rows += len(chunk)

                # RAW: пишем версии + дедуп на уровне БД
                raw_new_versions += int(insert_wb_orders_raw_dedup(chunk))

                # NORM: схлопываем в рамках страницы по srid
                compact = _dedupe_page_by_srid(chunk)

                norm_rows: list[dict[str, Any]] = []
                for r in compact:
                    nr = normalize_wb_order(r)
                    if nr is None:
                        # если нормализация вернула None — просто пропускаем (не падаем)
                        continue
                    norm_rows.append(nr)

                upsert_wb_orders_norm(norm_rows)
                norm_upserted += len(norm_rows)

                # курсор: максимум по странице
                cursor_max_seen = _max_cursor_from_rows(chunk, fallback=cursor_max_seen)

        cursor_new = _ensure_tz(cursor_max_seen)

        # 6) курсор не откатываем назад
        dt_old = _parse_iso_dt(cursor_old)
        dt_new = _parse_iso_dt(cursor_new)

        if dt_old is not None and dt_new is not None and dt_new < dt_old:
            log.warning(
                "[ЗАДАЧА] новый курсор пытается откатиться назад. Оставляю старый. новый=%s",
                cursor_new,
            )
            cursor_new = cursor_old
            dt_new = dt_old

        duplicates = max(api_rows - raw_new_versions, 0)
        dup_ratio = (duplicates / api_rows) if api_rows else 0.0  # 0..1

        log.info(
            "[ОТЧЁТ] строк_из_api=%s | новых_версий_raw=%s | дублей=%s | дублей_проц=%.2f%% | "
            "upsert_norm=%s | курсор_старый=%s | курсор_используем=%s | курсор_новый=%s",
            api_rows,
            raw_new_versions,
            duplicates,
            dup_ratio * 100.0,
            norm_upserted,
            cursor_old,
            cursor_used,
            cursor_new,
        )

        # 7) обновляем курсор только если реально продвинулись
        if dt_old is not None and dt_new is not None and dt_new > dt_old:
            set_job_cursor(JOB_NAME, cursor_new)
            log.info("[ЗАДАЧА] курсор обновлён")
        else:
            log.info("[ЗАДАЧА] курсор не продвинулся — обновление пропущено")

        # 8) чистка дедуп-таблицы
        deleted = cleanup_wb_orders_raw_dedup(cfg.raw_dedup_retention_days)
        log.info("[ЗАДАЧА] очистка_raw_dedup: удалено=%s (старше %s дней)", deleted, cfg.raw_dedup_retention_days)

        return 0

    except Exception as e:
        status = "fail"
        error = repr(e)
        log.exception("[ЗАДАЧА] ОШИБКА: %s", error)
        return 2

    finally:
        finished_at = _now_iso_utc()
        try:
            duplicates = max(api_rows - raw_new_versions, 0)
            dup_ratio = (duplicates / api_rows) if api_rows else 0.0

            insert_job_run(
                job_name=JOB_NAME,
                started_at_iso=started_at,
                finished_at_iso=finished_at,
                status=status,
                api_rows=api_rows,
                raw_new_versions=raw_new_versions,
                norm_upserted=norm_upserted,
                duplicates=duplicates,
                dup_pct=dup_ratio * 100.0,  # 0..100
                cursor_old=cursor_old,
                cursor_used=cursor_used,
                cursor_new=cursor_new,
                error=error,
            )
        finally:
            advisory_unlock(LOCK_ID)


if __name__ == "__main__":
    raise SystemExit(main())
