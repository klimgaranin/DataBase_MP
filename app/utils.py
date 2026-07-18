"""
app/utils.py
──────────────────────────────────────────────────────────────────────────────
Общие утилиты для всех ETL-джобов проекта DataBase_MP.

Что здесь:
  - Поиск и загрузка .env
  - Добавление нужных путей в sys.path (чтобы импорты работали из любой папки)
  - Настройка логирования (setup_logging)
  - Вспомогательные функции времени: now_iso_utc(), now_msk_label()
  - Работа с ISO-датами: ensure_tz(), parse_iso_dt(), first_run_cursor()
  - Telegram-алерт: tg_send()

Все функции — без side-эффектов при импорте.
TG_BOT_TOKEN и TG_CHAT_ID читаются из окружения один раз при вызове tg_send(),
чтобы .env успел загрузиться в джобе до первого обращения.
──────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

# ─────────────────────────────────────────────────────────────────────────────
# sys.path + .env
# ─────────────────────────────────────────────────────────────────────────────

def setup_sys_path(file: str | Path) -> None:
    """
    Добавляет папку файла и папку выше неё в sys.path.

    Зачем: джобы могут запускаться из разных директорий (Task Scheduler,
    VS Code, командная строка). Без этого Python не найдёт модули app/db.py,
    app/clients/ и т.д.

    Использование — первая строка в джобе, ДО любых from app.xxx import:
        from app.utils import setup_sys_path, load_env
        setup_sys_path(__file__)
        load_env(__file__)
    """
    p = Path(file).resolve()
    for candidate in (p.parent, p.parent.parent):
        s = str(candidate)
        if s not in sys.path:
            sys.path.insert(0, s)


def load_env(file: str | Path) -> None:
    """
    Ищет .env рядом с файлом или на уровень выше и загружает его.
    Если .env не найден — вызывает load_dotenv() без аргументов
    (он ищет в текущей рабочей директории).

    Зачем: в Task Scheduler рабочая папка может быть любой,
    поэтому ищем .env относительно самого скрипта.
    """
    from dotenv import load_dotenv  # импорт здесь — чтобы не падать если не установлен

    p = Path(file).resolve()
    for candidate in (p.parent / ".env", p.parent.parent / ".env"):
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
            return
    load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Логирование
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(
    job_name: str,
    *,
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Настраивает корневой логгер и возвращает логгер с именем job_name.

    log_level — строка: 'DEBUG', 'INFO', 'WARNING', 'ERROR'.
                Если None — берётся из переменной окружения LOG_LEVEL,
                а если и её нет — используется INFO.

    log_file  — путь к файлу для записи логов.
                Если None — пишем только в консоль.

    Формат: "2026-04-20 23:10:05 | INFO | wb_orders | текст"
    """
    level_str = (log_level or os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, level_str, logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    return logging.getLogger(job_name)


# ─────────────────────────────────────────────────────────────────────────────
# Время
# ─────────────────────────────────────────────────────────────────────────────

def now_iso_utc() -> str:
    """
    Текущее время UTC в формате ISO 8601 без микросекунд.
    Пример: '2026-04-20T20:10:05+00:00'

    Используется как started_at / finished_at при записи job_runs.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def now_msk_label() -> str:
    """
    Текущее московское время в читаемом формате для Telegram-алертов.
    Пример: '20.04.2026 23:10'
    """
    msk = datetime.now(timezone(timedelta(hours=3)))
    return msk.strftime("%d.%m.%Y %H:%M")


# ─────────────────────────────────────────────────────────────────────────────
# Работа с ISO-датами
# ─────────────────────────────────────────────────────────────────────────────

def ensure_tz(iso_str: str) -> str:
    """
    Гарантирует наличие часового пояса в ISO-строке.

    Зачем: WB API иногда возвращает даты без timezone-суффикса,
    например '2026-04-20T10:00:00'. Мы считаем такие даты московскими (+03:00).

    Примеры:
        '2026-04-20T10:00:00'        → '2026-04-20T10:00:00+03:00'
        '2026-04-20T10:00:00Z'       → без изменений
        '2026-04-20T10:00:00+03:00'  → без изменений
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    if s.endswith("Z"):
        return s
    # Проверяем суффикс вида +HH:MM или -HH:MM
    if len(s) >= 6 and s[-6] in ("+", "-") and s[-3] == ":":
        return s
    return s + "+03:00"


def parse_iso_dt(iso_str: str) -> Optional[datetime]:
    """
    Парсит ISO-строку в datetime с timezone.
    При ошибке возвращает None (не бросает исключений).

    Зачем: безопасный парсинг при сравнении курсоров,
    когда формат даты из API может быть нестандартным.
    """
    s = ensure_tz(iso_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def first_run_cursor(days_back: int) -> str:
    """
    Возвращает ISO-строку: текущий момент минус days_back дней (МСК).

    Используется при первом запуске джоба, когда в job_cursors ещё
    нет записи — нужно с чего-то начать выгрузку из API.

    Пример: first_run_cursor(3) → '2026-04-17T23:10:05+03:00'
    """
    tz_msk = timezone(timedelta(hours=3))
    return (
        datetime.now(tz_msk) - timedelta(days=max(1, days_back))
    ).replace(microsecond=0).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────────────────────

def tg_send(text: str, *, logger: Optional[logging.Logger] = None) -> None:
    """
    Отправляет сообщение в Telegram-бот. Никогда не бросает исключений.

    Токен и chat_id берутся из переменных окружения:
        TG_BOT_TOKEN — токен бота (от @BotFather)
        TG_CHAT_ID   — id чата или канала, куда слать сообщения

    Если переменные не заданы — функция молча выходит.

    parse_mode='HTML' — можно использовать <b>жирный</b>, <i>курсив</i> и т.д.

    logger — опционально: если передать логгер джоба, предупреждение
             об ошибке отправки попадёт в лог именно этого джоба.
    """
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    _log = logger or logging.getLogger(__name__)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        _log.warning("tg_send failed: %s", e)