from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from app.ops.health import dependency_status, safe_dsn_summary
from app.secrets import SENSITIVE_SECRET_NAMES, get_secret, secret_status


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ACTION_RUNS: list[dict[str, Any]] = []

OZON_STATUS_LABELS = {
    "awaiting_registration": "Ожидает регистрации",
    "acceptance_in_progress": "Идёт приёмка",
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает сборки",
    "awaiting_deliver": "Ожидает отгрузки",
    "arbitration": "Арбитраж",
    "client_arbitration": "Клиентский арбитраж",
    "delivering": "Доставляется",
    "driver_pickup": "Ожидает курьера",
    "delivered": "Доставлен",
    "cancelled": "Отменён",
    "not_accepted": "Не принят",
    "sent_by_seller": "Передан продавцом",
}

WB_STATUS_LABELS = {
    "active": "Активный",
    "cancelled": "Отменён",
}


JOB_ACTIONS: dict[str, dict[str, str]] = {
    "wb_orders": {
        "title": "WB заказы",
        "description": "Обновить текущий слой заказов WB в PostgreSQL.",
        "script": "scripts/run_wb_orders.cmd",
        "marketplace": "WB",
        "group": "База",
    },
    "wb_order_feed": {
        "title": "WB лента заказов",
        "description": "Обновить новую ленту заказов WB с историей статусов.",
        "script": "scripts/run_wb_order_feed.cmd",
        "marketplace": "WB",
        "group": "База",
    },
    "wb_stocks": {
        "title": "WB остатки",
        "description": "Обновить остатки WB по складам.",
        "script": "scripts/run_wb_stocks.cmd",
        "marketplace": "WB",
        "group": "База",
    },
    "ozon_orders": {
        "title": "Ozon FBO заказы",
        "description": "Обновить FBO отправления Ozon и историю изменений.",
        "script": "scripts/run_ozon_orders.cmd",
        "marketplace": "Ozon",
        "group": "База",
    },
    "ozon_stocks": {
        "title": "Ozon остатки",
        "description": "Обновить товары и остатки Ozon.",
        "script": "scripts/run_ozon_stocks.cmd",
        "marketplace": "Ozon",
        "group": "База",
    },
    "ozon_placement": {
        "title": "Ozon хранение",
        "description": "Запросить и загрузить отчёт платного хранения Ozon.",
        "script": "scripts/run_ozon_placement.cmd",
        "marketplace": "Ozon",
        "group": "Отчёты",
    },
    "source_files": {
        "title": "Файлы 1С",
        "description": "Обновить остатки МП и список заказов из файлов.",
        "script": "scripts/run_source_files_refresh.cmd",
        "marketplace": "1C",
        "group": "Файлы",
    },
    "source_costs": {
        "title": "Себестоимость",
        "description": "Обновить себестоимость и витрины для Sheets.",
        "script": "scripts/run_source_costs_refresh.cmd",
        "marketplace": "1C",
        "group": "Файлы",
    },
    "erp_tru_sales": {
        "title": "ERP/TRU продажи",
        "description": "Обновить статистику продаж ERP/TRU и выгрузку в Sheets.",
        "script": "scripts/run_api_erp_tru_sales_refresh.cmd",
        "marketplace": "ERP",
        "group": "Sheets",
    },
    "sheets_orders": {
        "title": "Заказы в Sheets",
        "description": "Выгрузить актуальные WB и Ozon заказы в Google Sheets.",
        "script": "scripts/run_sheets_orders_export.cmd",
        "marketplace": "Sheets",
        "group": "Sheets",
    },
}


def _db_fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    from app.db import connect

    conn_ctx = connect()
    conn = conn_ctx.__enter__()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description or []]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn_ctx.__exit__(None, None, None)


def _db_fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    rows = _db_fetch_all(query, params)
    return rows[0] if rows else None


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _jsonable_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _jsonable(value) for key, value in row.items()}


def _label_status(marketplace: str, status: str | None) -> str:
    status_key = (status or "").strip().lower()
    if marketplace == "ozon":
        return OZON_STATUS_LABELS.get(status_key, status or "-")
    if marketplace == "wb":
        return WB_STATUS_LABELS.get(status_key, status or "-")
    return status or "-"


def _wb_image_urls(nm_id: Any) -> list[str]:
    try:
        value = int(nm_id)
    except (TypeError, ValueError):
        return []
    if value <= 0:
        return []

    volume = value // 100000
    part = value // 1000
    if volume <= 143:
        basket = "01"
    elif volume <= 287:
        basket = "02"
    elif volume <= 431:
        basket = "03"
    elif volume <= 719:
        basket = "04"
    elif volume <= 1007:
        basket = "05"
    elif volume <= 1061:
        basket = "06"
    elif volume <= 1115:
        basket = "07"
    elif volume <= 1169:
        basket = "08"
    elif volume <= 1313:
        basket = "09"
    elif volume <= 1601:
        basket = "10"
    elif volume <= 1655:
        basket = "11"
    elif volume <= 1919:
        basket = "12"
    elif volume <= 2045:
        basket = "13"
    elif volume <= 2189:
        basket = "14"
    elif volume <= 2405:
        basket = "15"
    else:
        basket = "16"
    urls = [
        f"https://basket-{basket}.wbbasket.ru/vol{volume}/part{part}/{value}/images/c516x688/1.webp",
        f"https://basket-{basket}.wbbasket.ru/vol{volume}/part{part}/{value}/images/big/1.webp",
    ]
    for index in range(1, 31):
        candidate = f"{index:02d}"
        urls.append(f"https://basket-{candidate}.wbbasket.ru/vol{volume}/part{part}/{value}/images/c516x688/1.webp")
    return list(dict.fromkeys(urls))


def _action_run_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in sorted(_ACTION_RUNS, key=lambda item: item["started_at"], reverse=True):
        proc = run.get("proc")
        return_code = proc.poll() if proc is not None else None
        if return_code is None:
            status = "running"
            error = ""
        elif return_code == 0:
            status = "ok"
            error = ""
        else:
            status = "fail"
            error = f"Процесс завершился с кодом {return_code}. Подробности в лог-файле job."
        rows.append(
            {
                "job_name": run["title"],
                "started_at": run["started_at"],
                "finished_at": None if return_code is None else datetime.now().isoformat(),
                "status": status,
                "api_rows": None,
                "raw_new": None,
                "norm_upserted": None,
                "duplicates": None,
                "dup_pct": None,
                "error": error,
            }
        )
    return rows


def get_overview() -> dict[str, Any]:
    secrets = secret_status(SENSITIVE_SECRET_NAMES)
    deps = dependency_status()
    dsn = get_secret("PG_DSN") or ""

    db_status: dict[str, Any]
    try:
        row = _db_fetch_one("SELECT current_database() AS database, current_user AS username, NOW() AS checked_at")
        db_status = {"ok": True, **(_jsonable_row(row or {}))}
    except Exception as exc:
        db_status = {"ok": False, "error": str(exc)}

    jobs = get_jobs(since_hours=24, limit=500)
    failed_jobs = [job for job in jobs if str(job.get("status", "")).lower() not in {"ok", "running"}]

    return {
        "project": "DataBase_MP",
        "checked_at": datetime.now().isoformat(),
        "db": {"dsn": safe_dsn_summary(dsn), **db_status},
        "dependencies": deps,
        "secrets": secrets,
        "jobs": jobs,
        "alerts": {
            "failed_jobs": len(failed_jobs),
            "missing_required_secrets": [
                name
                for name in ("PG_DSN", "POSTGRES_PASSWORD", "WB_TOKEN", "OZON_CLIENT_ID", "OZON_API_KEY")
                if not secrets.get(name)
            ],
        },
    }


def get_jobs(*, limit: int = 20, since_hours: int | None = None) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 500))
    try:
        where_sql = ""
        params: tuple[Any, ...]
        if since_hours is not None:
            hours = max(1, min(since_hours, 168))
            where_sql = "WHERE started_at >= NOW() - (%s || ' hours')::interval"
            params = (hours, limit)
        else:
            params = (limit,)
        rows = _db_fetch_all(
            f"""
            SELECT job_name, started_at, finished_at, status,
                   api_rows, raw_new, norm_upserted, duplicates,
                   ROUND(dup_pct::numeric, 2) AS dup_pct,
                   LEFT(COALESCE(error, ''), 240) AS error
            FROM job_runs
            {where_sql}
            ORDER BY id DESC
            LIMIT %s
            """,
            params,
        )
    except Exception as exc:
        return [{"job_name": "DB", "status": "error", "error": str(exc)}]
    action_rows = _action_run_rows()
    return [_jsonable_row(row) for row in action_rows + rows]


def get_secrets_status() -> dict[str, bool]:
    return secret_status(SENSITIVE_SECRET_NAMES)


def get_orders_daily_summary(*, marketplace: str) -> dict[str, Any]:
    if marketplace == "ozon":
        row = _db_fetch_one(
            """
            WITH today_rows AS (
                SELECT
                    COALESCE(order_number, posting_number) AS order_group_key,
                    product_offer_id AS article,
                    product_quantity AS quantity,
                    product_price_amount AS price,
                    status
                FROM staging.ozon_fbo_order_items_full
                WHERE (COALESCE(in_process_at, created_at, updated_at) AT TIME ZONE 'Europe/Moscow')::date =
                      (NOW() AT TIME ZONE 'Europe/Moscow')::date
            )
            SELECT
                COUNT(DISTINCT order_group_key) AS orders_count,
                COUNT(DISTINCT article) AS articles_count,
                COALESCE(SUM(quantity), 0) AS quantity,
                COALESCE(SUM(quantity * price), 0) AS amount,
                COUNT(DISTINCT order_group_key) FILTER (WHERE status = 'cancelled') AS cancelled_orders_count
            FROM today_rows
            """
        ) or {}
        return _jsonable_row({"marketplace": "Ozon", **row})

    if marketplace == "wb":
        row = _db_fetch_one(
            """
            WITH today_rows AS (
                SELECT
                    COALESCE(g_number, srid) AS order_group_key,
                    supplier_article AS article,
                    1 AS quantity,
                    price_with_disc AS price,
                    is_cancel
                FROM wb_orders_norm
                WHERE (COALESCE(date_ts, last_change_ts) AT TIME ZONE 'Europe/Moscow')::date =
                      (NOW() AT TIME ZONE 'Europe/Moscow')::date
            )
            SELECT
                COUNT(DISTINCT order_group_key) AS orders_count,
                COUNT(DISTINCT article) AS articles_count,
                COALESCE(SUM(quantity), 0) AS quantity,
                COALESCE(SUM(quantity * price), 0) AS amount,
                COUNT(DISTINCT order_group_key) FILTER (WHERE is_cancel) AS cancelled_orders_count
            FROM today_rows
            """
        ) or {}
        return _jsonable_row({"marketplace": "WB", **row})

    raise ValueError("marketplace должен быть wb или ozon")


def get_job_actions() -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for key, action in JOB_ACTIONS.items():
        script_path = PROJECT_ROOT / action["script"]
        actions.append(
            {
                "key": key,
                "title": action["title"],
                "description": action["description"],
                "marketplace": action["marketplace"],
                "group": action["group"],
                "available": script_path.exists(),
            }
        )
    return actions


def start_job_action(key: str) -> dict[str, Any]:
    action = JOB_ACTIONS.get(key)
    if action is None:
        raise ValueError("Неизвестная команда запуска")

    script_path = (PROJECT_ROOT / action["script"]).resolve()
    if not script_path.exists():
        raise FileNotFoundError(f"Скрипт не найден: {action['script']}")

    job_id = str(uuid.uuid4())
    if sys.platform == "win32":
        cmd = ["cmd", "/c", str(script_path)]
        creationflags = subprocess.CREATE_NO_WINDOW
    else:
        cmd = [str(script_path)]
        creationflags = 0

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        creationflags=creationflags,
    )
    started_at = datetime.now().isoformat()
    _ACTION_RUNS.append(
        {
            "job_id": job_id,
            "key": key,
            "title": action["title"],
            "started_at": started_at,
            "proc": proc,
        }
    )
    del _ACTION_RUNS[:-30]
    return {
        "job_id": job_id,
        "key": key,
        "title": action["title"],
        "pid": proc.pid,
        "started_at": started_at,
    }


def get_orders_feed(*, marketplace: str, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    if marketplace == "ozon":
        rows = _db_fetch_all(
            """
            WITH groups AS (
                SELECT
                    COALESCE(order_number, posting_number) AS order_group_key,
                    MAX(COALESCE(in_process_at, created_at, updated_at)) AS sort_date
                FROM staging.ozon_fbo_order_items_full
                GROUP BY COALESCE(order_number, posting_number)
                ORDER BY sort_date DESC NULLS LAST
                LIMIT %s OFFSET %s
            )
            SELECT
                posting_number AS order_key,
                order_number,
                groups.order_group_key,
                status,
                substatus,
                in_process_at AS order_date,
                analytics_warehouse_name AS warehouse_name,
                product_offer_id AS article,
                product_name AS product_name,
                product_sku AS marketplace_sku,
                product_quantity AS quantity,
                product_price_amount AS price,
                financial_payout AS payout,
                p.primary_image AS image_url,
                staging.ozon_fbo_order_items_full.updated_at
            FROM staging.ozon_fbo_order_items_full
            JOIN groups
                ON groups.order_group_key = COALESCE(order_number, posting_number)
            LEFT JOIN LATERAL (
                SELECT primary_image
                FROM raw.ozon_product_info_items
                WHERE offer_id = staging.ozon_fbo_order_items_full.product_offer_id
                ORDER BY updated_at DESC
                LIMIT 1
            ) p ON TRUE
            ORDER BY COALESCE(in_process_at, created_at, updated_at) DESC NULLS LAST
            """,
            (limit, offset),
        )
        result = []
        for row in rows:
            row["order_group_key"] = row.get("order_number") or row.get("order_key")
            row["status_label"] = _label_status("ozon", row.get("status"))
            row["image_urls"] = [row.get("image_url")] if row.get("image_url") else []
            result.append(_jsonable_row({"marketplace": "Ozon", **row}))
        return result

    if marketplace == "wb":
        rows = _db_fetch_all(
            """
            WITH groups AS (
                SELECT
                    COALESCE(g_number, srid) AS order_group_key,
                    MAX(COALESCE(date_ts, last_change_ts)) AS sort_date
                FROM wb_orders_norm
                GROUP BY COALESCE(g_number, srid)
                ORDER BY sort_date DESC NULLS LAST
                LIMIT %s OFFSET %s
            )
            SELECT
                srid AS order_key,
                g_number AS order_number,
                groups.order_group_key,
                CASE WHEN is_cancel THEN 'cancelled' ELSE 'active' END AS status,
                NULL::text AS substatus,
                date_ts AS order_date,
                warehouse_name,
                supplier_article AS article,
                subject AS product_name,
                nm_id AS marketplace_sku,
                1 AS quantity,
                price_with_disc AS price,
                finished_price AS payout,
                last_change_ts AS updated_at
            FROM wb_orders_norm
            JOIN groups
                ON groups.order_group_key = COALESCE(g_number, srid)
            ORDER BY COALESCE(date_ts, last_change_ts) DESC NULLS LAST
            """,
            (limit, offset),
        )
        result = []
        for row in rows:
            row["order_group_key"] = row.get("order_number") or row.get("order_key")
            row["status_label"] = _label_status("wb", row.get("status"))
            image_urls = _wb_image_urls(row.get("marketplace_sku"))
            row["image_url"] = image_urls[0] if image_urls else None
            row["image_urls"] = image_urls
            result.append(_jsonable_row({"marketplace": "WB", **row}))
        return result

    raise ValueError("marketplace должен быть wb или ozon")
