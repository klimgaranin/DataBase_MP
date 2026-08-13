from __future__ import annotations

import json
import ast
import subprocess
import sys
import threading
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
    "wb_product_cards": {
        "title": "WB карточки",
        "description": "Обновить карточки WB и официальные ссылки на фото.",
        "script": "scripts/run_wb_product_cards.cmd",
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
    "ozon_product_cards": {
        "title": "Ozon карточки",
        "description": "Обновить карточки Ozon и официальные ссылки на фото.",
        "script": "scripts/run_ozon_product_cards.cmd",
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

JOB_RUN_ACTION_ALIASES = {
    "wb_orders": "wb_orders",
    "wb_order_feed": "wb_order_feed",
    "wb_product_cards": "wb_product_cards",
    "wb_stocks": "wb_stocks",
    "ozon_orders": "ozon_orders",
    "ozon_product_cards": "ozon_product_cards",
    "ozon_stocks": "ozon_stocks",
    "ozon_placement": "ozon_placement",
    "source_statistics": "source_files",
    "source_files": "source_files",
    "source_costs": "source_costs",
    "api_erp_tru_product_stats": "erp_tru_sales",
    "sheets_api_erp_tru_sales_export": "erp_tru_sales",
    "sheets_orders_export": "sheets_orders",
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
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
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


def _image_url_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = ast.literal_eval(text)
                except (SyntaxError, ValueError):
                    return [text]
                return _image_url_list(parsed)
            return [text]
        return _image_url_list(parsed)
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, "")]
    return []


def _ordered_image_urls(primary: Any, images: Any) -> list[str]:
    urls: list[str] = []
    primary_urls = _image_url_list(primary)
    primary_url = primary_urls[0] if primary_urls else ""
    if primary_url:
        urls.append(primary_url)
    for url in _image_url_list(images):
        if url and url not in urls:
            urls.append(url)
    return urls


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
    urls = []
    sizes = ("c516x688", "big", "tm")
    baskets = [basket, *[f"{index:02d}" for index in range(1, 31)]]
    for candidate in baskets:
        for size in sizes:
            urls.append(
                f"https://basket-{candidate}.wbbasket.ru/vol{volume}/part{part}/{value}/images/{size}/1.webp"
            )
    return list(dict.fromkeys(urls))


def _action_run_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in sorted(_ACTION_RUNS, key=lambda item: item["started_at"], reverse=True):
        if run.get("status") in {"running", "ok", "fail"} and run.get("proc") is None:
            rows.append(
                {
                    "job_name": run["title"],
                    "action_key": run.get("key"),
                    "started_at": run["started_at"],
                    "finished_at": run.get("finished_at"),
                    "status": run["status"],
                    "api_rows": None,
                    "raw_new": None,
                    "norm_upserted": None,
                    "duplicates": None,
                    "dup_pct": None,
                    "error": run.get("error", ""),
                }
            )
            continue

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
                "action_key": run.get("key"),
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


def _action_key_for_job(job_name: Any) -> str | None:
    key = JOB_RUN_ACTION_ALIASES.get(str(job_name or "").strip().lower())
    return key if key in JOB_ACTIONS else None


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
    result = []
    for row in action_rows + rows:
        if not row.get("action_key"):
            row["action_key"] = _action_key_for_job(row.get("job_name"))
        result.append(_jsonable_row(row))
    return result


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


def _action_command(action: dict[str, str]) -> tuple[list[str], int]:
    script_path = (PROJECT_ROOT / action["script"]).resolve()
    if not script_path.exists():
        raise FileNotFoundError(f"Скрипт не найден: {action['script']}")
    if sys.platform == "win32":
        return ["cmd", "/c", str(script_path)], subprocess.CREATE_NO_WINDOW
    return [str(script_path)], 0


def _failed_action_keys() -> list[str]:
    rows = _db_fetch_all(
        """
        SELECT job_name
        FROM job_runs
        WHERE started_at >= NOW() - INTERVAL '24 hours'
          AND LOWER(COALESCE(status, '')) NOT IN ('ok', 'running')
        ORDER BY id DESC
        LIMIT 200
        """
    )
    keys: list[str] = []
    for row in rows:
        job_name = str(row.get("job_name") or "").strip().lower()
        key = JOB_RUN_ACTION_ALIASES.get(job_name, job_name)
        if key in JOB_ACTIONS and key not in keys:
            keys.append(key)
    return keys


def start_job_action(key: str) -> dict[str, Any]:
    action = JOB_ACTIONS.get(key)
    if action is None:
        raise ValueError("Неизвестная команда запуска")

    cmd, creationflags = _action_command(action)

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        creationflags=creationflags,
    )
    job_id = str(uuid.uuid4())
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


def _run_action_batch(run: dict[str, Any], keys: list[str]) -> None:
    errors: list[str] = []
    for key in keys:
        action = JOB_ACTIONS[key]
        try:
            cmd, creationflags = _action_command(action)
            completed = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                creationflags=creationflags,
                check=False,
            )
        except Exception as exc:  # pragma: no cover - protective runtime branch
            errors.append(f"{action['title']}: {exc}")
            continue
        if completed.returncode != 0:
            errors.append(f"{action['title']}: код {completed.returncode}")
    run["finished_at"] = datetime.now().isoformat()
    if errors:
        run["status"] = "fail"
        run["error"] = "; ".join(errors)[:240]
    else:
        run["status"] = "ok"
        run["error"] = ""


def start_job_batch(scope: str) -> dict[str, Any]:
    if scope == "failed":
        keys = _failed_action_keys()
        title = "Перезапуск ошибочных jobs"
    elif scope == "all":
        keys = [key for key, action in JOB_ACTIONS.items() if (PROJECT_ROOT / action["script"]).exists()]
        title = "Запуск всех jobs"
    else:
        raise ValueError("scope должен быть failed или all")

    if not keys:
        return {
            "job_id": str(uuid.uuid4()),
            "scope": scope,
            "title": title,
            "started_at": datetime.now().isoformat(),
            "count": 0,
            "keys": [],
            "status": "skipped",
        }

    run = {
        "job_id": str(uuid.uuid4()),
        "key": f"batch_{scope}",
        "title": title,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "proc": None,
        "status": "running",
        "error": f"В очереди: {', '.join(keys)}",
    }
    _ACTION_RUNS.append(run)
    del _ACTION_RUNS[:-30]
    thread = threading.Thread(target=_run_action_batch, args=(run, keys), daemon=True)
    thread.start()
    return {
        "job_id": run["job_id"],
        "scope": scope,
        "title": title,
        "started_at": run["started_at"],
        "count": len(keys),
        "keys": keys,
        "status": "running",
    }


def _safe_fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    try:
        return _db_fetch_all(query, params)
    except Exception:
        return []


def get_order_detail(*, marketplace: str, key: str) -> dict[str, Any]:
    clean_key = key.strip()
    if not clean_key:
        raise ValueError("Номер заказа не задан")

    if marketplace == "ozon":
        rows = _db_fetch_all(
            """
            SELECT
                posting_number AS order_key,
                order_number,
                COALESCE(order_number, posting_number) AS order_group_key,
                order_id,
                status,
                substatus,
                cancel_reason_id,
                cancel_reason,
                cancellation_initiator,
                cancellation_type,
                created_at,
                in_process_at,
                shipment_date,
                analytics_warehouse_id,
                analytics_warehouse_name,
                analytics_city,
                analytics_delivery_type,
                analytics_payment_type_group_name,
                financial_cluster_from,
                financial_cluster_to,
                product_offer_id AS article,
                COALESCE(card.product_name, staging.ozon_fbo_order_items_full.product_name) AS product_name,
                product_sku AS marketplace_sku,
                product_quantity AS quantity,
                product_price_amount AS price,
                financial_payout AS payout,
                financial_commission_amount,
                financial_commission_percent,
                card.primary_image AS image_url,
                card.images AS image_urls,
                updated_at
            FROM staging.ozon_fbo_order_items_full
            LEFT JOIN LATERAL (
                SELECT primary_image, images, product_name
                FROM staging.marketplace_product_cards_current
                WHERE marketplace = 'ozon'
                  AND (
                      article = staging.ozon_fbo_order_items_full.product_offer_id
                      OR marketplace_sku = staging.ozon_fbo_order_items_full.product_sku
                  )
                ORDER BY updated_at DESC
                LIMIT 1
            ) card ON TRUE
            WHERE posting_number = %s
               OR order_number = %s
               OR COALESCE(order_number, posting_number) = %s
            ORDER BY line_number
            """,
            (clean_key, clean_key, clean_key),
        )
        postings = sorted({str(row.get("order_key") or "") for row in rows if row.get("order_key")})
        history: list[dict[str, Any]] = []
        raw_payload: dict[str, Any] | None = None
        if postings:
            history = _safe_fetch_all(
                """
                SELECT
                    posting_number AS order_key,
                    changed_at,
                    status,
                    previous_status,
                    status_changed,
                    substatus,
                    previous_substatus,
                    substatus_changed,
                    warehouse_name,
                    previous_warehouse_name,
                    warehouse_changed
                FROM analytics.ozon_fbo_posting_change_history_flat
                WHERE posting_number = ANY(%s)
                ORDER BY changed_at DESC
                LIMIT 30
                """,
                (postings,),
            )
            raw = _safe_fetch_all(
                """
                SELECT payload
                FROM raw.ozon_fbo_postings
                WHERE posting_number = %s
                LIMIT 1
                """,
                (postings[0],),
            )
            raw_payload = raw[0].get("payload") if raw else None
        for row in rows:
            row["status_label"] = _label_status("ozon", row.get("status"))
            image_urls = _ordered_image_urls(row.get("image_url"), row.get("image_urls"))
            row["image_urls"] = image_urls
        for row in history:
            row["status_label"] = _label_status("ozon", row.get("status"))
            row["previous_status_label"] = _label_status("ozon", row.get("previous_status"))
        return _jsonable_row(
            {
                "marketplace": "Ozon",
                "key": clean_key,
                "rows": rows,
                "history": history,
                "raw_payload": raw_payload,
            }
        )

    if marketplace == "wb":
        rows = _db_fetch_all(
            """
            SELECT
                srid AS order_key,
                g_number AS order_number,
                COALESCE(g_number, srid) AS order_group_key,
                CASE WHEN is_cancel THEN 'cancelled' ELSE 'active' END AS status,
                is_cancel,
                date_ts AS order_date,
                last_change_ts,
                warehouse_name,
                warehouse_type,
                country_name,
                oblast_okrug_name,
                region_name,
                supplier_article AS article,
                nm_id AS marketplace_sku,
                barcode,
                category,
                COALESCE(card.product_name, subject) AS product_name,
                brand,
                tech_size,
                income_id,
                total_price,
                discount_percent,
                spp,
                finished_price,
                price_with_disc AS price,
                cancel_date,
                sticker,
                card.primary_image AS image_url,
                card.images AS image_urls
            FROM wb_orders_norm
            LEFT JOIN LATERAL (
                SELECT primary_image, images, product_name
                FROM staging.marketplace_product_cards_current
                WHERE marketplace = 'wb'
                  AND (
                      article = wb_orders_norm.supplier_article
                      OR marketplace_sku = wb_orders_norm.nm_id
                  )
                ORDER BY updated_at DESC
                LIMIT 1
            ) card ON TRUE
            WHERE srid = %s
               OR g_number = %s
               OR COALESCE(g_number, srid) = %s
            ORDER BY COALESCE(date_ts, last_change_ts) DESC NULLS LAST
            """,
            (clean_key, clean_key, clean_key),
        )
        srids = sorted({str(row.get("order_key") or "") for row in rows if row.get("order_key")})
        history: list[dict[str, Any]] = []
        raw_payload: dict[str, Any] | None = None
        if srids:
            history = _safe_fetch_all(
                """
                SELECT
                    srid AS order_key,
                    changed_at,
                    status,
                    cancel_type,
                    payload#>>'{warehouseName}' AS warehouse_name,
                    payload#>>'{destinationCity}' AS destination_city
                FROM raw.wb_order_feed_order_versions
                WHERE srid = ANY(%s)
                ORDER BY changed_at DESC
                LIMIT 30
                """,
                (srids,),
            )
            raw = _safe_fetch_all(
                """
                SELECT payload
                FROM wb_orders_raw_dedup
                WHERE srid = %s
                ORDER BY last_change_ts DESC
                LIMIT 1
                """,
                (srids[0],),
            )
            raw_payload = raw[0].get("payload") if raw else None
        for row in rows:
            row["status_label"] = _label_status("wb", row.get("status"))
            image_urls = _ordered_image_urls(row.get("image_url"), row.get("image_urls"))
            if not image_urls:
                image_urls = _wb_image_urls(row.get("marketplace_sku"))
            row["image_url"] = row.get("image_url") or (image_urls[0] if image_urls else None)
            row["image_urls"] = image_urls
        for row in history:
            row["status_label"] = _label_status("wb", row.get("status"))
        return _jsonable_row(
            {
                "marketplace": "WB",
                "key": clean_key,
                "rows": rows,
                "history": history,
                "raw_payload": raw_payload,
            }
        )

    raise ValueError("marketplace должен быть wb или ozon")


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
                COALESCE(card.product_name, staging.ozon_fbo_order_items_full.product_name) AS product_name,
                product_sku AS marketplace_sku,
                product_quantity AS quantity,
                product_price_amount AS price,
                financial_payout AS payout,
                card.primary_image AS image_url,
                card.images AS image_urls,
                staging.ozon_fbo_order_items_full.updated_at
            FROM staging.ozon_fbo_order_items_full
            JOIN groups
                ON groups.order_group_key = COALESCE(order_number, posting_number)
            LEFT JOIN LATERAL (
                SELECT primary_image, images, product_name
                FROM staging.marketplace_product_cards_current
                WHERE marketplace = 'ozon'
                  AND (
                      article = staging.ozon_fbo_order_items_full.product_offer_id
                      OR marketplace_sku = staging.ozon_fbo_order_items_full.product_sku
                  )
                ORDER BY updated_at DESC
                LIMIT 1
            ) card ON TRUE
            ORDER BY COALESCE(in_process_at, created_at, updated_at) DESC NULLS LAST
            """,
            (limit, offset),
        )
        result = []
        for row in rows:
            row["order_group_key"] = row.get("order_number") or row.get("order_key")
            row["status_label"] = _label_status("ozon", row.get("status"))
            image_urls = _ordered_image_urls(row.get("image_url"), row.get("image_urls"))
            row["image_urls"] = image_urls
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
                COALESCE(card.product_name, subject) AS product_name,
                nm_id AS marketplace_sku,
                1 AS quantity,
                price_with_disc AS price,
                finished_price AS payout,
                card.primary_image AS image_url,
                card.images AS image_urls,
                last_change_ts AS updated_at
            FROM wb_orders_norm
            JOIN groups
                ON groups.order_group_key = COALESCE(g_number, srid)
            LEFT JOIN LATERAL (
                SELECT primary_image, images, product_name
                FROM staging.marketplace_product_cards_current
                WHERE marketplace = 'wb'
                  AND (
                      article = wb_orders_norm.supplier_article
                      OR marketplace_sku = wb_orders_norm.nm_id
                  )
                ORDER BY updated_at DESC
                LIMIT 1
            ) card ON TRUE
            ORDER BY COALESCE(date_ts, last_change_ts) DESC NULLS LAST
            """,
            (limit, offset),
        )
        result = []
        for row in rows:
            row["order_group_key"] = row.get("order_number") or row.get("order_key")
            row["status_label"] = _label_status("wb", row.get("status"))
            image_urls = _ordered_image_urls(row.get("image_url"), row.get("image_urls"))
            if not image_urls:
                image_urls = _wb_image_urls(row.get("marketplace_sku"))
            row["image_url"] = row.get("image_url") or (image_urls[0] if image_urls else None)
            row["image_urls"] = image_urls
            result.append(_jsonable_row({"marketplace": "WB", **row}))
        return result

    raise ValueError("marketplace должен быть wb или ozon")
