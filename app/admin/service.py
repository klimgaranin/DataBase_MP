from __future__ import annotations

from datetime import datetime
from typing import Any

from app.ops.health import dependency_status, safe_dsn_summary
from app.secrets import SENSITIVE_SECRET_NAMES, get_secret, secret_status


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

    jobs = get_jobs(limit=6)
    failed_jobs = [job for job in jobs if str(job.get("status", "")).lower() != "ok"]

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


def get_jobs(*, limit: int = 20) -> list[dict[str, Any]]:
    try:
        rows = _db_fetch_all(
            """
            SELECT job_name, started_at, finished_at, status,
                   api_rows, raw_new, norm_upserted, duplicates,
                   ROUND(dup_pct::numeric, 2) AS dup_pct,
                   LEFT(COALESCE(error, ''), 240) AS error
            FROM job_runs
            ORDER BY id DESC
            LIMIT %s
            """,
            (max(1, min(limit, 100)),),
        )
    except Exception as exc:
        return [{"job_name": "DB", "status": "error", "error": str(exc)}]
    return [_jsonable_row(row) for row in rows]


def get_secrets_status() -> dict[str, bool]:
    return secret_status(SENSITIVE_SECRET_NAMES)


def get_orders_feed(*, marketplace: str, limit: int = 100) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 500))
    if marketplace == "ozon":
        rows = _db_fetch_all(
            """
            SELECT
                posting_number AS order_key,
                order_number,
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
                updated_at
            FROM staging.ozon_fbo_order_items_full
            ORDER BY COALESCE(in_process_at, created_at, updated_at) DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return [_jsonable_row({"marketplace": "Ozon", **row}) for row in rows]

    if marketplace == "wb":
        rows = _db_fetch_all(
            """
            SELECT
                srid AS order_key,
                g_number AS order_number,
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
            ORDER BY COALESCE(date_ts, last_change_ts) DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return [_jsonable_row({"marketplace": "WB", **row}) for row in rows]

    raise ValueError("marketplace должен быть wb или ozon")
