"""
db.py — все DB-функции проекта DataBase_MP.
Зависимости: psycopg2-binary, python-dotenv.
"""
from __future__ import annotations

import json
import os
import re
import hashlib
from contextlib import contextmanager
from typing import Any, Generator, Optional
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


def _load_env() -> None:
    for candidate in [
        Path(__file__).parent / ".env",
        Path(__file__).parent.parent / ".env",
    ]:
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            return
    load_dotenv(override=False)


_load_env()

_ADVISORY_LOCK_CONNECTIONS: dict[int, psycopg2.extensions.connection] = {}


def _get_connection_kwargs() -> dict[str, Any] | str:
    dsn = os.getenv("PG_DSN", "")
    if not dsn:
        raise RuntimeError("PG_DSN не задан в .env")
    env_password = os.getenv("POSTGRES_PASSWORD", "")
    parsed = urlparse(dsn)
    if parsed.scheme:
        kwargs: dict[str, Any] = {
            "dbname": parsed.path.lstrip("/") or None,
            "user": parsed.username,
            "password": env_password or unquote(parsed.password or ""),
            "host": parsed.hostname,
            "port": parsed.port,
        }
        return {key: value for key, value in kwargs.items() if value not in (None, "")}

    values = _parse_libpq_dsn_shape(dsn)
    if env_password:
        values["password"] = env_password
    if values:
        return {
            "dbname": values.get("dbname"),
            "user": values.get("user"),
            "password": values.get("password"),
            "host": values.get("host") or values.get("hostaddr"),
            "port": values.get("port"),
        }
    return dsn


def _parse_libpq_dsn_shape(dsn: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)=", dsn))
    result: dict[str, str] = {}
    for idx, match in enumerate(matches):
        key = match.group(1)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(dsn)
        value = dsn[start:end].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        result[key] = value
    return result


@contextmanager
def connect() -> Generator[psycopg2.extensions.connection, None, None]:
    conn_kwargs = _get_connection_kwargs()
    conn = psycopg2.connect(conn_kwargs) if isinstance(conn_kwargs, str) else psycopg2.connect(**conn_kwargs)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_WB_ORDERS_RAW_DEDUP = """
CREATE TABLE IF NOT EXISTS wb_orders_raw_dedup (
    id             BIGSERIAL   PRIMARY KEY,
    srid           TEXT        NOT NULL,
    last_change_ts TIMESTAMPTZ NOT NULL,
    payload        JSONB       NOT NULL,
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (srid, last_change_ts)
);
"""

_DDL_WB_ORDERS_NORM = """
CREATE TABLE IF NOT EXISTS wb_orders_norm (
    srid               TEXT PRIMARY KEY,
    is_cancel          BOOLEAN,
    date_ts            TIMESTAMPTZ,
    last_change_ts     TIMESTAMPTZ,
    warehouse_name     TEXT,
    warehouse_type     TEXT,
    country_name       TEXT,
    oblast_okrug_name  TEXT,
    region_name        TEXT,
    supplier_article   TEXT,
    nm_id              BIGINT,
    barcode            TEXT,
    category           TEXT,
    subject            TEXT,
    brand              TEXT,
    tech_size          TEXT,
    income_id          BIGINT,
    is_supply          BOOLEAN,
    is_realization     BOOLEAN,
    total_price        NUMERIC(12,2),
    discount_percent   INT,
    spp                INT,
    finished_price     NUMERIC(12,2),
    price_with_disc    NUMERIC(12,2),
    cancel_date        DATE,
    sticker            TEXT,
    g_number           TEXT,
    inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_CURSORS = """
CREATE TABLE IF NOT EXISTS job_cursors (
    job_name   TEXT PRIMARY KEY,
    cursor_val TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_JOB_RUNS = """
CREATE TABLE IF NOT EXISTS job_runs (
    id           BIGSERIAL   PRIMARY KEY,
    job_name     TEXT        NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL,
    status       TEXT        NOT NULL,
    api_rows     INT         NOT NULL DEFAULT 0,
    raw_new      INT         NOT NULL DEFAULT 0,
    norm_upserted INT        NOT NULL DEFAULT 0,
    duplicates   INT         NOT NULL DEFAULT 0,
    dup_pct      NUMERIC(6,2) NOT NULL DEFAULT 0,
    cursor_old   TEXT,
    cursor_used  TEXT,
    cursor_new   TEXT,
    error        TEXT
);
"""

_DDL_WB_STOCKS_RAW = """
CREATE TABLE IF NOT EXISTS wb_stocks_raw (
    id           BIGSERIAL   PRIMARY KEY,
    nm_id        BIGINT      NOT NULL,
    chrt_id      BIGINT      NOT NULL,
    warehouse_id BIGINT      NOT NULL,
    payload      JSONB       NOT NULL,
    snapped_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_nm_id   ON wb_stocks_raw (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_chrt_id ON wb_stocks_raw (chrt_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_raw_snapped ON wb_stocks_raw (snapped_at);
"""

_DDL_WB_STOCKS_SNAP = """
CREATE TABLE IF NOT EXISTS wb_stocks_snap (
    nm_id              BIGINT      NOT NULL,
    chrt_id            BIGINT      NOT NULL,
    warehouse_id       BIGINT      NOT NULL,
    warehouse_name     TEXT,
    region_name        TEXT,
    quantity           INT,
    in_way_to_client   INT,
    in_way_from_client INT,
    snapped_at         TIMESTAMPTZ NOT NULL,
    inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nm_id, chrt_id, warehouse_id)
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_nm_id   ON wb_stocks_snap (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_snap_snapped ON wb_stocks_snap (snapped_at);
"""

_DDL_SOURCE_FILE_SNAPSHOTS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.source_file_snapshots (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    source_name    TEXT        NOT NULL,
    file_path      TEXT        NOT NULL,
    file_sha256    TEXT        NOT NULL,
    table_name     TEXT        NOT NULL,
    row_count      INT         NOT NULL DEFAULT 0,
    payload        JSONB       NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_file_snapshots_run
    ON raw.source_file_snapshots (run_id);
"""

_DDL_SOURCE_ORDERS_DAILY = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.source_orders_daily (
    source_system   TEXT        NOT NULL,
    article         TEXT        NOT NULL,
    fact_date       DATE        NOT NULL,
    status          TEXT        NOT NULL DEFAULT '',
    orders_qty      INT         NOT NULL DEFAULT 0,
    revenue         NUMERIC(14,2) NOT NULL DEFAULT 0,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, article, fact_date, status)
);
"""

_DDL_SOURCE_STOCK_SUMMARY = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.source_stock_summary (
    source_system   TEXT        NOT NULL,
    article         TEXT        NOT NULL,
    quantity        INT         NOT NULL DEFAULT 0,
    in_way_qty      INT         NOT NULL DEFAULT 0,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    snapped_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, article)
);
"""

_DDL_OZON_STORAGE_COSTS = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_storage_costs (
    article                    TEXT        PRIMARY KEY,
    paid_qty                   INT         NOT NULL DEFAULT 0,
    paid_liters                NUMERIC(14,3) NOT NULL DEFAULT 0,
    daily_writeoff_rub         NUMERIC(14,2) NOT NULL DEFAULT 0,
    days_until_first_paid      INT,
    payload                    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id              TEXT        NOT NULL,
    snapped_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_SUPPLY_PIPELINE_CURRENT = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.supply_pipeline_current (
    article             TEXT        PRIMARY KEY,
    approved_order_qty  INT         NOT NULL DEFAULT 0,
    in_production_qty   INT         NOT NULL DEFAULT 0,
    ready_qty           INT         NOT NULL DEFAULT 0,
    in_way_qty          INT         NOT NULL DEFAULT 0,
    minsk_date          DATE,
    payload             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id       TEXT        NOT NULL,
    snapped_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_FBO_POSTINGS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_fbo_postings (
    posting_number  TEXT PRIMARY KEY,
    order_id        BIGINT,
    status          TEXT,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    payload_sha256  TEXT,
    payload         JSONB       NOT NULL,
    source_run_id   TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_FBO_POSTING_VERSIONS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_fbo_posting_versions (
    id              BIGSERIAL   PRIMARY KEY,
    posting_number  TEXT        NOT NULL,
    payload_sha256  TEXT        NOT NULL,
    order_id        BIGINT,
    status          TEXT,
    substatus       TEXT,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    payload         JSONB       NOT NULL,
    source_run_id   TEXT        NOT NULL,
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_posting_versions_posting
    ON raw.ozon_fbo_posting_versions (posting_number, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_posting_versions_changed
    ON raw.ozon_fbo_posting_versions (changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_posting_versions_status
    ON raw.ozon_fbo_posting_versions (status);
"""

_DDL_OZON_FBO_ORDER_ITEMS_FULL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_fbo_order_items_full (
    posting_number  TEXT        NOT NULL,
    line_number     INT         NOT NULL,
    order_id        BIGINT,
    order_number    TEXT,
    status          TEXT,
    substatus       TEXT,
    created_at      TIMESTAMPTZ,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    cancel_reason_id BIGINT,
    cancel_reason   TEXT,
    cancellation_initiator TEXT,
    cancellation_type TEXT,
    analytics_city  TEXT,
    analytics_delivery_type TEXT,
    analytics_is_legal BOOLEAN,
    analytics_is_premium BOOLEAN,
    analytics_payment_type_group_name TEXT,
    analytics_warehouse_id BIGINT,
    analytics_warehouse_name TEXT,
    analytics_client_delivery_date_begin TIMESTAMPTZ,
    analytics_client_delivery_date_end TIMESTAMPTZ,
    financial_cluster_from TEXT,
    financial_cluster_to TEXT,
    legal_company_name TEXT,
    legal_inn TEXT,
    legal_kpp TEXT,
    external_order_is_external BOOLEAN,
    external_order_platform_name TEXT,
    products_count INT NOT NULL DEFAULT 0,
    financial_products_count INT NOT NULL DEFAULT 0,
    additional_data_count INT NOT NULL DEFAULT 0,
    additional_data JSONB NOT NULL DEFAULT '[]'::jsonb,
    product_is_marketplace_buyout BOOLEAN NOT NULL DEFAULT FALSE,
    product_offer_id TEXT NOT NULL DEFAULT '',
    product_name TEXT,
    product_sku BIGINT,
    product_quantity INT NOT NULL DEFAULT 0,
    product_price_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    product_price_currency TEXT,
    product_digital_codes_count INT NOT NULL DEFAULT 0,
    product_digital_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    financial_product_id BIGINT,
    financial_payout NUMERIC(14,2) NOT NULL DEFAULT 0,
    financial_old_price NUMERIC(14,2) NOT NULL DEFAULT 0,
    financial_price NUMERIC(14,2) NOT NULL DEFAULT 0,
    financial_total_discount_value NUMERIC(14,2) NOT NULL DEFAULT 0,
    financial_total_discount_percent NUMERIC(8,2) NOT NULL DEFAULT 0,
    financial_actions_count INT NOT NULL DEFAULT 0,
    financial_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
    financial_commission_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    financial_commission_percent NUMERIC(8,2) NOT NULL DEFAULT 0,
    financial_commission_currency TEXT,
    posting_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    product_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    financial_product_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (posting_number, line_number)
);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_order_items_full_in_process
    ON staging.ozon_fbo_order_items_full (in_process_at DESC);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_order_items_full_offer
    ON staging.ozon_fbo_order_items_full (product_offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_order_items_full_sku
    ON staging.ozon_fbo_order_items_full (product_sku);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_order_items_full_status
    ON staging.ozon_fbo_order_items_full (status);
"""

_OZON_FBO_ORDER_ITEMS_FULL_COLS = [
    "posting_number",
    "line_number",
    "order_id",
    "order_number",
    "status",
    "substatus",
    "created_at",
    "in_process_at",
    "shipment_date",
    "cancel_reason_id",
    "cancel_reason",
    "cancellation_initiator",
    "cancellation_type",
    "analytics_city",
    "analytics_delivery_type",
    "analytics_is_legal",
    "analytics_is_premium",
    "analytics_payment_type_group_name",
    "analytics_warehouse_id",
    "analytics_warehouse_name",
    "analytics_client_delivery_date_begin",
    "analytics_client_delivery_date_end",
    "financial_cluster_from",
    "financial_cluster_to",
    "legal_company_name",
    "legal_inn",
    "legal_kpp",
    "external_order_is_external",
    "external_order_platform_name",
    "products_count",
    "financial_products_count",
    "additional_data_count",
    "additional_data",
    "product_is_marketplace_buyout",
    "product_offer_id",
    "product_name",
    "product_sku",
    "product_quantity",
    "product_price_amount",
    "product_price_currency",
    "product_digital_codes_count",
    "product_digital_codes",
    "financial_product_id",
    "financial_payout",
    "financial_old_price",
    "financial_price",
    "financial_total_discount_value",
    "financial_total_discount_percent",
    "financial_actions_count",
    "financial_actions",
    "financial_commission_amount",
    "financial_commission_percent",
    "financial_commission_currency",
    "posting_payload",
    "product_payload",
    "financial_product_payload",
    "source_run_id",
]

_DDL_OZON_PRODUCT_LIST_ITEMS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_product_list_items (
    product_id     BIGINT      PRIMARY KEY,
    offer_id       TEXT,
    sku            BIGINT,
    archived       BOOLEAN,
    has_fbo_stocks BOOLEAN,
    has_fbs_stocks BOOLEAN,
    is_discounted  BOOLEAN,
    quants_count   INT         NOT NULL DEFAULT 0,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_PRODUCT_INFO_ITEMS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_product_info_items (
    product_id     BIGINT      PRIMARY KEY,
    offer_id       TEXT,
    sku            BIGINT,
    name           TEXT,
    currency_code  TEXT,
    price          NUMERIC(14,2),
    old_price      NUMERIC(14,2),
    min_price      NUMERIC(14,2),
    vat            NUMERIC(8,2),
    volume_weight  NUMERIC(14,4),
    description_category_id BIGINT,
    type_id        BIGINT,
    primary_image  TEXT,
    images_count   INT         NOT NULL DEFAULT 0,
    barcodes_count INT         NOT NULL DEFAULT 0,
    commissions_count INT      NOT NULL DEFAULT 0,
    is_archived    BOOLEAN,
    is_autoarchived BOOLEAN,
    is_discounted  BOOLEAN,
    has_discounted_fbo_item BOOLEAN,
    is_kgt         BOOLEAN,
    is_prepayment_allowed BOOLEAN,
    is_seasonal    BOOLEAN,
    is_super       BOOLEAN,
    status         TEXT,
    status_name    TEXT,
    status_description TEXT,
    moderate_status TEXT,
    validation_status TEXT,
    status_updated_at TIMESTAMPTZ,
    has_price      BOOLEAN,
    has_stock      BOOLEAN,
    model_id       BIGINT,
    model_count    INT,
    source_created_at TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_ANALYTICS_STOCKS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_analytics_stocks (
    id             BIGSERIAL   PRIMARY KEY,
    sku            BIGINT,
    offer_id       TEXT,
    cluster_id     BIGINT,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    snapped_at     TIMESTAMPTZ NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_STOCK_BY_CLUSTER = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_stock_by_cluster (
    sku                       BIGINT      NOT NULL,
    offer_id                  TEXT        NOT NULL DEFAULT '',
    cluster_id                BIGINT      NOT NULL DEFAULT 0,
    cluster_name              TEXT,
    available_stock_count     INT         NOT NULL DEFAULT 0,
    valid_stock_count         INT         NOT NULL DEFAULT 0,
    other_stock_count         INT         NOT NULL DEFAULT 0,
    requested_stock_count     INT         NOT NULL DEFAULT 0,
    transit_stock_count       INT         NOT NULL DEFAULT 0,
    return_from_customer_stock_count INT   NOT NULL DEFAULT 0,
    return_to_seller_stock_count INT       NOT NULL DEFAULT 0,
    stock_defect_stock_count  INT         NOT NULL DEFAULT 0,
    transit_defect_stock_count INT        NOT NULL DEFAULT 0,
    expiring_stock_count      INT         NOT NULL DEFAULT 0,
    waiting_docs_stock_count  INT         NOT NULL DEFAULT 0,
    waiting_docs_to_export_stock_count INT NOT NULL DEFAULT 0,
    excess_stock_count        INT         NOT NULL DEFAULT 0,
    in_way_to_warehouse_count INT         NOT NULL DEFAULT 0,
    ads                       NUMERIC(14,4),
    ads_cluster               NUMERIC(14,4),
    idc                       NUMERIC(14,4),
    idc_cluster               NUMERIC(14,4),
    days_without_sales        INT,
    days_without_sales_cluster INT,
    turnover_grade            TEXT,
    turnover_grade_cluster    TEXT,
    macrolocal_cluster_id     BIGINT,
    warehouse_id              BIGINT,
    warehouse_name            TEXT,
    placement_zone            TEXT,
    name                      TEXT,
    item_tags_count           INT         NOT NULL DEFAULT 0,
    payload                   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id             TEXT        NOT NULL,
    snapped_at                TIMESTAMPTZ NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sku, offer_id, cluster_id)
);
"""

_DDL_OZON_STOCK_DETAILS = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_stock_details (
    id                         BIGSERIAL   PRIMARY KEY,
    source_run_id              TEXT        NOT NULL,
    row_hash                   TEXT        NOT NULL,
    sku                        BIGINT      NOT NULL,
    offer_id                   TEXT        NOT NULL DEFAULT '',
    cluster_id                 BIGINT      NOT NULL DEFAULT 0,
    cluster_name               TEXT,
    available_stock_count      INT         NOT NULL DEFAULT 0,
    valid_stock_count          INT         NOT NULL DEFAULT 0,
    other_stock_count          INT         NOT NULL DEFAULT 0,
    requested_stock_count      INT         NOT NULL DEFAULT 0,
    transit_stock_count        INT         NOT NULL DEFAULT 0,
    return_from_customer_stock_count INT   NOT NULL DEFAULT 0,
    return_to_seller_stock_count INT       NOT NULL DEFAULT 0,
    stock_defect_stock_count   INT         NOT NULL DEFAULT 0,
    transit_defect_stock_count INT         NOT NULL DEFAULT 0,
    expiring_stock_count       INT         NOT NULL DEFAULT 0,
    waiting_docs_stock_count   INT         NOT NULL DEFAULT 0,
    waiting_docs_to_export_stock_count INT NOT NULL DEFAULT 0,
    excess_stock_count         INT         NOT NULL DEFAULT 0,
    in_way_to_warehouse_count  INT         NOT NULL DEFAULT 0,
    ads                        NUMERIC(14,4),
    ads_cluster                NUMERIC(14,4),
    idc                        NUMERIC(14,4),
    idc_cluster                NUMERIC(14,4),
    days_without_sales         INT,
    days_without_sales_cluster INT,
    turnover_grade             TEXT,
    turnover_grade_cluster     TEXT,
    macrolocal_cluster_id      BIGINT,
    warehouse_id               BIGINT,
    warehouse_name             TEXT,
    placement_zone             TEXT,
    name                       TEXT,
    item_tags_count            INT         NOT NULL DEFAULT 0,
    payload                    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    snapped_at                 TIMESTAMPTZ NOT NULL,
    loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_run_id, row_hash)
);
CREATE INDEX IF NOT EXISTS idx_ozon_stock_details_run
    ON staging.ozon_stock_details (source_run_id);
CREATE INDEX IF NOT EXISTS idx_ozon_stock_details_offer
    ON staging.ozon_stock_details (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_stock_details_sku
    ON staging.ozon_stock_details (sku);
"""

_DDL_OZON_PLACEMENT_REPORTS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.ozon_placement_reports (
    code           TEXT        PRIMARY KEY,
    date_from      DATE        NOT NULL,
    date_to        DATE        NOT NULL,
    status         TEXT,
    file_url       TEXT,
    file_sha256    TEXT,
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id  TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_DDL_OZON_PLACEMENT_BY_PRODUCTS = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_placement_by_products (
    report_code    TEXT        NOT NULL,
    row_number     INT         NOT NULL,
    sku            BIGINT,
    offer_id       TEXT,
    product_name   TEXT,
    placement_cost NUMERIC(14,2),
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_code, row_number)
);
"""

_DDL_OZON_PLACEMENT_CELLS = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ozon_placement_cells (
    report_code    TEXT        NOT NULL,
    row_number     INT         NOT NULL,
    column_number  INT         NOT NULL,
    column_name    TEXT        NOT NULL,
    value_text     TEXT,
    value_numeric  NUMERIC(18,4),
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_code, row_number, column_number)
);
CREATE INDEX IF NOT EXISTS idx_ozon_placement_cells_column
    ON staging.ozon_placement_cells (column_name);
"""


# ── Advisory locks ────────────────────────────────────────────────────────────

def try_advisory_lock(lock_id: int) -> bool:
    if lock_id in _ADVISORY_LOCK_CONNECTIONS:
        return False
    conn_kwargs = _get_connection_kwargs()
    conn = psycopg2.connect(conn_kwargs) if isinstance(conn_kwargs, str) else psycopg2.connect(**conn_kwargs)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            locked = bool(cur.fetchone()[0])
        if locked:
            _ADVISORY_LOCK_CONNECTIONS[lock_id] = conn
            return True
        conn.close()
        return False
    except Exception:
        conn.close()
        raise


def advisory_unlock(lock_id: int) -> None:
    conn = _ADVISORY_LOCK_CONNECTIONS.pop(lock_id, None)
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
    finally:
        conn.close()


# ── Job cursors ───────────────────────────────────────────────────────────────

def get_job_cursor(job_name: str) -> Optional[str]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_CURSORS)
            cur.execute("SELECT cursor_val FROM job_cursors WHERE job_name = %s", (job_name,))
            row = cur.fetchone()
            return row[0] if row else None


def set_job_cursor(job_name: str, cursor_val: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_CURSORS)
            cur.execute(
                """
                INSERT INTO job_cursors (job_name, cursor_val, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (job_name) DO UPDATE
                SET cursor_val = EXCLUDED.cursor_val,
                    updated_at = NOW()
                """,
                (job_name, cursor_val),
            )


# ── WB Orders ─────────────────────────────────────────────────────────────────

def insert_wb_orders_raw_dedup(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    records = []
    for r in rows:
        srid = r.get("srid") or r.get("sr_id") or ""
        lcd  = r.get("lastChangeDate") or r.get("last_change_date") or None
        records.append((srid, lcd, json.dumps(r, ensure_ascii=False)))
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_ORDERS_RAW_DEDUP)
            execute_values(
                cur,
                """
                INSERT INTO wb_orders_raw_dedup (srid, last_change_ts, payload)
                VALUES %s
                ON CONFLICT (srid, last_change_ts) DO NOTHING
                """,
                records,
                fetch=False,
            )
            return cur.rowcount if cur.rowcount >= 0 else 0


def cleanup_wb_orders_raw_dedup(retention_days: int) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM wb_orders_raw_dedup WHERE inserted_at < NOW() - INTERVAL '1 day' * %s",
                (retention_days,),
            )
            return cur.rowcount


def upsert_wb_orders_norm(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = [
        "srid", "is_cancel", "date_ts", "last_change_ts",
        "warehouse_name", "warehouse_type", "country_name",
        "oblast_okrug_name", "region_name",
        "supplier_article", "nm_id", "barcode",
        "category", "subject", "brand", "tech_size",
        "income_id", "is_supply", "is_realization",
        "total_price", "discount_percent", "spp",
        "finished_price", "price_with_disc",
        "cancel_date", "sticker", "g_number",
    ]

    def _row_tuple(r: dict[str, Any]) -> tuple:
        return tuple(r.get(c) for c in cols)

    update_cols = [c for c in cols if c != "srid"]
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    update_set += ", updated_at = NOW()"

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_ORDERS_NORM)
            execute_values(
                cur,
                f"""
                INSERT INTO wb_orders_norm ({', '.join(cols)})
                VALUES %s
                ON CONFLICT (srid) DO UPDATE SET {update_set}
                """,
                [_row_tuple(r) for r in rows],
            )
            return cur.rowcount


# ── WB Stocks ─────────────────────────────────────────────────────────────────

def insert_wb_stocks_raw(rows: list[dict[str, Any]], snapped_at: str) -> int:
    if not rows:
        return 0
    values = []
    for r in rows:
        nm_id        = r.get("nmId")        or r.get("nm_id")
        chrt_id      = r.get("chrtId")      or r.get("chrt_id")
        warehouse_id = r.get("warehouseId") or r.get("warehouse_id")
        if nm_id and chrt_id and warehouse_id:
            values.append((nm_id, chrt_id, warehouse_id, json.dumps(r, ensure_ascii=False), snapped_at))
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_RAW)
            execute_values(
                cur,
                """
                INSERT INTO wb_stocks_raw (nm_id, chrt_id, warehouse_id, payload, snapped_at)
                VALUES %s
                """,
                values,
                page_size=1000,
            )
            return len(values)


def upsert_wb_stocks_snap(rows: list[dict[str, Any]], snapped_at: str) -> int:
    if not rows:
        return 0
    cols = (
        "nm_id", "chrt_id", "warehouse_id",
        "warehouse_name", "region_name",
        "quantity", "in_way_to_client", "in_way_from_client",
        "snapped_at",
    )
    values = []
    for r in rows:
        if not (r.get("nm_id") and r.get("chrt_id") and r.get("warehouse_id")):
            continue
        values.append(tuple([r.get(c) for c in cols[:-1]] + [snapped_at]))
    if not values:
        return 0
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in cols
        if c not in ("nm_id", "chrt_id", "warehouse_id")
    ) + ", updated_at = NOW()"
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_SNAP)
            execute_values(
                cur,
                f"""
                INSERT INTO wb_stocks_snap ({', '.join(cols)}) VALUES %s
                ON CONFLICT (nm_id, chrt_id, warehouse_id) DO UPDATE SET {update_set}
                """,
                values,
                page_size=1000,
            )
            return len(values)


def cleanup_wb_stocks_raw(retention_days: int = 30) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_WB_STOCKS_RAW)
            cur.execute(
                "DELETE FROM wb_stocks_raw WHERE snapped_at < NOW() - INTERVAL '1 day' * %s",
                (retention_days,),
            )
            return cur.rowcount


# ── Source statistics from Excel/files ────────────────────────────────────────

def insert_source_file_snapshots(
    *,
    run_id: str,
    source_name: str,
    file_path: str,
    file_sha256: str,
    tables: list[dict[str, Any]],
) -> int:
    if not tables:
        return 0
    values = [
        (
            run_id,
            source_name,
            file_path,
            file_sha256,
            table["table_name"],
            int(table.get("row_count") or 0),
            json.dumps(table.get("payload", {}), ensure_ascii=False, default=str),
        )
        for table in tables
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_SOURCE_FILE_SNAPSHOTS)
            execute_values(
                cur,
                """
                INSERT INTO raw.source_file_snapshots
                    (run_id, source_name, file_path, file_sha256, table_name, row_count, payload)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb)",
            )
            return len(values)


def upsert_source_orders_daily(rows: list[dict[str, Any]], *, run_id: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r["source_system"],
            r["article"],
            r["fact_date"],
            r.get("status") or "",
            r.get("orders_qty") or 0,
            r.get("revenue") or 0,
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
        )
        for r in rows
        if r.get("source_system") and r.get("article") and r.get("fact_date")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_SOURCE_ORDERS_DAILY)
            execute_values(
                cur,
                """
                INSERT INTO staging.source_orders_daily
                    (source_system, article, fact_date, status, orders_qty, revenue, payload, source_run_id)
                VALUES %s
                ON CONFLICT (source_system, article, fact_date, status) DO UPDATE SET
                    orders_qty = EXCLUDED.orders_qty,
                    revenue = EXCLUDED.revenue,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return len(values)


def upsert_source_stock_summary(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r["source_system"],
            r["article"],
            r.get("quantity") or 0,
            r.get("in_way_qty") or 0,
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
            snapped_at,
        )
        for r in rows
        if r.get("source_system") and r.get("article")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_SOURCE_STOCK_SUMMARY)
            execute_values(
                cur,
                """
                INSERT INTO staging.source_stock_summary
                    (source_system, article, quantity, in_way_qty, payload, source_run_id, snapped_at)
                VALUES %s
                ON CONFLICT (source_system, article) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    in_way_qty = EXCLUDED.in_way_qty,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    snapped_at = EXCLUDED.snapped_at,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s::jsonb, %s, %s)",
                page_size=1000,
            )
            return len(values)


def upsert_ozon_storage_costs(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r["article"],
            r.get("paid_qty") or 0,
            r.get("paid_liters") or 0,
            r.get("daily_writeoff_rub") or 0,
            r.get("days_until_first_paid"),
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
            snapped_at,
        )
        for r in rows
        if r.get("article")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_STORAGE_COSTS)
            execute_values(
                cur,
                """
                INSERT INTO staging.ozon_storage_costs
                    (article, paid_qty, paid_liters, daily_writeoff_rub,
                     days_until_first_paid, payload, source_run_id, snapped_at)
                VALUES %s
                ON CONFLICT (article) DO UPDATE SET
                    paid_qty = EXCLUDED.paid_qty,
                    paid_liters = EXCLUDED.paid_liters,
                    daily_writeoff_rub = EXCLUDED.daily_writeoff_rub,
                    days_until_first_paid = EXCLUDED.days_until_first_paid,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    snapped_at = EXCLUDED.snapped_at,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
                page_size=1000,
            )
            return len(values)


def insert_production_inventory_snapshot(rows: list[dict[str, Any]], *, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r["article"],
            r.get("smp_qty") or 0,
            r.get("osn_qty") or 0,
            r.get("soh_qty") or 0,
            r.get("svh_qty") or 0,
            r.get("ts_qty") or 0,
            snapped_at,
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
        )
        for r in rows
        if r.get("article")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO core.production_inventory_snapshot
                    (article, smp_qty, osn_qty, soh_qty, svh_qty, ts_qty, snapped_at, payload)
                VALUES %s
                ON CONFLICT (article, snapped_at) DO UPDATE SET
                    smp_qty = EXCLUDED.smp_qty,
                    osn_qty = EXCLUDED.osn_qty,
                    soh_qty = EXCLUDED.soh_qty,
                    svh_qty = EXCLUDED.svh_qty,
                    ts_qty = EXCLUDED.ts_qty,
                    payload = EXCLUDED.payload
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
                page_size=1000,
            )
            return len(values)


def upsert_supply_pipeline_current(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r["article"],
            r.get("approved_order_qty") or 0,
            r.get("in_production_qty") or 0,
            r.get("ready_qty") or 0,
            r.get("in_way_qty") or 0,
            r.get("minsk_date"),
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
            snapped_at,
        )
        for r in rows
        if r.get("article")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_SUPPLY_PIPELINE_CURRENT)
            execute_values(
                cur,
                """
                INSERT INTO staging.supply_pipeline_current
                    (article, approved_order_qty, in_production_qty, ready_qty,
                     in_way_qty, minsk_date, payload, source_run_id, snapped_at)
                VALUES %s
                ON CONFLICT (article) DO UPDATE SET
                    approved_order_qty = EXCLUDED.approved_order_qty,
                    in_production_qty = EXCLUDED.in_production_qty,
                    ready_qty = EXCLUDED.ready_qty,
                    in_way_qty = EXCLUDED.in_way_qty,
                    minsk_date = EXCLUDED.minsk_date,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    snapped_at = EXCLUDED.snapped_at,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
                page_size=1000,
            )
            return len(values)


def insert_raw_api_responses(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [
        (
            r["run_id"],
            r["marketplace"],
            r["method_name"],
            r["http_method"],
            r["url"],
            json.dumps(r.get("request_payload"), ensure_ascii=False, default=str) if r.get("request_payload") is not None else None,
            r.get("response_status"),
            json.dumps(r.get("response_payload"), ensure_ascii=False, default=str) if r.get("response_payload") is not None else None,
            r.get("response_sha256"),
            r.get("duration_ms"),
            r.get("attempt") or 1,
            r.get("error"),
        )
        for r in rows
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw.api_responses
                    (run_id, marketplace, method_name, http_method, url,
                     request_payload, response_status, response_payload,
                     response_sha256, duration_ms, attempt, error)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s)",
                page_size=500,
            )
            return len(values)


def upsert_ozon_fbo_postings(rows: list[dict[str, Any]], *, run_id: str) -> int:
    if not rows:
        return 0
    values = []
    for row in rows:
        if not row.get("posting_number"):
            continue
        payload_json = json.dumps(row.get("payload", {}), ensure_ascii=False, sort_keys=True, default=str)
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        values.append(
            (
                row.get("posting_number"),
                row.get("order_id"),
                row.get("status"),
                row.get("substatus"),
                row.get("in_process_at"),
                row.get("shipment_date"),
                payload_sha256,
                payload_json,
                run_id,
            )
        )
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_FBO_POSTINGS)
            cur.execute("ALTER TABLE raw.ozon_fbo_postings ADD COLUMN IF NOT EXISTS payload_sha256 TEXT")
            cur.execute(_DDL_OZON_FBO_POSTING_VERSIONS)
            execute_values(
                cur,
                """
                WITH incoming (
                    posting_number, order_id, status, substatus, in_process_at,
                    shipment_date, payload_sha256, payload, source_run_id
                ) AS (
                    VALUES %s
                ),
                changed AS (
                    SELECT i.*
                    FROM incoming i
                    LEFT JOIN raw.ozon_fbo_postings c
                        ON c.posting_number = i.posting_number
                    WHERE c.posting_number IS NULL
                       OR c.payload IS DISTINCT FROM i.payload
                ),
                history_insert AS (
                    INSERT INTO raw.ozon_fbo_posting_versions
                        (posting_number, payload_sha256, order_id, status, substatus,
                         in_process_at, shipment_date, payload, source_run_id)
                    SELECT
                        posting_number, payload_sha256, order_id, status, substatus,
                        in_process_at, shipment_date, payload, source_run_id
                    FROM changed
                    RETURNING 1
                )
                INSERT INTO raw.ozon_fbo_postings
                    (posting_number, order_id, status, in_process_at, shipment_date,
                     payload_sha256, payload, source_run_id)
                SELECT
                    posting_number, order_id, status, in_process_at, shipment_date,
                    payload_sha256, payload, source_run_id
                FROM incoming
                ON CONFLICT (posting_number) DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    status = EXCLUDED.status,
                    in_process_at = EXCLUDED.in_process_at,
                    shipment_date = EXCLUDED.shipment_date,
                    payload_sha256 = EXCLUDED.payload_sha256,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    updated_at = NOW()
                WHERE raw.ozon_fbo_postings.payload IS DISTINCT FROM EXCLUDED.payload
                """,
                values,
                template="(%s, %s, %s, %s, %s::timestamptz, %s::timestamptz, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            cur.execute("SELECT COUNT(*) FROM raw.ozon_fbo_posting_versions WHERE source_run_id = %s", (run_id,))
            return int(cur.fetchone()[0])


def upsert_ozon_fbo_order_items_full(rows: list[dict[str, Any]], *, run_id: str) -> int:
    if not rows:
        return 0

    jsonb_columns = {
        "additional_data",
        "product_digital_codes",
        "financial_actions",
        "posting_payload",
        "product_payload",
        "financial_product_payload",
    }
    values = []
    for row in rows:
        if not row.get("posting_number") or not row.get("line_number"):
            continue
        item = []
        for column in _OZON_FBO_ORDER_ITEMS_FULL_COLS:
            if column == "source_run_id":
                item.append(run_id)
            elif column in jsonb_columns:
                item.append(json.dumps(row.get(column, [] if column.endswith(("codes", "actions", "data")) else {}), ensure_ascii=False, default=str))
            else:
                item.append(row.get(column))
        values.append(tuple(item))

    if not values:
        return 0

    insert_cols = ", ".join(_OZON_FBO_ORDER_ITEMS_FULL_COLS)
    update_cols = [c for c in _OZON_FBO_ORDER_ITEMS_FULL_COLS if c not in {"posting_number", "line_number"}]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    update_set += ", updated_at = NOW()"
    template_parts = [("%s::jsonb" if col in jsonb_columns else "%s") for col in _OZON_FBO_ORDER_ITEMS_FULL_COLS]
    template = "(" + ", ".join(template_parts) + ")"

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_FBO_ORDER_ITEMS_FULL)
            execute_values(
                cur,
                f"""
                INSERT INTO staging.ozon_fbo_order_items_full ({insert_cols})
                VALUES %s
                ON CONFLICT (posting_number, line_number) DO UPDATE SET {update_set}
                WHERE staging.ozon_fbo_order_items_full.posting_payload IS DISTINCT FROM EXCLUDED.posting_payload
                   OR staging.ozon_fbo_order_items_full.product_payload IS DISTINCT FROM EXCLUDED.product_payload
                   OR staging.ozon_fbo_order_items_full.financial_product_payload IS DISTINCT FROM EXCLUDED.financial_product_payload
                """,
                values,
                template=template,
                page_size=1000,
            )
            return cur.rowcount if cur.rowcount >= 0 else 0


def upsert_ozon_product_list_items(rows: list[dict[str, Any]], *, run_id: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r.get("product_id"),
            r.get("offer_id"),
            r.get("sku"),
            r.get("archived"),
            r.get("has_fbo_stocks"),
            r.get("has_fbs_stocks"),
            r.get("is_discounted"),
            r.get("quants_count") or 0,
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
        )
        for r in rows
        if r.get("product_id")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_PRODUCT_LIST_ITEMS)
            execute_values(
                cur,
                """
                INSERT INTO raw.ozon_product_list_items
                    (product_id, offer_id, sku, archived, has_fbo_stocks,
                     has_fbs_stocks, is_discounted, quants_count,
                     payload, source_run_id)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    offer_id = EXCLUDED.offer_id,
                    sku = EXCLUDED.sku,
                    archived = EXCLUDED.archived,
                    has_fbo_stocks = EXCLUDED.has_fbo_stocks,
                    has_fbs_stocks = EXCLUDED.has_fbs_stocks,
                    is_discounted = EXCLUDED.is_discounted,
                    quants_count = EXCLUDED.quants_count,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return len(values)


def upsert_ozon_product_info_items(rows: list[dict[str, Any]], *, run_id: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r.get("product_id"),
            r.get("offer_id"),
            r.get("sku"),
            r.get("name"),
            r.get("currency_code"),
            r.get("price"),
            r.get("old_price"),
            r.get("min_price"),
            r.get("vat"),
            r.get("volume_weight"),
            r.get("description_category_id"),
            r.get("type_id"),
            r.get("primary_image"),
            r.get("images_count") or 0,
            r.get("barcodes_count") or 0,
            r.get("commissions_count") or 0,
            r.get("is_archived"),
            r.get("is_autoarchived"),
            r.get("is_discounted"),
            r.get("has_discounted_fbo_item"),
            r.get("is_kgt"),
            r.get("is_prepayment_allowed"),
            r.get("is_seasonal"),
            r.get("is_super"),
            r.get("status"),
            r.get("status_name"),
            r.get("status_description"),
            r.get("moderate_status"),
            r.get("validation_status"),
            r.get("status_updated_at"),
            r.get("has_price"),
            r.get("has_stock"),
            r.get("model_id"),
            r.get("model_count"),
            r.get("created_at"),
            r.get("updated_at"),
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
        )
        for r in rows
        if r.get("product_id")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_PRODUCT_INFO_ITEMS)
            execute_values(
                cur,
                """
                INSERT INTO raw.ozon_product_info_items
                    (product_id, offer_id, sku, name,
                     currency_code, price, old_price, min_price, vat, volume_weight,
                     description_category_id, type_id, primary_image,
                     images_count, barcodes_count, commissions_count,
                     is_archived, is_autoarchived, is_discounted, has_discounted_fbo_item,
                     is_kgt, is_prepayment_allowed, is_seasonal, is_super,
                     status, status_name, status_description, moderate_status, validation_status,
                     status_updated_at, has_price, has_stock, model_id, model_count,
                     source_created_at, source_updated_at,
                     payload, source_run_id)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    offer_id = EXCLUDED.offer_id,
                    sku = EXCLUDED.sku,
                    name = EXCLUDED.name,
                    currency_code = EXCLUDED.currency_code,
                    price = EXCLUDED.price,
                    old_price = EXCLUDED.old_price,
                    min_price = EXCLUDED.min_price,
                    vat = EXCLUDED.vat,
                    volume_weight = EXCLUDED.volume_weight,
                    description_category_id = EXCLUDED.description_category_id,
                    type_id = EXCLUDED.type_id,
                    primary_image = EXCLUDED.primary_image,
                    images_count = EXCLUDED.images_count,
                    barcodes_count = EXCLUDED.barcodes_count,
                    commissions_count = EXCLUDED.commissions_count,
                    is_archived = EXCLUDED.is_archived,
                    is_autoarchived = EXCLUDED.is_autoarchived,
                    is_discounted = EXCLUDED.is_discounted,
                    has_discounted_fbo_item = EXCLUDED.has_discounted_fbo_item,
                    is_kgt = EXCLUDED.is_kgt,
                    is_prepayment_allowed = EXCLUDED.is_prepayment_allowed,
                    is_seasonal = EXCLUDED.is_seasonal,
                    is_super = EXCLUDED.is_super,
                    status = EXCLUDED.status,
                    status_name = EXCLUDED.status_name,
                    status_description = EXCLUDED.status_description,
                    moderate_status = EXCLUDED.moderate_status,
                    validation_status = EXCLUDED.validation_status,
                    status_updated_at = EXCLUDED.status_updated_at,
                    has_price = EXCLUDED.has_price,
                    has_stock = EXCLUDED.has_stock,
                    model_id = EXCLUDED.model_id,
                    model_count = EXCLUDED.model_count,
                    source_created_at = EXCLUDED.source_created_at,
                    source_updated_at = EXCLUDED.source_updated_at,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return len(values)


def upsert_core_ozon_marketplace_products(rows: list[dict[str, Any]]) -> int:
    values = [
        (
            "ozon",
            r.get("offer_id") or "",
            str(r.get("product_id") or ""),
            r.get("sku"),
            str(r.get("sku") or "") if r.get("sku") is not None else None,
            r.get("name"),
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            r.get("updated_at"),
        )
        for r in rows
        if r.get("offer_id")
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO core.marketplace_products
                    (marketplace, article, external_id, nm_id, sku, product_name, payload, source_updated_at)
                VALUES %s
                ON CONFLICT (marketplace, article) DO UPDATE SET
                    external_id = EXCLUDED.external_id,
                    nm_id = EXCLUDED.nm_id,
                    sku = EXCLUDED.sku,
                    product_name = EXCLUDED.product_name,
                    payload = EXCLUDED.payload,
                    source_updated_at = EXCLUDED.source_updated_at,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return len(values)


def insert_ozon_analytics_stocks(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r.get("sku"),
            r.get("offer_id"),
            r.get("cluster_id"),
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
            snapped_at,
        )
        for r in rows
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_ANALYTICS_STOCKS)
            execute_values(
                cur,
                """
                INSERT INTO raw.ozon_analytics_stocks
                    (sku, offer_id, cluster_id, payload, source_run_id, snapped_at)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s::jsonb, %s, %s)",
                page_size=1000,
            )
            return len(values)


def upsert_ozon_stock_by_cluster(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = [
        (
            r.get("sku"),
            r.get("offer_id") or "",
            r.get("cluster_id") or 0,
            r.get("cluster_name"),
            r.get("available_stock_count") or 0,
            r.get("valid_stock_count") or 0,
            r.get("other_stock_count") or 0,
            r.get("requested_stock_count") or 0,
            r.get("transit_stock_count") or 0,
            r.get("return_from_customer_stock_count") or 0,
            r.get("return_to_seller_stock_count") or 0,
            r.get("stock_defect_stock_count") or 0,
            r.get("transit_defect_stock_count") or 0,
            r.get("expiring_stock_count") or 0,
            r.get("waiting_docs_stock_count") or 0,
            r.get("waiting_docs_to_export_stock_count") or 0,
            r.get("excess_stock_count") or 0,
            r.get("in_way_to_warehouse_count") or 0,
            r.get("ads"),
            r.get("ads_cluster"),
            r.get("idc"),
            r.get("idc_cluster"),
            r.get("days_without_sales"),
            r.get("days_without_sales_cluster"),
            r.get("turnover_grade"),
            r.get("turnover_grade_cluster"),
            r.get("macrolocal_cluster_id"),
            r.get("warehouse_id"),
            r.get("warehouse_name"),
            r.get("placement_zone"),
            r.get("name"),
            r.get("item_tags_count") or 0,
            json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
            run_id,
            snapped_at,
        )
        for r in rows
        if r.get("sku") is not None
    ]
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_STOCK_BY_CLUSTER)
            execute_values(
                cur,
                """
                INSERT INTO staging.ozon_stock_by_cluster
                    (sku, offer_id, cluster_id, cluster_name,
                     available_stock_count, valid_stock_count, other_stock_count,
                     requested_stock_count, transit_stock_count,
                     return_from_customer_stock_count, return_to_seller_stock_count,
                     stock_defect_stock_count, transit_defect_stock_count,
                     expiring_stock_count, waiting_docs_stock_count,
                     waiting_docs_to_export_stock_count, excess_stock_count,
                     in_way_to_warehouse_count,
                     ads, ads_cluster, idc, idc_cluster,
                     days_without_sales, days_without_sales_cluster,
                     turnover_grade, turnover_grade_cluster,
                     macrolocal_cluster_id, warehouse_id, warehouse_name,
                     placement_zone, name, item_tags_count,
                     payload, source_run_id, snapped_at)
                VALUES %s
                ON CONFLICT (sku, offer_id, cluster_id) DO UPDATE SET
                    cluster_name = EXCLUDED.cluster_name,
                    available_stock_count = EXCLUDED.available_stock_count,
                    valid_stock_count = EXCLUDED.valid_stock_count,
                    other_stock_count = EXCLUDED.other_stock_count,
                    requested_stock_count = EXCLUDED.requested_stock_count,
                    transit_stock_count = EXCLUDED.transit_stock_count,
                    return_from_customer_stock_count = EXCLUDED.return_from_customer_stock_count,
                    return_to_seller_stock_count = EXCLUDED.return_to_seller_stock_count,
                    stock_defect_stock_count = EXCLUDED.stock_defect_stock_count,
                    transit_defect_stock_count = EXCLUDED.transit_defect_stock_count,
                    expiring_stock_count = EXCLUDED.expiring_stock_count,
                    waiting_docs_stock_count = EXCLUDED.waiting_docs_stock_count,
                    waiting_docs_to_export_stock_count = EXCLUDED.waiting_docs_to_export_stock_count,
                    excess_stock_count = EXCLUDED.excess_stock_count,
                    in_way_to_warehouse_count = EXCLUDED.in_way_to_warehouse_count,
                    ads = EXCLUDED.ads,
                    ads_cluster = EXCLUDED.ads_cluster,
                    idc = EXCLUDED.idc,
                    idc_cluster = EXCLUDED.idc_cluster,
                    days_without_sales = EXCLUDED.days_without_sales,
                    days_without_sales_cluster = EXCLUDED.days_without_sales_cluster,
                    turnover_grade = EXCLUDED.turnover_grade,
                    turnover_grade_cluster = EXCLUDED.turnover_grade_cluster,
                    macrolocal_cluster_id = EXCLUDED.macrolocal_cluster_id,
                    warehouse_id = EXCLUDED.warehouse_id,
                    warehouse_name = EXCLUDED.warehouse_name,
                    placement_zone = EXCLUDED.placement_zone,
                    name = EXCLUDED.name,
                    item_tags_count = EXCLUDED.item_tags_count,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    snapped_at = EXCLUDED.snapped_at,
                    updated_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
                page_size=1000,
            )
            return len(values)


def insert_ozon_stock_details(rows: list[dict[str, Any]], *, run_id: str, snapped_at: str) -> int:
    if not rows:
        return 0
    values = []
    for index, row in enumerate(rows):
        if row.get("sku") is None:
            continue
        payload_json = json.dumps(row.get("payload", {}), ensure_ascii=False, sort_keys=True, default=str)
        row_hash = hashlib.sha256(
            f"{index}|{row.get('sku')}|{row.get('offer_id') or ''}|{row.get('cluster_id') or 0}|{payload_json}".encode("utf-8")
        ).hexdigest()
        values.append(
            (
                run_id,
                row_hash,
                row.get("sku"),
                row.get("offer_id") or "",
                row.get("cluster_id") or 0,
                row.get("cluster_name"),
                row.get("available_stock_count") or 0,
                row.get("valid_stock_count") or 0,
                row.get("other_stock_count") or 0,
                row.get("requested_stock_count") or 0,
                row.get("transit_stock_count") or 0,
                row.get("return_from_customer_stock_count") or 0,
                row.get("return_to_seller_stock_count") or 0,
                row.get("stock_defect_stock_count") or 0,
                row.get("transit_defect_stock_count") or 0,
                row.get("expiring_stock_count") or 0,
                row.get("waiting_docs_stock_count") or 0,
                row.get("waiting_docs_to_export_stock_count") or 0,
                row.get("excess_stock_count") or 0,
                row.get("in_way_to_warehouse_count") or 0,
                row.get("ads"),
                row.get("ads_cluster"),
                row.get("idc"),
                row.get("idc_cluster"),
                row.get("days_without_sales"),
                row.get("days_without_sales_cluster"),
                row.get("turnover_grade"),
                row.get("turnover_grade_cluster"),
                row.get("macrolocal_cluster_id"),
                row.get("warehouse_id"),
                row.get("warehouse_name"),
                row.get("placement_zone"),
                row.get("name"),
                row.get("item_tags_count") or 0,
                payload_json,
                snapped_at,
            )
        )
    if not values:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_STOCK_DETAILS)
            execute_values(
                cur,
                """
                INSERT INTO staging.ozon_stock_details
                    (source_run_id, row_hash, sku, offer_id, cluster_id, cluster_name,
                     available_stock_count, valid_stock_count, other_stock_count,
                     requested_stock_count, transit_stock_count,
                     return_from_customer_stock_count, return_to_seller_stock_count,
                     stock_defect_stock_count, transit_defect_stock_count,
                     expiring_stock_count, waiting_docs_stock_count,
                     waiting_docs_to_export_stock_count, excess_stock_count,
                     in_way_to_warehouse_count,
                     ads, ads_cluster, idc, idc_cluster,
                     days_without_sales, days_without_sales_cluster,
                     turnover_grade, turnover_grade_cluster,
                     macrolocal_cluster_id, warehouse_id, warehouse_name,
                     placement_zone, name, item_tags_count,
                     payload, snapped_at)
                VALUES %s
                ON CONFLICT (source_run_id, row_hash) DO NOTHING
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return cur.rowcount


def upsert_ozon_placement_report(row: dict[str, Any], *, run_id: str) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_PLACEMENT_REPORTS)
            cur.execute(
                """
                INSERT INTO raw.ozon_placement_reports
                    (code, date_from, date_to, status, file_url, file_sha256, payload, source_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (code) DO UPDATE SET
                    status = EXCLUDED.status,
                    file_url = EXCLUDED.file_url,
                    file_sha256 = EXCLUDED.file_sha256,
                    payload = EXCLUDED.payload,
                    source_run_id = EXCLUDED.source_run_id,
                    updated_at = NOW()
                """,
                (
                    row.get("code"),
                    row.get("date_from"),
                    row.get("date_to"),
                    row.get("status"),
                    row.get("file_url"),
                    row.get("file_sha256"),
                    json.dumps(row.get("payload", {}), ensure_ascii=False, default=str),
                    run_id,
                ),
            )
            return 1


def replace_ozon_placement_rows(rows: list[dict[str, Any]], *, report_code: str, run_id: str) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_PLACEMENT_BY_PRODUCTS)
            cur.execute("DELETE FROM staging.ozon_placement_by_products WHERE report_code = %s", (report_code,))
            if not rows:
                return 0
            values = [
                (
                    report_code,
                    r.get("row_number"),
                    r.get("sku"),
                    r.get("offer_id"),
                    r.get("product_name"),
                    r.get("placement_cost"),
                    json.dumps(r.get("payload", {}), ensure_ascii=False, default=str),
                    run_id,
                )
                for r in rows
            ]
            execute_values(
                cur,
                """
                INSERT INTO staging.ozon_placement_by_products
                    (report_code, row_number, sku, offer_id, product_name, placement_cost, payload, source_run_id)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                page_size=1000,
            )
            return len(values)


def _numeric_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def replace_ozon_placement_cells(rows: list[dict[str, Any]], *, report_code: str, run_id: str) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_OZON_PLACEMENT_CELLS)
            cur.execute("DELETE FROM staging.ozon_placement_cells WHERE report_code = %s", (report_code,))
            values = []
            for row in rows:
                payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                for column_number, (column_name, value) in enumerate(payload.items(), start=1):
                    numeric_value = _numeric_or_none(value)
                    values.append(
                        (
                            report_code,
                            row.get("row_number"),
                            column_number,
                            str(column_name),
                            None if value is None else str(value),
                            numeric_value,
                            run_id,
                        )
                    )
            if not values:
                return 0
            execute_values(
                cur,
                """
                INSERT INTO staging.ozon_placement_cells
                    (report_code, row_number, column_number, column_name,
                     value_text, value_numeric, source_run_id)
                VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s)",
                page_size=1000,
            )
            return len(values)


# ── Job runs ──────────────────────────────────────────────────────────────────

def insert_job_run(
    *,
    job_name:        str,
    started_at_iso:  str,
    finished_at_iso: str,
    status:          str,
    api_rows:        int           = 0,
    raw_new_versions: int          = 0,
    norm_upserted:   int           = 0,
    duplicates:      int           = 0,
    dup_pct:         float         = 0.0,
    cursor_old:      Optional[str] = None,
    cursor_used:     Optional[str] = None,
    cursor_new:      Optional[str] = None,
    error:           Optional[str] = None,
) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL_JOB_RUNS)
            cur.execute(
                """
                INSERT INTO job_runs
                    (job_name, started_at, finished_at, status,
                     api_rows, raw_new, norm_upserted, duplicates, dup_pct,
                     cursor_old, cursor_used, cursor_new, error)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    job_name, started_at_iso, finished_at_iso, status,
                    api_rows, raw_new_versions, norm_upserted, duplicates, dup_pct,
                    cursor_old, cursor_used, cursor_new, error,
                ),
            )


def get_last_dup_pct(job_name: str) -> Optional[float]:
    with connect() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT dup_pct FROM job_runs
                    WHERE job_name = %s AND status = 'ok'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (job_name,),
                )
                row = cur.fetchone()
                return float(row[0]) if row else None
            except Exception:
                conn.rollback()
                return None


# ── WB Adv Campaigns (V8) ─────────────────────────────────────────────────────

def upsert_wb_adv_campaigns(items: list[dict]) -> int:
    """UPSERT кампаний из /adv/v1/promotion/count."""
    if not items:
        return 0
    records = [
        (
            r["advert_id"],
            r["status"],
            r.get("type"),
            r.get("change_time"),
            json.dumps(r.get("payload", {}), ensure_ascii=False),
        )
        for r in items
        if r.get("advert_id")
    ]
    if not records:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO wb_adv_campaigns
                    (advert_id, status, type, change_time, payload, fetched_at, updated_at)
                VALUES %s
                ON CONFLICT (advert_id) DO UPDATE SET
                    status      = EXCLUDED.status,
                    type        = EXCLUDED.type,
                    change_time = EXCLUDED.change_time,
                    payload     = EXCLUDED.payload,
                    fetched_at  = NOW(),
                    updated_at  = NOW()
                """,
                records,
                template="(%s, %s, %s, %s, %s::jsonb, NOW(), NOW())",
            )
            return cur.rowcount if cur.rowcount >= 0 else len(records)


def get_adv_campaign_ids(statuses: list[int] | None = None) -> list[int]:
    """
    Список advert_id из wb_adv_campaigns.
    Сортировка: change_time DESC NULLS LAST — свежие РК идут в первые батчи.
    """
    if statuses is None:
        statuses = [7, 9, 11]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT advert_id
                FROM   wb_adv_campaigns
                WHERE  status = ANY(%s)
                ORDER  BY change_time DESC NULLS LAST, advert_id
                """,
                (statuses,),
            )
            return [row[0] for row in cur.fetchall()]


# ── WB Adv Fullstats (V8 + V9) ───────────────────────────────────────────────

def atomic_save_fullstats(
    raw_items:  list[dict],
    begin_date: str,
    end_date:   str,
    norm_rows:  list[dict],
) -> tuple[int, int]:
    """
    Атомарно в одной транзакции:
      1. TRUNCATE wb_adv_fullstats_raw, wb_adv_fullstats_norm
      2. INSERT raw  — сырые ответы API
      3. INSERT norm — все метрики (spend, views, clicks, atbs, orders, shks, sum_price)

    Возвращает (raw_count, norm_count).
    При ошибке — ROLLBACK, старые данные в БД остаются целы.
    """
    raw_records = [
        (
            int(r["advertId"]),
            begin_date,
            end_date,
            json.dumps(r, ensure_ascii=False),
        )
        for r in raw_items
        if r.get("advertId")
    ]

    norm_records = [
        (
            r["advert_id"],
            r["nm_id"],
            r["stat_date"],
            r["spend_rub"],
            r["views"],
            r["clicks"],
            r["atbs"],
            r["orders"],
            r["shks"],
            r["sum_price"],
        )
        for r in norm_rows
        if r.get("advert_id") and r.get("nm_id") and r.get("stat_date")
    ]

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE wb_adv_fullstats_raw, wb_adv_fullstats_norm")

            if raw_records:
                execute_values(
                    cur,
                    """
                    INSERT INTO wb_adv_fullstats_raw
                        (advert_id, begin_date, end_date, payload, fetched_at)
                    VALUES %s
                    """,
                    raw_records,
                    template="(%s, %s, %s, %s::jsonb, NOW())",
                    page_size=500,
                )

            if norm_records:
                execute_values(
                    cur,
                    """
                    INSERT INTO wb_adv_fullstats_norm
                        (advert_id, nm_id, stat_date,
                         spend_rub, views, clicks, atbs, orders, shks, sum_price,
                         updated_at)
                    VALUES %s
                    """,
                    norm_records,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                    page_size=1000,
                )

    return len(raw_records), len(norm_records)
