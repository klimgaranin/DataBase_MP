-- WB Analytics API Order Feed: current raw state, change history and full technical layer.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.wb_order_feed_orders (
    srid              TEXT        PRIMARY KEY,
    nm_id             BIGINT,
    chrt_id           BIGINT,
    created_at        TIMESTAMPTZ,
    status_updated_at TIMESTAMPTZ,
    status            TEXT,
    cancel_type       TEXT,
    payload_sha256    TEXT        NOT NULL,
    payload           JSONB       NOT NULL,
    source_run_id     TEXT        NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_orders_updated ON raw.wb_order_feed_orders (status_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_orders_status ON raw.wb_order_feed_orders (status);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_orders_nm ON raw.wb_order_feed_orders (nm_id);

CREATE TABLE IF NOT EXISTS raw.wb_order_feed_order_versions (
    id                BIGSERIAL   PRIMARY KEY,
    srid              TEXT        NOT NULL,
    payload_sha256    TEXT        NOT NULL,
    nm_id             BIGINT,
    chrt_id           BIGINT,
    created_at        TIMESTAMPTZ,
    status_updated_at TIMESTAMPTZ,
    status            TEXT,
    cancel_type       TEXT,
    payload           JSONB       NOT NULL,
    source_run_id     TEXT        NOT NULL,
    changed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (srid, payload_sha256)
);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_versions_srid ON raw.wb_order_feed_order_versions (srid, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_versions_status ON raw.wb_order_feed_order_versions (status, changed_at DESC);

CREATE TABLE IF NOT EXISTS staging.wb_order_feed_orders_full (
    srid                 TEXT        PRIMARY KEY,
    nm_id                BIGINT,
    chrt_id              BIGINT,
    created_at           TIMESTAMPTZ,
    status_updated_at    TIMESTAMPTZ,
    status               TEXT,
    cancel_type          TEXT,
    warehouse_name       TEXT,
    warehouse_region     TEXT,
    is_mp                BOOLEAN,
    destination_city     TEXT,
    destination_district TEXT,
    seller_price         NUMERIC(14,2),
    currency             TEXT,
    is_b2b               BOOLEAN,
    payload              JSONB       NOT NULL,
    source_run_id        TEXT        NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_full_created ON staging.wb_order_feed_orders_full (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_full_status_updated ON staging.wb_order_feed_orders_full (status_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_full_nm ON staging.wb_order_feed_orders_full (nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_order_feed_full_status ON staging.wb_order_feed_orders_full (status);

CREATE OR REPLACE VIEW analytics.wb_order_feed_orders_flat AS
SELECT
    srid, nm_id, chrt_id, created_at, status_updated_at, status, cancel_type,
    warehouse_name, warehouse_region, is_mp, destination_city, destination_district,
    seller_price, currency, is_b2b, source_run_id, updated_at
FROM staging.wb_order_feed_orders_full;

CREATE OR REPLACE VIEW analytics.wb_order_feed_change_history_flat AS
WITH versions AS (
    SELECT
        v.*,
        row_number() OVER (PARTITION BY v.srid ORDER BY v.changed_at, v.id) AS version_no,
        lag(v.status) OVER (PARTITION BY v.srid ORDER BY v.changed_at, v.id) AS previous_status,
        lag(v.cancel_type) OVER (PARTITION BY v.srid ORDER BY v.changed_at, v.id) AS previous_cancel_type
    FROM raw.wb_order_feed_order_versions v
)
SELECT
    srid, version_no, changed_at, status_updated_at, nm_id, chrt_id,
    previous_status, status, previous_cancel_type, cancel_type,
    (previous_status IS DISTINCT FROM status) AS status_changed,
    (previous_cancel_type IS DISTINCT FROM cancel_type) AS cancel_type_changed,
    source_run_id
FROM versions;
