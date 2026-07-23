-- ============================================================
-- V21: история изменений Ozon FBO postings
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

ALTER TABLE raw.ozon_fbo_postings
    ADD COLUMN IF NOT EXISTS payload_sha256 TEXT;

UPDATE raw.ozon_fbo_postings
SET payload_sha256 = encode(digest(payload::text, 'sha256'), 'hex')
WHERE payload_sha256 IS NULL;

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

INSERT INTO raw.ozon_fbo_posting_versions
    (posting_number, payload_sha256, order_id, status, substatus,
     in_process_at, shipment_date, payload, source_run_id, changed_at)
SELECT
    posting_number,
    COALESCE(payload_sha256, encode(digest(payload::text, 'sha256'), 'hex')),
    order_id,
    status,
    payload->>'substatus',
    in_process_at,
    shipment_date,
    payload,
    source_run_id,
    COALESCE(updated_at, fetched_at, NOW())
FROM raw.ozon_fbo_postings
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.ozon_fbo_posting_versions v
    WHERE v.posting_number = raw.ozon_fbo_postings.posting_number
);

CREATE OR REPLACE VIEW analytics.ozon_fbo_posting_change_history_flat AS
WITH versions AS (
    SELECT
        v.*,
        row_number() OVER (
            PARTITION BY v.posting_number
            ORDER BY v.changed_at, v.id
        ) AS version_no,
        lag(v.status) OVER (
            PARTITION BY v.posting_number
            ORDER BY v.changed_at, v.id
        ) AS previous_status,
        lag(v.substatus) OVER (
            PARTITION BY v.posting_number
            ORDER BY v.changed_at, v.id
        ) AS previous_substatus,
        lag(v.payload#>>'{analytics_data,warehouse_name}') OVER (
            PARTITION BY v.posting_number
            ORDER BY v.changed_at, v.id
        ) AS previous_warehouse_name
    FROM raw.ozon_fbo_posting_versions v
)
SELECT
    posting_number,
    version_no,
    changed_at,
    payload_sha256,
    order_id,
    payload->>'order_number' AS order_number,
    previous_status,
    status,
    previous_substatus,
    substatus,
    previous_warehouse_name,
    payload#>>'{analytics_data,warehouse_name}' AS warehouse_name,
    in_process_at,
    shipment_date,
    source_run_id,
    (previous_status IS DISTINCT FROM status) AS status_changed,
    (previous_substatus IS DISTINCT FROM substatus) AS substatus_changed,
    (previous_warehouse_name IS DISTINCT FROM payload#>>'{analytics_data,warehouse_name}') AS warehouse_changed
FROM versions;
