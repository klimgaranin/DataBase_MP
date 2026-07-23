-- ============================================================
-- V12: Ozon API orders ingestion
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS raw.ozon_fbo_postings (
    posting_number  TEXT PRIMARY KEY,
    order_id        BIGINT,
    status          TEXT,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    payload         JSONB       NOT NULL,
    source_run_id   TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ozon_fbo_postings_status
    ON raw.ozon_fbo_postings (status);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_postings_in_process
    ON raw.ozon_fbo_postings (in_process_at DESC);

CREATE TABLE IF NOT EXISTS staging.ozon_order_items (
    posting_number  TEXT        NOT NULL,
    order_id        BIGINT,
    status          TEXT,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    sku             BIGINT,
    offer_id        TEXT,
    name            TEXT,
    quantity        INT         NOT NULL DEFAULT 0,
    price           NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency_code   TEXT,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (posting_number, sku, offer_id)
);

CREATE INDEX IF NOT EXISTS idx_ozon_order_items_offer_date
    ON staging.ozon_order_items (offer_id, in_process_at DESC);
CREATE INDEX IF NOT EXISTS idx_ozon_order_items_sku
    ON staging.ozon_order_items (sku);
