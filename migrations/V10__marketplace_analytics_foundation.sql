-- ============================================================
-- V10: foundation for marketplace analytics platform
-- ============================================================
-- Goal: move Google Sheet "Аналитика МП" logic into PostgreSQL
-- without touching existing public.* production ETL tables.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Unified raw API evidence. One row is one HTTP response or one logical page.
CREATE TABLE IF NOT EXISTS raw.api_responses (
    id                BIGSERIAL PRIMARY KEY,
    run_id            TEXT        NOT NULL,
    marketplace       TEXT        NOT NULL,
    method_name       TEXT        NOT NULL,
    http_method       TEXT        NOT NULL,
    url               TEXT        NOT NULL,
    request_payload   JSONB,
    response_status   INT,
    response_payload  JSONB,
    response_sha256   TEXT,
    duration_ms       INT,
    attempt           INT         NOT NULL DEFAULT 1,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error             TEXT,
    CONSTRAINT chk_raw_api_marketplace
        CHECK (marketplace IN ('wb', 'ozon', 'google_sheets', 'system'))
);

CREATE INDEX IF NOT EXISTS idx_raw_api_responses_run
    ON raw.api_responses (run_id);
CREATE INDEX IF NOT EXISTS idx_raw_api_responses_method_time
    ON raw.api_responses (marketplace, method_name, fetched_at DESC);

-- Job/scenario status for larger orchestrated workflows, not tiny endpoint jobs.
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    pipeline     TEXT        NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    status       TEXT        NOT NULL DEFAULT 'running',
    message      TEXT,
    metrics      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_pipeline_status CHECK (status IN ('running', 'ok', 'fail', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_time
    ON raw.pipeline_runs (pipeline, started_at DESC);

-- Product identity shared by WB/Ozon analytical screens.
CREATE TABLE IF NOT EXISTS core.products (
    article       TEXT PRIMARY KEY,
    product_name  TEXT,
    series        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Marketplace-specific product identity and payload.
CREATE TABLE IF NOT EXISTS core.marketplace_products (
    marketplace       TEXT        NOT NULL,
    article           TEXT        NOT NULL,
    external_id       TEXT,
    nm_id             BIGINT,
    sku               TEXT,
    product_name      TEXT,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_updated_at TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, article),
    CONSTRAINT chk_marketplace_products_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

CREATE INDEX IF NOT EXISTS idx_marketplace_products_external_id
    ON core.marketplace_products (marketplace, external_id);
CREATE INDEX IF NOT EXISTS idx_marketplace_products_nm_id
    ON core.marketplace_products (marketplace, nm_id);
CREATE INDEX IF NOT EXISTS idx_marketplace_products_sku
    ON core.marketplace_products (marketplace, sku);

-- Current stock by location, mirrors "Остатки ЧЗ" detailed WB/Ozon blocks.
CREATE TABLE IF NOT EXISTS staging.marketplace_stock_locations (
    marketplace          TEXT        NOT NULL,
    article              TEXT        NOT NULL,
    location_id          TEXT        NOT NULL,
    location_name        TEXT        NOT NULL,
    quantity             INT         NOT NULL DEFAULT 0,
    delivered_chz_qty    INT         NOT NULL DEFAULT 0,
    non_chz_qty          INT         NOT NULL DEFAULT 0,
    snapped_at           TIMESTAMPTZ NOT NULL,
    payload              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (marketplace, article, location_id),
    CONSTRAINT chk_stock_locations_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

CREATE INDEX IF NOT EXISTS idx_stock_locations_article
    ON staging.marketplace_stock_locations (marketplace, article);
CREATE INDEX IF NOT EXISTS idx_stock_locations_qty
    ON staging.marketplace_stock_locations (marketplace, quantity DESC);

-- Production/internal stock statuses from the "ИНФО" and source DATA blocks.
CREATE TABLE IF NOT EXISTS core.production_inventory_snapshot (
    article          TEXT        NOT NULL,
    smp_qty          INT         NOT NULL DEFAULT 0,
    osn_qty          INT         NOT NULL DEFAULT 0,
    soh_qty          INT         NOT NULL DEFAULT 0,
    svh_qty          INT         NOT NULL DEFAULT 0,
    ts_qty           INT         NOT NULL DEFAULT 0,
    minsk_plan_qty   INT         NOT NULL DEFAULT 0,
    approved_order_qty INT      NOT NULL DEFAULT 0,
    in_production_qty INT       NOT NULL DEFAULT 0,
    ready_qty        INT         NOT NULL DEFAULT 0,
    in_way_qty       INT         NOT NULL DEFAULT 0,
    snapped_at       TIMESTAMPTZ NOT NULL,
    payload          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (article, snapped_at)
);

CREATE INDEX IF NOT EXISTS idx_production_inventory_article_time
    ON core.production_inventory_snapshot (article, snapped_at DESC);

-- Prices and unit-economy fields used by "Аналитика WB" and "Аналитика OZON".
CREATE TABLE IF NOT EXISTS core.marketplace_unit_economy (
    marketplace          TEXT        NOT NULL,
    article              TEXT        NOT NULL,
    current_price        NUMERIC(14,2),
    margin_pct           NUMERIC(8,4),
    margin_no_ads_pct    NUMERIC(8,4),
    payload              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_updated_at    TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, article),
    CONSTRAINT chk_unit_economy_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

-- Article-level current summary, mirrors WB M:O and Ozon Q:S blocks.
CREATE TABLE IF NOT EXISTS staging.marketplace_stock_summary (
    marketplace  TEXT        NOT NULL,
    article      TEXT        NOT NULL,
    quantity     INT         NOT NULL DEFAULT 0,
    in_way_qty   INT         NOT NULL DEFAULT 0,
    snapped_at   TIMESTAMPTZ NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (marketplace, article),
    CONSTRAINT chk_stock_summary_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

-- Daily product facts, mirrors date columns in "Аналитика WB" and "Аналитика OZON".
CREATE TABLE IF NOT EXISTS analytics.marketplace_product_daily (
    marketplace       TEXT        NOT NULL,
    article           TEXT        NOT NULL,
    fact_date         DATE        NOT NULL,
    orders_qty        INT         NOT NULL DEFAULT 0,
    sales_qty         INT         NOT NULL DEFAULT 0,
    revenue           NUMERIC(14,2) NOT NULL DEFAULT 0,
    cancelled_qty     INT         NOT NULL DEFAULT 0,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    calculated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, article, fact_date),
    CONSTRAINT chk_product_daily_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

CREATE INDEX IF NOT EXISTS idx_product_daily_date
    ON analytics.marketplace_product_daily (marketplace, fact_date DESC);
CREATE INDEX IF NOT EXISTS idx_product_daily_article
    ON analytics.marketplace_product_daily (marketplace, article);

-- Advertising daily spend/order aggregates used by DRR columns.
CREATE TABLE IF NOT EXISTS staging.advertising_daily (
    marketplace       TEXT        NOT NULL,
    advert_id         TEXT        NOT NULL,
    article           TEXT        NOT NULL,
    fact_date         DATE        NOT NULL,
    campaign_name     TEXT,
    campaign_type     TEXT,
    spend             NUMERIC(14,2) NOT NULL DEFAULT 0,
    orders_qty        INT         NOT NULL DEFAULT 0,
    revenue           NUMERIC(14,2) NOT NULL DEFAULT 0,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, advert_id, article, fact_date),
    CONSTRAINT chk_advertising_daily_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

CREATE INDEX IF NOT EXISTS idx_advertising_daily_article_date
    ON staging.advertising_daily (marketplace, article, fact_date DESC);

-- First web-facing current analytics surface.
CREATE TABLE IF NOT EXISTS analytics.marketplace_current (
    marketplace          TEXT        NOT NULL,
    article              TEXT        NOT NULL,
    product_name         TEXT,
    series               TEXT,
    current_price        NUMERIC(14,2),
    margin_pct           NUMERIC(8,4),
    competitor_price     NUMERIC(14,2),
    competitor_margin_pct NUMERIC(8,4),
    margin_diff_pct      NUMERIC(8,4),
    available_stock      INT         NOT NULL DEFAULT 0,
    non_chz_stock        INT         NOT NULL DEFAULT 0,
    in_way_qty           INT         NOT NULL DEFAULT 0,
    ads_prev             NUMERIC(14,4),
    ads_current          NUMERIC(14,4),
    ads_28d              NUMERIC(14,4),
    turnover_30d         NUMERIC(14,4),
    status_light         TEXT,
    source_snapshot_at   TIMESTAMPTZ,
    calculated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (marketplace, article),
    CONSTRAINT chk_marketplace_current_marketplace CHECK (marketplace IN ('wb', 'ozon')),
    CONSTRAINT chk_marketplace_current_light CHECK (
        status_light IS NULL OR status_light IN ('green', 'yellow', 'red', 'gray')
    )
);

CREATE INDEX IF NOT EXISTS idx_marketplace_current_light
    ON analytics.marketplace_current (marketplace, status_light);
CREATE INDEX IF NOT EXISTS idx_marketplace_current_stock
    ON analytics.marketplace_current (marketplace, available_stock DESC);
