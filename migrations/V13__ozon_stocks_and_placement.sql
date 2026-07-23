-- ============================================================
-- V13: Ozon stocks and placement reports
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS raw.ozon_product_list_items (
    product_id     BIGINT      PRIMARY KEY,
    offer_id       TEXT,
    sku            BIGINT,
    archived       BOOLEAN,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ozon_product_list_offer
    ON raw.ozon_product_list_items (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_product_list_sku
    ON raw.ozon_product_list_items (sku);

CREATE TABLE IF NOT EXISTS raw.ozon_product_info_items (
    product_id     BIGINT      PRIMARY KEY,
    offer_id       TEXT,
    sku            BIGINT,
    name           TEXT,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ozon_product_info_offer
    ON raw.ozon_product_info_items (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_product_info_sku
    ON raw.ozon_product_info_items (sku);

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

CREATE INDEX IF NOT EXISTS idx_ozon_analytics_stocks_sku
    ON raw.ozon_analytics_stocks (sku);
CREATE INDEX IF NOT EXISTS idx_ozon_analytics_stocks_offer
    ON raw.ozon_analytics_stocks (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_analytics_stocks_snapshot
    ON raw.ozon_analytics_stocks (snapped_at DESC);

CREATE TABLE IF NOT EXISTS staging.ozon_stock_by_cluster (
    sku                       BIGINT      NOT NULL,
    offer_id                  TEXT        NOT NULL DEFAULT '',
    cluster_id                BIGINT      NOT NULL DEFAULT 0,
    cluster_name              TEXT,
    available_stock_count     INT         NOT NULL DEFAULT 0,
    requested_stock_count     INT         NOT NULL DEFAULT 0,
    transit_stock_count       INT         NOT NULL DEFAULT 0,
    return_from_customer_stock_count INT   NOT NULL DEFAULT 0,
    in_way_to_warehouse_count INT         NOT NULL DEFAULT 0,
    ads                       NUMERIC(14,4),
    ads_cluster               NUMERIC(14,4),
    idc                       NUMERIC(14,4),
    idc_cluster               NUMERIC(14,4),
    turnover_grade            TEXT,
    turnover_grade_cluster    TEXT,
    payload                   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id             TEXT        NOT NULL,
    snapped_at                TIMESTAMPTZ NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sku, offer_id, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_ozon_stock_by_cluster_offer
    ON staging.ozon_stock_by_cluster (offer_id);

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

CREATE INDEX IF NOT EXISTS idx_ozon_placement_offer
    ON staging.ozon_placement_by_products (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_placement_sku
    ON staging.ozon_placement_by_products (sku);
