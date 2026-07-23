-- ============================================================
-- V11: file-based source statistics ingestion
-- ============================================================
-- Goal: replace Excel "Статистика.xlsm" source blocks with PostgreSQL tables
-- without duplicating existing WB orders/stocks production jobs.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

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
CREATE INDEX IF NOT EXISTS idx_source_file_snapshots_table_time
    ON raw.source_file_snapshots (source_name, table_name, loaded_at DESC);

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
    PRIMARY KEY (source_system, article, fact_date, status),
    CONSTRAINT chk_source_orders_daily_system
        CHECK (source_system IN ('ozon', 'yandex_market'))
);

CREATE INDEX IF NOT EXISTS idx_source_orders_daily_date
    ON staging.source_orders_daily (source_system, fact_date DESC);
CREATE INDEX IF NOT EXISTS idx_source_orders_daily_article
    ON staging.source_orders_daily (source_system, article);

CREATE TABLE IF NOT EXISTS staging.source_stock_summary (
    source_system   TEXT        NOT NULL,
    article         TEXT        NOT NULL,
    quantity        INT         NOT NULL DEFAULT 0,
    in_way_qty      INT         NOT NULL DEFAULT 0,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    snapped_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, article),
    CONSTRAINT chk_source_stock_summary_system
        CHECK (source_system IN ('ozon', 'yandex_market'))
);

CREATE INDEX IF NOT EXISTS idx_source_stock_summary_qty
    ON staging.source_stock_summary (source_system, quantity DESC);

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
