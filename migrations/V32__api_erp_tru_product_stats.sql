-- ERP/TRU product statistics: daily raw snapshot, current technical layer and Sheets aggregate.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.api_erp_tru_product_stat_rows (
    id                         BIGSERIAL PRIMARY KEY,
    source_run_id              TEXT        NOT NULL,
    period_from                DATE        NOT NULL,
    period_to                  DATE        NOT NULL,
    external_id                BIGINT,
    article                    TEXT        NOT NULL,
    series_name                TEXT,
    brand_name                 TEXT,
    name_1s                    TEXT,
    barcode                    TEXT,
    remains_warehouse_count    INT         NOT NULL DEFAULT 0,
    warehouse_count            INT         NOT NULL DEFAULT 0,
    presence_count             INT         NOT NULL DEFAULT 0,
    for_marketplaces_count     INT         NOT NULL DEFAULT 0,
    reserved_total_count       INT         NOT NULL DEFAULT 0,
    reserved_invoice_count     INT         NOT NULL DEFAULT 0,
    reserved_cash_count        INT         NOT NULL DEFAULT 0,
    avg_price                  NUMERIC(14,2),
    sales_count                INT         NOT NULL DEFAULT 0,
    sales_sum                  NUMERIC(14,2) NOT NULL DEFAULT 0,
    payload                    JSONB       NOT NULL,
    loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_erp_tru_product_stat_rows_run
    ON raw.api_erp_tru_product_stat_rows (source_run_id);
CREATE INDEX IF NOT EXISTS idx_api_erp_tru_product_stat_rows_article
    ON raw.api_erp_tru_product_stat_rows (article);

CREATE TABLE IF NOT EXISTS staging.api_erp_tru_product_stats_current (
    external_id                BIGINT,
    article                    TEXT        NOT NULL,
    series_name                TEXT,
    brand_name                 TEXT,
    name_1s                    TEXT,
    barcode                    TEXT,
    remains_warehouse_count    INT         NOT NULL DEFAULT 0,
    warehouse_count            INT         NOT NULL DEFAULT 0,
    presence_count             INT         NOT NULL DEFAULT 0,
    for_marketplaces_count     INT         NOT NULL DEFAULT 0,
    reserved_total_count       INT         NOT NULL DEFAULT 0,
    reserved_invoice_count     INT         NOT NULL DEFAULT 0,
    reserved_cash_count        INT         NOT NULL DEFAULT 0,
    avg_price                  NUMERIC(14,2),
    sales_count                INT         NOT NULL DEFAULT 0,
    sales_sum                  NUMERIC(14,2) NOT NULL DEFAULT 0,
    payload                    JSONB       NOT NULL,
    period_from                DATE        NOT NULL,
    period_to                  DATE        NOT NULL,
    source_run_id              TEXT        NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_erp_tru_product_stats_current_article
    ON staging.api_erp_tru_product_stats_current (article);

CREATE OR REPLACE VIEW analytics.api_erp_tru_sales_for_sheets AS
SELECT
    article,
    SUM(sales_count)::int AS sales_count
FROM staging.api_erp_tru_product_stats_current
GROUP BY article
HAVING SUM(sales_count) <> 0
ORDER BY article;
