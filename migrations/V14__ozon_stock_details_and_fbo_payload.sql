-- ============================================================
-- V14: full normalized Ozon stock details
-- ============================================================

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
    requested_stock_count      INT         NOT NULL DEFAULT 0,
    transit_stock_count        INT         NOT NULL DEFAULT 0,
    return_from_customer_stock_count INT   NOT NULL DEFAULT 0,
    in_way_to_warehouse_count  INT         NOT NULL DEFAULT 0,
    ads                        NUMERIC(14,4),
    ads_cluster                NUMERIC(14,4),
    idc                        NUMERIC(14,4),
    idc_cluster                NUMERIC(14,4),
    turnover_grade             TEXT,
    turnover_grade_cluster     TEXT,
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

CREATE TABLE IF NOT EXISTS staging.ozon_order_item_details (
    posting_number  TEXT        NOT NULL,
    line_number     INT         NOT NULL,
    order_id        BIGINT,
    status          TEXT,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    sku             BIGINT,
    offer_id        TEXT        NOT NULL DEFAULT '',
    name            TEXT,
    quantity        INT         NOT NULL DEFAULT 0,
    price           NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency_code   TEXT,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (posting_number, line_number)
);

CREATE INDEX IF NOT EXISTS idx_ozon_order_item_details_offer
    ON staging.ozon_order_item_details (offer_id);
CREATE INDEX IF NOT EXISTS idx_ozon_order_item_details_sku
    ON staging.ozon_order_item_details (sku);
