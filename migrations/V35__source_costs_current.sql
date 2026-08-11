-- ============================================================
-- V35: current 1C source costs by warehouse
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.source_cost_by_warehouse_current (
    article         TEXT        NOT NULL,
    warehouse_name  TEXT        NOT NULL,
    row_number      INT         NOT NULL DEFAULT 0,
    code            TEXT        NOT NULL DEFAULT '',
    product_name    TEXT        NOT NULL DEFAULT '',
    tnved_code      TEXT        NOT NULL DEFAULT '',
    quantity        NUMERIC(14,3) NOT NULL DEFAULT 0,
    unit_cost_byn   NUMERIC(14,4) NOT NULL DEFAULT 0,
    total_cost_byn  NUMERIC(14,4) NOT NULL DEFAULT 0,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (article, warehouse_name)
);

CREATE INDEX IF NOT EXISTS idx_source_cost_current_warehouse
    ON staging.source_cost_by_warehouse_current (warehouse_name, article);

CREATE OR REPLACE VIEW analytics.source_cost_marketplace_for_sheets AS
WITH grouped AS (
    SELECT
        CASE
            WHEN warehouse_name = 'OZON - товар, переданный на склад МП' THEN 'ozon'
            WHEN warehouse_name = 'Wildberries- товар, переданный на склад МП' THEN 'wb'
            ELSE NULL
        END AS marketplace,
        article,
        SUM(quantity) AS quantity,
        SUM(total_cost_byn) AS total_cost_byn,
        SUM(unit_cost_byn * quantity) AS weighted_unit_cost_byn
    FROM staging.source_cost_by_warehouse_current
    WHERE warehouse_name IN (
        'OZON - товар, переданный на склад МП',
        'Wildberries- товар, переданный на склад МП'
    )
      AND quantity <> 0
    GROUP BY 1, article
)
SELECT
    marketplace,
    article,
    CASE
        WHEN quantity = 0 THEN NULL
        WHEN total_cost_byn <> 0 THEN ROUND(total_cost_byn / quantity, 2)
        ELSE ROUND(weighted_unit_cost_byn / quantity, 2)
    END::numeric(14,2) AS unit_cost_byn,
    quantity::numeric(14,3) AS quantity
FROM grouped
WHERE marketplace IS NOT NULL;
