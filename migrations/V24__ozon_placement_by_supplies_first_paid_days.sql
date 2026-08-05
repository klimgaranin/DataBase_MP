-- ============================================================
-- V24: Ozon placement by-supplies report and first paid days
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.ozon_placement_by_supplies_reports (
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

CREATE TABLE IF NOT EXISTS raw.ozon_placement_by_supplies_report_files (
    code           TEXT        PRIMARY KEY,
    file_sha256    TEXT        NOT NULL,
    content        BYTEA       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ozon_placement_by_supplies_report_rows (
    report_code    TEXT        NOT NULL,
    row_number     INT         NOT NULL,
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_code, row_number)
);

CREATE INDEX IF NOT EXISTS idx_ozon_placement_by_supplies_report_rows_payload
    ON raw.ozon_placement_by_supplies_report_rows USING GIN (payload);

CREATE TABLE IF NOT EXISTS staging.ozon_placement_by_supplies_cells (
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

CREATE INDEX IF NOT EXISTS idx_ozon_placement_by_supplies_cells_column
    ON staging.ozon_placement_by_supplies_cells (column_name);

CREATE OR REPLACE VIEW analytics.ozon_placement_by_supplies_first_paid AS
WITH latest_report AS (
    SELECT code
    FROM raw.ozon_placement_by_supplies_reports
    WHERE status = 'success'
    ORDER BY date_to DESC, updated_at DESC
    LIMIT 1
),
date_column AS (
    SELECT MAX(c.column_number) AS column_number
    FROM staging.ozon_placement_by_supplies_cells c
    JOIN latest_report lr ON lr.code = c.report_code
),
pivoted AS (
    SELECT
        c.report_code,
        c.row_number,
        MAX(c.value_numeric) FILTER (WHERE c.column_name = 'SKU') AS sku,
        MAX(c.value_numeric) FILTER (WHERE c.column_name LIKE 'Остаток на складах на (%') AS stock_qty,
        MAX(c.value_text) FILTER (WHERE c.column_name = 'Номер поставки') AS supply_number,
        MAX(c.value_text) FILTER (WHERE c.column_name = 'Склад поставки') AS supply_warehouse,
        MAX(c.value_numeric) FILTER (WHERE c.column_name = 'Дней до конца периода') AS days_until_period_end,
        MAX(c.value_text) FILTER (WHERE c.column_name = 'Дата окончания периода по поставкам') AS period_end_text,
        MAX(c.value_numeric) FILTER (WHERE c.column_number = dc.column_number) AS free_qty_on_report_date
    FROM staging.ozon_placement_by_supplies_cells c
    JOIN latest_report lr ON lr.code = c.report_code
    CROSS JOIN date_column dc
    GROUP BY c.report_code, c.row_number
),
summary_rows AS (
    SELECT
        sku::bigint AS sku,
        MAX(stock_qty) AS stock_qty,
        MAX(free_qty_on_report_date) AS free_qty_on_report_date
    FROM pivoted
    WHERE sku IS NOT NULL
      AND stock_qty IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(supply_number, '')), '') IS NULL
    GROUP BY sku
),
supply_rows AS (
    SELECT
        sku::bigint AS sku,
        row_number,
        supply_number,
        supply_warehouse,
        days_until_period_end::int AS days_until_period_end,
        CASE
            WHEN period_end_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                THEN period_end_text::timestamp::date
            ELSE NULL
        END AS period_end_date,
        COALESCE(free_qty_on_report_date, 0) AS supply_free_qty
    FROM pivoted
    WHERE sku IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(supply_number, '')), '') IS NOT NULL
      AND days_until_period_end IS NOT NULL
      AND COALESCE(free_qty_on_report_date, 0) > 0
),
ordered_supply_rows AS (
    SELECT
        sr.*,
        SUM(supply_free_qty) OVER (
            PARTITION BY sku
            ORDER BY days_until_period_end, period_end_date NULLS LAST, row_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS expired_free_qty
    FROM supply_rows sr
),
first_paid AS (
    SELECT
        s.sku,
        CASE
            WHEN s.stock_qty > s.free_qty_on_report_date THEN 0
            ELSE MIN(o.days_until_period_end) FILTER (
                WHERE s.free_qty_on_report_date - o.expired_free_qty < s.stock_qty
            )
        END AS days_until_first_paid,
        MIN(o.period_end_date) FILTER (
            WHERE s.free_qty_on_report_date - o.expired_free_qty < s.stock_qty
        ) AS first_paid_period_end_date
    FROM summary_rows s
    LEFT JOIN ordered_supply_rows o ON o.sku = s.sku
    GROUP BY s.sku, s.stock_qty, s.free_qty_on_report_date
)
SELECT
    s.sku,
    s.stock_qty::int AS stock_qty,
    s.free_qty_on_report_date::int AS free_qty_on_report_date,
    f.days_until_first_paid::int AS days_until_first_paid,
    f.first_paid_period_end_date
FROM summary_rows s
LEFT JOIN first_paid f ON f.sku = s.sku;

CREATE OR REPLACE VIEW analytics.ozon_placement_latest_for_sheets AS
WITH latest_product_report AS (
    SELECT code
    FROM raw.ozon_placement_reports
    WHERE status = 'success'
    ORDER BY date_to DESC, updated_at DESC
    LIMIT 1
),
product_rows AS (
    SELECT r.payload
    FROM raw.ozon_placement_report_rows r
    JOIN latest_product_report lr
        ON lr.code = r.report_code
),
prepared_products AS (
    SELECT
        CASE
            WHEN regexp_replace(COALESCE(payload->>'SKU', ''), '\s+', '', 'g') ~ '^[0-9]+$'
                THEN regexp_replace(COALESCE(payload->>'SKU', ''), '\s+', '', 'g')::bigint
            ELSE NULL
        END AS sku,
        COALESCE(NULLIF(payload->>'Артикул', ''), NULLIF(payload->>'SKU', '')) AS article,
        regexp_replace(replace(COALESCE(payload->>'Кол-во платных экземпляров', ''), ',', '.'), '\s+', '', 'g') AS paid_qty_text,
        regexp_replace(replace(COALESCE(payload->>'Платный объем в миллилитрах', ''), ',', '.'), '\s+', '', 'g') AS paid_ml_text,
        regexp_replace(replace(COALESCE(payload->>'Начисленная стоимость размещения', ''), ',', '.'), '\s+', '', 'g') AS cost_text
    FROM product_rows
),
product_totals AS (
    SELECT
        sku,
        article,
        SUM(CASE WHEN paid_qty_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN paid_qty_text::numeric ELSE 0 END)::int AS paid_qty,
        (SUM(CASE WHEN paid_ml_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN paid_ml_text::numeric ELSE 0 END) / 1000)::numeric(14,3) AS paid_liters,
        SUM(CASE WHEN cost_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN cost_text::numeric ELSE 0 END)::numeric(14,2) AS daily_writeoff_rub
    FROM prepared_products
    WHERE article IS NOT NULL
    GROUP BY sku, article
)
SELECT
    p.article,
    p.paid_qty,
    p.paid_liters,
    p.daily_writeoff_rub,
    f.days_until_first_paid
FROM product_totals p
LEFT JOIN analytics.ozon_placement_by_supplies_first_paid f
    ON f.sku = p.sku;
