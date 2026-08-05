-- ============================================================
-- V22: Ozon placement raw file storage and business views
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.ozon_placement_report_files (
    code           TEXT        PRIMARY KEY,
    file_sha256    TEXT        NOT NULL,
    content        BYTEA       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ozon_placement_report_rows (
    report_code    TEXT        NOT NULL,
    row_number     INT         NOT NULL,
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_code, row_number)
);

CREATE INDEX IF NOT EXISTS idx_ozon_placement_report_rows_payload
    ON raw.ozon_placement_report_rows USING GIN (payload);

CREATE OR REPLACE VIEW analytics.ozon_placement_by_products_flat AS
SELECT
    p.report_code,
    r.date_from,
    r.date_to,
    p.row_number,
    p.sku,
    p.offer_id,
    p.product_name,
    p.placement_cost,
    p.source_run_id,
    p.loaded_at
FROM staging.ozon_placement_by_products p
LEFT JOIN raw.ozon_placement_reports r
    ON r.code = p.report_code;

CREATE OR REPLACE VIEW analytics.ozon_placement_latest_for_sheets AS
WITH latest_report AS (
    SELECT code
    FROM raw.ozon_placement_reports
    WHERE status = 'success'
    ORDER BY date_to DESC, updated_at DESC
    LIMIT 1
)
SELECT
    COALESCE(NULLIF(p.offer_id, ''), p.sku::text, '') AS article,
    COALESCE(MAX(q.value_numeric), 0)::int AS paid_qty,
    COALESCE(MAX(l.value_numeric), 0)::numeric(14,3) AS paid_liters,
    COALESCE(MAX(w.value_numeric), MAX(p.placement_cost), 0)::numeric(14,2) AS daily_writeoff_rub,
    MAX(d.value_numeric)::int AS days_until_first_paid
FROM staging.ozon_placement_by_products p
JOIN latest_report lr
    ON lr.code = p.report_code
LEFT JOIN staging.ozon_placement_cells q
    ON q.report_code = p.report_code
   AND q.row_number = p.row_number
   AND lower(q.column_name) IN ('платно, шт', 'платно шт', 'платное хранение, шт', 'количество платного хранения')
LEFT JOIN staging.ozon_placement_cells l
    ON l.report_code = p.report_code
   AND l.row_number = p.row_number
   AND lower(l.column_name) IN ('платно, л', 'платно л', 'платное хранение, л', 'объем платного хранения')
LEFT JOIN staging.ozon_placement_cells w
    ON w.report_code = p.report_code
   AND w.row_number = p.row_number
   AND lower(w.column_name) IN ('списано в день, rub', 'списано в день', 'списание в день', 'daily writeoff')
LEFT JOIN staging.ozon_placement_cells d
    ON d.report_code = p.report_code
   AND d.row_number = p.row_number
   AND lower(d.column_name) IN ('дней до первой платности', 'дни до первой платности', 'days until first paid')
WHERE COALESCE(NULLIF(p.offer_id, ''), p.sku::text, '') <> ''
GROUP BY COALESCE(NULLIF(p.offer_id, ''), p.sku::text, '');
