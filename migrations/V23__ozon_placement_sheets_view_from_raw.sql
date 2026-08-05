-- ============================================================
-- V23: Ozon placement Sheets view from raw report rows
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.ozon_placement_latest_for_sheets AS
WITH latest_report AS (
    SELECT code
    FROM raw.ozon_placement_reports
    WHERE status = 'success'
    ORDER BY date_to DESC, updated_at DESC
    LIMIT 1
),
raw_rows AS (
    SELECT r.payload
    FROM raw.ozon_placement_report_rows r
    JOIN latest_report lr
        ON lr.code = r.report_code
),
prepared AS (
    SELECT
        COALESCE(NULLIF(payload->>'Артикул', ''), NULLIF(payload->>'SKU', '')) AS article,
        regexp_replace(replace(COALESCE(payload->>'Кол-во платных экземпляров', ''), ',', '.'), '\s+', '', 'g') AS paid_qty_text,
        regexp_replace(replace(COALESCE(payload->>'Платный объем в миллилитрах', ''), ',', '.'), '\s+', '', 'g') AS paid_ml_text,
        regexp_replace(replace(COALESCE(payload->>'Начисленная стоимость размещения', ''), ',', '.'), '\s+', '', 'g') AS cost_text
    FROM raw_rows
)
SELECT
    article,
    SUM(CASE WHEN paid_qty_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN paid_qty_text::numeric ELSE 0 END)::int AS paid_qty,
    (SUM(CASE WHEN paid_ml_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN paid_ml_text::numeric ELSE 0 END) / 1000)::numeric(14,3) AS paid_liters,
    SUM(CASE WHEN cost_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN cost_text::numeric ELSE 0 END)::numeric(14,2) AS daily_writeoff_rub,
    NULL::int AS days_until_first_paid
FROM prepared
WHERE article IS NOT NULL
GROUP BY article;
