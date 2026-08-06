-- ============================================================
-- V28: Ozon placement first paid days from by-supplies report only
-- ============================================================

CREATE OR REPLACE VIEW analytics.ozon_placement_by_supplies_first_paid AS
WITH latest_product_report AS (
    SELECT p.code, p.date_from, p.date_to
    FROM raw.ozon_placement_reports p
    WHERE p.status = 'success'
      AND EXISTS (
          SELECT 1
          FROM raw.ozon_placement_report_rows r
          WHERE r.report_code = p.code
      )
    ORDER BY p.date_to DESC, p.updated_at DESC
    LIMIT 1
),
latest_report AS (
    SELECT s.code
    FROM raw.ozon_placement_by_supplies_reports s
    JOIN latest_product_report p
      ON p.date_from = s.date_from
     AND p.date_to = s.date_to
    WHERE s.status = 'success'
    ORDER BY s.updated_at DESC
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
        MAX(c.value_numeric) FILTER (WHERE c.column_number = 1) AS sku,
        MAX(c.value_text) FILTER (WHERE c.column_number = 6) AS supply_number,
        MAX(c.value_numeric) FILTER (WHERE c.column_number = 8) AS days_until_first_paid,
        MAX(c.value_text) FILTER (WHERE c.column_number = 9) AS first_paid_period_end_text,
        MAX(c.value_numeric) FILTER (WHERE c.column_number = dc.column_number) AS free_qty_on_report_date
    FROM staging.ozon_placement_by_supplies_cells c
    JOIN latest_report lr ON lr.code = c.report_code
    CROSS JOIN date_column dc
    GROUP BY c.report_code, c.row_number
),
summary_rows AS (
    SELECT
        sku::bigint AS sku,
        MAX(days_until_first_paid)::int AS days_until_first_paid,
        MAX(first_paid_period_end_text) AS first_paid_period_end_text,
        MAX(free_qty_on_report_date)::int AS free_qty_on_report_date
    FROM pivoted
    WHERE sku IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(supply_number, '')), '') IS NULL
      AND days_until_first_paid IS NOT NULL
    GROUP BY sku
)
SELECT
    sku,
    NULL::int AS stock_qty,
    free_qty_on_report_date,
    days_until_first_paid,
    CASE
        WHEN first_paid_period_end_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN first_paid_period_end_text::timestamp::date
        ELSE NULL
    END AS first_paid_period_end_date
FROM summary_rows;
