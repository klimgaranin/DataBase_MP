-- ============================================================
-- V27: Ozon placement first paid days use current stock layer
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
        MAX(c.value_text) FILTER (WHERE c.column_number = 7) AS supply_warehouse,
        MAX(c.value_numeric) FILTER (WHERE c.column_number = 8) AS days_until_period_end,
        MAX(c.value_text) FILTER (WHERE c.column_number = 9) AS period_end_text,
        MAX(c.value_numeric) FILTER (WHERE c.column_number = dc.column_number) AS free_qty_on_report_date
    FROM staging.ozon_placement_by_supplies_cells c
    JOIN latest_report lr ON lr.code = c.report_code
    CROSS JOIN date_column dc
    GROUP BY c.report_code, c.row_number
),
current_stock AS (
    SELECT
        sku,
        SUM(available_stock_count)::numeric AS stock_qty
    FROM staging.ozon_stock_by_cluster
    GROUP BY sku
),
summary_rows AS (
    SELECT
        p.sku::bigint AS sku,
        COALESCE(NULLIF(cs.stock_qty, 0), 0) AS stock_qty,
        MAX(p.free_qty_on_report_date) AS free_qty_on_report_date
    FROM pivoted p
    LEFT JOIN current_stock cs ON cs.sku = p.sku::bigint
    WHERE p.sku IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(p.supply_number, '')), '') IS NULL
      AND p.free_qty_on_report_date IS NOT NULL
    GROUP BY p.sku, cs.stock_qty
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
            WHEN s.stock_qty <= 0 THEN NULL
            WHEN s.stock_qty > s.free_qty_on_report_date THEN 0
            ELSE MIN(o.days_until_period_end) FILTER (
                WHERE s.free_qty_on_report_date - o.expired_free_qty < s.stock_qty
            )
        END AS days_until_first_paid,
        MIN(o.period_end_date) FILTER (
            WHERE s.stock_qty > 0
              AND s.free_qty_on_report_date - o.expired_free_qty < s.stock_qty
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
