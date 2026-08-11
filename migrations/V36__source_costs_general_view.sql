-- ============================================================
-- V36: add general 1C source cost block for Sheets
-- ============================================================

CREATE OR REPLACE VIEW analytics.source_cost_marketplace_for_sheets AS
WITH grouped AS (
    SELECT
        CASE
            WHEN warehouse_name = 'OZON - товар, переданный на склад МП' THEN 'ozon'
            WHEN warehouse_name = 'Wildberries- товар, переданный на склад МП' THEN 'wb'
            WHEN warehouse_name IN (
                'основной',
                'ДЛЯ МАРКЕТПЛЕЙСОВ',
                'Ответственное хранение Великий камень'
            ) THEN 'general'
            ELSE NULL
        END AS marketplace,
        article,
        SUM(quantity) AS quantity,
        SUM(total_cost_byn) AS total_cost_byn,
        SUM(unit_cost_byn * quantity) AS weighted_unit_cost_byn
    FROM staging.source_cost_by_warehouse_current
    WHERE warehouse_name IN (
        'OZON - товар, переданный на склад МП',
        'Wildberries- товар, переданный на склад МП',
        'основной',
        'ДЛЯ МАРКЕТПЛЕЙСОВ',
        'Ответственное хранение Великий камень'
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
