-- ============================================================
-- V18: плоская view последних детальных остатков Ozon
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.ozon_stock_details_latest_flat AS
SELECT
    source_run_id,
    sku,
    offer_id,
    name,
    cluster_id,
    cluster_name,
    macrolocal_cluster_id,
    warehouse_id,
    warehouse_name,
    placement_zone,
    available_stock_count,
    valid_stock_count,
    other_stock_count,
    requested_stock_count,
    transit_stock_count,
    return_from_customer_stock_count,
    return_to_seller_stock_count,
    stock_defect_stock_count,
    transit_defect_stock_count,
    expiring_stock_count,
    waiting_docs_stock_count,
    waiting_docs_to_export_stock_count,
    excess_stock_count,
    in_way_to_warehouse_count,
    ads,
    ads_cluster,
    idc,
    idc_cluster,
    days_without_sales,
    days_without_sales_cluster,
    turnover_grade,
    turnover_grade_cluster,
    item_tags_count,
    snapped_at,
    loaded_at
FROM staging.ozon_stock_details
WHERE source_run_id = (
    SELECT source_run_id
    FROM staging.ozon_stock_details
    ORDER BY snapped_at DESC, loaded_at DESC
    LIMIT 1
);
