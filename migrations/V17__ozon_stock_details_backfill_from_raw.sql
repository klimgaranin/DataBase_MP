-- ============================================================
-- V17: восстановить детальные рабочие остатки Ozon из raw API
-- ============================================================

INSERT INTO staging.ozon_stock_details
    (source_run_id, row_hash, sku, offer_id, cluster_id, cluster_name,
     available_stock_count, valid_stock_count, other_stock_count,
     requested_stock_count, transit_stock_count,
     return_from_customer_stock_count, return_to_seller_stock_count,
     stock_defect_stock_count, transit_defect_stock_count,
     expiring_stock_count, waiting_docs_stock_count,
     waiting_docs_to_export_stock_count, excess_stock_count,
     in_way_to_warehouse_count,
     ads, ads_cluster, idc, idc_cluster,
     days_without_sales, days_without_sales_cluster,
     turnover_grade, turnover_grade_cluster,
     macrolocal_cluster_id, warehouse_id, warehouse_name,
     placement_zone, name, item_tags_count,
     payload, snapped_at)
SELECT
    source_run_id,
    md5(id::text || '|' || payload::text),
    sku,
    COALESCE(offer_id, ''),
    COALESCE(cluster_id, 0),
    payload->>'cluster_name',
    COALESCE((payload->>'available_stock_count')::int, 0),
    COALESCE((payload->>'valid_stock_count')::int, 0),
    COALESCE((payload->>'other_stock_count')::int, 0),
    COALESCE((payload->>'requested_stock_count')::int, 0),
    COALESCE((payload->>'transit_stock_count')::int, 0),
    COALESCE((payload->>'return_from_customer_stock_count')::int, 0),
    COALESCE((payload->>'return_to_seller_stock_count')::int, 0),
    COALESCE((payload->>'stock_defect_stock_count')::int, 0),
    COALESCE((payload->>'transit_defect_stock_count')::int, 0),
    COALESCE((payload->>'expiring_stock_count')::int, 0),
    COALESCE((payload->>'waiting_docs_stock_count')::int, 0),
    COALESCE((payload->>'waiting_docs_to_export_stock_count')::int, 0),
    COALESCE((payload->>'excess_stock_count')::int, 0),
    COALESCE((payload->>'requested_stock_count')::int, 0)
        + COALESCE((payload->>'transit_stock_count')::int, 0)
        + COALESCE((payload->>'return_from_customer_stock_count')::int, 0),
    CASE WHEN replace(COALESCE(payload->>'ads', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'ads', ',', '.')::numeric ELSE NULL END,
    CASE WHEN replace(COALESCE(payload->>'ads_cluster', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'ads_cluster', ',', '.')::numeric ELSE NULL END,
    CASE WHEN replace(COALESCE(payload->>'idc', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'idc', ',', '.')::numeric ELSE NULL END,
    CASE WHEN replace(COALESCE(payload->>'idc_cluster', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'idc_cluster', ',', '.')::numeric ELSE NULL END,
    NULLIF(payload->>'days_without_sales', '')::int,
    NULLIF(payload->>'days_without_sales_cluster', '')::int,
    payload->>'turnover_grade',
    payload->>'turnover_grade_cluster',
    NULLIF(payload->>'macrolocal_cluster_id', '')::bigint,
    NULLIF(payload->>'warehouse_id', '')::bigint,
    payload->>'warehouse_name',
    payload->>'placement_zone',
    payload->>'name',
    CASE WHEN jsonb_typeof(payload->'item_tags') = 'array' THEN jsonb_array_length(payload->'item_tags') ELSE 0 END,
    payload,
    snapped_at
FROM raw.ozon_analytics_stocks
WHERE sku IS NOT NULL
ON CONFLICT (source_run_id, row_hash) DO NOTHING;

UPDATE staging.ozon_stock_by_cluster s
SET
    valid_stock_count = COALESCE(m.valid_stock_count, s.valid_stock_count),
    other_stock_count = COALESCE(m.other_stock_count, s.other_stock_count),
    return_to_seller_stock_count = COALESCE(m.return_to_seller_stock_count, s.return_to_seller_stock_count),
    stock_defect_stock_count = COALESCE(m.stock_defect_stock_count, s.stock_defect_stock_count),
    transit_defect_stock_count = COALESCE(m.transit_defect_stock_count, s.transit_defect_stock_count),
    expiring_stock_count = COALESCE(m.expiring_stock_count, s.expiring_stock_count),
    waiting_docs_stock_count = COALESCE(m.waiting_docs_stock_count, s.waiting_docs_stock_count),
    waiting_docs_to_export_stock_count = COALESCE(m.waiting_docs_to_export_stock_count, s.waiting_docs_to_export_stock_count),
    excess_stock_count = COALESCE(m.excess_stock_count, s.excess_stock_count),
    days_without_sales = COALESCE(m.days_without_sales, s.days_without_sales),
    days_without_sales_cluster = COALESCE(m.days_without_sales_cluster, s.days_without_sales_cluster),
    macrolocal_cluster_id = COALESCE(m.macrolocal_cluster_id, s.macrolocal_cluster_id),
    warehouse_id = COALESCE(m.warehouse_id, s.warehouse_id),
    warehouse_name = COALESCE(m.warehouse_name, s.warehouse_name),
    placement_zone = COALESCE(m.placement_zone, s.placement_zone),
    name = COALESCE(m.name, s.name),
    item_tags_count = COALESCE(m.item_tags_count, s.item_tags_count)
FROM (
    SELECT
        source_run_id,
        sku,
        COALESCE(offer_id, '') AS offer_id,
        COALESCE(cluster_id, 0) AS cluster_id,
        SUM(valid_stock_count) AS valid_stock_count,
        SUM(other_stock_count) AS other_stock_count,
        SUM(return_to_seller_stock_count) AS return_to_seller_stock_count,
        SUM(stock_defect_stock_count) AS stock_defect_stock_count,
        SUM(transit_defect_stock_count) AS transit_defect_stock_count,
        SUM(expiring_stock_count) AS expiring_stock_count,
        SUM(waiting_docs_stock_count) AS waiting_docs_stock_count,
        SUM(waiting_docs_to_export_stock_count) AS waiting_docs_to_export_stock_count,
        SUM(excess_stock_count) AS excess_stock_count,
        MAX(days_without_sales) AS days_without_sales,
        MAX(days_without_sales_cluster) AS days_without_sales_cluster,
        MAX(macrolocal_cluster_id) AS macrolocal_cluster_id,
        MAX(warehouse_id) AS warehouse_id,
        MAX(warehouse_name) AS warehouse_name,
        MAX(placement_zone) AS placement_zone,
        MAX(name) AS name,
        MAX(item_tags_count) AS item_tags_count
    FROM staging.ozon_stock_details
    GROUP BY source_run_id, sku, COALESCE(offer_id, ''), COALESCE(cluster_id, 0)
) m
WHERE s.source_run_id = m.source_run_id
  AND s.sku = m.sku
  AND s.offer_id = m.offer_id
  AND s.cluster_id = m.cluster_id;
