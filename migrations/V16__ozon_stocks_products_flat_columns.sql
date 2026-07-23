-- ============================================================
-- V16: раскрыть Ozon stocks/product endpoint-ы в рабочие колонки
-- ============================================================

ALTER TABLE raw.ozon_product_list_items
    ADD COLUMN IF NOT EXISTS has_fbo_stocks BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_fbs_stocks BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_discounted BOOLEAN,
    ADD COLUMN IF NOT EXISTS quants_count INT NOT NULL DEFAULT 0;

UPDATE raw.ozon_product_list_items
SET
    has_fbo_stocks = COALESCE((payload->>'has_fbo_stocks')::boolean, has_fbo_stocks),
    has_fbs_stocks = COALESCE((payload->>'has_fbs_stocks')::boolean, has_fbs_stocks),
    is_discounted = COALESCE((payload->>'is_discounted')::boolean, is_discounted),
    quants_count = CASE
        WHEN jsonb_typeof(payload->'quants') = 'array' THEN jsonb_array_length(payload->'quants')
        ELSE 0
    END;

ALTER TABLE raw.ozon_product_info_items
    ADD COLUMN IF NOT EXISTS currency_code TEXT,
    ADD COLUMN IF NOT EXISTS price NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS old_price NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS min_price NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS vat NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS volume_weight NUMERIC(14,4),
    ADD COLUMN IF NOT EXISTS description_category_id BIGINT,
    ADD COLUMN IF NOT EXISTS type_id BIGINT,
    ADD COLUMN IF NOT EXISTS primary_image TEXT,
    ADD COLUMN IF NOT EXISTS images_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS barcodes_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS commissions_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_archived BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_autoarchived BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_discounted BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_discounted_fbo_item BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_kgt BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_prepayment_allowed BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_seasonal BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_super BOOLEAN,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS status_name TEXT,
    ADD COLUMN IF NOT EXISTS status_description TEXT,
    ADD COLUMN IF NOT EXISTS moderate_status TEXT,
    ADD COLUMN IF NOT EXISTS validation_status TEXT,
    ADD COLUMN IF NOT EXISTS status_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS has_price BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_stock BOOLEAN,
    ADD COLUMN IF NOT EXISTS model_id BIGINT,
    ADD COLUMN IF NOT EXISTS model_count INT,
    ADD COLUMN IF NOT EXISTS source_created_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ;

UPDATE raw.ozon_product_info_items
SET
    currency_code = COALESCE(NULLIF(payload->>'currency_code', ''), currency_code),
    price = CASE WHEN replace(COALESCE(payload->>'price', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'price', ',', '.')::numeric ELSE price END,
    old_price = CASE WHEN replace(COALESCE(payload->>'old_price', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'old_price', ',', '.')::numeric ELSE old_price END,
    min_price = CASE WHEN replace(COALESCE(payload->>'min_price', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'min_price', ',', '.')::numeric ELSE min_price END,
    vat = CASE WHEN replace(COALESCE(payload->>'vat', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'vat', ',', '.')::numeric ELSE vat END,
    volume_weight = CASE WHEN replace(COALESCE(payload->>'volume_weight', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN replace(payload->>'volume_weight', ',', '.')::numeric ELSE volume_weight END,
    description_category_id = CASE WHEN COALESCE(payload->>'description_category_id', '') ~ '^[0-9]+$'
        THEN (payload->>'description_category_id')::bigint ELSE description_category_id END,
    type_id = CASE WHEN COALESCE(payload->>'type_id', '') ~ '^[0-9]+$'
        THEN (payload->>'type_id')::bigint ELSE type_id END,
    primary_image = COALESCE(payload#>>'{primary_image,0}', primary_image),
    images_count = CASE WHEN jsonb_typeof(payload->'images') = 'array' THEN jsonb_array_length(payload->'images') ELSE 0 END,
    barcodes_count = CASE WHEN jsonb_typeof(payload->'barcodes') = 'array' THEN jsonb_array_length(payload->'barcodes') ELSE 0 END,
    commissions_count = CASE WHEN jsonb_typeof(payload->'commissions') = 'array' THEN jsonb_array_length(payload->'commissions') ELSE 0 END,
    is_archived = COALESCE((payload->>'is_archived')::boolean, is_archived),
    is_autoarchived = COALESCE((payload->>'is_autoarchived')::boolean, is_autoarchived),
    is_discounted = COALESCE((payload->>'is_discounted')::boolean, is_discounted),
    has_discounted_fbo_item = COALESCE((payload->>'has_discounted_fbo_item')::boolean, has_discounted_fbo_item),
    is_kgt = COALESCE((payload->>'is_kgt')::boolean, is_kgt),
    is_prepayment_allowed = COALESCE((payload->>'is_prepayment_allowed')::boolean, is_prepayment_allowed),
    is_seasonal = COALESCE((payload->>'is_seasonal')::boolean, is_seasonal),
    is_super = COALESCE((payload->>'is_super')::boolean, is_super),
    status = COALESCE(payload#>>'{statuses,status}', status),
    status_name = COALESCE(payload#>>'{statuses,status_name}', status_name),
    status_description = COALESCE(payload#>>'{statuses,status_description}', status_description),
    moderate_status = COALESCE(payload#>>'{statuses,moderate_status}', moderate_status),
    validation_status = COALESCE(payload#>>'{statuses,validation_status}', validation_status),
    status_updated_at = COALESCE(NULLIF(payload#>>'{statuses,status_updated_at}', '')::timestamptz, status_updated_at),
    has_price = COALESCE((payload#>>'{visibility_details,has_price}')::boolean, has_price),
    has_stock = COALESCE((payload#>>'{visibility_details,has_stock}')::boolean, has_stock),
    model_id = CASE WHEN COALESCE(payload#>>'{model_info,model_id}', '') ~ '^[0-9]+$'
        THEN (payload#>>'{model_info,model_id}')::bigint ELSE model_id END,
    model_count = CASE WHEN COALESCE(payload#>>'{model_info,count}', '') ~ '^[0-9]+$'
        THEN (payload#>>'{model_info,count}')::int ELSE model_count END,
    source_created_at = COALESCE(NULLIF(payload->>'created_at', '')::timestamptz, source_created_at),
    source_updated_at = COALESCE(NULLIF(payload->>'updated_at', '')::timestamptz, source_updated_at);

ALTER TABLE staging.ozon_stock_details
    ADD COLUMN IF NOT EXISTS valid_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS other_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_to_seller_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stock_defect_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS transit_defect_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS expiring_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS waiting_docs_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS waiting_docs_to_export_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS excess_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS days_without_sales INT,
    ADD COLUMN IF NOT EXISTS days_without_sales_cluster INT,
    ADD COLUMN IF NOT EXISTS macrolocal_cluster_id BIGINT,
    ADD COLUMN IF NOT EXISTS warehouse_id BIGINT,
    ADD COLUMN IF NOT EXISTS warehouse_name TEXT,
    ADD COLUMN IF NOT EXISTS placement_zone TEXT,
    ADD COLUMN IF NOT EXISTS name TEXT,
    ADD COLUMN IF NOT EXISTS item_tags_count INT NOT NULL DEFAULT 0;

ALTER TABLE staging.ozon_stock_by_cluster
    ADD COLUMN IF NOT EXISTS valid_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS other_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_to_seller_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stock_defect_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS transit_defect_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS expiring_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS waiting_docs_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS waiting_docs_to_export_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS excess_stock_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS days_without_sales INT,
    ADD COLUMN IF NOT EXISTS days_without_sales_cluster INT,
    ADD COLUMN IF NOT EXISTS macrolocal_cluster_id BIGINT,
    ADD COLUMN IF NOT EXISTS warehouse_id BIGINT,
    ADD COLUMN IF NOT EXISTS warehouse_name TEXT,
    ADD COLUMN IF NOT EXISTS placement_zone TEXT,
    ADD COLUMN IF NOT EXISTS name TEXT,
    ADD COLUMN IF NOT EXISTS item_tags_count INT NOT NULL DEFAULT 0;

UPDATE staging.ozon_stock_details
SET
    valid_stock_count = COALESCE((payload->>'valid_stock_count')::int, valid_stock_count),
    other_stock_count = COALESCE((payload->>'other_stock_count')::int, other_stock_count),
    return_to_seller_stock_count = COALESCE((payload->>'return_to_seller_stock_count')::int, return_to_seller_stock_count),
    stock_defect_stock_count = COALESCE((payload->>'stock_defect_stock_count')::int, stock_defect_stock_count),
    transit_defect_stock_count = COALESCE((payload->>'transit_defect_stock_count')::int, transit_defect_stock_count),
    expiring_stock_count = COALESCE((payload->>'expiring_stock_count')::int, expiring_stock_count),
    waiting_docs_stock_count = COALESCE((payload->>'waiting_docs_stock_count')::int, waiting_docs_stock_count),
    waiting_docs_to_export_stock_count = COALESCE((payload->>'waiting_docs_to_export_stock_count')::int, waiting_docs_to_export_stock_count),
    excess_stock_count = COALESCE((payload->>'excess_stock_count')::int, excess_stock_count),
    days_without_sales = COALESCE((payload->>'days_without_sales')::int, days_without_sales),
    days_without_sales_cluster = COALESCE((payload->>'days_without_sales_cluster')::int, days_without_sales_cluster),
    macrolocal_cluster_id = COALESCE((payload->>'macrolocal_cluster_id')::bigint, macrolocal_cluster_id),
    warehouse_id = COALESCE((payload->>'warehouse_id')::bigint, warehouse_id),
    warehouse_name = COALESCE(NULLIF(payload->>'warehouse_name', ''), warehouse_name),
    placement_zone = COALESCE(NULLIF(payload->>'placement_zone', ''), placement_zone),
    name = COALESCE(NULLIF(payload->>'name', ''), name),
    item_tags_count = CASE WHEN jsonb_typeof(payload->'item_tags') = 'array' THEN jsonb_array_length(payload->'item_tags') ELSE 0 END;

UPDATE staging.ozon_stock_by_cluster
SET
    valid_stock_count = COALESCE((payload->>'valid_stock_count')::int, valid_stock_count),
    other_stock_count = COALESCE((payload->>'other_stock_count')::int, other_stock_count),
    return_to_seller_stock_count = COALESCE((payload->>'return_to_seller_stock_count')::int, return_to_seller_stock_count),
    stock_defect_stock_count = COALESCE((payload->>'stock_defect_stock_count')::int, stock_defect_stock_count),
    transit_defect_stock_count = COALESCE((payload->>'transit_defect_stock_count')::int, transit_defect_stock_count),
    expiring_stock_count = COALESCE((payload->>'expiring_stock_count')::int, expiring_stock_count),
    waiting_docs_stock_count = COALESCE((payload->>'waiting_docs_stock_count')::int, waiting_docs_stock_count),
    waiting_docs_to_export_stock_count = COALESCE((payload->>'waiting_docs_to_export_stock_count')::int, waiting_docs_to_export_stock_count),
    excess_stock_count = COALESCE((payload->>'excess_stock_count')::int, excess_stock_count),
    days_without_sales = COALESCE((payload->>'days_without_sales')::int, days_without_sales),
    days_without_sales_cluster = COALESCE((payload->>'days_without_sales_cluster')::int, days_without_sales_cluster),
    macrolocal_cluster_id = COALESCE((payload->>'macrolocal_cluster_id')::bigint, macrolocal_cluster_id),
    warehouse_id = COALESCE((payload->>'warehouse_id')::bigint, warehouse_id),
    warehouse_name = COALESCE(NULLIF(payload->>'warehouse_name', ''), warehouse_name),
    placement_zone = COALESCE(NULLIF(payload->>'placement_zone', ''), placement_zone),
    name = COALESCE(NULLIF(payload->>'name', ''), name),
    item_tags_count = CASE
        WHEN jsonb_typeof(payload->'item_tags') = 'array' THEN jsonb_array_length(payload->'item_tags')
        WHEN jsonb_typeof(payload->'merged_rows') = 'array' THEN jsonb_array_length(payload#>'{merged_rows,0,item_tags}')
        ELSE 0
    END;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.ozon_products_flat AS
SELECT
    i.product_id,
    i.offer_id,
    i.sku,
    i.name,
    i.currency_code,
    i.price,
    i.old_price,
    i.min_price,
    i.vat,
    i.volume_weight,
    i.description_category_id,
    i.type_id,
    i.primary_image,
    i.images_count,
    i.barcodes_count,
    i.commissions_count,
    i.is_archived,
    l.archived AS list_archived,
    l.has_fbo_stocks,
    l.has_fbs_stocks,
    i.has_stock,
    i.has_price,
    i.status,
    i.status_name,
    i.status_description,
    i.moderate_status,
    i.validation_status,
    i.status_updated_at,
    i.model_id,
    i.model_count,
    i.source_created_at,
    i.source_updated_at,
    i.source_run_id,
    i.updated_at
FROM raw.ozon_product_info_items i
LEFT JOIN raw.ozon_product_list_items l
    ON l.product_id = i.product_id;

CREATE OR REPLACE VIEW analytics.ozon_stock_details_flat AS
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
FROM staging.ozon_stock_details;
