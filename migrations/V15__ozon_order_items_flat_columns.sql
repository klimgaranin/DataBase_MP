-- ============================================================
-- V15: flatten important Ozon FBO product fields
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.ozon_fbo_posting_details (
    posting_number  TEXT        PRIMARY KEY,
    order_id        BIGINT,
    order_number    TEXT,
    status          TEXT,
    substatus       TEXT,
    created_at      TIMESTAMPTZ,
    in_process_at   TIMESTAMPTZ,
    shipment_date   TIMESTAMPTZ,
    cancel_reason_id BIGINT,
    cancel_reason   TEXT,
    cancellation_initiator TEXT,
    cancellation_type TEXT,
    analytics_city  TEXT,
    analytics_delivery_type TEXT,
    analytics_is_legal BOOLEAN,
    analytics_is_premium BOOLEAN,
    analytics_payment_type_group_name TEXT,
    analytics_warehouse_id BIGINT,
    analytics_warehouse_name TEXT,
    financial_cluster_from TEXT,
    financial_cluster_to TEXT,
    legal_company_name TEXT,
    legal_inn TEXT,
    legal_kpp TEXT,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id   TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ozon_fbo_posting_details_status
    ON staging.ozon_fbo_posting_details (status, in_process_at DESC);
CREATE INDEX IF NOT EXISTS idx_ozon_fbo_posting_details_order
    ON staging.ozon_fbo_posting_details (order_id);

ALTER TABLE staging.ozon_order_items
    ADD COLUMN IF NOT EXISTS is_marketplace_buyout BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS digital_codes_count INT NOT NULL DEFAULT 0;

ALTER TABLE staging.ozon_order_item_details
    ADD COLUMN IF NOT EXISTS is_marketplace_buyout BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS digital_codes_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS digital_codes JSONB NOT NULL DEFAULT '[]'::jsonb;

INSERT INTO staging.ozon_fbo_posting_details
    (posting_number, order_id, order_number, status, substatus,
     created_at, in_process_at, shipment_date,
     cancel_reason_id, cancel_reason, cancellation_initiator, cancellation_type,
     analytics_city, analytics_delivery_type, analytics_is_legal, analytics_is_premium,
     analytics_payment_type_group_name, analytics_warehouse_id, analytics_warehouse_name,
     financial_cluster_from, financial_cluster_to,
     legal_company_name, legal_inn, legal_kpp,
     payload, source_run_id)
SELECT
    posting_number,
    order_id,
    payload->>'order_number',
    status,
    payload->>'substatus',
    NULLIF(payload->>'created_at', '')::timestamptz,
    in_process_at,
    shipment_date,
    NULLIF(payload->>'cancel_reason_id', '')::bigint,
    payload#>>'{cancellation,cancel_reason}',
    payload#>>'{cancellation,cancellation_initiator}',
    payload#>>'{cancellation,cancellation_type}',
    payload#>>'{analytics_data,city}',
    payload#>>'{analytics_data,delivery_type}',
    CASE WHEN lower(COALESCE(payload#>>'{analytics_data,is_legal}', 'false')) = 'true' THEN TRUE ELSE FALSE END,
    CASE WHEN lower(COALESCE(payload#>>'{analytics_data,is_premium}', 'false')) = 'true' THEN TRUE ELSE FALSE END,
    payload#>>'{analytics_data,payment_type_group_name}',
    NULLIF(payload#>>'{analytics_data,warehouse_id}', '')::bigint,
    payload#>>'{analytics_data,warehouse_name}',
    payload#>>'{financial_data,cluster_from}',
    payload#>>'{financial_data,cluster_to}',
    payload#>>'{legal_info,company_name}',
    payload#>>'{legal_info,inn}',
    payload#>>'{legal_info,kpp}',
    payload,
    source_run_id
FROM raw.ozon_fbo_postings
ON CONFLICT (posting_number) DO UPDATE SET
    order_id = EXCLUDED.order_id,
    order_number = EXCLUDED.order_number,
    status = EXCLUDED.status,
    substatus = EXCLUDED.substatus,
    created_at = EXCLUDED.created_at,
    in_process_at = EXCLUDED.in_process_at,
    shipment_date = EXCLUDED.shipment_date,
    cancel_reason_id = EXCLUDED.cancel_reason_id,
    cancel_reason = EXCLUDED.cancel_reason,
    cancellation_initiator = EXCLUDED.cancellation_initiator,
    cancellation_type = EXCLUDED.cancellation_type,
    analytics_city = EXCLUDED.analytics_city,
    analytics_delivery_type = EXCLUDED.analytics_delivery_type,
    analytics_is_legal = EXCLUDED.analytics_is_legal,
    analytics_is_premium = EXCLUDED.analytics_is_premium,
    analytics_payment_type_group_name = EXCLUDED.analytics_payment_type_group_name,
    analytics_warehouse_id = EXCLUDED.analytics_warehouse_id,
    analytics_warehouse_name = EXCLUDED.analytics_warehouse_name,
    financial_cluster_from = EXCLUDED.financial_cluster_from,
    financial_cluster_to = EXCLUDED.financial_cluster_to,
    legal_company_name = EXCLUDED.legal_company_name,
    legal_inn = EXCLUDED.legal_inn,
    legal_kpp = EXCLUDED.legal_kpp,
    payload = EXCLUDED.payload,
    source_run_id = EXCLUDED.source_run_id,
    updated_at = NOW();

UPDATE staging.ozon_order_items
SET
    price = CASE
        WHEN jsonb_typeof(payload->'price') = 'object'
             AND replace(payload#>>'{price,amount}', ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN replace(payload#>>'{price,amount}', ',', '.')::numeric
        WHEN jsonb_typeof(payload->'price') = 'string'
             AND replace(payload->>'price', ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN replace(payload->>'price', ',', '.')::numeric
        ELSE price
    END,
    currency_code = COALESCE(NULLIF(payload#>>'{price,currency}', ''), currency_code),
    is_marketplace_buyout = CASE
        WHEN lower(COALESCE(payload->>'is_marketplace_buyout', 'false')) = 'true' THEN TRUE
        ELSE FALSE
    END,
    digital_codes_count = CASE
        WHEN jsonb_typeof(payload->'digital_codes') = 'array' THEN jsonb_array_length(payload->'digital_codes')
        ELSE 0
    END;

UPDATE staging.ozon_order_item_details
SET
    price = CASE
        WHEN jsonb_typeof(payload->'price') = 'object'
             AND replace(payload#>>'{price,amount}', ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN replace(payload#>>'{price,amount}', ',', '.')::numeric
        WHEN jsonb_typeof(payload->'price') = 'string'
             AND replace(payload->>'price', ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN replace(payload->>'price', ',', '.')::numeric
        ELSE price
    END,
    currency_code = COALESCE(NULLIF(payload#>>'{price,currency}', ''), currency_code),
    is_marketplace_buyout = CASE
        WHEN lower(COALESCE(payload->>'is_marketplace_buyout', 'false')) = 'true' THEN TRUE
        ELSE FALSE
    END,
    digital_codes_count = CASE
        WHEN jsonb_typeof(payload->'digital_codes') = 'array' THEN jsonb_array_length(payload->'digital_codes')
        ELSE 0
    END,
    digital_codes = CASE
        WHEN jsonb_typeof(payload->'digital_codes') = 'array' THEN payload->'digital_codes'
        ELSE '[]'::jsonb
    END;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.ozon_fbo_order_items_flat AS
SELECT
    i.posting_number,
    i.line_number,
    p.order_id,
    p.order_number,
    p.status,
    p.substatus,
    p.created_at,
    p.in_process_at,
    p.shipment_date,
    p.cancel_reason_id,
    p.cancel_reason,
    p.cancellation_initiator,
    p.cancellation_type,
    p.analytics_city,
    p.analytics_delivery_type,
    p.analytics_is_legal,
    p.analytics_is_premium,
    p.analytics_payment_type_group_name,
    p.analytics_warehouse_id,
    p.analytics_warehouse_name,
    p.financial_cluster_from,
    p.financial_cluster_to,
    p.legal_company_name,
    p.legal_inn,
    p.legal_kpp,
    i.sku,
    i.offer_id,
    i.name,
    i.quantity,
    i.price,
    i.currency_code,
    i.is_marketplace_buyout,
    i.digital_codes_count,
    i.source_run_id,
    i.updated_at
FROM staging.ozon_order_item_details i
LEFT JOIN staging.ozon_fbo_posting_details p
    ON p.posting_number = i.posting_number;
