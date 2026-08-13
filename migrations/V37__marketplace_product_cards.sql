CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS raw.wb_content_cards (
    nm_id          BIGINT      PRIMARY KEY,
    imt_id         BIGINT,
    vendor_code    TEXT,
    subject_id     BIGINT,
    subject_name   TEXT,
    brand          TEXT,
    title          TEXT,
    photo_big      TEXT,
    photos_count   INT         NOT NULL DEFAULT 0,
    sizes_count    INT         NOT NULL DEFAULT 0,
    photos         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    payload        JSONB       NOT NULL,
    source_run_id  TEXT        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wb_content_cards_vendor
    ON raw.wb_content_cards (vendor_code);
CREATE INDEX IF NOT EXISTS idx_wb_content_cards_updated
    ON raw.wb_content_cards (updated_at DESC);

CREATE TABLE IF NOT EXISTS staging.marketplace_product_cards_current (
    marketplace     TEXT        NOT NULL,
    article         TEXT        NOT NULL,
    product_id      TEXT,
    marketplace_sku BIGINT,
    product_name    TEXT,
    brand           TEXT,
    primary_image   TEXT,
    images          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    images_count    INT         NOT NULL DEFAULT 0,
    payload         JSONB       NOT NULL,
    source_run_id   TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, article),
    CONSTRAINT chk_marketplace_product_cards_marketplace CHECK (marketplace IN ('wb', 'ozon'))
);

CREATE INDEX IF NOT EXISTS idx_product_cards_marketplace_sku
    ON staging.marketplace_product_cards_current (marketplace, marketplace_sku);
CREATE INDEX IF NOT EXISTS idx_product_cards_updated
    ON staging.marketplace_product_cards_current (updated_at DESC);

INSERT INTO staging.marketplace_product_cards_current
    (marketplace, article, product_id, marketplace_sku, product_name, brand,
     primary_image, images, images_count, payload, source_run_id, fetched_at, updated_at)
SELECT
    'ozon',
    COALESCE(NULLIF(offer_id, ''), product_id::text),
    product_id::text,
    sku,
    name,
    NULL,
    primary_image,
    CASE
        WHEN jsonb_typeof(payload->'images') = 'array' THEN payload->'images'
        WHEN primary_image IS NOT NULL THEN jsonb_build_array(primary_image)
        ELSE '[]'::jsonb
    END,
    images_count,
    payload,
    source_run_id,
    fetched_at,
    updated_at
FROM raw.ozon_product_info_items
WHERE COALESCE(NULLIF(offer_id, ''), product_id::text) IS NOT NULL
ON CONFLICT (marketplace, article) DO UPDATE SET
    product_id = EXCLUDED.product_id,
    marketplace_sku = EXCLUDED.marketplace_sku,
    product_name = EXCLUDED.product_name,
    primary_image = EXCLUDED.primary_image,
    images = EXCLUDED.images,
    images_count = EXCLUDED.images_count,
    payload = EXCLUDED.payload,
    source_run_id = EXCLUDED.source_run_id,
    fetched_at = EXCLUDED.fetched_at,
    updated_at = NOW();
