CREATE TABLE IF NOT EXISTS staging.supply_order_specs_current (
    source_sheet       TEXT        NOT NULL,
    source_row_number  INT         NOT NULL DEFAULT 0,
    article            TEXT        NOT NULL,
    specification      TEXT        NOT NULL DEFAULT '',
    production_date    DATE,
    payload            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_run_id      TEXT        NOT NULL,
    snapped_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_sheet, source_row_number, article)
);

CREATE INDEX IF NOT EXISTS idx_supply_order_specs_current_article
    ON staging.supply_order_specs_current (article);

CREATE INDEX IF NOT EXISTS idx_supply_order_specs_current_date
    ON staging.supply_order_specs_current (production_date);
