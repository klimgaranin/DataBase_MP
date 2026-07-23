-- ============================================================
-- V19: оригинальные колонки Ozon placement report без разбора payload
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.ozon_placement_cells (
    report_code    TEXT        NOT NULL,
    row_number     INT         NOT NULL,
    column_number  INT         NOT NULL,
    column_name    TEXT        NOT NULL,
    value_text     TEXT,
    value_numeric  NUMERIC(18,4),
    source_run_id  TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_code, row_number, column_number)
);

CREATE INDEX IF NOT EXISTS idx_ozon_placement_cells_column
    ON staging.ozon_placement_cells (column_name);

INSERT INTO staging.ozon_placement_cells
    (report_code, row_number, column_number, column_name,
     value_text, value_numeric, source_run_id, loaded_at)
SELECT
    p.report_code,
    p.row_number,
    e.ordinality::int AS column_number,
    e.key AS column_name,
    e.value #>> '{}' AS value_text,
    CASE
        WHEN replace(e.value #>> '{}', ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN replace(e.value #>> '{}', ',', '.')::numeric
        ELSE NULL
    END AS value_numeric,
    p.source_run_id,
    p.loaded_at
FROM staging.ozon_placement_by_products p
CROSS JOIN LATERAL jsonb_each(p.payload) WITH ORDINALITY AS e(key, value, ordinality)
ON CONFLICT (report_code, row_number, column_number) DO UPDATE SET
    column_name = EXCLUDED.column_name,
    value_text = EXCLUDED.value_text,
    value_numeric = EXCLUDED.value_numeric,
    source_run_id = EXCLUDED.source_run_id,
    loaded_at = EXCLUDED.loaded_at;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.ozon_placement_cells_flat AS
SELECT
    report_code,
    row_number,
    column_number,
    column_name,
    value_text,
    value_numeric,
    source_run_id,
    loaded_at
FROM staging.ozon_placement_cells;
