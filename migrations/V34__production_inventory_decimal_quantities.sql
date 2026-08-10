-- ============================================================
-- V34: preserve fractional internal inventory quantities
-- ============================================================

ALTER TABLE core.production_inventory_snapshot
    ALTER COLUMN smp_qty TYPE NUMERIC(14,3) USING smp_qty::numeric,
    ALTER COLUMN osn_qty TYPE NUMERIC(14,3) USING osn_qty::numeric,
    ALTER COLUMN soh_qty TYPE NUMERIC(14,3) USING soh_qty::numeric,
    ALTER COLUMN svh_qty TYPE NUMERIC(14,3) USING svh_qty::numeric,
    ALTER COLUMN ts_qty TYPE NUMERIC(14,3) USING ts_qty::numeric;
