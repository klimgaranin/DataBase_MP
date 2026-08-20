ALTER TABLE staging.supply_order_specs_current
    ADD COLUMN IF NOT EXISTS manager_name TEXT NOT NULL DEFAULT '';
