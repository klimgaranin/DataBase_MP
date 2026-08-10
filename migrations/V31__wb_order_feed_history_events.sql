-- WB Order Feed history must store change events, not only unique payload variants.
-- A WB order can return to a payload/status that was already seen earlier.
-- In that case the current state changes again, so history needs a new event row.

ALTER TABLE raw.wb_order_feed_order_versions
    DROP CONSTRAINT IF EXISTS wb_order_feed_order_versions_srid_payload_sha256_key;

ALTER TABLE raw.wb_order_feed_order_versions
    ADD CONSTRAINT wb_order_feed_order_versions_srid_payload_run_key
    UNIQUE (srid, payload_sha256, source_run_id);

CREATE INDEX IF NOT EXISTS idx_wb_order_feed_versions_payload
    ON raw.wb_order_feed_order_versions (srid, payload_sha256, changed_at DESC);
