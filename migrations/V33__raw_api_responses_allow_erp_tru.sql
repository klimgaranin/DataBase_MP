-- Allow non-marketplace ERP/TRU API logs in the shared raw HTTP evidence table.
ALTER TABLE raw.api_responses
    DROP CONSTRAINT IF EXISTS chk_raw_api_marketplace;

ALTER TABLE raw.api_responses
    ADD CONSTRAINT chk_raw_api_marketplace
    CHECK (marketplace IN ('wb', 'ozon', 'google_sheets', 'system', 'erp_tru'));
