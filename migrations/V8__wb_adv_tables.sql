-- ============================================================
-- V8: Таблицы для WB Advertising API
-- ============================================================

-- Список рекламных кампаний (/adv/v1/promotion/count)
-- Стратегия: UPSERT, данные накапливаются
CREATE TABLE IF NOT EXISTS wb_adv_campaigns (
    advert_id   BIGINT      PRIMARY KEY,
    status      INT         NOT NULL,           -- 7=завершена, 9=активна, 11=пауза
    type        INT,                            -- тип кампании
    change_time TIMESTAMPTZ,                    -- changeTime из advert_list
    payload     JSONB       NOT NULL,           -- полный объект для истории
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE wb_adv_campaigns IS 'Список РК WB из /adv/v1/promotion/count';
COMMENT ON COLUMN wb_adv_campaigns.status IS '7=завершена, 9=активна, 11=пауза';

-- Сырые ответы /adv/v3/fullstats
-- Стратегия: TRUNCATE + INSERT при каждом запуске
CREATE TABLE IF NOT EXISTS wb_adv_fullstats_raw (
    id          BIGSERIAL   PRIMARY KEY,
    advert_id   BIGINT      NOT NULL,
    begin_date  DATE        NOT NULL,
    end_date    DATE        NOT NULL,
    payload     JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wb_adv_fullstats_raw_dates
    ON wb_adv_fullstats_raw (begin_date, end_date);
CREATE INDEX IF NOT EXISTS idx_wb_adv_fullstats_raw_advert
    ON wb_adv_fullstats_raw (advert_id);

COMMENT ON TABLE wb_adv_fullstats_raw IS 'Raw ответы /adv/v3/fullstats; полная перезапись при каждом ETL-запуске';

-- Нормализованные данные: затраты по (кампания, артикул, дата)
-- Стратегия: TRUNCATE + INSERT при каждом запуске
CREATE TABLE IF NOT EXISTS wb_adv_fullstats_norm (
    advert_id   BIGINT          NOT NULL,
    nm_id       BIGINT          NOT NULL,
    stat_date   DATE            NOT NULL,
    spend_rub   NUMERIC(12,2)   NOT NULL,
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (advert_id, nm_id, stat_date)
);

CREATE INDEX IF NOT EXISTS idx_wb_adv_fullstats_norm_date
    ON wb_adv_fullstats_norm (stat_date);
CREATE INDEX IF NOT EXISTS idx_wb_adv_fullstats_norm_nm
    ON wb_adv_fullstats_norm (nm_id);

COMMENT ON TABLE wb_adv_fullstats_norm IS
    'Нормализованные затраты из /adv/v3/fullstats; полная перезапись при каждом ETL-запуске';
