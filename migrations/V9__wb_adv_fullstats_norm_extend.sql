-- ============================================================
-- V9: расширение wb_adv_fullstats_norm — все метрики из API
-- ============================================================
-- /adv/v3/fullstats возвращает по каждому nm:
--   sum       — расходы (руб)
--   views     — показы
--   clicks    — клики
--   atbs      — добавления в корзину
--   orders    — заказы (кол-во)
--   shks      — заказы (шт, физические единицы)
--   sum_price — сумма заказов (руб)
-- Производные метрики (ctr, cpc, cr) не хранятся — считаются при аналитике.

ALTER TABLE wb_adv_fullstats_norm
    ADD COLUMN IF NOT EXISTS views      BIGINT          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS clicks     BIGINT          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS atbs       BIGINT          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS orders     BIGINT          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shks       BIGINT          NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sum_price  NUMERIC(14,2)   NOT NULL DEFAULT 0;

COMMENT ON COLUMN wb_adv_fullstats_norm.views      IS 'Показы';
COMMENT ON COLUMN wb_adv_fullstats_norm.clicks     IS 'Клики';
COMMENT ON COLUMN wb_adv_fullstats_norm.atbs       IS 'Добавления в корзину';
COMMENT ON COLUMN wb_adv_fullstats_norm.orders     IS 'Заказы (кол-во)';
COMMENT ON COLUMN wb_adv_fullstats_norm.shks       IS 'Заказы (шт)';
COMMENT ON COLUMN wb_adv_fullstats_norm.sum_price  IS 'Сумма заказов (руб)';
