create table if not exists wb_orders_norm (
  srid text primary key,

  is_cancel boolean,
  date_ts timestamptz,
  last_change_ts timestamptz,

  warehouse_name text,
  warehouse_type text,
  country_name text,
  oblast_okrug_name text,
  region_name text,

  supplier_article text,
  nm_id bigint,
  barcode text,

  category text,
  subject text,
  brand text,
  tech_size text,

  income_id bigint,
  is_supply boolean,
  is_realization boolean,

  total_price numeric(12,2),
  discount_percent int,
  spp int,
  finished_price numeric(12,2),
  price_with_disc numeric(12,2),

  cancel_date date,

  sticker text,
  g_number text,

  loaded_at timestamptz not null default now()
);

create index if not exists idx_wb_orders_norm_last_change_ts
  on wb_orders_norm (last_change_ts);

create index if not exists idx_wb_orders_norm_date_ts
  on wb_orders_norm (date_ts);

create index if not exists idx_wb_orders_norm_supplier_article
  on wb_orders_norm (supplier_article);
