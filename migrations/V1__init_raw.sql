create table if not exists wb_orders_raw (
  id bigserial primary key,
  loaded_at timestamptz not null default now(),
  payload jsonb not null
);

create index if not exists idx_wb_orders_raw_loaded_at
  on wb_orders_raw (loaded_at);

create table if not exists ozon_orders_raw (
  id bigserial primary key,
  loaded_at timestamptz not null default now(),
  payload jsonb not null
);

create index if not exists idx_ozon_orders_raw_loaded_at
  on ozon_orders_raw (loaded_at);
