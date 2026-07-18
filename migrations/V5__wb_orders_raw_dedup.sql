create table if not exists wb_orders_raw_dedup (
  srid text not null,
  last_change_ts timestamptz not null,
  payload jsonb not null,
  loaded_at timestamptz not null default now(),
  primary key (srid, last_change_ts)
);

create index if not exists idx_wb_orders_raw_dedup_loaded_at
  on wb_orders_raw_dedup (loaded_at);

create index if not exists idx_wb_orders_raw_dedup_last_change_ts
  on wb_orders_raw_dedup (last_change_ts);
