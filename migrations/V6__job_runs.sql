create table if not exists job_runs (
  id bigserial primary key,
  job_name text not null,
  started_at timestamptz not null,
  finished_at timestamptz not null,
  status text not null,              -- ok / fail
  api_rows int not null default 0,
  raw_new_versions int not null default 0,
  norm_upserted int not null default 0,
  cursor_old text,
  cursor_used text,
  cursor_new text,
  error text
);

create index if not exists idx_job_runs_job_time
  on job_runs (job_name, started_at desc);
