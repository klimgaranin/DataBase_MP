create table if not exists job_state (
  job_name text primary key,
  cursor text,
  updated_at timestamptz not null default now()
);
