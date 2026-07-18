alter table job_runs
  add column if not exists duplicates int not null default 0;

alter table job_runs
  add column if not exists dup_pct numeric(6,2) not null default 0;
