
-- 1) target databases
create table if not exists target_database (
  id            bigserial primary key,
  unique_name   varchar(255) not null,       
  host          varchar(255) not null,       
  dbname        varchar(63)  not null,       
  port          integer      not null default 5432 check (port between 1 and 65535),
  username      varchar(63)  not null,       
  created_at    timestamptz  not null default now(),
  constraint target_database_unique_name_key unique (unique_name)
);


-- 2) reports
create table if not exists report_run (
  id                  bigserial primary key,
  target_database_id  bigint not null references target_database(id) on delete restrict,
  started_at          timestamptz not null default now(),
  finished_at         timestamptz,
  success             boolean not null default false,   
  app_version         varchar(32)
);

-- 3) chapters
create table if not exists chapter_run (
  id               bigserial primary key,
  report_run_id    bigint not null references report_run(id) on delete cascade,
  chapter_name     varchar(256) not null,
  query_id         varchar(128) not null,
  executed_sql     text not null,
  row_count        integer not null default 0,
  success          boolean not null default false,     
  result_json      jsonb not null
);
