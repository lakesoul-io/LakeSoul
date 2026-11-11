-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Detached query jobs: the durable definition, progress and result metadata
-- behind `lakesoul_submit` and the `lakesoul_query_*` table functions.

create schema if not exists job;

create table if not exists job.queries (
    query_id           uuid primary key,
    job_key            text,
    user_name          text   not null,
    database_name      text   not null,
    sql_text           text   not null,
    settings           jsonb  not null,
    state              text   not null default 'queued',
    attempt            integer not null default 0,
    max_attempts       integer not null default 3,
    retry_on_restart   boolean not null default true,
    timeout_ms         bigint,
    created_at         bigint not null,
    started_at         bigint,
    finished_at        bigint,
    lease_expires_at   bigint,
    owner              text,
    cancel_requested   boolean not null default false,
    command_tag        text,
    rows_produced      bigint not null default 0,
    bytes_produced     bigint not null default 0,
    error_sqlstate     text,
    error_message      text,
    result_kind        text   not null default 'none',
    result_rows        bigint,
    result_bytes       bigint,
    result_location    text,
    result_schema      jsonb,
    result_sets        jsonb,
    result_expires_at  bigint
);

alter table if exists job.queries
    add column if not exists result_schema jsonb;
alter table if exists job.queries
    add column if not exists result_sets jsonb;
alter table if exists job.queries
    add column if not exists attempt integer not null default 0;
alter table if exists job.queries
    add column if not exists max_attempts integer not null default 3;
alter table if exists job.queries
    add column if not exists retry_on_restart boolean not null default true;
alter table if exists job.queries
    add column if not exists timeout_ms bigint;

create index if not exists job_queries_state_idx
    on job.queries (state, created_at);

create index if not exists job_queries_user_idx
    on job.queries (user_name, created_at desc);

drop index if exists job.job_queries_user_key_idx;

create unique index if not exists job_queries_scope_key_idx
    on job.queries (database_name, user_name, job_key);
