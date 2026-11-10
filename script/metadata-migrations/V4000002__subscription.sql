-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- PostgreSQL logical-replication subscriber state.
--
-- The subscriber writes its definition and progress here instead of a local
-- state file, so a restart on another node can resume from the metadata PG.
-- Data progress remains authoritative in the target table's commit ids; these
-- rows add identity checks and record transactions that touched no mapped
-- table.

create schema if not exists subscription;

create table if not exists subscription.subscriptions
(
    subscription_id       text primary key,
    slot_name             text not null unique,
    publication           text not null,
    -- The source connection string is stored so a supervised child process can
    -- be restarted without repeating it on a command line. It contains the
    -- source credentials; protect access to this metadata database.
    source_url            text not null,
    state                 text not null default 'init',
    enabled               boolean not null default true,
    -- Only SQL-created subscriptions are restarted by the server supervisor;
    -- a manually started CLI subscriber must not be duplicated.
    supervised            boolean not null default false,
    commit_rows           bigint not null default 50000,
    commit_interval_ms    bigint not null default 1000,
    snapshot_batch_rows   bigint not null default 50000,
    snapshot_batch_bytes  bigint not null default 67108864,
    snapshot_parallelism  bigint not null default 4,
    snapshot_memory_bytes bigint not null default 1073741824,
    -- Target DDL options. auto_create creates a missing target from the
    -- source schema; schema_evolution=auto adds nullable columns, widens safe
    -- types and relaxes NOT NULL. warehouse_prefix is used only for creation.
    auto_create           boolean not null default false,
    schema_evolution      text not null default 'auto',
    warehouse_prefix      text,
    cdc_column            text not null default 'rowKinds',
    supervisor_pid        integer,
    log_path              text,
    last_error            text,
    created_at            bigint not null,
    updated_at            bigint not null
);

create table if not exists subscription.tables
(
    subscription_id     text   not null references subscription.subscriptions (subscription_id) on delete cascade,
    source_schema       text   not null,
    source_table        text   not null,
    source_relation_oid bigint not null,
    target_namespace    text   not null,
    target_table        text   not null,
    target_table_id     text   not null,
    primary_key         text   not null,
    data_lsn            numeric(20, 0) not null default 0,
    state               text   not null default 'init',
    snapshot_state      text   not null default 'pending',
    snapshot_mode       text   not null default 'managed',
    snapshot_lsn        numeric(20, 0),
    snapshot_started_at bigint,
    snapshot_completed_at bigint,
    snapshot_dump_path  text,
    last_error          text,
    created_at          bigint not null,
    updated_at          bigint not null,
    primary key (subscription_id, source_schema, source_table),
    unique (subscription_id, target_namespace, target_table),
    -- Two subscriber processes must not write the same target table.
    unique (target_namespace, target_table)
);

create table if not exists subscription.progress
(
    subscription_id  text primary key references subscription.subscriptions (subscription_id) on delete cascade,
    processed_lsn    numeric(20, 0) not null default 0,
    updated_at       bigint not null,
    owner            text,
    lease_expires_at bigint
);
