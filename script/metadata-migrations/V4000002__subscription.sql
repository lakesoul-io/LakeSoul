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
    subscription_id text primary key,
    slot_name       text not null unique,
    publication     text not null,
    state           text not null default 'init',
    last_error      text,
    created_at      bigint not null,
    updated_at      bigint not null
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
    unique (subscription_id, target_namespace, target_table)
);

create table if not exists subscription.progress
(
    subscription_id  text primary key references subscription.subscriptions (subscription_id) on delete cascade,
    processed_lsn    numeric(20, 0) not null default 0,
    updated_at       bigint not null,
    owner            text,
    lease_expires_at bigint
);
