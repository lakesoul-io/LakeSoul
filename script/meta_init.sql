-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Complete metadata schema for a new LakeSoul deployment. Existing
-- deployments apply script/metadata-migrations/ instead of recreating the
-- database. Keep this file in sync with the release migration; server code
-- must not create metadata tables at runtime.

create table if not exists namespace
(
    namespace  text,
    properties json,
    comment    text,
    domain     text default 'public',
    primary key (namespace)
);

insert into namespace(namespace, properties, comment) values ('default', '{}', '')
ON CONFLICT DO NOTHING;

create table if not exists table_info
(
    table_id        text,
    table_namespace text default 'default',
    table_name      text,
    table_path      text,
    table_schema    text,
    table_schema_arrow_ipc bytea,
    table_schema_arrow_ipc_json_hash text,
    properties      json,
    partitions      text,
    domain          text default 'public',
    primary key (table_id)
);
ALTER TABLE table_info ADD COLUMN IF NOT EXISTS table_schema_arrow_ipc bytea;
ALTER TABLE table_info ADD COLUMN IF NOT EXISTS table_schema_arrow_ipc_json_hash text;
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_info_name_index ON table_info (table_namespace, table_name);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_info_path_index ON table_info (table_path);

create table if not exists table_name_id
(
    table_name      text,
    table_id        text,
    table_namespace text default 'default',
    domain          text default 'public',
    primary key (table_name, table_namespace)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_name_id_id_index ON table_name_id (table_id);

create table if not exists table_path_id
(
    table_path      text,
    table_id        text,
    table_namespace text default 'default',
    domain          text default 'public',
    primary key (table_path)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_path_id_id_index ON table_path_id (table_id);

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'data_file_op') THEN
            create type data_file_op as
            (
                path            text,
                file_op         text,
                size            bigint,
                file_exist_cols text
            );
        END IF;
    END
$$;

create table if not exists data_commit_info
(
    table_id       text,
    partition_desc text,
    commit_id      UUID,
    file_ops       data_file_op[],
    commit_op      text,
    committed      boolean default 'false',
    timestamp      bigint,
    domain         text default 'public',
    primary key (table_id, partition_desc, commit_id)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS data_commit_info_commit_id ON data_commit_info (commit_id);
ALTER TABLE data_commit_info REPLICA IDENTITY FULL;

create table if not exists partition_info
(
    table_id       text,
    partition_desc text,
    version        int,
    commit_op      text,
    timestamp      bigint DEFAULT (date_part('epoch'::text, now()) * (1000)::double precision),
    snapshot       UUID[],
    expression     text,
    domain         text default 'public',
    primary key (table_id, partition_desc, version)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS partition_info_timestamp ON partition_info (timestamp);
CREATE INDEX CONCURRENTLY IF NOT EXISTS partition_info_desc_gin_tsvector_index ON partition_info USING GIN (to_tsvector('english', partition_desc));

CREATE OR REPLACE FUNCTION partition_insert() RETURNS TRIGGER AS
$$
DECLARE
    rs_version         integer;
    rs_table_path      text;
    rs_table_namespace text;
BEGIN
    if NEW.commit_op <> 'CompactionCommit' then
        select version
        INTO rs_version
        from partition_info
        where table_id = NEW.table_id
          and partition_desc = NEW.partition_desc
          and version != NEW.version
          and commit_op = 'CompactionCommit'
        order by version desc
        limit 1;
        if rs_version >= 0 then
            if NEW.version - rs_version >= 10 then
                select table_path, table_namespace
                into rs_table_path, rs_table_namespace
                from table_info
                where table_id = NEW.table_id;
                perform pg_notify('lakesoul_compaction_notify',
                                  concat('{"table_path":"', rs_table_path, '","table_partition_desc":"',
                                         NEW.partition_desc, '","table_namespace":"', rs_table_namespace, '"}'));
            end if;
        else
            if NEW.version >= 10 then
                select table_path, table_namespace
                into rs_table_path, rs_table_namespace
                from table_info
                where table_id = NEW.table_id;
                perform pg_notify('lakesoul_compaction_notify',
                                  concat('{"table_path":"', rs_table_path, '","table_partition_desc":"',
                                         NEW.partition_desc, '","table_namespace":"', rs_table_namespace, '"}'));
            end if;
        end if;
        RETURN NULL;
    end if;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS partition_table_change ON partition_info;
CREATE TRIGGER partition_table_change
    AFTER INSERT
    ON partition_info
    FOR EACH ROW
EXECUTE PROCEDURE partition_insert();

create table if not exists global_config
(
    key  text,
    value text,
    primary key (key)
);

create table if not exists discard_compressed_file_info
(
    file_path text,
    table_path text,
    partition_desc text,
    timestamp bigint DEFAULT (date_part('epoch'::text, now()) * (1000)::double precision),
    t_date date,
    PRIMARY KEY (file_path)
);

-- Index control plane: the index data (segments/splits) lives in object
-- storage; these tables track the current commit per shard (scoped by the
-- index kind), the files it references, and the leases held by readers
-- while they load.
create table if not exists index_shard
(
    shard_id     bigserial primary key,
    kind         text not null,
    index_prefix text not null,
    unique (kind, index_prefix)
);

create table if not exists index_commit
(
    commit_id     bigserial primary key,
    shard_id      bigint      not null references index_shard (shard_id) on delete cascade,
    generation    bigint      not null,
    version       bigint      not null,
    header        bytea       not null,
    segments      jsonb       not null default '[]'::jsonb,
    created_at    timestamptz not null default now(),
    superseded_at timestamptz,
    unique (shard_id, generation, version)
);

create index if not exists index_commit_shard_key_index
    on index_commit (shard_id, generation desc, version desc);

create table if not exists index_lease
(
    lease_id    uuid primary key,
    shard_id    bigint      not null references index_shard (shard_id) on delete cascade,
    generation  bigint      not null,
    version     bigint      not null,
    owner       text        not null,
    acquired_at timestamptz not null default now(),
    expires_at  timestamptz not null
);

create index if not exists index_lease_expiry_index
    on index_lease (shard_id, expires_at);

-- Table-level snapshots and tags. A snapshot freezes the version and commit
-- ids of every partition; a tag keeps a snapshot alive so the Flink clean job
-- must not delete its files or metadata. `pinned` on data_commit_info and
-- partition_info is the materialized filter the clean job reads.
create table if not exists table_snapshot
(
    table_id    text   not null,
    snapshot_id bigserial,
    created_at  bigint not null,
    description text,
    primary key (table_id, snapshot_id)
);

create table if not exists snapshot_commit
(
    table_id       text   not null,
    snapshot_id    bigint not null,
    partition_desc text   not null,
    version        int    not null,
    commit_id      uuid   not null,
    primary key (table_id, snapshot_id, partition_desc, commit_id)
);

create index if not exists snapshot_commit_commit_id_index
    on snapshot_commit (table_id, commit_id);

create index if not exists snapshot_commit_version_index
    on snapshot_commit (table_id, partition_desc, version);

create table if not exists table_snapshot_tag
(
    table_id    text   not null,
    tag         text   not null,
    snapshot_id bigint not null,
    created_at  bigint not null,
    expire_at   bigint,
    primary key (table_id, tag)
);

alter table data_commit_info
    add column if not exists pinned boolean not null default false;
alter table partition_info
    add column if not exists pinned boolean not null default false;

create index if not exists data_commit_info_pinned_index
    on data_commit_info (table_id, pinned);
create index if not exists partition_info_pinned_index
    on partition_info (table_id, pinned);

-- ---------------------------------------------------------------------------
-- PostgreSQL and MySQL logical-replication subscriptions
-- ---------------------------------------------------------------------------
-- The subscriber writes its definition and progress here instead of a local
-- state file, so a restart on another node can resume from the metadata PG.
-- Data progress remains authoritative in the target table's commit ids; these
-- rows add identity checks and record transactions that touched no mapped
-- table. The source connection string contains credentials: protect access to
-- this metadata database.
create schema if not exists subscription;

create table if not exists subscription.subscriptions
(
    subscription_id       text primary key,
    slot_name             text not null unique,
    publication           text not null,
    source_url            text not null,
    source_kind           text not null default 'postgres',
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

-- A MySQL position is a GTID set plus a binlog file/offset, so it lives in
-- its own row. Target commit ids still carry a local sequence for ordering;
-- stream_offset is authoritative for resuming the binlog.
create table if not exists subscription.mysql_state
(
    subscription_id   text primary key references subscription.subscriptions (subscription_id) on delete cascade,
    server_id         integer not null,
    gtid_mode         text not null default 'auto',
    snapshot_locking  text not null default 'none',
    source_database   text not null,
    stream_offset     jsonb,
    stream_sequence   bigint not null default 0,
    snapshot_boundary jsonb,
    snapshot_rows     bigint not null default 0,
    snapshot_state    text not null default 'pending',
    updated_at        bigint not null
);

-- ---------------------------------------------------------------------------
-- Federation: foreign servers, user mappings and foreign tables
-- ---------------------------------------------------------------------------
-- The PostgreSQL front end has no PostgreSQL system catalogs, so it keeps the
-- FDW objects here. Credentials are intentionally stored in this metadata
-- database; restrict access to it. Plan payloads carry only the server name.
create schema if not exists federation;

create table if not exists federation.servers
(
    server_name text primary key,
    backend     text not null default 'adbc',
    driver      text not null default 'postgresql',
    dialect     text not null default 'postgres',
    options     text not null default '{}',
    created_at  bigint not null,
    updated_at  bigint not null
);

create table if not exists federation.user_mappings
(
    server_name text not null references federation.servers (server_name) on delete cascade,
    pg_user     text not null,
    options     text not null default '{}',
    updated_at  bigint not null,
    primary key (server_name, pg_user)
);

create table if not exists federation.foreign_tables
(
    database      text not null,
    local_schema  text not null,
    local_name    text not null,
    server_name   text not null references federation.servers (server_name) on delete cascade,
    remote_schema text not null,
    remote_table  text not null,
    schema_json   text not null,
    options       text not null default '{}',
    created_at    bigint not null,
    updated_at    bigint not null,
    primary key (database, local_schema, local_name)
);

-- ---------------------------------------------------------------------------
-- Detached query jobs
-- ---------------------------------------------------------------------------
-- The durable definition, progress and result metadata behind
-- `lakesoul_submit` and the `lakesoul_query_*` table functions.
create schema if not exists job;

create table if not exists job.queries
(
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

create index if not exists job_queries_state_idx
    on job.queries (state, created_at);

create index if not exists job_queries_user_idx
    on job.queries (user_name, created_at desc);

create unique index if not exists job_queries_scope_key_idx
    on job.queries (database_name, user_name, job_key);

-- ---------------------------------------------------------------------------
-- Incremental view maintenance (IVM)
-- ---------------------------------------------------------------------------
-- The engine keeps view definitions, source cursors, refresh epochs and the
-- registration of internal state tables here.
create schema if not exists ivm;

create table if not exists ivm.views
(
    view_id              text primary key,
    spec                 jsonb not null,
    refresh_interval_ms  bigint not null default 0,
    status               text not null default 'active',
    last_epoch           bigint not null default 0,
    generation           bigint not null default 0,
    definition_hash      text,
    source_sql           text,
    last_error           text,
    last_refresh_at      bigint,
    last_check_at        bigint,
    last_action          text,
    last_duration_ms     bigint not null default 0,
    last_source_lag_ms   bigint,
    last_rows_read       bigint not null default 0,
    last_rows_written    bigint not null default 0,
    last_error_at        bigint,
    consecutive_failures bigint not null default 0,
    created_at           bigint not null
);

create table if not exists ivm.cursors
(
    view_id         text not null,
    source_table_id text not null,
    partition_desc  text not null,
    last_version    bigint not null,
    last_timestamp  bigint not null,
    primary key (view_id, source_table_id, partition_desc)
);

create table if not exists ivm.epochs
(
    view_id               text   not null,
    generation            bigint not null,
    epoch                 bigint not null,
    window_key            text   not null,
    status                text   not null,
    to_versions           jsonb  not null,
    mv_versions_before    jsonb  not null default '[]'::jsonb,
    state_versions_before jsonb  not null default '[]'::jsonb,
    mv_versions           jsonb  not null default '[]'::jsonb,
    state_versions        jsonb  not null default '[]'::jsonb,
    commit_ids            jsonb  not null default '[]'::jsonb,
    commit_plan           jsonb  not null default '{}'::jsonb,
    created_at            bigint not null,
    committed_at          bigint,
    primary key (view_id, generation, epoch)
);

create unique index if not exists ivm_epochs_window_key
    on ivm.epochs (view_id, generation, window_key);

create table if not exists ivm.states
(
    view_id    text   not null,
    role       text   not null,
    table_id   text   not null,
    table_name text   not null,
    namespace  text   not null default 'default',
    table_path text   not null,
    created_at bigint not null,
    primary key (view_id, role)
);

create table if not exists ivm.consumers
(
    view_id     text   not null,
    consumer_id text   not null,
    last_epoch  bigint not null,
    updated_at  bigint not null,
    primary key (view_id, consumer_id)
);

create table if not exists ivm.view_commits
(
    view_id        text   not null,
    commit_id      text   not null,
    partition_desc text   not null,
    created_at     bigint not null,
    primary key (view_id, commit_id)
);

create table if not exists ivm.view_locks
(
    view_id    text   not null primary key,
    owner      text   not null,
    expires_at bigint not null
);

create table if not exists ivm.view_partitions
(
    view_id        text   not null,
    partition_desc text   not null,
    updated_at     bigint not null,
    primary key (view_id, partition_desc)
);
