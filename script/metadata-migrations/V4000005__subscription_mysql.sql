-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- MySQL binlog subscriber state.
--
-- The PostgreSQL subscriber keeps its source watermark in target commit ids
-- and subscription.progress. A MySQL position is a GTID set plus a binlog
-- file/offset, so it lives in its own row. target commit ids still carry a
-- local sequence for ordering; stream_offset is authoritative for resuming
-- the binlog.

alter table if exists subscription.subscriptions
    add column if not exists source_kind text not null default 'postgres';

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
