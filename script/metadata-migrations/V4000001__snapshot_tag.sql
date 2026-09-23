-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Table snapshots, tags and pin flags for retention-aware cleanup.
-- See docs/embodied-snapshot-tag-design.md.

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
