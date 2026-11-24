-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
-- SPDX-License-Identifier: Apache-2.0

-- Run through psql against the postgres-lakesoul endpoint, using the README command.
\set source_url `printenv lakesoul_mirror_source_dsn`
CREATE DATABASE IF NOT EXISTS lakesoul_meta;

CREATE SUBSCRIPTION lakesoul_meta_mirror
CONNECTION :'source_url'
PUBLICATION lakesoul_meta_publication
TABLES (
    public.table_info TO lakesoul_meta.table_info,
    public.partition_info TO lakesoul_meta.partition_info,
    public.data_commit_info TO lakesoul_meta.data_commit_info
)
WITH (
    snapshot = 'managed',
    auto_create = true,
    slot_name = 'lakesoul_meta_mirror'
);
