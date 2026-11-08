-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Foreign servers, user mappings and foreign tables for the PostgreSQL
-- front end. The endpoint has no PostgreSQL system catalogs, so it keeps the
-- FDW objects here. Credentials are intentionally stored in this metadata
-- database; restrict access to it. Plan payloads carry only the server name.

create schema if not exists federation;

create table if not exists federation.servers
(
    server_name text primary key,
    driver      text not null default 'adbc_driver_postgresql',
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
