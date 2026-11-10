-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- The SQL dialect is fixed when a foreign server is created. Existing
-- servers were always planned with the PostgreSQL dialect, so backfill the
-- column from the ADBC driver for the drivers that now have a built-in
-- dialect.

alter table federation.servers
    add column if not exists dialect text not null default 'postgres';

update federation.servers
set dialect = case driver
    when 'postgres' then 'postgres'
    when 'postgresql' then 'postgres'
    when 'mysql' then 'mysql'
    when 'sqlite' then 'sqlite'
    when 'duckdb' then 'duckdb'
    when 'bigquery' then 'bigquery'
    when 'snowflake' then 'snowflake'
    when 'trino' then 'generic'
    else dialect
end
where dialect = 'postgres';
