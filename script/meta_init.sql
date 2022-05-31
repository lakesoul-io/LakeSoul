create table if not exists table_info (
    table_id text,
    table_name text,
    table_path text,
    table_schema text,
    properties json,
    partitions text,
    primary key(table_id)
);

create table if not exists table_name_id (
    table_name text,
    table_id text,
    primary key(table_name)
);

create table if not exists table_path_id (
    table_path text,
    table_id text,
    primary key(table_path)
);

create type data_file_op as (
    path text,
    file_op text,
    size bigint,
    file_exist_cols text
);

create table if not exists data_commit_info (
    table_id text,
    partition_desc text,
    commit_id UUID,
    file_ops data_file_op [],
    commit_op text,
    timestamp bigint,
    primary key(table_id, partition_desc, commit_id)
);

create table if not exists partition_info (
    table_id text,
    partition_desc text,
    version int,
    commit_op text,
    snapshot UUID [],
    expression text,
    primary key(table_id, partition_desc, version)
);