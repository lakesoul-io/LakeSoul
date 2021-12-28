CREATE KEYSPACE IF NOT EXISTS test_lakesoul_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;

CREATE TABLE test_lakesoul_meta.table_info (
  table_name text,
  table_id text,
  table_schema text,
  range_column text,
  hash_column text,
  setting map<text,text>,
  read_version int,
  pre_write_version int,
  bucket_num int,
  short_table_name text,
  is_material_view boolean,
  PRIMARY KEY (table_name)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'meta service table version control';

CREATE TABLE test_lakesoul_meta.partition_info (
  table_id text,
  range_value text,
  range_id text,
  table_name text,
  read_version bigint,
  pre_write_version bigint,
  last_update_timestamp bigint,
  delta_file_num int,
  be_compacted boolean,
  PRIMARY KEY (table_id,range_value)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'meta service partition Info control';

CREATE TABLE test_lakesoul_meta.data_info (
  table_id text,
  range_id text,
  file_path text,
  hash_value map<text,text>,
  write_version bigint,
  expire_version bigint,
  commit_id text,
  size bigint,
  modification_time bigint,
  file_exist_cols text,
  is_base_file boolean,
  PRIMARY KEY ((table_id,range_id), file_path)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'meta service table data info';

CREATE TABLE test_lakesoul_meta.undo_log (
  commit_type text,
  table_id text,
  commit_id text,
  range_id text,
  file_path text,
  table_name text,
  range_value text,
  tag int,
  write_version bigint,
  timestamp bigint,
  size bigint,
  modification_time bigint,
  table_schema text,
  setting map<text,text>,
  file_exist_cols text,
  delta_file_num int,
  be_compacted boolean,
  is_base_file boolean,
  query_id text,
  batch_id bigint,
  short_table_name text,
  sql_text text,
  relation_tables text,
  auto_update boolean,
  is_creating_view boolean,
  view_info text,
  PRIMARY KEY ((commit_type,table_id), commit_id, range_id, file_path)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'meta service table data info';

CREATE TABLE test_lakesoul_meta.fragment_value (
  table_id text,
  key_id text,
  value text,
  timestamp bigint,
  PRIMARY KEY (table_id,key_id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'split large column value into fragment values';

CREATE TABLE test_lakesoul_meta.lock_info (
  lock_id text,
  commit_id text,
  PRIMARY KEY (lock_id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'meta service lock info';

CREATE TABLE test_lakesoul_meta.streaming_info (
  table_id text,
  query_id text,
  batch_id bigint,
  timestamp bigint,
  PRIMARY KEY (table_id,query_id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'record streaming info for recovery';

CREATE TABLE test_lakesoul_meta.table_relation (
  short_table_name text,
  table_name text,
  PRIMARY KEY (short_table_name)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'matching short table name';

CREATE TABLE test_lakesoul_meta.material_view (
  view_name text,
  table_name text,
  table_id text,
  relation_tables text,
  sql_text text,
  auto_update boolean,
  view_info text,
  PRIMARY KEY (view_name)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'material view info';

CREATE TABLE test_lakesoul_meta.material_relation (
  table_id text,
  table_name text,
  material_views text,
  PRIMARY KEY (table_id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
    AND gc_grace_seconds = 3600
    AND comment = 'material views of table';