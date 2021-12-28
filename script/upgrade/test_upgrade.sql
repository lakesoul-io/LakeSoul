alter table test_lakesoul_meta.table_info add short_table_name text;
alter table test_lakesoul_meta.table_info add is_material_view boolean;

alter table test_lakesoul_meta.undo_log add short_table_name text;
alter table test_lakesoul_meta.undo_log add sql_text text;
alter table test_lakesoul_meta.undo_log add relation_tables text;
alter table test_lakesoul_meta.undo_log add auto_update boolean;
alter table test_lakesoul_meta.undo_log add is_creating_view boolean;


CREATE TABLE test_lakesoul_meta.table_relation (
short_table_name text,
table_name text,
PRIMARY KEY (short_table_name)
) WITH bloom_filter_fp_chance = 0.01
AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
AND gc_grace_seconds = 3600
AND comment = 'material view info';

CREATE TABLE test_lakesoul_meta.material_view (
view_name text,
table_name text,
table_id text,
relation_tables text,
sql_text text,
auto_update boolean,
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
AND comment = 'related material views of table';

