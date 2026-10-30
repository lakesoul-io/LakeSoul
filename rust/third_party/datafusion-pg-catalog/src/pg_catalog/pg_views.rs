use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::empty_table::EmptyTable;

pub fn pg_views() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schemaname", DataType::Utf8, true),
        Field::new("viewname", DataType::Utf8, true),
        Field::new("viewowner", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
    ]));
    EmptyTable::new(schema)
}

pub fn pg_matviews() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schemaname", DataType::Utf8, true),
        Field::new("matviewname", DataType::Utf8, true),
        Field::new("matviewowner", DataType::Utf8, true),
        Field::new("tablespace", DataType::Utf8, true),
        Field::new("hasindexes", DataType::Boolean, true),
        Field::new("ispopulated", DataType::Boolean, true),
        Field::new("definition", DataType::Utf8, true),
    ]));

    EmptyTable::new(schema)
}

pub fn pg_stat_user_tables() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("relid", DataType::Int32, false),
        Field::new("schemaname", DataType::Utf8, false),
        Field::new("relname", DataType::Utf8, false),
        Field::new("seq_scan", DataType::Int64, false),
        Field::new(
            "last_seq_scan",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("seq_tup_read", DataType::Int64, false),
        Field::new("idx_scan", DataType::Int64, false),
        Field::new(
            "last_idx_scan",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("idx_tup_fetch", DataType::Int64, false),
        Field::new("n_tup_ins", DataType::Int64, false),
        Field::new("n_tup_upd", DataType::Int64, false),
        Field::new("n_tup_del", DataType::Int64, false),
        Field::new("n_tup_hot_upd", DataType::Int64, false),
        Field::new("n_tup_newpage_upd", DataType::Int64, false),
        Field::new("n_live_tup", DataType::Int64, false),
        Field::new("n_dead_tup", DataType::Int64, false),
        Field::new("n_mod_since_analyze", DataType::Int64, false),
        Field::new("n_ins_since_vacuum", DataType::Int64, false),
        Field::new(
            "last_vacuum",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "last_autovacuum",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "last_analyze",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "last_autoanalyze",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("vacuum_count", DataType::Int64, false),
        Field::new("autovacuum_count", DataType::Int64, false),
        Field::new("analyze_count", DataType::Int64, false),
        Field::new("autoanalyze_count", DataType::Int64, false),
        Field::new("total_vacuum_time", DataType::Float64, false),
        Field::new("total_autovacuum_time", DataType::Float64, false),
        Field::new("total_analyze_time", DataType::Float64, false),
        Field::new("total_autoanalyze_time", DataType::Float64, false),
    ]));

    EmptyTable::new(schema)
}
