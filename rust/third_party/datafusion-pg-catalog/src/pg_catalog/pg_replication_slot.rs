use crate::pg_catalog::empty_table::EmptyTable;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub(crate) fn pg_replication_slots() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("slot_name", DataType::Utf8, true),
        Field::new("plugin", DataType::Utf8, true),
        Field::new("slot_type", DataType::Utf8, true),
        Field::new("datoid", DataType::Int32, true),
        Field::new("database", DataType::Utf8, true),
        Field::new("temporary", DataType::Boolean, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("active_pid", DataType::Int32, true),
        Field::new("xmin", DataType::Int32, true),
        Field::new("catalog_xmin", DataType::Int32, true),
        Field::new("restart_lsn", DataType::Utf8, true), // TODO: is this the correct type to use?
        Field::new("confirmed_flush_lsn", DataType::Utf8, true), // TODO: is this the correct type to use?
        Field::new("wal_status", DataType::Utf8, true),
        Field::new("safe_wal_size", DataType::Int64, true),
        Field::new("two_phase", DataType::Boolean, false),
        Field::new("conflicting", DataType::Boolean, false),
    ]));

    EmptyTable::new(schema)
}
