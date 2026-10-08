// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The fixtures and analysis helpers shared by the analyzer unit tests.

pub(crate) use std::sync::Arc;

pub(crate) use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub(crate) use datafusion::datasource::memory::MemTable;
pub(crate) use datafusion::prelude::SessionContext;
pub(crate) use lakesoul_io::file_format::PhysicalFormat;

use super::*;

pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

pub(crate) fn source_table(name: &str) -> IvmTable {
    IvmTable {
        table_id: format!("table_{name}"),
        table_name: name.to_string(),
        namespace: "default".to_string(),
        table_path: format!("file:///tmp/{name}"),
        schema: schema(),
        primary_keys: vec!["k".to_string()],
        bucket_columns: Vec::new(),
        hash_bucket_num: "1".to_string(),
        cdc_column: None,
        file_format: PhysicalFormat::Parquet,
    }
}

pub(crate) async fn plan(sql: &str) -> LogicalPlan {
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(table)).unwrap();
    ctx.sql(sql).await.unwrap().logical_plan().clone()
}

pub(crate) fn request() -> AnalyzeRequest {
    AnalyzeRequest {
        view_id: "view_1".to_string(),
        mv_table_id: "table_mv".to_string(),
        state_table_id: Some("table_state".to_string()),
    }
}

pub(crate) async fn analyze(sql: &str) -> Result<AnalyzedView> {
    let tables = HashMap::from([("src".to_string(), source_table("src"))]);
    analyze_select(&plan(sql).await, &tables, &request())
}

/// The rendered predicate without grouping parentheses or identifier
/// quotes, so the assertions do not depend on the unparser's formatting.
pub(crate) fn normalized(filter: Option<&str>) -> Option<String> {
    filter.map(|filter| filter.replace(['(', ')', '"'], ""))
}

pub(crate) async fn analyze_optimized(sql: &str) -> Result<AnalyzedView> {
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
    ctx.register_table("src", Arc::new(table)).unwrap();
    let tables = HashMap::from([("src".to_string(), source_table("src"))]);
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    let optimized = ctx.state().optimize(&plan).unwrap();
    analyze_select(&optimized, &tables, &request())
}

pub(crate) async fn analyze_multi(
    sql: &str,
    tables: Vec<IvmTable>,
) -> Result<AnalyzedView> {
    let ctx = SessionContext::new();
    let mut map = HashMap::new();
    for table in &tables {
        let mem = MemTable::try_new(table.schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table(table.table_name.as_str(), Arc::new(mem))
            .unwrap();
        map.insert(table.table_name.clone(), table.clone());
    }
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    let optimized = ctx.state().optimize(&plan).unwrap();
    analyze_select(&optimized, &map, &request())
}
