// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use async_trait::async_trait;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};

use datafusion::physical_plan::ExecutionPlan;
use datafusion::{execution::context::SessionState, logical_expr::Expr};

use lakesoul_io::datasource::file_format::LakeSoulParquetFormat;
use lakesoul_io::datasource::listing::LakeSoulListingTable;
use lakesoul_io::lakesoul_io_config::LakeSoulIOConfig;
use proto::proto::entity::TableInfo;

use crate::catalog::parse_table_info_partitions;
use crate::serialize::arrow_java::schema_from_metadata_str;

use super::file_format::LakeSoulMetaDataParquetFormat;

/// Reads data from LakeSoul
///
/// # Features
///
/// 1. Merges schemas if the files have compatible but not indentical schemas
///
/// 2. Hive-style partitioning support, where a path such as
/// `/files/date=1/1/2022/data.parquet` is injected as a `date` column.
///
/// 3. Projection pushdown for formats that support it such as such as
/// Parquet
///
/// ```
pub struct LakeSoulTableProvider {
    listing_table: Arc<LakeSoulListingTable>,
    table_info: Arc<TableInfo>,
    schema: SchemaRef,
    primary_keys: Vec<String>,
}

impl LakeSoulTableProvider {
    pub async fn try_new(
        session_state: &SessionState,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
        as_sink: bool,
    ) -> crate::error::Result<Self> {
        let schema = schema_from_metadata_str(&table_info.table_schema);
        let (_, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone());

        let file_format: Arc<dyn FileFormat> = match as_sink {
            true => {
                Arc::new(LakeSoulMetaDataParquetFormat::new(Arc::new(ParquetFormat::new()), table_info.clone()).await?)
            }
            false => Arc::new(LakeSoulParquetFormat::new(
                Arc::new(ParquetFormat::new()),
                lakesoul_io_config.clone(),
            )),
        };
        Ok(Self {
            listing_table: Arc::new(
                LakeSoulListingTable::new_with_config_and_format(
                    session_state,
                    lakesoul_io_config,
                    file_format,
                    as_sink,
                )
                .await?,
            ),
            table_info,
            schema,
            primary_keys: hash_partitions,
        })
    }

    fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }
}

#[async_trait]
impl TableProvider for LakeSoulTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        self.listing_table.supports_filters_pushdown(filters)
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.insert_into(state, input, overwrite).await
    }
}
