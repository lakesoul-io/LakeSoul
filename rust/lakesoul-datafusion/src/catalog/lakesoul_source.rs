// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, any::Any};

use lakesoul_io::datafusion::logical_expr::TableProviderFilterPushDown;
use lakesoul_io::datafusion::physical_plan::ExecutionPlan;
use lakesoul_io::lakesoul_io_config::create_session_context;
use lakesoul_io::{datafusion, arrow};

use lakesoul_io::datasource::parquet_source::LakeSoulParquetProvider;
use lakesoul_metadata::{MetaDataClientRef, MetaDataClient};

use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::{datasource::TableProvider, logical_expr::TableType, prelude::Expr};

use arrow::datatypes::SchemaRef;

use async_trait::async_trait;


use super::create_io_config_builder;

#[derive(Debug, Clone)]
pub struct LakeSoulSourceProvider {
    table_name: String,
    inner: LakeSoulParquetProvider,
    postgres_config: Option<String>,
}

impl LakeSoulSourceProvider {
    pub async fn new(table_name: &str) -> crate::error::Result<Self> {
        Self::create_provider(table_name, Arc::new(MetaDataClient::from_env().await?), None).await
    }

    pub async fn new_with_postgres_config(table_name: &str, postgres_config: &str) -> crate::error::Result<Self> {
        Self::create_provider(table_name, Arc::new(MetaDataClient::from_config(postgres_config.to_string()).await?), Some(postgres_config.to_string())).await
    }

    pub async fn create_provider(table_name: &str, client: MetaDataClientRef, postgres_config: Option<String>) -> crate::error::Result<Self> {
        let io_config = create_io_config_builder(client, Some(table_name))
            .await?
            .build();
        let context = create_session_context(&mut io_config.clone())?;
        let inner = LakeSoulParquetProvider::from_config(io_config).build_with_context(&context).await?;
        let table_name = table_name.to_string();
        Ok(Self {
            table_name,
            inner,
            postgres_config,
        })
    }
}

#[async_trait]
impl TableProvider for LakeSoulSourceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projections: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(_state, projections, _filters, _limit).await
    }
}

