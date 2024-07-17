// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod helpers;

use std::{ops::Deref, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use arrow_cast::pretty::pretty_format_batches;
use datafusion::sql::TableReference;
use datafusion::{
    dataframe::DataFrame,
    datasource::TableProvider,
    execution::context::{SessionContext, SessionState},
    logical_expr::LogicalPlanBuilder,
};
use lakesoul_io::{lakesoul_io_config::create_session_context_with_planner, lakesoul_reader::RecordBatch};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use proto::proto::entity::TableInfo;
use tracing::debug;

use crate::{
    catalog::{create_io_config_builder, parse_table_info_partitions, LakeSoulTableProperty},
    error::Result,
    planner::query_planner::LakeSoulQueryPlanner,
    serialize::arrow_java::schema_from_metadata_str,
};

use crate::datasource::table_provider::LakeSoulTableProvider;

pub struct LakeSoulTable {
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
    table_name: String,
    table_schema: SchemaRef,
    primary_keys: Vec<String>,
    range_partitions: Vec<String>,
    properties: LakeSoulTableProperty,
}

impl LakeSoulTable {
    pub async fn for_path(_path: String) -> Result<Self> {
        todo!()
    }

    pub async fn for_path_snapshot(path: String) -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_info = client.get_table_info_by_table_path(&path).await?;
        Self::try_new_with_client_and_table_info(client, table_info).await
    }

    pub async fn for_name(table_name: &str) -> Result<Self> {
        Self::for_namespace_and_name("default", table_name).await
    }

    pub async fn for_namespace_and_name(namespace: &str, table_name: &str) -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_info = client.get_table_info_by_table_name(table_name, namespace).await?;
        Self::try_new_with_client_and_table_info(client, table_info).await
    }

    pub async fn try_new_with_client_and_table_info(client: MetaDataClientRef, table_info: TableInfo) -> Result<Self> {
        let table_schema = schema_from_metadata_str(&table_info.table_schema);

        let table_name = table_info.table_name.clone();
        let properties = serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties)?;
        let (range_partitions, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone())?;

        Ok(Self {
            client,
            table_info: Arc::new(table_info),
            table_name,
            table_schema,
            primary_keys: hash_partitions,
            range_partitions,
            properties,
        })
    }

    pub async fn upsert_dataframe(&self, dataframe: DataFrame) -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let builder = create_io_config_builder(client, None, false, self.table_namespace()).await?;
        let sess_ctx =
            create_session_context_with_planner(&mut builder.clone().build(), Some(LakeSoulQueryPlanner::new_ref()))?;

        let schema: Schema = dataframe.schema().into();
        let logical_plan = LogicalPlanBuilder::insert_into(
            dataframe.into_unoptimized_plan(),
            TableReference::partial(self.table_namespace().to_string(), self.table_name().to_string()),
            &schema,
            false,
        )?
        .build()?;
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);

        let results = dataframe
            // .explain(true, false)?
            .collect()
            .await?;
        // dbg!(&results);

        Ok(())
    }

    pub async fn execute_upsert(&self, record_batch: RecordBatch) -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let builder = create_io_config_builder(client, None, false, self.table_namespace()).await?;
        let sess_ctx =
            create_session_context_with_planner(&mut builder.clone().build(), Some(LakeSoulQueryPlanner::new_ref()))?;

        let schema = record_batch.schema();
        let logical_plan = LogicalPlanBuilder::insert_into(
            sess_ctx.read_batch(record_batch)?.into_unoptimized_plan(),
            TableReference::partial(self.table_namespace().to_string(), self.table_name().to_string()),
            schema.deref(),
            false,
        )?
        .build()?;
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);

        let results = dataframe
            // .explain(true, false)?
            .collect()
            .await?;

        debug!("{}", pretty_format_batches(&results)?);
        Ok(())
    }

    pub async fn to_dataframe(&self, context: &SessionContext) -> Result<DataFrame> {
        let config_builder =
            create_io_config_builder(self.client(), Some(self.table_name()), true, self.table_namespace()).await?;
        let provider = Arc::new(
            LakeSoulTableProvider::try_new(
                &context.state(),
                self.client(),
                config_builder.build(),
                self.table_info(),
                false,
            )
            .await?,
        );
        Ok(context.read_table(provider)?)
    }

    pub async fn as_sink_provider(&self, session_state: &SessionState) -> Result<Arc<dyn TableProvider>> {
        let config_builder =
            create_io_config_builder(self.client(), Some(self.table_name()), false, self.table_namespace())
                .await?
                .with_prefix(self.table_info.table_path.clone());
        Ok(Arc::new(
            LakeSoulTableProvider::try_new(
                session_state,
                self.client(),
                config_builder.build(),
                self.table_info(),
                true,
            )
            .await?,
        ))
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    pub fn primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn range_partitions(&self) -> &Vec<String> {
        &self.range_partitions
    }

    pub fn hash_bucket_num(&self) -> usize {
        self.properties.hash_bucket_num.unwrap_or(1)
    }

    pub fn table_namespace(&self) -> &str {
        &self.table_info.table_namespace
    }

    pub fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}
