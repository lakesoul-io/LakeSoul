// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use arrow::datatypes::{SchemaBuilder, SchemaRef};

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::Expr};

use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion_common::{FileTypeWriterOptions, Result};

use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::transform::uniform_schema;

pub struct LakeSoulListingTable {
    listing_table: Arc<ListingTable>,

    lakesoul_io_config: LakeSoulIOConfig,
}

impl LakeSoulListingTable {
    pub fn new(listing_table: Arc<ListingTable>, lakesoul_io_config: LakeSoulIOConfig) -> Self {
        Self {
            listing_table,
            lakesoul_io_config,
        }
    }

    pub async fn new_with_config_and_format(
        session_state: &SessionState,
        lakesoul_io_config: LakeSoulIOConfig,
        file_format: Arc<dyn FileFormat>,
        as_sink: bool,
    ) -> Result<Self> {
        let config = match as_sink {
            false => {
                // Parse the path
                let table_paths = lakesoul_io_config
                    .files
                    .iter()
                    .map(ListingTableUrl::parse)
                    .collect::<Result<Vec<_>>>()?;
                // Create default parquet options
                let object_store_url = table_paths.first().unwrap().object_store();
                let store = session_state.runtime_env().object_store(object_store_url.clone())?;
                let target_schema = uniform_schema(lakesoul_io_config.schema());

                let listing_options = ListingOptions::new(file_format.clone()).with_file_extension(".parquet");
                // .with_table_partition_cols(table_partition_cols);

                let mut objects = vec![];
                for url in &table_paths {
                    objects.push(store.head(url.prefix()).await?);
                }
                // Resolve the schema
                let resolved_schema = file_format.infer_schema(session_state, &store, &objects).await?;

                let mut builder = SchemaBuilder::from(target_schema.fields());
                for field in resolved_schema.fields() {
                    if target_schema.field_with_name(field.name()).is_err() {
                        builder.push(field.clone());
                    }
                }

                ListingTableConfig::new_with_multi_paths(table_paths)
                    .with_listing_options(listing_options)
                    .with_schema(Arc::new(builder.finish()))
            }
            true => {
                let target_schema = uniform_schema(lakesoul_io_config.schema());
                let table_partition_cols = lakesoul_io_config
                    .range_partitions
                    .iter()
                    .map(|col| Ok((col.clone(), target_schema.field_with_name(col)?.data_type().clone())))
                    .collect::<Result<Vec<_>>>()?;

                let listing_options = ListingOptions::new(file_format.clone())
                    .with_file_extension(".parquet")
                    .with_table_partition_cols(table_partition_cols)
                    .with_insert_mode(datafusion::datasource::listing::ListingTableInsertMode::AppendNewFiles);
                let prefix =
                    ListingTableUrl::parse_create_local_if_not_exists(lakesoul_io_config.prefix.clone(), true)?;

                ListingTableConfig::new(prefix)
                    .with_listing_options(listing_options)
                    .with_schema(target_schema)
            }
        };

        // Create a new TableProvider
        let listing_table = Arc::new(ListingTable::try_new(config)?);

        Ok(Self {
            listing_table,
            lakesoul_io_config,
        })
    }

    fn options(&self) -> &ListingOptions {
        self.listing_table.options()
    }

    fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.listing_table.table_paths()
    }
}

#[async_trait]
impl TableProvider for LakeSoulListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.listing_table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.lakesoul_io_config.primary_keys.is_empty() {
            if self.lakesoul_io_config.parquet_filter_pushdown {
                Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
            } else {
                Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
            }
        } else {
            filters
                .iter()
                .map(|f| {
                    if let Ok(cols) = f.to_columns() {
                        if self.lakesoul_io_config.parquet_filter_pushdown
                            && cols
                                .iter()
                                .all(|col| self.lakesoul_io_config.primary_keys.contains(&col.name))
                        {
                            Ok(TableProviderFilterPushDown::Inexact)
                        } else {
                            Ok(TableProviderFilterPushDown::Unsupported)
                        }
                    } else {
                        Ok(TableProviderFilterPushDown::Unsupported)
                    }
                })
                .collect()
        }
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check that the schema of the plan matches the schema of this table.
        // if !self
        //     .schema()
        //     .logically_equivalent_names_and_types(&input.schema())
        // {
        //     return plan_err!(
        //         // Return an error if schema of the input query does not match with the table schema.
        //         "Inserting query must have the same schema with the table."
        //     );
        // }

        let table_path = &self.listing_table.table_paths()[0];
        // Get the object store for the table path.
        let _store = state.runtime_env().object_store(table_path)?;

        // let file_list_stream = pruned_partition_list(
        //     state,
        //     store.as_ref(),
        //     table_path,
        //     &[],
        //     &self.options.file_extension,
        //     &self.options.table_partition_cols,
        // )
        // .await?;

        // let file_groups = file_list_stream.try_collect::<Vec<_>>().await?;
        //if we are writing a single output_partition to a table backed by a single file
        //we can append to that file. Otherwise, we can write new files into the directory
        //adding new files to the listing table in order to insert to the table.
        let _input_partitions = input.output_partitioning().partition_count();
        // let writer_mode = match self.options().insert_mode {
        //     ListingTableInsertMode::AppendToFile => {
        //         if input_partitions > file_groups.len() {
        //             return Err(DataFusionError::Plan(
        //                 format!("Cannot append {input_partitions} partitions to {} files!",
        //                     file_groups.len())
        //             ));
        //         }

        //         datafusion::datasource::file_format::write::FileWriterMode::Append
        //     }
        //     ListingTableInsertMode::AppendNewFiles => {
        //         datafusion::datasource::file_format::write::FileWriterMode::PutMultipart
        //     }
        //     ListingTableInsertMode::Error => {
        //         return Err(DataFusionError::Plan(
        //             format!("Invalid plan attempting write to table with TableWriteMode::Error!")
        //         ));
        //     }
        // };

        let file_format = self.options().format.as_ref();

        let file_type_writer_options = match &self.options().file_type_write_options {
            Some(opt) => opt.clone(),
            None => FileTypeWriterOptions::build_default(&file_format.file_type(), state.config_options())?,
        };

        // Sink related option, apart from format
        let config = FileSinkConfig {
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_groups: vec![],
            output_schema: self.schema(),
            table_partition_cols: self.options().table_partition_cols.clone(),
            writer_mode: datafusion::datasource::file_format::write::FileWriterMode::PutMultipart,
            // A plan can produce finite number of rows even if it has unbounded sources, like LIMIT
            // queries. Thus, we can check if the plan is streaming to ensure file sink input is
            // unbounded. When `unbounded_input` flag is `true` for sink, we occasionally call `yield_now`
            // to consume data at the input. When `unbounded_input` flag is `false` (e.g non-streaming data),
            // all of the data at the input is sink after execution finishes. See discussion for rationale:
            // https://github.com/apache/arrow-datafusion/pull/7610#issuecomment-1728979918
            unbounded_input: false,
            single_file_output: self.options().single_file,
            overwrite,
            file_type_writer_options,
        };

        let unsorted: Vec<Vec<Expr>> = vec![];
        let order_requirements = if self.options().file_sort_order != unsorted {
            // if matches!(
            //     self.options().insert_mode,
            //     ListingTableInsertMode::AppendToFile
            // ) {
            //     return Err(DataFusionError::Plan(
            //         format!("Cannot insert into a sorted ListingTable with mode append!")
            //     ));
            // }
            // // Multiple sort orders in outer vec are equivalent, so we pass only the first one
            // let ordering = self
            //     .try_create_output_ordering()?
            //     .get(0)
            //     .ok_or(DataFusionError::Internal(
            //         "Expected ListingTable to have a sort order, but none found!".into(),
            //     ))?
            //     .clone();
            // // Converts Vec<Vec<SortExpr>> into type required by execution plan to specify its required input ordering
            // Some(
            //     ordering
            //         .into_iter()
            //         .map(PhysicalSortRequirement::from)
            //         .collect::<Vec<_>>(),
            // )
            todo!()
        } else {
            None
        };

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }
}
