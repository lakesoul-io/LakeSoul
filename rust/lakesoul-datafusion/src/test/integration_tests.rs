// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod integration_tests {
    use std::{path::Path, sync::Arc};

    use arrow::util::pretty::print_batches;
    use datafusion::{execution::context::SessionContext, datasource::{TableProvider, file_format::{FileFormat, csv::CsvFormat}, listing::{ListingOptions, ListingTableUrl, ListingTableConfig, ListingTable}}};
    use lakesoul_io::lakesoul_io_config::{create_session_context_with_planner, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::MetaDataClient;

    use crate::{error::{Result, LakeSoulError}, benchmarks::tpch::{TPCH_TABLES, get_tbl_tpch_table_schema, get_tpch_table_schema, get_tbl_tpch_table_primary_keys}, lakesoul_table::LakeSoulTable, catalog::{create_io_config_builder, create_table}, planner::query_planner::LakeSoulQueryPlanner};

    async fn get_table(
        ctx: &SessionContext,
        table: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let path = get_tpch_data_path()?;

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) = {
            let path = format!("{path}/{table}.tbl");
            let format = CsvFormat::default()
                .with_delimiter(b'|')
                .with_has_header(false);

            (Arc::new(format), path, ".tbl")
        };

        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_collect_stat(state.config().collect_statistics());

        let table_path = ListingTableUrl::parse(path)?;
        let config = ListingTableConfig::new(table_path).with_listing_options(options);

        let config =  {
            config.with_schema(Arc::new(get_tbl_tpch_table_schema(table)))
        };
        Ok(Arc::new(ListingTable::try_new(config)?))

    }
    
    fn get_tpch_data_path() -> Result<String> {
        let path =
            std::env::var("TPCH_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
        if !Path::new(&path).exists() {
            return Err(LakeSoulError::Internal(format!(
                "Benchmark data not found (set TPCH_DATA env var to override): {}",
                path
            )));
        }
        Ok(path)
    }


    #[tokio::test]
    async fn load_tpch_data() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let builder = create_io_config_builder(client.clone(), None, false, "default").await?;
        let ctx =
            create_session_context_with_planner(&mut builder.clone().build(), Some(LakeSoulQueryPlanner::new_ref()))?;
        
        for table in TPCH_TABLES {
            let table_provider = get_table(&ctx, table).await?;
            ctx.register_table(*table, table_provider)?;
            let dataframe = ctx.sql(format!("select * from {}", table).as_str())
                .await?;

            let schema = get_tpch_table_schema(table);

            let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(Arc::new(schema))
                .with_primary_keys(get_tbl_tpch_table_primary_keys(table));

            create_table(client.clone(), &table, builder.build()).await?;
            let lakesoul_table = LakeSoulTable::for_name(table).await?;
            lakesoul_table.upsert_dataframe(dataframe).await?;
            dbg!(table);
        }

        Ok(())
    }
}