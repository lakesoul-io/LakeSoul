// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod update_tests {
    use std::collections::{HashSet, HashMap};
    use std::env;
    use std::path::PathBuf;
    use std::{sync::Arc, time::SystemTime};

    use arrow::util::pretty::print_batches;
    use datafusion::prelude::DataFrame;
    use datasource::parquet_source::LakeSoulParquetProvider;

    use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
    use datafusion::assert_batches_eq;
    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfigBuilder, create_session_context};
    use lakesoul_io::lakesoul_reader::{SyncSendableMutableLakeSoulReader, LakeSoulReader};
    use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;
    use arrow::{record_batch::RecordBatch, array::{Int32Array, ArrayRef}};
    use tokio::runtime::Builder;
    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

    use crate::error::Result;
    use proto::proto::entity::{DataCommitInfo, PartitionInfo};

    use crate::catalog::{create_table, create_io_config_builder, commit_data};
    use crate::transaction::{Transaction, DataFileInfo};

    async fn init_table(batch: RecordBatch, table_name: &str, client: MetaDataClientRef) -> Result<()> {
        let schema = batch.schema();
        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(schema.clone());
                // .with_primary_keys(pks);
        create_table(client.clone(), table_name, builder.build()).await?;
        execute_append(batch, table_name, client).await
    }

    async fn execute_append(batch: RecordBatch, table_name: &str, client: MetaDataClientRef) -> Result<()> {
        let file = [env::temp_dir().to_str().unwrap(), table_name, format!("{}.parquet", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string()).as_str()].iter().collect::<PathBuf>().to_str().unwrap().to_string();
        let builder = create_io_config_builder(client.clone(), Some(table_name)).await?.with_file(file.clone()).with_schema(batch.schema());
        let config = builder.clone().build();

        let writer = SyncSendableMutableLakeSoulWriter::try_new(config, Builder::new_current_thread().build().unwrap()).unwrap();
        writer.write_batch(batch)?;
        writer.flush_and_close()?;
        commit_data(client, table_name, builder.clone().build()).await
    }


    fn split_metadata_and_data_predicates() -> Result<(String, String)> {
        Ok(("".to_string(), "".to_string()))
    }


    async fn perform_update(client: MetaDataClientRef, update_condition: Option<&str>) -> Result<()> {
        let mut transaction = Transaction::new();
        transaction.table_info = client.get_table_info_by_table_name("basic_case", "default").await?;

        // get candidateFiles
        let (metadata_predicates, data_predicates) = split_metadata_and_data_predicates()?;
        let candidate_files = transaction.filter_files(client.clone(), &metadata_predicates, &data_predicates)?;
        let path_to_file = candidate_files.iter().map(|f| (f.path.clone(), f.clone())).collect::<HashMap<_, _>>();

        let (add_files, expire_files) : (Vec<DataFileInfo>, Vec<DataFileInfo>) = if candidate_files.is_empty() {
            // Case 1: Do nothing if no row qualifies the partition predicates
            // that are part of Update condition
            (vec![], vec![])
        } else if data_predicates.is_empty() {
            // Case 2: Update all the rows from the files that are in the specified partitions
            // when the data filter is empty
            let files_to_rewrite = candidate_files.iter().map(|file| file.path.clone()).collect::<Vec<_>>();
            let current = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
            let mut delete_files = candidate_files.iter().map(|file| file.clone()).collect::<Vec<_>>();
            delete_files.iter_mut().for_each(move |file| file.timestamp = current);
            dbg!(&delete_files);
            let rewritten_files = rewrite_files(&mut transaction, files_to_rewrite, path_to_file);
            (rewritten_files, delete_files)
        } else {
            // Case 3: Find all the affected files using the user-specified condition

            // Keep everything from the resolved target except a new LakeSoulFileIndex
            // that only involves the affected files instead of all files.


            // Case 3.1: Do nothing if no row qualifies the UPDATE condition

            // Case 3.2: Delete the old files and generate the new files containing the updated
            // values

            (vec![], vec![])
        };

        if !add_files.is_empty() || !expire_files.is_empty() {
            transaction.commit(add_files, expire_files)?;
        }
        Ok(())
    }

    fn rewrite_files(transaction: &mut Transaction, input_leaf_files: Vec<String>, path_to_file: HashMap<String, DataFileInfo>) -> Vec<DataFileInfo> {
        let rewrite_file_info = 
            input_leaf_files
                .iter()
                .filter_map(|file| path_to_file.get(file))
                .collect::<Vec<&DataFileInfo>>();
        

        todo!()
    }

    async fn execute_update(client: MetaDataClientRef, target: &str, set: &str, condition: Option<&str>) -> Result<()> {
        let builder = create_io_config_builder(client.clone(), Some(target)).await?;
        let mut sess_ctx = create_session_context(&mut builder.clone().build())?;
        
        let provider = LakeSoulParquetProvider::from_config(builder.build())
            .build_with_context(&sess_ctx)
            .await
            .unwrap();
        sess_ctx.register_table(target, Arc::new(provider)).unwrap();
        let sql = format!("insert into {} values (1, 2)", target);
        dbg!(&sql);
        let statement = sess_ctx.state().sql_to_statement(sql.as_str(), "Generic").unwrap();
        let logical_plan = sess_ctx.state().statement_to_plan(statement).await.unwrap();
        dbg!(&logical_plan);
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);
        
        let results = dataframe
            .explain(true, false)
            .unwrap()
            .collect()
            .await
            .unwrap();

        let _ = print_batches(&results);

        
        // perform_update(client, None)?;
        Ok(())
    }

    async fn check_update(
        client: MetaDataClientRef,
        condition: Option<&str>,
        set_clauses: &str,
        table_name: &str,
        selected_cols: Vec<&str>, 
        expected: &[&str],
    ) -> Result<()> {
        execute_update(client.clone(), table_name, set_clauses, condition).await?;
        let builder = create_io_config_builder(client, Some(table_name)).await?;
        let builder = builder
            .with_schema(SchemaRef::new(Schema::new(
                selected_cols.iter().map(|col| Field::new(*col, DataType::Int32, true)).collect::<Vec<_>>()
            )));
        // let builder = if let Some(filters) = filters {
        //     builder.with_filter_str(filters)
        // } else {
        //     builder
        // };
        let mut reader = LakeSoulReader::new(builder.build())?;
        reader.start().await?;
        let result = reader.next_rb().await;
        match result {
            Some(result) => {
                assert_batches_eq!(expected, &[result?]);
                Ok(())
            },
            None => Ok(())
        }
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }


    // #[test]
    async fn test_basic_case() -> Result<()> {
        let table_name = "basic_case";
        let mut client = Arc::new(MetaDataClient::from_env().await?);
        client.meta_cleanup().await?;
        init_table(
            create_batch_i32(vec!["key", "value"], vec![&[2, 1, 1, 0], &[2, 4, 1, 3]]),
            table_name,
            client.clone(),
        ).await?;
        check_update(
            client.clone(),
            None,
            "key = 1, value = 2",
            table_name,
            vec!["key", "value"],
            &[
                "+-----+-------+",
                "| key | value |",
                "+-----+-------+",
                "| 1   | 2     |",
                "| 1   | 2     |",
                "| 1   | 2     |",
                "| 1   | 2     |",
                "+-----+-------+",],
        ).await
    }
}