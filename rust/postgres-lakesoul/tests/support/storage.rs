// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical fixtures for LakeSoul's last-contribution and CDC contracts.
//! Missing columns are omitted from actual Parquet schemas, not synthesized
//! as NULL by SQL INSERT. Reference tables describe the independently derived
//! final relations; PostgreSQL is not used as an oracle for merge mechanics.

mod cdc;
mod merge;
mod partition;

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use lakesoul_datafusion::catalog::create_io_config_builder;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::writer::async_writer::{
    AsyncBatchWriter, AsyncSendableMutableLakeSoulWriter,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    OPERATION_TIMEOUT, StartupState, bounded, compare, management_execute,
    reference_execute,
};

impl StartupState {
    pub(super) async fn create_storage_fixtures(&mut self) -> Result<()> {
        merge::seed(self).await?;
        partition::seed(self).await?;
        cdc::seed(self).await
    }

    async fn create_storage_table(
        &mut self,
        name: &str,
        columns: &str,
        range_partitions: &[&str],
        cdc_column: Option<&str>,
    ) -> Result<()> {
        let partitioning = if range_partitions.is_empty() {
            String::new()
        } else {
            format!(" PARTITIONED BY ({})", range_partitions.join(", "))
        };
        let cdc = cdc_column.map_or_else(String::new, |column| {
            format!(", 'use_cdc' 'true', 'cdc_column' '{column}'")
        });
        let sql = format!(
            "CREATE EXTERNAL TABLE lakesoul.{}.{name} ({columns}) \
             STORED AS LAKESOUL{partitioning} LOCATION '{}/{}/{name}' \
             OPTIONS ('file_format' 'parquet', 'hashBucketNum' '1'{cdc})",
            self.namespace,
            self.warehouse_url.trim_end_matches('/'),
            self.namespace,
        );
        management_execute(&self.management, &sql).await?;
        // Register before writing: any subsequent fixture failure must still
        // remove this table's metadata and files through the normal teardown.
        self.created_tables.push(name.to_owned());
        Ok(())
    }

    async fn reference_storage_table(
        &self,
        name: &str,
        columns: &str,
        final_values: &str,
    ) -> Result<()> {
        let client = self.reference_client.as_ref().context("reference client")?;
        reference_execute(
            client,
            &format!(
                "CREATE TABLE \"{}\".{name} ({columns}); \
                 INSERT INTO \"{}\".{name} VALUES {final_values}",
                self.reference_schema, self.reference_schema,
            ),
        )
        .await
    }

    async fn write_storage_batch(&self, name: &str, batch: RecordBatch) -> Result<()> {
        bounded(
            OPERATION_TIMEOUT,
            "commit physical storage fixture",
            async {
                self.write_storage_batch_inner(name, batch)
                    .await
                    .map_err(|error| format!("{error:#}"))
            },
        )
        .await
        .with_context(|| format!("physical fixture table={name}"))
    }

    async fn write_storage_batch_inner(
        &self,
        name: &str,
        batch: RecordBatch,
    ) -> Result<()> {
        let info = self
            .meta
            .get_table_info_by_table_name(name, &self.namespace)
            .await
            .map_err(|error| anyhow!("{error:?}"))?
            .with_context(|| format!("missing fixture table {name}"))?;
        let (ranges, _) = info
            .partitions
            .split_once(';')
            .context("fixture metadata partition syntax")?;
        let range_columns: BTreeSet<_> =
            ranges.split(',').filter(|s| !s.is_empty()).collect();
        let physical_names: BTreeSet<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| !range_columns.contains(name.as_str()))
            .collect();
        let names: Vec<_> = physical_names.iter().map(String::as_str).collect();
        let expected = physical_rows(&batch, &names)?;
        let before: BTreeSet<_> = self.storage_files(name).await?.into_iter().collect();
        let table = LakeSoulTable::for_namespace_and_name(
            &self.namespace,
            name,
            Some(Arc::clone(&self.meta)),
        )
        .await
        .map_err(|error| anyhow!("{error:?}"))?;
        let config = create_io_config_builder(
            Arc::clone(&self.meta),
            Some(name),
            false,
            &self.namespace,
            Default::default(),
            Default::default(),
        )
        .await
        .map_err(|error| anyhow!("{error:?}"))?
        .with_schema(batch.schema())
        .with_physical_format(PhysicalFormat::Parquet)
        .build();
        let mut writer = AsyncSendableMutableLakeSoulWriter::from_io_config(config)
            .await
            .map_err(|error| anyhow!("{error:?}"))?;
        writer
            .write_record_batch(batch)
            .await
            .map_err(|error| anyhow!("{error:?}"))?;
        let outputs = Box::new(writer)
            .flush_and_close()
            .await
            .map_err(|error| anyhow!("{error:?}"))?;
        table
            .commit_flush_result(outputs)
            .await
            .map_err(|error| anyhow!("{error:?}"))?;

        let after = self.storage_files(name).await?;
        let new_files: Vec<_> = after
            .iter()
            .filter(|file| !before.contains(*file))
            .collect();
        ensure!(
            !new_files.is_empty(),
            "{name}: write published no new active files"
        );
        ensure!(
            before.iter().all(|file| after.contains(file)),
            "{name}: fixture commit unexpectedly replaced earlier files"
        );
        let mut actual = Vec::new();
        for file in &new_files {
            let path = local_file(file)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
                .with_context(|| format!("{name}: read physical Parquet {file}"))?;
            let stored_names: BTreeSet<_> = builder
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            ensure!(
                stored_names == physical_names,
                "{name}: physical columns expected {physical_names:?}, actual {stored_names:?}, file={file}"
            );
            for stored_batch in builder.build()? {
                actual.extend(physical_rows(&stored_batch?, &names)?);
            }
        }
        compare::compare_rows(&actual, &expected, false).with_context(|| {
            format!("{name}: raw persisted rows differ from this commit's input")
        })?;
        eprintln!(
            "physical fixture proof: table={name} new_files={} columns={physical_names:?} rows={} exact_raw_parquet=true",
            new_files.len(),
            expected.len(),
        );
        Ok(())
    }

    async fn storage_files(&self, name: &str) -> Result<Vec<String>> {
        self.meta
            .get_data_files_by_table_name(name, &self.namespace)
            .await
            .map_err(|error| anyhow!("{error:?}"))
    }

    async fn prove_storage_table(
        &self,
        name: &str,
        expected_partitions: usize,
        min_commits_per_partition: usize,
    ) -> Result<()> {
        bounded(OPERATION_TIMEOUT, "prove published storage versions", async {
            let table = self.meta.get_table_info_by_table_name(name, &self.namespace)
                .await.map_err(|error| anyhow!("{error:?}"))?
                .with_context(|| format!("missing fixture metadata {name}"))?;
            let partitions = self.meta.get_all_partition_info(&table.table_id)
                .await.map_err(|error| anyhow!("{error:?}"))?;
            ensure!(partitions.len() == expected_partitions,
                "{name}: expected {expected_partitions} partitions, actual {}", partitions.len());
            for partition in &partitions {
                ensure!(partition.snapshot.len() >= min_commits_per_partition,
                    "{name}: partition={} only {} active commits; expected >={min_commits_per_partition}",
                    partition.partition_desc, partition.snapshot.len());
            }
            let files = self.storage_files(name).await?;
            ensure!(files.len() >= expected_partitions * min_commits_per_partition,
                "{name}: too few active physical files for separate commits: {}", files.len());
            for file in &files {
                local_file(file)?;
            }
            eprintln!(
                "storage fixture proof: table={name} partitions={} commits_per_partition={:?} active_files={} existing_local_parquet=true",
                partitions.len(), partitions.iter().map(|part| part.snapshot.len()).collect::<Vec<_>>(), files.len(),
            );
            Ok::<_, anyhow::Error>(())
        }).await
    }
}

fn local_file(file: &str) -> Result<&Path> {
    let path = Path::new(file.strip_prefix("file://").unwrap_or(file));
    ensure!(
        path.is_absolute() && path.is_file(),
        "not an existing absolute local file: {file}"
    );
    Ok(path)
}

fn physical_rows(batch: &RecordBatch, names: &[&str]) -> Result<compare::Rows> {
    let columns = names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|index| batch.column(index))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    (0..batch.num_rows())
        .map(|row| {
            columns
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        Ok(None)
                    } else {
                        array_value_to_string(column.as_ref(), row)
                            .map(Some)
                            .map_err(Into::into)
                    }
                })
                .collect()
        })
        .collect()
}
