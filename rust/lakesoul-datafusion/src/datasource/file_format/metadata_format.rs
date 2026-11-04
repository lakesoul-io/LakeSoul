// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::datasource::file_format::FileFormat`] implementation
//! for LakeSoul tables: the metadata/listing callback DataFusion uses for
//! LakeSoul tables, plus the planning of their write path.

use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::Statistics;
use datafusion::common::not_impl_err;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileSource;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexRequirement;
use datafusion::{
    datasource::physical_plan::{FileScanConfig, FileSinkConfig},
    error::Result as DFResult,
    physical_plan::ExecutionPlan,
};
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::file_format::{
    LakeSoulFormatRegistry, PhysicalFormat, merge_schema_refs,
};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::TableInfo;
use object_store::{ObjectMeta, ObjectStore};
use rootcause::compat::boxed_error::IntoBoxedError;

use super::insert_only::InsertOnly;
use super::write_exec::{LakeSoulCommitExec, LakeSoulWriteExec};
use crate::Result;
use crate::datasource::table_provider::LakeSoulTableProvider;

/// The LakeSoul metadata [`FileFormat`]: it presents a LakeSoul table to
/// DataFusion's listing machinery and plans the LakeSoul write path as the
/// [`LakeSoulWriteExec`] / [`LakeSoulCommitExec`] node pair.
///
/// It is deliberately *not* a physical file reader. A LakeSoul table can hold
/// files of several physical formats at the same time (historical Parquet
/// files next to newer Vortex files), so everything that touches data files
/// dispatches through [`LakeSoulFormatRegistry`] on the format of the file at
/// hand: schema inference groups the objects by their own format, and scan
/// planning (`LakeSoulTableProvider::scan`) and the write node do the same.
/// The table's declared format only answers the table-level questions
/// DataFusion asks without a file at hand (`get_ext`, `compression_type`).
pub struct LakeSoulMetaDataFormat {
    /// The metadata client.
    client: MetaDataClientRef,
    /// The table info.
    table_info: Arc<TableInfo>,
    /// The io config.
    conf: LakeSoulIOConfig,
    /// The readers/writers of the physical formats a data file may use.
    format_registry: Arc<LakeSoulFormatRegistry>,
    /// The format the table declares for its data files. It is the default for
    /// table-level answers and for files whose path carries no known
    /// extension; never assume it for a specific file.
    physical_format: PhysicalFormat,
}

impl Debug for LakeSoulMetaDataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulMetaDataFormat")
            .field("physical_format", &self.physical_format)
            .finish()
    }
}

impl LakeSoulMetaDataFormat {
    /// Builds the metadata format of one table.
    ///
    /// The table's declared format answers the table-level questions. The io
    /// config is not consulted: one built from files alone would answer with
    /// the extension of a single data file (or the enum default when it holds
    /// no file at all), which is a per-file fact, not the table's
    /// declaration.
    ///
    /// Fails when the stored table properties cannot be read or declare a
    /// format this build cannot write: such a table must not be presented as
    /// a Parquet one.
    pub fn new(
        client: MetaDataClientRef,
        table_info: Arc<TableInfo>,
        conf: LakeSoulIOConfig,
        format_registry: Arc<LakeSoulFormatRegistry>,
    ) -> Result<Self> {
        let table = format!("{}.{}", table_info.table_namespace, table_info.table_name);
        let physical_format = crate::catalog::table_file_format(&table_info.properties)
            .map_err(|report| report.attach(table))?;
        debug!(
            "LakeSoulMetaDataFormat::new, physical_format: {}, conf: {:?}",
            physical_format, conf
        );
        Ok(Self {
            client,
            table_info,
            conf,
            format_registry,
            physical_format,
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    /// The reader/writer of the table's declared physical format.
    fn table_format(&self) -> Arc<dyn FileFormat> {
        self.format_registry.file_format(self.physical_format)
    }

    /// The physical format of one data file, taken from its path so a table
    /// holding files of several formats dispatches per file.
    fn format_for_path(&self, path: &str) -> PhysicalFormat {
        PhysicalFormat::from_extension(path).unwrap_or_else(|_| {
            debug!(
                "file '{}' has no known physical format; assuming {}",
                path, self.physical_format
            );
            self.physical_format
        })
    }
}

#[async_trait]
impl FileFormat for LakeSoulMetaDataFormat {
    fn get_ext(&self) -> String {
        self.physical_format.extension().to_string()
    }

    /// The extension/compression pair DataFusion lists and writes with. Both
    /// come from the table's own physical format, so a Vortex table never
    /// answers with Parquet's compression behavior.
    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.table_format()
            .get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.table_format().compression_type()
    }

    /// Infers the schema of the files this listing was built over.
    ///
    /// The objects are grouped by the physical format of each file, because a
    /// LakeSoul table keeps files of several formats at once: every group is
    /// inferred by the format that can read those files (Vortex reads its own
    /// footer and widens mixed integer widths), and the per-group schemas are
    /// merged with LakeSoul's schema-evolution rule.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        let mut groups: Vec<(PhysicalFormat, Vec<ObjectMeta>)> = vec![];
        for object in objects {
            let physical_format = self.format_for_path(object.location.as_ref());
            match groups
                .iter_mut()
                .find(|(group_format, _)| *group_format == physical_format)
            {
                Some((_, group)) => group.push(object.clone()),
                None => groups.push((physical_format, vec![object.clone()])),
            }
        }

        let mut schemas = Vec::with_capacity(groups.len());
        for (physical_format, objects) in groups {
            let format = self.format_registry.file_format(physical_format);
            schemas.push(format.infer_schema(state, store, &objects).await?);
        }
        merge_schema_refs(schemas)
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        self.format_registry
            .file_format(self.format_for_path(object.location.as_ref()))
            .infer_stats(state, store, table_schema, object)
            .await
    }

    /// A LakeSoul read is planned by the table provider, not here.
    ///
    /// `LakeSoulTableProvider::scan` resolves the committed files from the
    /// metadata, groups them by physical format and by work unit, and builds
    /// the merge/CDC plan. Planning a scan from a `ListingTable` would
    /// duplicate that planner, and it could not serve a table whose files are
    /// of several physical formats, so it is rejected explicitly instead of
    /// silently returning unmerged rows.
    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "LakeSoul scans are planned by LakeSoulTableProvider; \
             scan the table through its provider"
        )
    }

    /// Plan a write into this LakeSoul table.
    ///
    /// The plan is the [`LakeSoulWriteExec`] / [`LakeSoulCommitExec`] pair:
    /// the write node writes every input partition to object storage and
    /// reports the files it wrote, and the commit node publishes all of the
    /// reports with exactly one metadata commit.
    ///
    /// Overwrites replace the whole table and are not implemented.
    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if conf.insert_op == InsertOp::Overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented yet for LakeSoul tables".to_string(),
            ));
        }

        // In a single-node plan the write node and the commit node are
        // adjacent; a distributed plan can move the write node (and only the
        // write node) to workers.
        let mut write =
            LakeSoulWriteExec::new(input, order_requirements, self.table_info())
                .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
        let primary_keys = write.primary_keys().to_vec();
        let remote =
            crate::distributed::planner::distributed_writes_enabled(state.config());
        if remote && (!primary_keys.is_empty() || !write.range_partitions().is_empty()) {
            write = write.with_task_partitioning();
        }
        let mut insert_only_snapshot = None;
        if conf.insert_op == InsertOp::Append && !primary_keys.is_empty() {
            // Both version capture and file listing must use the primary:
            // different replica snapshots could miss a committed duplicate.
            let client = Arc::new(self.client.primary_only().await);
            let provider = LakeSoulTableProvider::try_new_without_session(
                crate::catalog::LakeSoulProviderOptions::from_session(state),
                client.clone(),
                self.conf.clone(),
                self.table_info(),
            )
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
            // Capture versions before the fresh scan is planned. A concurrent
            // commit can invalidate this snapshot, never silently rebase it.
            let snapshot = client
                .get_all_partition_info(&self.table_info.table_id)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let projection = write
                .range_partitions()
                .iter()
                .chain(write.primary_keys())
                .map(|name| provider.schema().index_of(name))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let key_schema = Arc::new(provider.schema().project(&projection)?);
            let local_scan = remote
                .then(|| crate::distributed::planner::local_key_scan_session(state))
                .transpose()?;
            let scan_session: &dyn Session = local_scan
                .as_ref()
                .map_or(state, |session| session as &dyn Session);
            let existing_keys = provider
                .scan(scan_session, Some(&projection), &[], None)
                .await?;
            let insert_only = InsertOnly {
                existing_keys,
                key_schema,
                conflict_mode: crate::write_mode::LakeSoulWriteMode::from_session(state),
                table_name: format!(
                    "{}.{}",
                    self.table_info.table_namespace, self.table_info.table_name
                ),
            };
            insert_only_snapshot = Some(snapshot);
            write = write.with_insert_only(insert_only);
        }
        let attempted_files = write.attempted_files();
        let write_id = write.write_id().to_owned();
        let mut reports: Arc<dyn ExecutionPlan> = Arc::new(write);
        if remote {
            // Let the distributed planner inject every source-query boundary
            // before it places the write stage below this report coalesce.
            reports = Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                    reports,
                ),
            );
        }
        let mut commit = LakeSoulCommitExec::new(
            reports,
            self.table_info(),
            self.client(),
            primary_keys,
            self.conf.object_store_options().clone(),
            insert_only_snapshot,
            attempted_files,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        if remote {
            commit = commit.with_remote_write(write_id);
        }
        Ok(Arc::new(commit))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.table_format().file_source(table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_metadata::MetaDataClient;

    fn table_info(properties: &str) -> Arc<TableInfo> {
        Arc::new(TableInfo {
            table_namespace: "default".to_string(),
            table_name: "format_metadata".to_string(),
            properties: properties.to_string(),
            ..Default::default()
        })
    }

    /// Unusable stored format metadata must fail the construction instead of
    /// being presented to DataFusion as a Parquet table. An absent
    /// `file_format` property is not an error: it means the connector default.
    #[tokio::test(flavor = "multi_thread")]
    async fn construction_rejects_unusable_table_format_metadata() {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let registry = Arc::new(
            LakeSoulFormatRegistry::new(LakeSoulIOConfig::default(), false).unwrap(),
        );

        for properties in [
            // a format this build has no reader/writer for
            r#"{"file_format": "orc"}"#,
            // properties that are not the table property JSON at all
            "{not json",
        ] {
            let error = LakeSoulMetaDataFormat::new(
                client.clone(),
                table_info(properties),
                LakeSoulIOConfig::default(),
                registry.clone(),
            )
            .expect_err("unusable format metadata must not build a format");
            assert!(
                error.to_string().contains("format_metadata"),
                "the error must name the table, got: {error}"
            );
        }

        let format = LakeSoulMetaDataFormat::new(
            client,
            table_info("{}"),
            LakeSoulIOConfig::default(),
            registry,
        )
        .expect("a table without a file_format property follows the default format");
        assert_eq!(format.get_ext(), "vortex");
    }
}
