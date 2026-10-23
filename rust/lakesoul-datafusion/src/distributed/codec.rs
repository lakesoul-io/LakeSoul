// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan codec for LakeSoul-specific execution plan nodes.
//!
//! The distributed planner serializes every worker stage with
//! [`datafusion_proto::physical_plan::PhysicalPlanNode`]. Standard DataFusion
//! nodes (including `DataSourceExec` over `FileScanConfig` with
//! `ParquetSource`) are handled by `datafusion-proto` itself; LakeSoul's
//! [`MergeParquetExec`] is encoded by this codec, which is composed after the
//! distributed codec via `with_distributed_user_codec` on both the coordinator
//! and the worker sessions.
//!
//! A file scan whose source has no `try_to_proto` hook of its own crosses the
//! wire through an extension codec instead. Vortex scans are such a source and
//! [`VortexScanCodec`] is their codec; [`user_codecs`] is the one place that
//! defines the ordered list every distributed session registers.
//!
//! The composition order *is* the wire format: `datafusion-proto` stamps each
//! extension payload with the position of the codec that wrote it and the
//! receiver resolves that position against its own list, so a coordinator and
//! its workers must register the same codecs in the same order.
//!
//! The wire format is versioned by [`CODEC_VERSION`]: the encoder stamps it and
//! the decoder refuses any other value, so a mixed-version cluster fails loudly
//! at decode time instead of misinterpreting a plan.
//!
//! Credentials are deliberately not part of the format: only
//! [`LakeSoulIOConfig::options`](lakesoul_io::config::LakeSoulIOConfig::options)
//! is encoded, never `object_store_options`, where the S3/HDFS credentials
//! live. Workers obtain storage credentials from their own environment
//! (workload identity, IRSA, mounted secrets).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::common::exec_err;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::vortex_scan_session;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_io::physical_plan::merge::MergeWorkUnit;
use prost::Message as _;
use vortex_datafusion::{VortexScanCodec, VortexSource};

/// Wire-format version of [`MergeParquetExecProto`].
///
/// The encoder stamps it into every plan and the decoder refuses any other
/// value. Bump it whenever the encoding changes — and bump
/// [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`] with it, since worker
/// discovery filters by that version (a test keeps the two in sync). Version 0
/// means "written before the field existed" and is always refused.
///
/// The generation also covers the *list* of codecs (see [`user_codecs`]): its
/// order decides which codec reads an extension payload, so registering a
/// codec a worker does not know is a protocol change even when no existing
/// message layout moved. Version 2 added [`VortexScanCodec`] at position 2.
pub const CODEC_VERSION: u32 = 2;

/// Wire format for [`MergeParquetExec`].
///
/// Children are re-attached by the codec driver. Work units retain their
/// spans into those children, files, constants, and output lanes; primary keys,
/// merge operators, and the `options` map carrying the `is_compacted` /
/// `skip_merge_on_read` flags remain exec-wide.
/// Object-store credentials are not part of the format; see the module docs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MergeParquetExecProto {
    #[prost(message, optional, tag = "1")]
    pub schema: Option<datafusion_proto::protobuf::Schema>,
    #[prost(string, repeated, tag = "2")]
    pub primary_keys: Vec<String>,
    #[prost(map = "string, string", tag = "3")]
    pub merge_operators: HashMap<String, String>,
    #[prost(message, repeated, tag = "4")]
    pub work_units: Vec<MergeWorkUnitProto>,
    #[prost(uint64, tag = "5")]
    pub partition_count: u64,
    #[prost(map = "string, string", tag = "6")]
    pub options: HashMap<String, String>,
    /// [`CODEC_VERSION`] of the encoder; appended rather than numbered first so
    /// the layout stays additively evolvable.
    #[prost(uint32, tag = "7")]
    pub codec_version: u32,
}

/// A merge work unit's child span and lane-local execution state.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MergeWorkUnitProto {
    #[prost(uint64, tag = "1")]
    pub input_start: u64,
    #[prost(uint64, tag = "2")]
    pub input_end: u64,
    #[prost(string, repeated, tag = "3")]
    pub files: Vec<String>,
    #[prost(map = "string, string", tag = "4")]
    pub default_column_value: HashMap<String, String>,
    #[prost(uint64, tag = "5")]
    pub lane: u64,
}

/// [`PhysicalExtensionCodec`] for LakeSoul execution plan nodes.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulCodec;

/// The user codecs a distributed session registers, in registration order.
///
/// The coordinator's planner and every worker build their codec list from this
/// one function so the positions an extension payload refers to cannot drift:
/// position 0 is always the distributed codec itself, position 1
/// [`LakeSoulCodec`] (merge-on-read stages), position 2 the vortex scan codec.
/// A worker that registers a different list either fails to decode a stage or
/// would hand a payload to the wrong codec, so the list is part of the wire
/// generation ([`CODEC_VERSION`]).
///
/// The vortex codec is built from [`vortex_scan_session`], the session
/// [`lakesoul_io::file_format::LakeSoulFormatRegistry`] plans scans with, so a
/// rebuilt worker-side scan decodes files exactly like the coordinator's.
pub fn user_codecs() -> Vec<Arc<dyn PhysicalExtensionCodec>> {
    vec![
        Arc::new(LakeSoulCodec),
        Arc::new(VortexScanCodec::new(vortex_scan_session())),
    ]
}

/// Composed codec used to (de)serialize worker stage plans: the distributed
/// codec first, then the [`user_codecs`] in order.
///
/// Both sides of a cluster must build this list in the same order — decoding
/// is position-addressed, so reordering the list breaks cross-version plans.
pub fn composed_codec() -> ComposedPhysicalExtensionCodec {
    let mut codecs: Vec<Arc<dyn PhysicalExtensionCodec>> =
        vec![Arc::new(datafusion_distributed::DistributedCodec)];
    codecs.extend(user_codecs());
    ComposedPhysicalExtensionCodec::new(codecs)
}

/// Whether a worker can decode a stage whose scan leaf reads through `source`.
///
/// Two ways a file source gets a wire form: `datafusion-proto` serializes it
/// itself when it implements `try_to_proto` (parquet, and the csv/json sources
/// LakeSoul does not build), or an extension codec in [`user_codecs`] carries
/// it — currently [`VortexScanCodec`] for [`VortexSource`]. Anything else is
/// refused while planning, where the caller's fallback policy can still act on
/// it, instead of failing when the stage is sent to a worker.
///
/// Keep this in sync with [`user_codecs`]: a source is only encodable here
/// because a codec in that list encodes it.
pub fn source_has_wire_form(source: &dyn FileSource) -> bool {
    source.is::<ParquetSource>() || source.is::<VortexSource>()
}

impl LakeSoulCodec {
    fn proto_from_merge_exec(exec: &MergeParquetExec) -> MergeParquetExecProto {
        MergeParquetExecProto {
            codec_version: CODEC_VERSION,
            schema: Some(
                exec.schema()
                    .as_ref()
                    .try_into()
                    .expect("encode schema to protobuf should succeed"),
            ),
            primary_keys: exec.primary_keys().to_vec(),
            merge_operators: exec
                .merge_operators()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            work_units: exec
                .work_units()
                .iter()
                .map(|unit| MergeWorkUnitProto {
                    input_start: unit.input_range.start as u64,
                    input_end: unit.input_range.end as u64,
                    files: unit.files.clone(),
                    default_column_value: unit.default_column_value.as_ref().clone(),
                    lane: unit.lane as u64,
                })
                .collect(),
            partition_count: exec.output_partitioning().partition_count() as u64,
            options: exec
                .io_config()
                .options()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl PhysicalExtensionCodec for LakeSoulCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = MergeParquetExecProto::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("decode MergeParquetExec: {err}"))
        })?;
        // Fail loudly instead of guessing: a plan from another build may use a
        // different encoding for the same fields.
        if proto.codec_version != CODEC_VERSION {
            return Err(DataFusionError::Internal(format!(
                "MergeParquetExec plan encoded with codec version {} but this build \
                 speaks {CODEC_VERSION}: coordinator and workers must run compatible \
                 LakeSoul builds",
                proto.codec_version
            )));
        }
        let schema: SchemaRef = Arc::new(
            proto
                .schema
                .as_ref()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "MergeParquetExecProto is missing schema".into(),
                    )
                })?
                .try_into()?,
        );

        let mut builder =
            LakeSoulIOConfigBuilder::new().with_primary_keys(proto.primary_keys.clone());
        for (key, value) in &proto.options {
            builder = builder.with_option(key, value);
        }
        for (field, op) in &proto.merge_operators {
            builder = builder.with_merge_op(field.clone(), op.clone());
        }
        let decode_index = |value: u64, field: &str| {
            usize::try_from(value).map_err(|_| {
                DataFusionError::Internal(format!(
                    "MergeParquetExecProto {field} exceeds this platform's index range"
                ))
            })
        };
        let partition_count = decode_index(proto.partition_count, "partition_count")?;
        let work_units = proto
            .work_units
            .into_iter()
            .map(|unit| {
                Ok(MergeWorkUnit {
                    input_range: decode_index(unit.input_start, "input_start")?
                        ..decode_index(unit.input_end, "input_end")?,
                    files: unit.files,
                    default_column_value: Arc::new(unit.default_column_value),
                    lane: decode_index(unit.lane, "lane")?,
                })
            })
            .collect::<DFResult<Vec<_>>>()?;

        let exec = MergeParquetExec::from_parts(
            schema,
            Arc::new(proto.primary_keys),
            Arc::new(proto.merge_operators),
            inputs.to_vec(),
            builder.build(),
            work_units,
            partition_count,
        )?;
        Ok(Arc::new(exec))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        // Must not succeed for foreign nodes: the composed codec picks the
        // first encoder that returns `Ok`.
        let Some(exec) = node.downcast_ref::<MergeParquetExec>() else {
            return exec_err!("LakeSoulCodec cannot encode {}", node.name());
        };
        let proto = Self::proto_from_merge_exec(exec);
        buf.reserve(proto.encoded_len());
        proto.encode(buf).map_err(|err| {
            DataFusionError::Internal(format!("encode MergeParquetExec: {err}"))
        })?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod roundtrip {
    //! Encode/decode helper mirroring the coordinator↔worker codec list.

    use super::*;
    use datafusion::execution::SessionStateBuilder;
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    pub(crate) fn exec(node: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let codec = composed_codec();
        let buf = PhysicalPlanNode::try_from_physical_plan(node, &codec)?.encode_to_vec();
        // Decode in a session-backed `TaskContext`, like the worker does when
        // it decodes a stage (`session_state.task_ctx()`). A bare
        // `TaskContext::default()` has empty function registries, so a plan
        // carrying a built-in aggregate (`sum`, `avg`, ...) would fail to
        // decode for a reason that cannot happen over the wire.
        let ctx = SessionStateBuilder::new_with_default_features()
            .build()
            .task_ctx();
        PhysicalPlanNode::try_decode(buf.as_slice())?.try_into_physical_plan(&ctx, &codec)
    }
}

#[cfg(test)]
mod tests {
    use super::roundtrip::exec as roundtrip;
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::utils::collect_columns;
    use datafusion_datasource::PartitionedFile;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::config::OPTION_KEY_IS_COMPACTED;
    use lakesoul_io::file_format::{
        LakeSoulFormatRegistry, PhysicalFormat, append_only_scan_exec,
    };
    fn merge_exec(inputs: Vec<Arc<dyn ExecutionPlan>>) -> MergeParquetExec {
        let work_units = vec![MergeWorkUnit {
            input_range: 0..inputs.len(),
            files: vec!["s3://bucket/t/file-0.parquet".to_string()],
            default_column_value: Arc::new(HashMap::from([(
                "part".to_string(),
                "p0".to_string(),
            )])),
            lane: 0,
        }];
        merge_exec_with_work_units(inputs, work_units, 1)
    }

    fn merge_exec_with_work_units(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        work_units: Vec<MergeWorkUnit>,
        partition_count: usize,
    ) -> MergeParquetExec {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_primary_keys(vec!["id".to_string()])
            .with_merge_op("name".to_string(), "UseLast".to_string())
            .with_option(OPTION_KEY_IS_COMPACTED.to_string(), "true".to_string())
            .build();
        MergeParquetExec::from_parts(
            schema,
            Arc::new(vec!["id".to_string()]),
            Arc::new(HashMap::from([("name".to_string(), "UseLast".to_string())])),
            inputs,
            io_config,
            work_units,
            partition_count,
        )
        .expect("valid merge work units")
    }

    fn parquet_input(path: &str) -> Arc<dyn ExecutionPlan> {
        let file_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = ParquetSource::new(TableSchema::new(file_schema, Vec::new()));
        let file = PartitionedFile::new(path.to_string(), 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").unwrap(),
            Arc::new(source),
        )
        .with_file_groups(vec![FileGroup::new(vec![file])])
        .build();
        DataSourceExec::from_data_source(config)
    }

    /// A vortex scan leaf, the source with no `try_to_proto` hook: it only
    /// crosses the wire through the vortex codec in [`user_codecs`].
    fn vortex_input(path: &str) -> Arc<dyn ExecutionPlan> {
        let file_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = VortexSource::new(
            TableSchema::new(file_schema, Vec::new()),
            vortex_scan_session(),
        );
        let file = PartitionedFile::new(path.to_string(), 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").unwrap(),
            Arc::new(source),
        )
        .with_file_groups(vec![FileGroup::new(vec![file])])
        .build();
        DataSourceExec::from_data_source(config)
    }

    /// The gate treats a vortex leaf as shippable (`source_has_wire_form`), so
    /// the composed codec has to actually carry it: a decode that dropped the
    /// files or rebuilt a parquet source would fail a distributed vortex query
    /// when its stage reaches a worker.
    #[test]
    fn vortex_scan_leaf_roundtrips_through_the_composed_codec() {
        let decoded = roundtrip(vortex_input("s3://bucket/t/a.vortex")).unwrap();

        let exec = decoded
            .downcast_ref::<DataSourceExec>()
            .expect("a vortex scan decodes as a DataSourceExec");
        let config = exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("over a file scan config");
        assert!(config.file_source().is::<VortexSource>());
        assert!(source_has_wire_form(config.file_source().as_ref()));
        // `PartitionedFile` stores the store-relative path, which normalizes
        // the double slash.
        assert_eq!(
            config
                .file_groups
                .iter()
                .flat_map(|group| group.files())
                .map(|file| file.object_meta.location.to_string())
                .collect::<Vec<_>>(),
            [object_store::path::Path::from("s3://bucket/t/a.vortex").to_string()]
        );
    }

    /// An append-only leaf reading `p_brand` and `p_size` of the TPC-H `part`
    /// table in `physical_format`, with `p_size IN (1, 2, 3, 4, 5) AND
    /// p_brand <> 'Brand#45'` built against the whole table, as the table
    /// provider builds it.
    fn narrowed_part_leaf(physical_format: PhysicalFormat) -> Arc<dyn ExecutionPlan> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{binary, col, in_list, lit};

        let table = Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
        ]);
        let predicates = vec![
            in_list(
                col("p_size", &table).unwrap(),
                (1..=5).map(lit).collect(),
                &false,
                &table,
            )
            .unwrap(),
            binary(
                col("p_brand", &table).unwrap(),
                Operator::NotEq,
                lit("Brand#45"),
                &table,
            )
            .unwrap(),
        ];
        let registry =
            LakeSoulFormatRegistry::new(LakeSoulIOConfigBuilder::new().build(), false)
                .unwrap();
        let format = registry.file_format(physical_format);
        let state = datafusion::prelude::SessionContext::new().state();
        let source = format
            .file_source(TableSchema::new(Arc::new(table), Vec::new()))
            .try_pushdown_filters(predicates, state.config_options())
            .unwrap()
            .updated_node
            .expect("the source takes the predicates");
        let file = PartitionedFile::new(
            format!("s3://bucket/part/a.{}", physical_format.extension()),
            1024,
        );
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::parse("file://").unwrap(), source)
                .with_file_groups(vec![FileGroup::new(vec![file])])
                .build();
        let merged: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
        ]));
        append_only_scan_exec(&state, format.as_ref(), &[config], &merged)
            .expect("the leaf builder must not fail")
            .expect("an append-only work unit scans as a plain leaf")
    }

    /// A worker decodes a scan leaf's predicate against the leaf's own schema,
    /// and an `IN` list of more than three items checks its column while
    /// decoding. A leaf whose predicate stayed bound to the table schema
    /// failed every such stage on the workers (TPC-H q16 and q19) in either
    /// file format.
    #[test]
    fn narrowed_scan_leaf_predicate_decodes_against_the_leaf_schema() {
        for physical_format in [PhysicalFormat::Parquet, PhysicalFormat::Vortex] {
            let decoded = roundtrip(narrowed_part_leaf(physical_format))
                .unwrap_or_else(|err| panic!("{physical_format}: {err}"));

            let config = decoded
                .downcast_ref::<DataSourceExec>()
                .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
                .unwrap_or_else(|| panic!("{physical_format}: not a file scan"));
            let schema = config.file_source().table_schema().table_schema();
            let filter = config.file_source().filter().unwrap_or_else(|| {
                panic!("{physical_format}: predicate lost on the wire")
            });
            let columns = collect_columns(&filter);
            assert_eq!(columns.len(), 2, "{physical_format}: {filter}");
            for column in columns {
                assert_eq!(
                    schema
                        .fields()
                        .get(column.index())
                        .map(|field| field.name().as_str()),
                    Some(column.name()),
                    "{physical_format}: {filter} is not bound to {schema}"
                );
            }
        }
    }

    #[test]
    fn encode_requires_merge_exec() {
        let other: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty())),
        );
        let mut buf = Vec::new();
        assert!(
            LakeSoulCodec
                .try_encode(other, &mut buf, &DefaultPhysicalProtoConverter {})
                .is_err()
        );
    }

    #[test]
    fn merge_exec_roundtrip() {
        let exec = merge_exec(vec![parquet_input("s3://bucket/t/a.parquet")]);
        let decoded = roundtrip(Arc::new(exec)).unwrap();
        assert_eq!(decoded.name(), "MergeParquetExec");

        let merge = decoded.downcast_ref::<MergeParquetExec>().unwrap();
        assert_eq!(merge.primary_keys(), Arc::new(vec!["id".to_string()]));
        assert_eq!(
            merge.merge_operators().get("name").map(String::as_str),
            Some("UseLast")
        );
        assert_eq!(
            merge.work_units()[0]
                .default_column_value
                .get("part")
                .map(String::as_str),
            Some("p0")
        );
        assert!(merge.io_config().is_compacted());
        assert_eq!(
            merge.work_units()[0].files,
            ["s3://bucket/t/file-0.parquet"]
        );
        assert_eq!(merge.work_units()[0].input_range, 0..1);
        assert_eq!(merge.work_units()[0].lane, 0);
        assert_eq!(merge.output_partitioning().partition_count(), 1);
        assert!(merge.output_ordering().is_none());
        assert_eq!(merge.schema(), decoded.schema());
        assert_eq!(merge.children().len(), 1);
        assert_eq!(merge.children()[0].name(), "DataSourceExec");
    }

    #[test]
    fn merge_exec_multiple_children_roundtrip() {
        let exec = merge_exec(vec![
            parquet_input("s3://bucket/t/a.parquet"),
            parquet_input("s3://bucket/t/b.parquet"),
        ]);
        let decoded = roundtrip(Arc::new(exec)).unwrap();
        let merge = decoded.downcast_ref::<MergeParquetExec>().unwrap();
        assert_eq!(merge.children().len(), 2);
    }

    #[test]
    fn merge_exec_multiple_work_units_and_lanes_roundtrip() {
        let work_units = vec![
            MergeWorkUnit {
                input_range: 0..2,
                files: vec![
                    "s3://bucket/t/first-base.parquet".to_string(),
                    "s3://bucket/t/first-delta.parquet".to_string(),
                ],
                default_column_value: Arc::new(HashMap::from([(
                    "part".to_string(),
                    "first".to_string(),
                )])),
                lane: 1,
            },
            MergeWorkUnit {
                input_range: 2..3,
                files: vec!["s3://bucket/t/second.parquet".to_string()],
                default_column_value: Arc::new(HashMap::from([(
                    "part".to_string(),
                    "second".to_string(),
                )])),
                lane: 0,
            },
            MergeWorkUnit {
                input_range: 3..4,
                files: vec!["s3://bucket/t/third.parquet".to_string()],
                default_column_value: Arc::new(HashMap::from([(
                    "part".to_string(),
                    "third".to_string(),
                )])),
                lane: 1,
            },
        ];
        let exec = merge_exec_with_work_units(
            vec![
                parquet_input("s3://bucket/t/first-base.parquet"),
                parquet_input("s3://bucket/t/first-delta.parquet"),
                parquet_input("s3://bucket/t/second.parquet"),
                parquet_input("s3://bucket/t/third.parquet"),
            ],
            work_units.clone(),
            2,
        );
        let schema = exec.schema();
        let decoded = roundtrip(Arc::new(exec)).unwrap();
        let merge = decoded.downcast_ref::<MergeParquetExec>().unwrap();

        assert_eq!(merge.work_units(), work_units.as_slice());
        assert_eq!(merge.output_partitioning().partition_count(), 2);
        assert!(merge.output_ordering().is_none());
        assert_eq!(merge.schema(), schema);
        assert_eq!(merge.children().len(), 4);
        assert!(
            merge
                .children()
                .iter()
                .all(|child| child.name() == "DataSourceExec")
        );
        assert!(merge.io_config().is_compacted());
    }

    #[test]
    fn decode_rejects_malformed_work_units_and_partition_counts() {
        let inputs = vec![
            parquet_input("s3://bucket/t/a.parquet"),
            parquet_input("s3://bucket/t/b.parquet"),
        ];
        let valid = LakeSoulCodec::proto_from_merge_exec(&merge_exec(inputs.clone()));
        let mut cases = Vec::new();

        let mut proto = valid.clone();
        proto.work_units[0].input_start = 1;
        cases.push(("gap before first span", proto));
        let mut proto = valid.clone();
        proto.work_units[0].input_start = 2;
        proto.work_units[0].input_end = 1;
        cases.push(("reversed span", proto));
        let mut proto = valid.clone();
        proto.work_units[0].input_end = 3;
        cases.push(("span past children", proto));
        let mut proto = valid.clone();
        proto.work_units[0].input_end = 1;
        cases.push(("uncovered child", proto));
        let mut proto = valid.clone();
        proto.work_units.push(proto.work_units[0].clone());
        cases.push(("overlapping spans", proto));
        let mut proto = valid.clone();
        proto.work_units.clear();
        cases.push(("missing work units", proto));
        let mut proto = valid.clone();
        proto.partition_count = 0;
        cases.push(("zero partition count", proto));
        let mut proto = valid;
        proto.work_units[0].lane = proto.partition_count;
        cases.push(("lane outside partition count", proto));

        for (case, proto) in cases {
            let result = LakeSoulCodec.try_decode(
                &proto.encode_to_vec(),
                &inputs,
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            );
            assert!(result.is_err(), "{case} must fail decoding");
        }
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = LakeSoulCodec
            .try_decode(
                &[0xff, 0xff],
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    /// Every encoded plan carries the codec version it was written with.
    #[test]
    fn encode_stamps_the_codec_version() {
        let proto = LakeSoulCodec::proto_from_merge_exec(&merge_exec(vec![]));
        assert_eq!(proto.codec_version, CODEC_VERSION);
    }

    /// A plan from an incompatible build is refused instead of being decoded
    /// with the wrong field interpretation.
    #[test]
    fn decode_rejects_other_codec_versions() {
        let mut proto = LakeSoulCodec::proto_from_merge_exec(&merge_exec(vec![]));
        proto.codec_version = CODEC_VERSION + 1;
        let bytes = proto.encode_to_vec();
        let err = LakeSoulCodec
            .try_decode(
                &bytes,
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, DataFusionError::Internal(_)), "{message}");
        assert!(message.contains("codec version"), "{message}");
    }

    /// The generation that advertised itself without a codec version must be
    /// rejected on both sides: discovery drops its workers (they advertise a
    /// different protocol), and its plans decode as codec version 0, which is
    /// refused — protobuf would otherwise ignore the unknown version field.
    #[test]
    fn previous_generation_is_rejected_on_both_sides() {
        const UNVERSIONED_PROTOCOL_VERSION: &str = "lakesoul-distributed/1";
        assert_ne!(
            crate::distributed::DISTRIBUTED_PROTOCOL_VERSION,
            UNVERSIONED_PROTOCOL_VERSION,
            "the versioned format is a new protocol generation"
        );

        let mut unversioned = LakeSoulCodec::proto_from_merge_exec(&merge_exec(vec![]));
        unversioned.codec_version = 0;
        let err = LakeSoulCodec
            .try_decode(
                &unversioned.encode_to_vec(),
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(err.to_string().contains("codec version"), "{err}");
    }

    /// Discovery filters workers by the distributed protocol version, so the
    /// wire-format version must move with it.
    #[test]
    fn protocol_version_matches_codec_version() {
        let protocol = crate::distributed::DISTRIBUTED_PROTOCOL_VERSION;
        let suffix = protocol.rsplit('/').next().expect("version suffix");
        assert_eq!(
            suffix.parse::<u32>().expect("numeric protocol version"),
            CODEC_VERSION,
            "bump DISTRIBUTED_PROTOCOL_VERSION together with CODEC_VERSION ({protocol})"
        );
    }

    /// Object-store credentials live in `object_store_options`, which the wire
    /// format deliberately omits: workers read them from their own
    /// environment, so a plan must never carry them.
    #[test]
    fn encoded_plan_carries_no_object_store_credentials() {
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://bucket/t/file-0.parquet"])
            .with_primary_keys(vec!["id".to_string()])
            .with_object_store_option("fs.s3a.access.key", "ACCESS-KEY-SENTINEL")
            .with_object_store_option("fs.s3a.secret.key", "SECRET-KEY-SENTINEL")
            .with_object_store_option("fs.s3a.endpoint", "https://s3.invalid:9000")
            .with_option(OPTION_KEY_IS_COMPACTED.to_string(), "true".to_string())
            .build();
        let work_units = vec![MergeWorkUnit {
            input_range: 0..0,
            files: io_config.files_slice().to_vec(),
            default_column_value: Arc::new(HashMap::new()),
            lane: 0,
        }];
        let exec = MergeParquetExec::from_parts(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            Arc::new(vec!["id".to_string()]),
            Arc::new(HashMap::new()),
            vec![],
            io_config,
            work_units,
            1,
        )
        .expect("valid credential-omission fixture");

        let mut buf = Vec::new();
        LakeSoulCodec
            .try_encode(Arc::new(exec), &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("encode");
        let encoded = String::from_utf8_lossy(&buf);

        // Positive control: the plan does carry what execution needs, so the
        // assertions below cannot pass vacuously.
        assert!(encoded.contains("bucket/t/file-0.parquet"), "{encoded}");
        for secret in ["ACCESS-KEY-SENTINEL", "SECRET-KEY-SENTINEL", "s3.invalid"] {
            assert!(!encoded.contains(secret), "plan leaked {secret}: {encoded}");
        }
    }
}
