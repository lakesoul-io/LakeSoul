// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan codec for LakeSoul-specific execution plan nodes.
//!
//! The distributed planner serializes every worker stage with
//! [`datafusion_proto::physical_plan::PhysicalPlanNode`]. Standard DataFusion
//! nodes (including `DataSourceExec` over `FileScanConfig` with
//! `ParquetSource`) are handled by `datafusion-proto` itself; LakeSoul's
//! [`MergeParquetExec`] is encoded by [`LakeSoulCodec`] and the metadata-only
//! [`LakeSoulScanExec`] by [`LakeSoulScanCodec`], both composed after the
//! distributed codec via `with_distributed_user_codec` on the coordinator and
//! worker sessions.
//!
//! A file scan whose source has no `try_to_proto` hook of its own crosses the
//! wire through an extension codec instead. Vortex scans are such a source and
//! [`VortexScanCodec`] is their codec; [`user_codecs`] is the one place that
//! defines the ordered list every distributed session registers.
//!
//! The lazy scan codec carries only the planning descriptors — schemas,
//! snapshot files with their physical format and optional size/range, work
//! units, the safe pre-merge predicate, the pk-locator constraint and the
//! resolved read options. Decoding rebuilds the node without touching an
//! object store; the first consumed stream initializes the current unit.
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
use datafusion::common::{exec_err, internal_err};
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::{PhysicalFormat, vortex_scan_session};
use lakesoul_io::physical_plan::merge::MergeWorkUnit;
use lakesoul_io::physical_plan::{
    LakeSoulScanConfig, LakeSoulScanExec, MergeParquetExec, ScanFile, ScanWorkUnit,
};
use lakesoul_io::pk_locator::{Key, KeyConstraint, KeyValue};
use prost::Message as _;
use vortex_datafusion::{VortexScanCodec, VortexSource};

/// Wire-format version of the LakeSoul extension payloads.
///
/// The encoder stamps it into every plan and the decoder refuses any other
/// value. Bump it for incompatible changes to a released encoding, together
/// with [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`], since worker
/// discovery filters by that version. Version 0 means "written before the
/// field existed" and is always refused.
///
/// The generation also covers the *list* of codecs (see [`user_codecs`]): its
/// order decides which codec reads an extension payload, so registering a
/// codec a worker does not know is a protocol change even when no existing
/// message layout moved. Version 2 added [`VortexScanCodec`] at position 2.
/// Version 3 adds [`LakeSoulScanCodec`] at position 3, including its scan limit;
/// version-2 workers cannot decode these metadata-only scan descriptors.
pub const CODEC_VERSION: u32 = 3;

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

/// Wire format for the metadata-only [`LakeSoulScanExec`].
///
/// Everything execution needs is planned on the coordinator from PostgreSQL
/// metadata: the fixed schemas, the descriptor files of the captured snapshot,
/// the work units, the safe pre-merge predicate, the pk-locator constraint and
/// the resolved read options. Decoding rebuilds the node without reading a
/// file; workers obtain storage credentials from their own environment, so
/// `object_store_options` are never encoded.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LakeSoulScanExecProto {
    /// [`CODEC_VERSION`] of the encoder.
    #[prost(uint32, tag = "1")]
    pub codec_version: u32,
    /// Logical full table schema including partition columns.
    #[prost(message, optional, tag = "2")]
    pub table_schema: Option<datafusion_proto::protobuf::Schema>,
    /// Intermediate output including the columns the merge/CDC step needs.
    #[prost(message, optional, tag = "3")]
    pub output_schema: Option<datafusion_proto::protobuf::Schema>,
    /// Schema of the partition columns as constants of a work unit.
    #[prost(message, optional, tag = "4")]
    pub partition_schema: Option<datafusion_proto::protobuf::Schema>,
    /// Work allocation; files stay in immutable snapshot/version order.
    #[prost(message, repeated, tag = "5")]
    pub work_units: Vec<LakeSoulScanWorkUnitProto>,
    /// Local lanes (output partitions) of the node, merge-on-read included.
    #[prost(uint64, tag = "6")]
    pub partition_count: u64,
    /// Primary-key columns of the scan's [`lakesoul_io::config::LakeSoulIOConfig`].
    #[prost(string, repeated, tag = "7")]
    pub primary_keys: Vec<String>,
    #[prost(map = "string, string", tag = "8")]
    pub merge_operators: HashMap<String, String>,
    /// Non-secret `LakeSoulIOConfig::options` map.
    #[prost(map = "string, string", tag = "9")]
    pub options: HashMap<String, String>,
    /// `hash_bucket_num` is its own config field, not an option.
    #[prost(string, tag = "10")]
    pub hash_bucket_num: String,
    #[prost(string, repeated, tag = "11")]
    pub range_partitions: Vec<String>,
    #[prost(string, repeated, tag = "12")]
    pub hash_partitioning_columns: Vec<String>,
    /// Safe pre-merge predicate, bound to `table_schema`.
    #[prost(message, optional, tag = "13")]
    pub predicate: Option<datafusion_proto::protobuf::PhysicalExprNode>,
    /// Candidate set for the pk index, if the query filtered on key prefixes.
    #[prost(message, optional, tag = "14")]
    pub key_constraint: Option<KeyConstraintProto>,
    #[prost(bool, tag = "15")]
    pub parquet_force_view_types: bool,
    /// Resolved semantic format/reader options (not worker resource budgets).
    #[prost(map = "string, string", tag = "16")]
    pub read_options: HashMap<String, String>,
    #[prost(uint64, tag = "17")]
    pub max_pk_candidates: u64,
    #[prost(bool, tag = "18")]
    pub bulk_pk_lookup: bool,
    /// Row cap an append-only leaf may stop reading at. Absent means the scan
    /// has to read everything; a merge-on-read scan never carries one.
    #[prost(uint64, optional, tag = "19")]
    pub limit: Option<u64>,
    /// Row count one work unit must hold before the key lookup builds local
    /// indexes for it. Absent means the query-wide constant: the planner that
    /// wrote the payload did not split the threshold over its units.
    #[prost(uint64, optional, tag = "20")]
    pub min_index_rows: Option<u64>,
}

/// One lazy-scan work unit: an atomic unit for merge-on-read, or a
/// dispatcher-splittable piece for append-only scans.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LakeSoulScanWorkUnitProto {
    #[prost(message, repeated, tag = "1")]
    pub files: Vec<LakeSoulScanFileProto>,
    #[prost(map = "string, string", tag = "2")]
    pub default_column_value: HashMap<String, String>,
    #[prost(uint64, tag = "3")]
    pub lane: u64,
}

/// A descriptor file, not a read: no credentials, no object-store metadata
/// beyond the size the descriptor already carried.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LakeSoulScanFileProto {
    /// Full original URL.
    #[prost(string, tag = "1")]
    pub path: String,
    /// `None` when the size is missing or untrusted; never an invented zero.
    #[prost(uint64, optional, tag = "2")]
    pub size: Option<u64>,
    #[prost(string, tag = "3")]
    pub physical_format: String,
    /// A legal split of the file, only where the format supports one.
    #[prost(int64, optional, tag = "4")]
    pub range_start: Option<i64>,
    #[prost(int64, optional, tag = "5")]
    pub range_end: Option<i64>,
}

/// A finite candidate set on a prefix of a table's primary keys.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyConstraintProto {
    #[prost(string, repeated, tag = "1")]
    pub columns: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    pub values: Vec<KeyProto>,
}

/// One composite candidate key.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyProto {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<KeyValueProto>,
}

/// One scalar key value; the oneof preserves the [`KeyValue`] variant so a
/// worker can rebuild the same typed candidate set.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValueProto {
    #[prost(oneof = "key_value_proto::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub value: Option<key_value_proto::Value>,
}

pub mod key_value_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag = "1")]
        Bool(bool),
        #[prost(sint64, tag = "2")]
        Int(i64),
        #[prost(uint64, tag = "3")]
        UInt(u64),
        /// Canonicalised float bits, matching [`KeyValue::Float`].
        #[prost(fixed64, tag = "4")]
        Float(u64),
        #[prost(string, tag = "5")]
        Utf8(String),
        #[prost(bytes, tag = "6")]
        Binary(Vec<u8>),
        /// The little-endian 16 bytes of the `i128`, as stored in the key.
        #[prost(bytes, tag = "7")]
        Decimal(Vec<u8>),
    }
}

/// [`PhysicalExtensionCodec`] for LakeSoul execution plan nodes.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulCodec;

/// The user codecs a distributed session registers, in registration order.
///
/// The coordinator's planner and every worker build their codec list from this
/// one function so the positions an extension payload refers to cannot drift:
/// position 0 is always the distributed codec itself, position 1
/// [`LakeSoulCodec`] (merge-on-read stages), position 2 the vortex scan codec,
/// position 3 [`LakeSoulScanCodec`] (metadata-only lazy scans). A worker that
/// registers a different list either fails to decode a stage or would hand a
/// payload to the wrong codec, so the list is part of the wire generation
/// ([`CODEC_VERSION`]).
///
/// The vortex codec is built from [`vortex_scan_session`], the session
/// [`lakesoul_io::file_format::LakeSoulFormatRegistry`] plans scans with, so a
/// rebuilt worker-side scan decodes files exactly like the coordinator's.
pub fn user_codecs() -> Vec<Arc<dyn PhysicalExtensionCodec>> {
    vec![
        Arc::new(LakeSoulCodec),
        Arc::new(VortexScanCodec::new(vortex_scan_session())),
        Arc::new(LakeSoulScanCodec),
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

/// Whether a worker can decode and read a metadata-only [`LakeSoulScanExec`].
///
/// The node itself crosses the wire through [`LakeSoulScanCodec`]; its files
/// are descriptors, so the remaining question is whether every physical format
/// it names has a reader on the worker. The match is exhaustive on purpose: a
/// new [`PhysicalFormat`] variant fails to compile here until it is either
/// accepted (the worker's format registry reads it) or refused, so this
/// opaque leaf cannot silently bypass the gate that a `DataSourceExec` leaf
/// goes through in [`source_has_wire_form`].
pub fn lazy_scan_has_wire_form(exec: &LakeSoulScanExec) -> bool {
    exec.work_units()
        .iter()
        .flat_map(|unit| unit.files.iter())
        .all(|file| physical_format_has_worker_reader(file.physical_format))
}

fn physical_format_has_worker_reader(format: PhysicalFormat) -> bool {
    match format {
        PhysicalFormat::Parquet => true,
        PhysicalFormat::Vortex | PhysicalFormat::VortexCompact => true,
    }
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

/// [`PhysicalExtensionCodec`] for the metadata-only [`LakeSoulScanExec`].
///
/// It carries the planning descriptors, never credentials: schemas, files of
/// the captured snapshot with optional size/legal range, work units, the safe
/// pre-merge predicate bound to the table schema, the pk-locator constraint
/// and the resolved read options. Decoding is synchronous and I/O-free; the
/// node revalidates the descriptors in `try_new`, so a malformed payload fails
/// while planning instead of when a worker first consumes its stream.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulScanCodec;

impl LakeSoulScanCodec {
    fn proto_from_scan_exec(
        exec: &LakeSoulScanExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<LakeSoulScanExecProto> {
        let config = exec.scan_config();
        let io_config = &config.io_config;
        let schema = |schema: &SchemaRef, field: &str| {
            TryInto::<datafusion_proto::protobuf::Schema>::try_into(schema.as_ref())
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "encode LakeSoulScanExec {field}: {err}"
                    ))
                })
        };
        Ok(LakeSoulScanExecProto {
            codec_version: CODEC_VERSION,
            table_schema: Some(schema(&config.table_schema, "table_schema")?),
            output_schema: Some(schema(&config.output_schema, "output_schema")?),
            partition_schema: Some(schema(&config.partition_schema, "partition_schema")?),
            work_units: exec
                .work_units()
                .iter()
                .map(|unit| LakeSoulScanWorkUnitProto {
                    files: unit
                        .files
                        .iter()
                        .map(|file| LakeSoulScanFileProto {
                            path: file.path.clone(),
                            size: file.size,
                            physical_format: file.physical_format.name().to_string(),
                            range_start: file.range.as_ref().map(|range| range.start),
                            range_end: file.range.as_ref().map(|range| range.end),
                        })
                        .collect(),
                    default_column_value: unit.default_column_value.clone(),
                    lane: unit.lane as u64,
                })
                .collect(),
            partition_count: exec.output_partitioning().partition_count() as u64,
            primary_keys: io_config.primary_keys_slice().to_vec(),
            merge_operators: io_config
                .merge_operators()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            options: io_config
                .options()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            hash_bucket_num: io_config.hash_bucket_num().to_string(),
            range_partitions: io_config.range_partitions_slice().to_vec(),
            hash_partitioning_columns: io_config
                .hash_partitioning_columns_slice()
                .to_vec(),
            predicate: config
                .predicate
                .as_ref()
                .map(|expr| proto_converter.physical_expr_to_proto(expr, codec))
                .transpose()?,
            key_constraint: config.key_constraint.as_ref().map(|constraint| {
                KeyConstraintProto {
                    columns: constraint.columns.clone(),
                    values: constraint
                        .values
                        .iter()
                        .map(|key| KeyProto {
                            values: key.iter().map(key_value_to_proto).collect(),
                        })
                        .collect(),
                }
            }),
            parquet_force_view_types: config.parquet_force_view_types,
            read_options: config.read_options.clone(),
            max_pk_candidates: io_config.max_pk_candidates() as u64,
            bulk_pk_lookup: io_config.bulk_pk_lookup(),
            limit: config.limit.map(|limit| limit as u64),
            min_index_rows: Some(config.min_index_rows),
        })
    }

    fn scan_exec_from_proto(
        proto: LakeSoulScanExecProto,
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "LakeSoulScanExec is a scan leaf and takes no input plans, got {}",
                inputs.len()
            );
        }
        let decode_index = |value: u64, field: &str| {
            usize::try_from(value).map_err(|_| {
                DataFusionError::Internal(format!(
                    "LakeSoulScanExecProto {field} exceeds this platform's index range"
                ))
            })
        };
        let schema = |schema: Option<datafusion_proto::protobuf::Schema>,
                      field: &str|
         -> DFResult<SchemaRef> {
            let proto = schema.as_ref().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "LakeSoulScanExecProto is missing {field}"
                ))
            })?;
            let schema: SchemaRef = Arc::new(proto.try_into().map_err(|err| {
                DataFusionError::Internal(format!(
                    "decode LakeSoulScanExec {field}: {err}"
                ))
            })?);
            Ok(schema)
        };
        let table_schema = schema(proto.table_schema, "table_schema")?;
        let output_schema = schema(proto.output_schema, "output_schema")?;
        let partition_schema = schema(proto.partition_schema, "partition_schema")?;
        let partition_count = decode_index(proto.partition_count, "partition_count")?;
        if partition_count == 0 {
            return internal_err!("LakeSoulScanExecProto has zero output partitions");
        }

        // The constructor validates descriptor ranges and overlap across units.
        let work_units = proto
            .work_units
            .into_iter()
            .map(|unit| {
                let lane = decode_index(unit.lane, "lane")?;
                if lane >= partition_count {
                    return internal_err!(
                        "LakeSoulScanExecProto unit lane {lane} is outside its \
                         {partition_count} output partitions"
                    );
                }
                let files = unit
                    .files
                    .into_iter()
                    .map(scan_file_from_proto)
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(ScanWorkUnit {
                    files,
                    default_column_value: unit.default_column_value,
                    lane,
                })
            })
            .collect::<DFResult<Vec<_>>>()?;

        let predicate = match proto.predicate {
            Some(predicate) => {
                let decode_ctx = PhysicalPlanDecodeContext::new(ctx, &LakeSoulScanCodec);
                Some(proto_converter.proto_to_physical_expr(
                    &predicate,
                    table_schema.as_ref(),
                    &decode_ctx,
                )?)
            }
            None => None,
        };

        let key_constraint = match proto.key_constraint {
            Some(constraint) => {
                if constraint.columns.is_empty() || constraint.values.is_empty() {
                    return internal_err!(
                        "LakeSoulScanExecProto key constraint must name columns and values"
                    );
                }
                let columns = constraint.columns;
                let values = constraint
                    .values
                    .into_iter()
                    .map(|key| {
                        if key.values.len() != columns.len() {
                            return internal_err!(
                                "LakeSoulScanExecProto key constraint has {} columns but \
                                 a candidate key of {} values",
                                columns.len(),
                                key.values.len()
                            );
                        }
                        Ok(Key::from_vec(
                            key.values
                                .into_iter()
                                .map(key_value_from_proto)
                                .collect::<DFResult<Vec<_>>>()?,
                        ))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                Some(KeyConstraint { columns, values })
            }
            None => None,
        };

        let mut builder = LakeSoulIOConfigBuilder::new()
            .with_primary_keys(proto.primary_keys)
            .with_hash_bucket_num(proto.hash_bucket_num)
            .with_range_partitions(proto.range_partitions)
            .with_hash_partitioning_columns(proto.hash_partitioning_columns)
            .with_max_pk_candidates(decode_index(
                proto.max_pk_candidates,
                "max_pk_candidates",
            )?)
            .with_bulk_pk_lookup(proto.bulk_pk_lookup);
        for (key, value) in &proto.options {
            builder = builder.with_option(key, value);
        }
        for (field, op) in &proto.merge_operators {
            builder = builder.with_merge_op(field.clone(), op.clone());
        }

        let config = LakeSoulScanConfig {
            table_schema,
            output_schema,
            partition_schema,
            io_config: builder.build(),
            predicate,
            key_constraint,
            parquet_force_view_types: proto.parquet_force_view_types,
            read_options: proto.read_options,
            limit: proto
                .limit
                .map(|limit| decode_index(limit, "limit"))
                .transpose()?,
            // A payload written before the threshold was split per unit keeps
            // the query-wide constant it was planned with.
            min_index_rows: proto
                .min_index_rows
                .unwrap_or(lakesoul_io::pk_locator::MIN_INDEX_ROWS),
        };
        let exec = LakeSoulScanExec::try_new(config, work_units, partition_count)?;
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}

fn scan_file_from_proto(file: LakeSoulScanFileProto) -> DFResult<ScanFile> {
    if file.path.is_empty() {
        return internal_err!(
            "LakeSoulScanExecProto has a file descriptor without a path"
        );
    }
    let physical_format =
        file.physical_format
            .parse::<PhysicalFormat>()
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "decode LakeSoulScanExec physical format '{}': {err}",
                    file.physical_format
                ))
            })?;
    let range = match (file.range_start, file.range_end) {
        (None, None) => None,
        (Some(start), Some(end)) => {
            if start < 0 || end <= start {
                return internal_err!(
                    "LakeSoulScanExecProto file '{}' has an invalid range {start}..{end}",
                    file.path
                );
            }
            if let Some(size) = file.size
                && u64::try_from(end).is_ok_and(|end| end > size)
            {
                return internal_err!(
                    "LakeSoulScanExecProto file '{}' range {start}..{end} exceeds its \
                     declared size {size}",
                    file.path
                );
            }
            Some(start..end)
        }
        _ => {
            return internal_err!(
                "LakeSoulScanExecProto file '{}' must carry both range bounds or neither",
                file.path
            );
        }
    };
    Ok(ScanFile {
        path: file.path,
        size: file.size,
        physical_format,
        range,
    })
}

fn key_value_to_proto(value: &KeyValue) -> KeyValueProto {
    let value = match value {
        KeyValue::Bool(value) => key_value_proto::Value::Bool(*value),
        KeyValue::Int(value) => key_value_proto::Value::Int(*value),
        KeyValue::UInt(value) => key_value_proto::Value::UInt(*value),
        KeyValue::Float(value) => key_value_proto::Value::Float(*value),
        KeyValue::Utf8(value) => key_value_proto::Value::Utf8(value.to_string()),
        KeyValue::Binary(value) => key_value_proto::Value::Binary(value.to_vec()),
        KeyValue::Decimal(value) => {
            key_value_proto::Value::Decimal(value.to_le_bytes().to_vec())
        }
    };
    KeyValueProto { value: Some(value) }
}

fn key_value_from_proto(proto: KeyValueProto) -> DFResult<KeyValue> {
    let value = proto.value.ok_or_else(|| {
        DataFusionError::Internal("LakeSoulScanExecProto key value has no variant".into())
    })?;
    Ok(match value {
        key_value_proto::Value::Bool(value) => KeyValue::Bool(value),
        key_value_proto::Value::Int(value) => KeyValue::Int(value),
        key_value_proto::Value::UInt(value) => KeyValue::UInt(value),
        key_value_proto::Value::Float(value) => KeyValue::Float(value),
        key_value_proto::Value::Utf8(value) => KeyValue::Utf8(value.into_boxed_str()),
        key_value_proto::Value::Binary(value) => {
            KeyValue::Binary(value.into_boxed_slice())
        }
        key_value_proto::Value::Decimal(value) => {
            let bytes: [u8; 16] = value.as_slice().try_into().map_err(|_| {
                DataFusionError::Internal(format!(
                    "LakeSoulScanExecProto decimal key value must be 16 bytes, got {}",
                    value.len()
                ))
            })?;
            KeyValue::Decimal(i128::from_le_bytes(bytes))
        }
    })
}

impl PhysicalExtensionCodec for LakeSoulScanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = LakeSoulScanExecProto::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("decode LakeSoulScanExec: {err}"))
        })?;
        if proto.codec_version != CODEC_VERSION {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulScanExec plan encoded with codec version {} but this build \
                 speaks {CODEC_VERSION}: coordinator and workers must run compatible \
                 LakeSoul builds",
                proto.codec_version
            )));
        }
        Self::scan_exec_from_proto(proto, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        // Must not succeed for foreign nodes: the composed codec picks the
        // first encoder that returns `Ok`.
        let Some(exec) = node.downcast_ref::<LakeSoulScanExec>() else {
            return exec_err!("LakeSoulScanCodec cannot encode {}", node.name());
        };
        let proto = Self::proto_from_scan_exec(exec, self, proto_converter)?;
        buf.reserve(proto.encoded_len());
        proto.encode(buf).map_err(|err| {
            DataFusionError::Internal(format!("encode LakeSoulScanExec: {err}"))
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

    /// A metadata-only scan of `format` with two work units on three lanes:
    /// a sized Parquet file, an unsized one, a range-split Parquet descriptor,
    /// default partition values, a predicate, a pk candidate set and
    /// non-default read options. Everything a worker needs is planned, so the
    /// codec must round-trip all of it.
    fn lazy_scan(format: PhysicalFormat) -> Arc<dyn ExecutionPlan> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{binary, col, lit};

        let table: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let partition_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)]));
        let predicate = binary(
            col("v", table.as_ref()).unwrap(),
            Operator::Gt,
            lit(3i32),
            table.as_ref(),
        )
        .unwrap();
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_primary_keys(vec!["id".to_string()])
            .with_merge_op("v".to_string(), "UseLast".to_string())
            .with_hash_bucket_num("4")
            .with_range_partitions(vec!["part".to_string()])
            .with_max_pk_candidates(2048)
            .with_bulk_pk_lookup(true)
            .with_option(OPTION_KEY_IS_COMPACTED.to_string(), "true".to_string())
            .build();
        let units = vec![
            ScanWorkUnit {
                files: vec![
                    ScanFile {
                        path: format!("s3://bucket/t/a.{}", format.extension()),
                        size: Some(4096),
                        physical_format: format,
                        range: None,
                    },
                    ScanFile {
                        path: format!("s3://bucket/t/b.{}", format.extension()),
                        size: None,
                        physical_format: format,
                        range: None,
                    },
                ],
                default_column_value: HashMap::from([(
                    "part".to_string(),
                    "p0".to_string(),
                )]),
                lane: 0,
            },
            ScanWorkUnit {
                files: vec![ScanFile {
                    path: "s3://bucket/t/c.parquet".to_string(),
                    size: Some(1024),
                    physical_format: PhysicalFormat::Parquet,
                    range: None,
                }],
                default_column_value: HashMap::from([(
                    "part".to_string(),
                    "p2".to_string(),
                )]),
                lane: 2,
            },
        ];
        let config = LakeSoulScanConfig {
            table_schema: Arc::clone(&table),
            output_schema: Arc::clone(&table),
            partition_schema,
            io_config,
            predicate: Some(predicate),
            key_constraint: Some(KeyConstraint {
                columns: vec!["id".to_string()],
                values: vec![
                    Key::from_vec(vec![KeyValue::Int(7)]),
                    Key::from_vec(vec![KeyValue::Int(8)]),
                ],
            }),
            parquet_force_view_types: true,
            read_options: HashMap::from([(
                "datafusion.execution.parquet.pushdown_filters".to_string(),
                "true".to_string(),
            )]),
            limit: Some(3),
            // Not the default: the roundtrip has to show the planner's
            // per-unit share travels, not that the decoder falls back.
            min_index_rows: 256,
        };
        Arc::new(
            LakeSoulScanExec::try_new(config, units, 3)
                .expect("a valid metadata-only scan leaf"),
        )
    }

    /// A worker must execute a lazy scan exactly as the coordinator planned
    /// it: the same schemas, descriptors, lanes, read options and pk
    /// candidate set, with no object-store access during decode.
    #[test]
    fn lazy_scan_roundtrips_through_the_composed_codec() {
        for format in [
            PhysicalFormat::Parquet,
            PhysicalFormat::Vortex,
            PhysicalFormat::VortexCompact,
        ] {
            let original = lazy_scan(format);
            let decoded = roundtrip(Arc::clone(&original))
                .unwrap_or_else(|err| panic!("{format}: {err}"));
            let before = original
                .downcast_ref::<LakeSoulScanExec>()
                .expect("fixture is a metadata-only scan");
            let after = decoded
                .downcast_ref::<LakeSoulScanExec>()
                .unwrap_or_else(|| panic!("{format}: not a metadata-only scan"));

            assert_eq!(after.schema(), before.schema(), "{format}");
            assert_eq!(
                after.output_partitioning().partition_count(),
                before.output_partitioning().partition_count(),
                "{format}"
            );
            assert_eq!(after.is_merge_on_read(), before.is_merge_on_read());

            let before_config = before.scan_config();
            let after_config = after.scan_config();
            assert_eq!(after_config.table_schema, before_config.table_schema);
            assert_eq!(after_config.output_schema, before_config.output_schema);
            assert_eq!(
                after_config.partition_schema,
                before_config.partition_schema
            );
            assert_eq!(
                after_config.parquet_force_view_types,
                before_config.parquet_force_view_types
            );
            assert_eq!(after_config.read_options, before_config.read_options);
            assert_eq!(after_config.limit, before_config.limit, "{format}");
            assert_eq!(
                after_config.min_index_rows, before_config.min_index_rows,
                "{format}"
            );
            assert_eq!(
                after_config.predicate.as_ref().map(ToString::to_string),
                before_config.predicate.as_ref().map(ToString::to_string)
            );
            let before_keys = before_config
                .key_constraint
                .as_ref()
                .expect("fixture has a key constraint");
            let after_keys = after_config
                .key_constraint
                .as_ref()
                .expect("decode lost the key constraint");
            assert_eq!(after_keys.columns, before_keys.columns);
            assert_eq!(after_keys.values, before_keys.values);

            assert_eq!(
                after_config.io_config.primary_keys_slice(),
                before_config.io_config.primary_keys_slice()
            );
            assert_eq!(
                after_config.io_config.merge_operators(),
                before_config.io_config.merge_operators()
            );
            assert_eq!(
                after_config
                    .io_config
                    .options()
                    .iter()
                    .collect::<HashMap<_, _>>(),
                before_config
                    .io_config
                    .options()
                    .iter()
                    .collect::<HashMap<_, _>>()
            );
            assert_eq!(
                after_config.io_config.hash_bucket_num(),
                before_config.io_config.hash_bucket_num()
            );
            assert_eq!(
                after_config.io_config.range_partitions_slice(),
                before_config.io_config.range_partitions_slice()
            );
            assert_eq!(
                after_config.io_config.hash_partitioning_columns_slice(),
                before_config.io_config.hash_partitioning_columns_slice()
            );
            assert_eq!(
                after_config.io_config.max_pk_candidates(),
                before_config.io_config.max_pk_candidates()
            );
            assert_eq!(
                after_config.io_config.bulk_pk_lookup(),
                before_config.io_config.bulk_pk_lookup()
            );

            assert_eq!(after.work_units().len(), before.work_units().len());
            for (after_unit, before_unit) in
                after.work_units().iter().zip(before.work_units())
            {
                assert_eq!(after_unit.lane, before_unit.lane);
                assert_eq!(
                    after_unit.default_column_value,
                    before_unit.default_column_value
                );
                assert_eq!(after_unit.files.len(), before_unit.files.len());
                for (after_file, before_file) in
                    after_unit.files.iter().zip(&before_unit.files)
                {
                    assert_eq!(after_file.path, before_file.path);
                    assert_eq!(after_file.size, before_file.size);
                    assert_eq!(after_file.physical_format, before_file.physical_format);
                    assert_eq!(after_file.range, before_file.range);
                }
            }
        }
    }

    /// The lazy codec must not claim foreign nodes: the composed codec picks
    /// the first encoder that succeeds.
    #[test]
    fn lazy_scan_codec_rejects_foreign_nodes() {
        let other: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty())),
        );
        let mut buf = Vec::new();
        assert!(
            LakeSoulScanCodec
                .try_encode(other, &mut buf, &DefaultPhysicalProtoConverter {})
                .is_err()
        );
    }

    /// A malformed descriptor fails while decoding — i.e. while planning the
    /// worker side — instead of when the node first reads a file. The cases
    /// mirror the validation `LakeSoulScanExec::try_new` repeats.
    #[test]
    fn decode_rejects_malformed_lazy_scan_descriptors() {
        let exec = lazy_scan(PhysicalFormat::Parquet);
        let scan = exec.downcast_ref::<LakeSoulScanExec>().unwrap();
        let valid = LakeSoulScanCodec::proto_from_scan_exec(
            scan,
            &LakeSoulScanCodec,
            &DefaultPhysicalProtoConverter {},
        )
        .unwrap();

        let mut cases: Vec<(&str, LakeSoulScanExecProto)> = Vec::new();
        for (case, mutate) in [
            (
                "zero output partitions",
                (|p: &mut LakeSoulScanExecProto| p.partition_count = 0)
                    as fn(&mut LakeSoulScanExecProto),
            ),
            ("lane outside output partitions", |p| {
                p.work_units[0].lane = 3
            }),
            ("missing table schema", |p| p.table_schema = None),
            ("missing output schema", |p| p.output_schema = None),
            ("missing partition schema", |p| p.partition_schema = None),
            ("empty file path", |p| p.work_units[1].files[0].path.clear()),
            ("unknown physical format", |p| {
                p.work_units[1].files[0].physical_format = "orca".to_string()
            }),
            ("repeated descriptor", |p| {
                let repeated = p.work_units[0].files[0].clone();
                p.work_units[1].files.push(repeated);
            }),
            ("key constraint without columns", |p| {
                let constraint = p.key_constraint.as_mut().unwrap();
                constraint.columns.clear();
            }),
            ("key constraint without values", |p| {
                let constraint = p.key_constraint.as_mut().unwrap();
                constraint.values.clear();
            }),
            ("key constraint arity mismatch", |p| {
                p.key_constraint.as_mut().unwrap().values.push(KeyProto {
                    values: vec![
                        KeyValueProto {
                            value: Some(key_value_proto::Value::Int(1)),
                        },
                        KeyValueProto {
                            value: Some(key_value_proto::Value::Int(2)),
                        },
                    ],
                });
            }),
            ("previous codec version", |p| {
                p.codec_version = CODEC_VERSION - 1
            }),
        ] {
            let mut proto = valid.clone();
            mutate(&mut proto);
            cases.push((case, proto));
        }

        for (case, proto) in cases {
            let result = LakeSoulScanCodec.try_decode(
                &proto.encode_to_vec(),
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            );
            assert!(result.is_err(), "{case} must fail decoding");
        }

        let error = LakeSoulScanCodec
            .try_decode(
                &[0xff, 0xff],
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(matches!(error, DataFusionError::Internal(_)), "{error}");

        // The leaf takes no input plans.
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty())),
        );
        let error = LakeSoulScanCodec
            .try_decode(
                &valid.encode_to_vec(),
                &[input],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(error.to_string().contains("takes no input"), "{error}");
    }

    /// A descriptor file carries a byte range only when both bounds are
    /// present, ordered and within the declared size; this is checked while
    /// decoding, before any plan node is built.
    #[test]
    fn decode_rejects_malformed_file_ranges() {
        let base = || LakeSoulScanFileProto {
            path: "s3://bucket/t/a.parquet".to_string(),
            physical_format: "parquet".to_string(),
            size: Some(1024),
            range_start: None,
            range_end: None,
        };

        let mut one_bound = base();
        one_bound.range_start = Some(256);
        assert!(scan_file_from_proto(one_bound).is_err());

        let mut reversed = base();
        reversed.range_start = Some(800);
        reversed.range_end = Some(256);
        assert!(scan_file_from_proto(reversed).is_err());

        let mut negative = base();
        negative.range_start = Some(-1);
        negative.range_end = Some(256);
        assert!(scan_file_from_proto(negative).is_err());

        let mut past_size = base();
        past_size.range_start = Some(256);
        past_size.range_end = Some(4096);
        assert!(scan_file_from_proto(past_size).is_err());

        let mut legal = base();
        legal.range_start = Some(256);
        legal.range_end = Some(768);
        assert!(scan_file_from_proto(legal).is_ok());
    }

    /// Every [`KeyValue`] variant, including the ones no comparison operator
    /// produces directly, survives the typed oneof unchanged.
    #[test]
    fn key_values_roundtrip_through_the_wire_type() {
        let values = [
            KeyValue::Bool(true),
            KeyValue::Int(-7),
            KeyValue::UInt(7),
            KeyValue::Float(1.5f64.to_bits()),
            KeyValue::Utf8("p0".into()),
            KeyValue::Binary(vec![0, 255, 3].into_boxed_slice()),
            KeyValue::Decimal(-123_456_789_012_345_i128),
        ];
        for value in values {
            let decoded = key_value_from_proto(key_value_to_proto(&value))
                .expect("a key value round-trips");
            assert_eq!(decoded, value);
        }

        let short_decimal = KeyValueProto {
            value: Some(key_value_proto::Value::Decimal(vec![1, 2, 3])),
        };
        assert!(key_value_from_proto(short_decimal).is_err());
        let empty = KeyValueProto { value: None };
        assert!(key_value_from_proto(empty).is_err());
    }

    /// The gate must accept every format the worker's registry can read from a
    /// descriptor; this is also the compile-time decision point for a future
    /// format (`physical_format_has_worker_reader`).
    #[test]
    fn every_physical_format_of_a_lazy_scan_has_a_worker_reader() {
        for format in [
            PhysicalFormat::Parquet,
            PhysicalFormat::Vortex,
            PhysicalFormat::VortexCompact,
        ] {
            let exec = lazy_scan(format);
            let scan = exec.downcast_ref::<LakeSoulScanExec>().unwrap();
            assert!(lazy_scan_has_wire_form(scan), "{format}");
        }
    }

    /// Credentials never cross the wire: only the non-secret `options` map is
    /// encoded, not `object_store_options`.
    #[test]
    fn lazy_scan_plan_carries_no_object_store_credentials() {
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_primary_keys(vec!["id".to_string()])
            .with_object_store_option("fs.s3a.access.key", "ACCESS-KEY-SENTINEL")
            .with_object_store_option("fs.s3a.secret.key", "SECRET-KEY-SENTINEL")
            .with_object_store_option("fs.s3a.endpoint", "https://s3.invalid:9000")
            .with_option(OPTION_KEY_IS_COMPACTED.to_string(), "true".to_string())
            .build();
        let table: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Utf8, false),
        ]));
        let config = LakeSoulScanConfig {
            table_schema: Arc::clone(&table),
            output_schema: Arc::clone(&table),
            partition_schema: Arc::new(Schema::new(vec![Field::new(
                "part",
                DataType::Utf8,
                false,
            )])),
            io_config,
            predicate: None,
            key_constraint: None,
            parquet_force_view_types: false,
            read_options: HashMap::new(),
            limit: None,
            min_index_rows: lakesoul_io::pk_locator::MIN_INDEX_ROWS,
        };
        let units = vec![ScanWorkUnit {
            files: vec![ScanFile {
                path: "s3://bucket/t/file-0.parquet".to_string(),
                size: Some(1024),
                physical_format: PhysicalFormat::Parquet,
                range: None,
            }],
            default_column_value: HashMap::from([("part".to_string(), "p0".to_string())]),
            lane: 0,
        }];
        let exec = LakeSoulScanExec::try_new(config, units, 1).expect("valid scan");
        let mut buf = Vec::new();
        LakeSoulScanCodec
            .try_encode(Arc::new(exec), &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("encode");
        let encoded = String::from_utf8_lossy(&buf);

        assert!(encoded.contains("bucket/t/file-0.parquet"), "{encoded}");
        for secret in ["ACCESS-KEY-SENTINEL", "SECRET-KEY-SENTINEL", "s3.invalid"] {
            assert!(!encoded.contains(secret), "plan leaked {secret}: {encoded}");
        }
    }
}
