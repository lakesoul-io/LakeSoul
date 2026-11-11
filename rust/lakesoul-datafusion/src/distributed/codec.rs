// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan codec for LakeSoul-specific execution plan nodes.
//!
//! The distributed planner serializes every worker stage with
//! [`datafusion_proto::protobuf::PhysicalPlanNode`]. Standard DataFusion
//! nodes (including `DataSourceExec` over `FileScanConfig` with
//! `ParquetSource`) are handled by `datafusion-proto` itself. The
//! metadata-only [`LakeSoulScanExec`] is encoded by [`LakeSoulScanCodec`]
//! and the INSERT nodes [`LakeSoulWriteExec`]/[`LakeSoulCommitExec`] by
//! [`LakeSoulWriteCodec`], both composed after the distributed codec via
//! `with_distributed_user_codec` on the coordinator and worker sessions. A
//! [`MergeParquetExec`] has no wire form: its payload used to describe spans
//! into the scan children it merges, so a stage that would have to ship one is
//! refused while planning instead.
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
//! The write half of an INSERT crosses the wire as [`LakeSoulWriteExec`], the
//! file-writing node, and is carried by [`LakeSoulWriteCodec`]. A keyed write
//! also carries its insert-only state, including the complete captured
//! existing-key scan as a nested plan, so a worker validates keys itself. Its sibling
//! [`LakeSoulCommitExec`] publishes the statement with the metadata client,
//! the plan-time snapshot, and the object-store credentials, so it is
//! coordinator-only and this codec refuses to encode it.
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

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::common::{exec_err, internal_err};
use datafusion::datasource::physical_plan::{
    CsvSource, FileSource, JsonSource, ParquetSource,
};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::{PhysicalFormat, vortex_scan_session};
use lakesoul_io::physical_plan::{
    LakeSoulScanConfig, LakeSoulScanExec, ScanFile, ScanWorkUnit,
};
use lakesoul_io::pk_locator::{Key, KeyConstraint, KeyValue};
use lakesoul_metadata_proto::entity::TableInfo;
use prost::Message as _;
use rootcause::compat::boxed_error::IntoBoxedError;
use vortex_datafusion::{VortexScanCodec, VortexSource};

use crate::datasource::file_format::{InsertOnly, LakeSoulCommitExec, LakeSoulWriteExec};
use crate::datasource::upsert_exec::{UpsertExec, UpsertMode};
use crate::federation::external_file::{ExternalFileScanCodec, ExternalFileWriteCodec};
use crate::write_mode::ConflictMode;

/// Wire-format version of the LakeSoul extension payloads
/// ([`LakeSoulScanExecProto`] and [`LakeSoulWriteExecProto`]).
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
/// message layout moved. The distributed format is still unreleased, so the
/// generation stays at 2 while the list grows: every coordinator and worker
/// must run the same build. Do not bump it for an unreleased change, including
/// registering a new codec: append the codec to [`user_codecs`] and keep the
/// number at 2. Bump it only once a wire format has shipped or two
/// independently deployed builds must interoperate. It covers the vortex scan
/// codec at position 1, the metadata-only scan codec at position 2, the write
/// codec at position 3, the federation codec at position 4, the IVM refresh
/// codec at position 5 when the `ivm` feature is enabled, then
/// [`ExternalFileScanCodec`], [`ExternalFileWriteCodec`] and
/// [`UpsertExecCodec`] for the ordered per-key upsert state machine. The
/// merge-on-read payload that `/1` implied lost its codec, which is the same
/// kind of list change.
pub const CODEC_VERSION: u32 = 2;

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

/// Wire format for [`LakeSoulWriteExec`], the worker-side half of an INSERT.
///
/// Everything execution needs from the coordinator travels here: the table
/// metadata, the statement id stamped into this statement's file names, the
/// table's required input ordering, whether a worker task sorts and coalesces
/// its share locally, and the insert-only validation state when the statement
/// rejects or skips conflicting keys. The write's input is always input child 0
/// and only child 0; the insert-only state embeds the complete captured
/// existing-key scan as a nested physical plan, so every worker streams the
/// whole history independently instead of joining a shared broadcast subtree.
///
/// Not encoded, by design:
///
/// - the metadata client, the plan-time snapshot, and the object-store
///   credentials of [`LakeSoulCommitExec`], which stays on the coordinator:
///   a worker validates against the embedded scan and never publishes, so the
///   snapshot exists only in the commit node that captured it;
/// - IO-config `object_store_options`: workers read storage credentials from
///   their own environment, exactly like the scan codecs (see the module docs);
/// - a statement's `attempted_files` registry: each task starts its own, and a
///   failed distributed statement is cleaned up by the coordinator from the
///   `write_id` prefix of the files it never committed.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LakeSoulWriteExecProto {
    /// Prost-encoded [`TableInfo`]; the table's schema, partitions, properties,
    /// table path and identity. It carries no credentials.
    #[prost(bytes = "vec", tag = "1")]
    pub table_info: Vec<u8>,
    /// Statement id shared by every task of the statement; becomes part of
    /// every written file name, so decoding validates it.
    #[prost(string, tag = "2")]
    pub write_id: String,
    /// The table's required input ordering, empty when it has none.
    #[prost(message, repeated, tag = "3")]
    pub sort_order: Vec<WriteSortRequirementProto>,
    /// [`CODEC_VERSION`] of the encoder; appended rather than numbered first so
    /// the layout stays additively evolvable.
    #[prost(uint32, tag = "4")]
    pub codec_version: u32,
    /// Whether this worker stage sorts and coalesces its share of the input
    /// before routing rows to LakeSoul's persisted buckets.
    #[prost(bool, tag = "5")]
    pub task_partitioning: bool,
    /// Existing-key validation state; present exactly when the write validates
    /// keys. Its scan is embedded in the payload, never an input child.
    #[prost(message, optional, tag = "6")]
    pub insert_only: Option<WriteInsertOnlyProto>,
}

/// Wire format for the insert-only validation state of a [`LakeSoulWriteExec`].
///
/// The key schema travels as a protobuf [`Schema`](datafusion_proto::protobuf::Schema)
/// because validation casts both sides of the comparison with it. The conflict
/// mode is an enum with explicit values: an unknown value is refused at decode
/// rather than silently falling back to reject or skip. The existing-key scan is
/// a nested [`PhysicalPlanNode`](datafusion_proto::protobuf::PhysicalPlanNode)
/// encoded with the same composed codec as the stage itself: the worker
/// decodes and executes the complete captured scan locally, so it needs no
/// second input child and no shared broadcast/replay state.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteInsertOnlyProto {
    /// Range columns followed by primary-key columns, in stored types.
    #[prost(message, optional, tag = "1")]
    pub key_schema: Option<datafusion_proto::protobuf::Schema>,
    /// [`WriteConflictModeProto`] of the statement.
    #[prost(enumeration = "WriteConflictModeProto", tag = "2")]
    pub conflict_mode: i32,
    /// Qualified table name used by the violation message.
    #[prost(string, tag = "3")]
    pub table_name: String,
    /// Complete captured existing-key scan, encoded as a
    /// [`PhysicalPlanNode`](datafusion_proto::protobuf::PhysicalPlanNode)
    /// with the production composed codec.
    #[prost(bytes = "vec", tag = "4")]
    pub existing_keys: Vec<u8>,
}

/// How an insert-only statement handles a conflicting key.
///
/// The wire values are explicit and stable; decoding refuses anything else.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
)]
#[repr(i32)]
pub enum WriteConflictModeProto {
    /// Fail the statement on the first conflicting key (`INSERT`).
    Reject = 0,
    /// Drop conflicting rows from the statement (`ON CONFLICT DO NOTHING`).
    Skip = 1,
}

/// One required input ordering entry of [`LakeSoulWriteExec`].
///
/// A [`PhysicalSortRequirement`] may leave the sort options unspecified ("no
/// constraint"), which `PhysicalSortExprNode` cannot express, so both options
/// are optional here and must be present or absent together.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteSortRequirementProto {
    #[prost(message, optional, tag = "1")]
    pub expr: Option<datafusion_proto::protobuf::PhysicalExprNode>,
    #[prost(bool, optional, tag = "2")]
    pub ascending: Option<bool>,
    #[prost(bool, optional, tag = "3")]
    pub nulls_first: Option<bool>,
}

/// Wire format for [`UpsertExec`], the sequential upsert state machine.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertExecProto {
    #[prost(uint32, tag = "1")]
    pub codec_version: u32,
    #[prost(message, optional, tag = "2")]
    pub output_schema: Option<datafusion_proto::protobuf::Schema>,
    #[prost(uint64, repeated, tag = "3")]
    pub key_columns: Vec<u64>,
    #[prost(uint64, repeated, tag = "4")]
    pub old_columns: Vec<u64>,
    #[prost(uint64, repeated, tag = "5")]
    pub new_columns: Vec<u64>,
    /// `-1` means the source has no match marker (legacy insert-only mode).
    #[prost(int64, tag = "6")]
    pub matched_column: i64,
    #[prost(uint64, repeated, tag = "7")]
    pub not_null_columns: Vec<u64>,
    #[prost(string, repeated, tag = "8")]
    pub not_null_names: Vec<String>,
    #[prost(message, optional, tag = "9")]
    pub where_expr: Option<datafusion_proto::protobuf::PhysicalExprNode>,
    #[prost(message, repeated, tag = "10")]
    pub assignments: Vec<UpsertAssignmentProto>,
    #[prost(string, tag = "11")]
    pub table_name: String,
    #[prost(enumeration = "UpsertModeProto", tag = "12")]
    pub mode: i32,
    #[prost(uint64, optional, tag = "13")]
    pub order_column: Option<u64>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertAssignmentProto {
    #[prost(bool, tag = "1")]
    pub present: bool,
    #[prost(message, optional, tag = "2")]
    pub expr: Option<datafusion_proto::protobuf::PhysicalExprNode>,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
)]
#[repr(i32)]
pub enum UpsertModeProto {
    Update = 0,
    LegacyInsertOnly = 1,
}

/// [`PhysicalExtensionCodec`] for [`UpsertExec`].
#[derive(Debug, Clone, Default)]
pub struct UpsertExecCodec;

impl PhysicalExtensionCodec for UpsertExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = UpsertExecProto::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("decode UpsertExec: {err}"))
        })?;
        if proto.codec_version != CODEC_VERSION {
            return Err(DataFusionError::Internal(format!(
                "UpsertExec plan encoded with codec version {} but this build \
                 speaks {CODEC_VERSION}: coordinator and workers must run \
                 compatible LakeSoul builds",
                proto.codec_version
            )));
        }
        let [input] = inputs else {
            return Err(DataFusionError::Internal(
                "UpsertExec requires exactly one input".to_string(),
            ));
        };
        let output_schema: SchemaRef = Arc::new(
            proto
                .output_schema
                .as_ref()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "UpsertExec is missing output_schema".to_string(),
                    )
                })?
                .try_into()?,
        );
        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let where_expr = proto_converter
            .proto_to_physical_expr(
                proto.where_expr.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "UpsertExec is missing where_expr".to_string(),
                    )
                })?,
                input.schema().as_ref(),
                &decode_ctx,
            )
            .map_err(|err| {
                DataFusionError::Internal(format!("decode UpsertExec where_expr: {err}"))
            })?;
        let mut assignments = Vec::with_capacity(proto.assignments.len());
        for assignment in &proto.assignments {
            let expression = match (&assignment.expr, assignment.present) {
                (Some(expr), true) => Some(
                    proto_converter
                        .proto_to_physical_expr(
                            expr,
                            input.schema().as_ref(),
                            &decode_ctx,
                        )
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "decode UpsertExec assignment: {err}"
                            ))
                        })?,
                ),
                (None, false) => None,
                _ => {
                    return Err(DataFusionError::Internal(
                        "UpsertExec assignment flag/expression mismatch".to_string(),
                    ));
                }
            };
            assignments.push(expression);
        }
        if proto.not_null_columns.len() != proto.not_null_names.len() {
            return Err(DataFusionError::Internal(
                "UpsertExec not-null index/name length mismatch".to_string(),
            ));
        }
        let not_null = proto
            .not_null_columns
            .iter()
            .zip(proto.not_null_names.iter())
            .map(|(index, name)| {
                Ok((
                    usize::try_from(*index).map_err(|_| {
                        DataFusionError::Internal(
                            "UpsertExec not-null index does not fit usize".to_string(),
                        )
                    })?,
                    name.clone(),
                ))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let matched_column = if proto.matched_column == -1 {
            None
        } else if proto.matched_column < -1 {
            return Err(DataFusionError::Internal(
                "UpsertExec match marker must be -1 or a column index".into(),
            ));
        } else {
            Some(usize::try_from(proto.matched_column).map_err(|_| {
                DataFusionError::Internal(
                    "UpsertExec matched column does not fit usize".to_string(),
                )
            })?)
        };
        let mode = UpsertModeProto::try_from(proto.mode).map_err(|_| {
            DataFusionError::Internal(format!("unknown UpsertExec mode {}", proto.mode))
        })?;
        let mode = match mode {
            UpsertModeProto::Update => UpsertMode::Update,
            UpsertModeProto::LegacyInsertOnly => UpsertMode::LegacyInsertOnly,
        };
        let decode = |name: &str, values: &[u64]| {
            values
                .iter()
                .map(|value| {
                    usize::try_from(*value).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "UpsertExec {name} index does not fit usize"
                        ))
                    })
                })
                .collect::<DFResult<Vec<_>>>()
        };
        let exec = UpsertExec::try_new(
            Arc::clone(input),
            output_schema,
            decode("key", &proto.key_columns)?,
            usize::try_from(proto.order_column.ok_or_else(|| {
                DataFusionError::Internal(
                    "UpsertExec is missing its source-order column".into(),
                )
            })?)
            .map_err(|_| {
                DataFusionError::Internal(
                    "UpsertExec source-order index does not fit usize".into(),
                )
            })?,
            decode("old", &proto.old_columns)?,
            decode("new", &proto.new_columns)?,
            matched_column,
            not_null,
            where_expr,
            assignments,
            proto.table_name,
            mode,
        )?;
        Ok(Arc::new(exec))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        let Some(exec) = node.downcast_ref::<UpsertExec>() else {
            return exec_err!("UpsertExecCodec cannot encode {}", node.name());
        };
        let output_schema = exec.output_schema().as_ref().try_into().map_err(|err| {
            DataFusionError::Internal(format!("encode UpsertExec output_schema: {err}"))
        })?;
        let where_expr = proto_converter
            .physical_expr_to_proto(exec.where_expr(), self)
            .map_err(|err| {
                DataFusionError::Internal(format!("encode UpsertExec where_expr: {err}"))
            })?;
        let mut assignments = Vec::with_capacity(exec.assignments().len());
        for assignment in exec.assignments() {
            let expression = match assignment {
                Some(expression) => Some(
                    proto_converter
                        .physical_expr_to_proto(expression, self)
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "encode UpsertExec assignment: {err}"
                            ))
                        })?,
                ),
                None => None,
            };
            assignments.push(UpsertAssignmentProto {
                present: expression.is_some(),
                expr: expression,
            });
        }
        let proto = UpsertExecProto {
            codec_version: CODEC_VERSION,
            output_schema: Some(output_schema),
            key_columns: exec.key_columns().iter().map(|v| *v as u64).collect(),
            order_column: Some(exec.order_column() as u64),
            old_columns: exec.old_columns().iter().map(|v| *v as u64).collect(),
            new_columns: exec.new_columns().iter().map(|v| *v as u64).collect(),
            matched_column: exec.matched_column().map_or(-1, |v| v as i64),
            not_null_columns: exec
                .not_null_columns()
                .iter()
                .map(|(index, _)| *index as u64)
                .collect(),
            not_null_names: exec
                .not_null_columns()
                .iter()
                .map(|(_, name)| name.clone())
                .collect(),
            where_expr: Some(where_expr),
            assignments,
            table_name: exec.table_name().to_string(),
            mode: match exec.mode() {
                UpsertMode::Update => UpsertModeProto::Update as i32,
                UpsertMode::LegacyInsertOnly => UpsertModeProto::LegacyInsertOnly as i32,
            },
        };
        buf.reserve(proto.encoded_len());
        proto.encode(buf).map_err(|err| {
            DataFusionError::Internal(format!("encode UpsertExec: {err}"))
        })?;
        Ok(())
    }
}

/// [`PhysicalExtensionCodec`] for [`LakeSoulWriteExec`], the worker-side write
/// node of an INSERT.
///
/// Kept separate from [`LakeSoulScanCodec`] rather than folded into it: the
/// position in [`user_codecs`] is the dispatch key, and each codec only has to
/// recognise its own node. Encoding refuses [`LakeSoulCommitExec`]
/// explicitly — that node holds the metadata client and the coordinator's
/// credentials and must never reach a worker.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulWriteCodec;

/// Longest accepted statement id. The coordinator generates a 16-character
/// alphanumeric id; the cap only keeps a tampered plan from inflating a path.
const MAX_WRITE_ID_LEN: usize = 64;

/// The user codecs a distributed session registers, in registration order.
///
/// The coordinator's planner and every worker build their codec list from this
/// one function so the positions an extension payload refers to cannot drift:
/// position 0 is always the distributed codec itself, position 1 the vortex
/// scan codec, position 2 [`LakeSoulScanCodec`] (metadata-only lazy scans) and
/// position 3 [`LakeSoulWriteCodec`] (distributed write stages), position 4
/// [`crate::federation::FederationPlanCodec`] (ADBC foreign scans and writes),
/// position 5 the IVM refresh codec when the `ivm` feature is enabled, then
/// [`ExternalFileScanCodec`], [`ExternalFileWriteCodec`] and finally
/// [`UpsertExecCodec`] for the ordered per-key upsert state machine. A worker
/// registers a different list either fails to decode a stage or would hand a
/// payload to the wrong codec, so the list is part of the wire generation
/// ([`CODEC_VERSION`]).
///
/// The vortex codec is built from [`vortex_scan_session`], the session
/// [`lakesoul_io::file_format::LakeSoulFormatRegistry`] plans scans with, so a
/// rebuilt worker-side scan decodes files exactly like the coordinator's.
pub fn user_codecs() -> Vec<Arc<dyn PhysicalExtensionCodec>> {
    #[allow(unused_mut)]
    let mut codecs: Vec<Arc<dyn PhysicalExtensionCodec>> = vec![
        Arc::new(VortexScanCodec::new(vortex_scan_session())),
        Arc::new(LakeSoulScanCodec),
        Arc::new(LakeSoulWriteCodec),
        Arc::new(crate::federation::FederationPlanCodec),
    ];
    #[cfg(feature = "ivm")]
    codecs.push(Arc::new(lakesoul_ivm::distributed::IvmRefreshCodec));
    codecs.push(Arc::new(ExternalFileScanCodec));
    codecs.push(Arc::new(ExternalFileWriteCodec));
    codecs.push(Arc::new(UpsertExecCodec));
    codecs
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
/// itself when it implements `try_to_proto` (parquet, csv and json), or an
/// extension codec in [`user_codecs`] carries it — currently
/// [`VortexScanCodec`] for [`VortexSource`] and [`ExternalFileScanCodec`] for
/// external-file scans. Anything else is
/// refused while planning, where the caller's fallback policy can still act on
/// it, instead of failing when the stage is sent to a worker.
///
/// Keep this in sync with [`user_codecs`]: a source is only encodable here
/// because a codec in that list encodes it.
pub fn source_has_wire_form(source: &dyn FileSource) -> bool {
    source.is::<ParquetSource>()
        || source.is::<CsvSource>()
        || source.is::<JsonSource>()
        || source.is::<VortexSource>()
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

impl LakeSoulWriteCodec {
    fn proto_from_write_exec(
        exec: &LakeSoulWriteExec,
        codec: &dyn PhysicalExtensionCodec,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<LakeSoulWriteExecProto> {
        // Expressions are encoded through `codec` because the extension hook
        // does not receive the composed codec. LakeSoul's order requirements
        // are columns (a write's required input ordering cannot be an
        // extension expression), and anything else fails here, at encode time,
        // where the planner's fallback policy can still act on it.
        let sort_order = match exec.sort_order() {
            Some(requirements) => requirements
                .iter()
                .map(|requirement| -> DFResult<WriteSortRequirementProto> {
                    let expr = proto_converter
                        .physical_expr_to_proto(&requirement.expr, codec)?;
                    Ok(WriteSortRequirementProto {
                        expr: Some(expr),
                        ascending: requirement.options.map(|options| !options.descending),
                        nulls_first: requirement
                            .options
                            .map(|options| options.nulls_first),
                    })
                })
                .collect::<DFResult<Vec<_>>>()?,
            None => Vec::new(),
        };
        let insert_only = match exec.insert_only() {
            Some(insert) => Some(WriteInsertOnlyProto {
                key_schema: Some(insert.key_schema.as_ref().try_into().map_err(
                    |err| {
                        DataFusionError::Internal(format!(
                            "encode LakeSoulWriteExec insert_only.key_schema: {err}"
                        ))
                    },
                )?),
                conflict_mode: conflict_mode_to_proto(insert.conflict_mode) as i32,
                table_name: insert.table_name.clone(),
                // The nested plan is encoded with the composed codec, the same
                // one a worker decodes stages with, so its scans rebuild there.
                existing_keys: PhysicalPlanNode::try_from_physical_plan(
                    Arc::clone(&insert.existing_keys),
                    &composed_codec(),
                )
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "encode LakeSoulWriteExec insert_only.existing_keys: {err}"
                    ))
                })?
                .encode_to_vec(),
            }),
            None => None,
        };
        Ok(LakeSoulWriteExecProto {
            table_info: exec.table_info().encode_to_vec(),
            write_id: exec.write_id().to_string(),
            sort_order,
            codec_version: CODEC_VERSION,
            task_partitioning: exec.task_partitioning(),
            insert_only,
        })
    }

    /// Validate a statement id decoded from the wire.
    ///
    /// The id becomes part of every file name this task writes, so anything
    /// outside ASCII alphanumerics (`/`, `.`, `\`) could move a file outside
    /// the table directory. The coordinator only ever generates alphanumerics.
    fn validate_write_id(write_id: &str) -> DFResult<()> {
        if write_id.is_empty()
            || write_id.len() > MAX_WRITE_ID_LEN
            || !write_id.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulWriteExecProto carries invalid write_id {write_id:?}: it becomes part \
                 of the written file names, so it must be 1..={MAX_WRITE_ID_LEN} ASCII \
                 alphanumeric characters"
            )));
        }
        Ok(())
    }
}

fn conflict_mode_to_proto(mode: ConflictMode) -> WriteConflictModeProto {
    match mode {
        ConflictMode::Reject => WriteConflictModeProto::Reject,
        ConflictMode::Skip => WriteConflictModeProto::Skip,
    }
}

/// Refuse an unknown conflict-mode value instead of defaulting: the two modes
/// disagree about whether a statement may succeed, so guessing could turn a
/// rejected INSERT into a silent partial load.
fn conflict_mode_from_proto(value: i32) -> DFResult<ConflictMode> {
    let mode = WriteConflictModeProto::try_from(value).map_err(|_| {
        DataFusionError::Internal(format!(
            "LakeSoulWriteExecProto insert_only.conflict_mode {value} is not a known \
             conflict mode (reject = 0, skip = 1): coordinator and workers must run \
             compatible LakeSoul builds"
        ))
    })?;
    Ok(match mode {
        WriteConflictModeProto::Reject => ConflictMode::Reject,
        WriteConflictModeProto::Skip => ConflictMode::Skip,
    })
}

impl PhysicalExtensionCodec for LakeSoulWriteCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto = LakeSoulWriteExecProto::decode(buf).map_err(|err| {
            DataFusionError::Internal(format!("decode LakeSoulWriteExec: {err}"))
        })?;
        // Fail loudly instead of guessing: a plan from another build may use a
        // different encoding for the same fields.
        if proto.codec_version != CODEC_VERSION {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulWriteExec plan encoded with codec version {} but this build \
                 speaks {CODEC_VERSION}: coordinator and workers must run compatible \
                 LakeSoul builds",
                proto.codec_version
            )));
        }
        // Child 0 is the write's input and the only child, insert-only or not;
        // the historical scan travels inside the payload. A plan with any other
        // shape would silently drop or misbind part of the source tree.
        let [input] = inputs else {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulWriteExecProto expects exactly one input child, got {}",
                inputs.len()
            )));
        };
        Self::validate_write_id(&proto.write_id)?;

        let table_info =
            TableInfo::decode(proto.table_info.as_slice()).map_err(|err| {
                DataFusionError::Internal(format!(
                    "decode LakeSoulWriteExec table_info: {err}"
                ))
            })?;
        // Without a table path every written file name would be relative.
        if table_info.table_path.is_empty() {
            return Err(DataFusionError::Internal(
                "LakeSoulWriteExecProto is missing table_info.table_path".into(),
            ));
        }
        let table_info = Arc::new(table_info);

        let decode_ctx = PhysicalPlanDecodeContext::new(ctx, self);
        let mut sort_order = Vec::with_capacity(proto.sort_order.len());
        for requirement in proto.sort_order {
            let expr = requirement.expr.ok_or_else(|| {
                DataFusionError::Internal(
                    "LakeSoulWriteExecProto sort requirement is missing its expression"
                        .into(),
                )
            })?;
            let expr = proto_converter.proto_to_physical_expr(
                &expr,
                input.schema().as_ref(),
                &decode_ctx,
            )?;
            let options = match (requirement.ascending, requirement.nulls_first) {
                (Some(ascending), Some(nulls_first)) => Some(SortOptions {
                    descending: !ascending,
                    nulls_first,
                }),
                // "No constraint on the sort direction" is a distinct state,
                // so half-specified options are malformed rather than defaulted.
                (None, None) => None,
                _ => {
                    return Err(DataFusionError::Internal(
                        "LakeSoulWriteExecProto sort requirement must carry both or \
                         neither of ascending and nulls_first"
                            .into(),
                    ));
                }
            };
            sort_order.push(PhysicalSortRequirement { expr, options });
        }

        let insert_only = match proto.insert_only {
            Some(insert) => {
                let key_schema: SchemaRef = Arc::new(
                    insert
                        .key_schema
                        .as_ref()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "LakeSoulWriteExecProto insert_only is missing key_schema"
                                    .into(),
                            )
                        })?
                        .try_into()?,
                );
                let conflict_mode = conflict_mode_from_proto(insert.conflict_mode)?;
                // The nested plan was encoded with the composed codec, so it
                // rebuilds exactly like a stage: every worker streams the whole
                // captured scan locally.
                let existing_keys = PhysicalPlanNode::try_decode(
                    insert.existing_keys.as_slice(),
                )
                .and_then(|node| node.try_into_physical_plan(ctx, &composed_codec()))
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "decode LakeSoulWriteExec insert_only.existing_keys: {err}"
                    ))
                })?;
                Some(InsertOnly {
                    existing_keys,
                    key_schema,
                    conflict_mode,
                    table_name: insert.table_name,
                })
            }
            None => None,
        };
        let exec = LakeSoulWriteExec::try_new(
            Arc::clone(input),
            LexRequirement::new(sort_order),
            table_info,
            proto.write_id,
        )
        .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
        let exec = match insert_only {
            Some(insert_only) => exec.with_insert_only(insert_only),
            None => exec,
        };
        let exec = if proto.task_partitioning {
            exec.with_task_partitioning()
        } else {
            exec
        };
        Ok(Arc::new(exec))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        // The commit node holds the metadata client, the plan-time snapshot,
        // and the object-store credentials. Shipping it would publish a
        // statement from every task independently, so refuse it by name.
        if node.downcast_ref::<LakeSoulCommitExec>().is_some() {
            return exec_err!(
                "LakeSoulCommitExec is coordinator-only and cannot be encoded for a \
                 worker: only LakeSoulWriteExec crosses the wire"
            );
        }
        // Must not succeed for foreign nodes: the composed codec picks the
        // first encoder that returns `Ok`.
        let Some(exec) = node.downcast_ref::<LakeSoulWriteExec>() else {
            return exec_err!("LakeSoulWriteCodec cannot encode {}", node.name());
        };
        let proto = Self::proto_from_write_exec(exec, self, proto_converter)?;
        buf.reserve(proto.encoded_len());
        proto.encode(buf).map_err(|err| {
            DataFusionError::Internal(format!("encode LakeSoulWriteExec: {err}"))
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
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::utils::collect_columns;
    use datafusion_datasource::PartitionedFile;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::config::OPTION_KEY_IS_COMPACTED;
    use lakesoul_io::file_format::{
        LakeSoulFormatRegistry, PhysicalFormat, append_only_scan_exec,
    };
    use lakesoul_io::writer::async_writer::WriteInvariants;

    use crate::write_mode::WriteViolation;
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

    /// The payload of a metadata-only scan, the format every generation test
    /// mutates. Encoding it is the coordinator side; the assertions below only
    /// touch the envelope the version lives in.
    fn encoded_lazy_scan() -> Vec<u8> {
        let exec = lazy_scan(PhysicalFormat::Parquet);
        let mut buf = Vec::new();
        LakeSoulScanCodec
            .try_encode(exec, &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("a metadata-only scan encodes");
        buf
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = LakeSoulScanCodec
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
        let proto = LakeSoulScanExecProto::decode(encoded_lazy_scan().as_slice())
            .expect("a scan payload decodes");
        assert_eq!(proto.codec_version, CODEC_VERSION);
    }

    /// A plan from an incompatible build is refused instead of being decoded
    /// with the wrong field interpretation.
    #[test]
    fn decode_rejects_other_codec_versions() {
        let mut proto = LakeSoulScanExecProto::decode(encoded_lazy_scan().as_slice())
            .expect("a scan payload decodes");
        proto.codec_version = CODEC_VERSION + 1;
        let bytes = proto.encode_to_vec();
        let err = LakeSoulScanCodec
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

        let mut unversioned =
            LakeSoulScanExecProto::decode(encoded_lazy_scan().as_slice())
                .expect("a scan payload decodes");
        unversioned.codec_version = 0;
        let err = LakeSoulScanCodec
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
        let generation = suffix
            .split('-')
            .next()
            .expect("numeric protocol generation");
        assert_eq!(
            generation
                .parse::<u32>()
                .expect("numeric protocol generation"),
            CODEC_VERSION,
            "bump DISTRIBUTED_PROTOCOL_VERSION together with CODEC_VERSION ({protocol})"
        );
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

    /// A table whose stored schema is the one-field `id: Int32 NOT NULL` the
    /// write fixtures feed, with no partitions.
    fn write_table_info() -> Arc<TableInfo> {
        Arc::new(TableInfo {
            table_id: "write-codec-table".to_string(),
            table_namespace: "default".to_string(),
            table_name: "write_codec".to_string(),
            table_path: "file:///tmp/write-codec/".to_string(),
            table_schema: r#"{"fields":[{"name":"id","nullable":false,"type":{"name":"int","isSigned":true,"bitWidth":32}}]}"#.to_string(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            ..Default::default()
        })
    }

    fn write_exec(
        write_id: &str,
        sort_order: Option<LexRequirement>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            LakeSoulWriteExec::try_new(
                parquet_input("s3://bucket/t/a.parquet"),
                sort_order,
                write_table_info(),
                write_id.to_string(),
            )
            .expect("valid write fixture"),
        )
    }

    fn write_proto(write_id: &str) -> LakeSoulWriteExecProto {
        LakeSoulWriteExecProto {
            table_info: write_table_info().encode_to_vec(),
            write_id: write_id.to_string(),
            sort_order: Vec::new(),
            codec_version: CODEC_VERSION,
            task_partitioning: false,
            insert_only: None,
        }
    }

    /// The range-column-then-PK key schema of the one-key fixtures.
    fn key_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn key_schema_proto() -> datafusion_proto::protobuf::Schema {
        key_schema()
            .as_ref()
            .try_into()
            .expect("encode key schema to protobuf")
    }

    /// Encode a scan the way the write codec embeds the historical key scan.
    fn nested_scan_bytes(exec: Arc<dyn ExecutionPlan>) -> Vec<u8> {
        PhysicalPlanNode::try_from_physical_plan(exec, &composed_codec())
            .expect("nested scan encodes")
            .encode_to_vec()
    }

    fn insert_only_proto(
        mode: WriteConflictModeProto,
        key_schema: Option<datafusion_proto::protobuf::Schema>,
        table_name: &str,
    ) -> WriteInsertOnlyProto {
        WriteInsertOnlyProto {
            key_schema,
            conflict_mode: mode as i32,
            table_name: table_name.to_string(),
            existing_keys: nested_scan_bytes(parquet_input(
                "s3://bucket/t/existing.parquet",
            )),
        }
    }

    /// A write plan whose statement validates keys against `existing_keys`.
    fn keyed_write(
        existing_keys: Arc<dyn ExecutionPlan>,
        conflict_mode: ConflictMode,
    ) -> LakeSoulWriteExec {
        LakeSoulWriteExec::try_new(
            parquet_input("s3://bucket/t/input.parquet"),
            None,
            write_table_info(),
            "keyedwrite000001".to_string(),
        )
        .expect("valid write fixture")
        .with_insert_only(InsertOnly {
            existing_keys,
            key_schema: key_schema(),
            conflict_mode,
            table_name: "default.write_codec".to_string(),
        })
    }

    /// A single-partition in-memory input, so a decoded write task can run its
    /// key validation without object storage.
    fn memory_input(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0].schema();
        datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
            &[batches],
            schema,
            None,
        )
        .expect("valid memory input")
    }

    fn id_batch(ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            key_schema(),
            vec![Arc::new(arrow::array::Int32Array::from(ids.to_vec()))],
        )
        .expect("valid id batch")
    }

    fn id_sort_requirement(options: Option<SortOptions>) -> LexRequirement {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        LexRequirement::new(vec![PhysicalSortRequirement {
            expr: col("id", &schema).expect("column expression"),
            options,
        }])
        .expect("non-empty ordering")
    }

    /// The statement id is what the coordinator uses to name and, on failure,
    /// to clean up a distributed statement's files, so it must survive the
    /// wire unchanged together with the table it writes into.
    #[test]
    fn write_exec_roundtrips_with_its_statement_id() {
        let decoded = roundtrip(write_exec("statement0000001", None)).unwrap();

        assert_eq!(decoded.name(), "LakeSoulWriteExec");
        let write = decoded
            .downcast_ref::<LakeSoulWriteExec>()
            .expect("a write stage decodes as LakeSoulWriteExec");
        assert_eq!(write.write_id(), "statement0000001");
        assert_eq!(write.table_info().table_id, "write-codec-table");
        assert_eq!(write.table_info().table_path, "file:///tmp/write-codec/");
        assert!(write.sort_order().is_none());
        assert_eq!(decoded.children().len(), 1);
    }

    /// A required input ordering decides `required_input_ordering` of the
    /// rebuilt node; losing it would let the optimizer reorder a PK writer's
    /// input on the worker.
    #[test]
    fn write_exec_sort_order_roundtrips() {
        let sort_order = id_sort_requirement(Some(SortOptions {
            descending: true,
            nulls_first: false,
        }));
        let decoded =
            roundtrip(write_exec("sortedstatement01", Some(sort_order))).unwrap();
        let write = decoded.downcast_ref::<LakeSoulWriteExec>().unwrap();

        let order = write.sort_order().as_ref().expect("ordering preserved");
        assert_eq!(order.len(), 1);
        assert_eq!(
            order[0].options,
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            })
        );
        let column = order[0]
            .expr
            .downcast_ref::<datafusion::physical_expr::expressions::Column>()
            .expect("the requirement is a column");
        assert_eq!((column.name(), column.index()), ("id", 0));
        assert!(decoded.required_input_ordering()[0].is_some());
    }

    /// "No constraint on the sort direction" is not the same as ascending
    /// nulls-first, so it needs its own wire representation.
    #[test]
    fn write_exec_unconstrained_sort_options_stay_unconstrained() {
        let decoded = roundtrip(write_exec(
            "unconstrained001",
            Some(id_sort_requirement(None)),
        ))
        .unwrap();
        let write = decoded.downcast_ref::<LakeSoulWriteExec>().unwrap();
        let order = write.sort_order().as_ref().expect("ordering preserved");
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].options, None);
    }

    #[test]
    fn write_codec_encode_requires_write_exec() {
        let other: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty())),
        );
        let mut buf = Vec::new();
        assert!(
            LakeSoulWriteCodec
                .try_encode(other, &mut buf, &DefaultPhysicalProtoConverter {})
                .is_err()
        );
    }

    /// A write stage's input is a LakeSoul merge/scan tree, so the two
    /// LakeSoul codecs have to compose on one wire payload: the write codec
    /// carries the write, position 1 carries the child it re-attaches.
    #[test]
    fn write_exec_roundtrips_over_a_scan_child() {
        let table_info = Arc::new(TableInfo {
            table_path: "file:///tmp/write-codec/".to_string(),
            table_schema: r#"{"fields":[{"name":"id","nullable":false,"type":{"name":"int","isSigned":true,"bitWidth":32}},{"name":"name","nullable":true,"type":{"name":"utf8"}}]}"#.to_string(),
            partitions: ";".to_string(),
            ..Default::default()
        });
        // The production shape: a write over the metadata-only scan the
        // coordinator ships, so the child is carried by its own codec.
        let input_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let config = LakeSoulScanConfig {
            table_schema: Arc::clone(&input_schema),
            output_schema: Arc::clone(&input_schema),
            partition_schema: Arc::new(Schema::empty()),
            io_config: LakeSoulIOConfigBuilder::new()
                .with_primary_keys(vec!["id".to_string()])
                .build(),
            predicate: None,
            key_constraint: None,
            parquet_force_view_types: false,
            read_options: HashMap::new(),
            limit: None,
            min_index_rows: lakesoul_io::pk_locator::MIN_INDEX_ROWS,
        };
        let units = vec![ScanWorkUnit {
            files: vec![ScanFile {
                path: "s3://bucket/t/input.parquet".to_string(),
                size: Some(1024),
                physical_format: PhysicalFormat::Parquet,
                range: None,
            }],
            default_column_value: HashMap::new(),
            lane: 0,
        }];
        let child: Arc<dyn ExecutionPlan> = Arc::new(
            LakeSoulScanExec::try_new(config, units, 1)
                .expect("a valid metadata-only scan"),
        );
        let write = LakeSoulWriteExec::try_new(
            child,
            None,
            table_info,
            "nestedwrite00001".to_string(),
        )
        .expect("valid nested write fixture");

        let decoded = roundtrip(Arc::new(write)).unwrap();
        let write = decoded.downcast_ref::<LakeSoulWriteExec>().unwrap();
        assert_eq!(write.write_id(), "nestedwrite00001");
        assert_eq!(decoded.children().len(), 1);
        let scan = decoded.children()[0]
            .downcast_ref::<LakeSoulScanExec>()
            .expect("the scan child survives the wire");
        assert_eq!(scan.work_units().len(), 1, "the child's unit survives");
    }

    /// A keyed write is encodable end to end: the decoded worker task keeps the
    /// statement's conflict transition and the flag that makes it sort its own
    /// share, instead of falling back to a coordinator-only plan.
    #[test]
    fn write_exec_insert_only_roundtrips_as_a_worker_stage() {
        for mode in [ConflictMode::Reject, ConflictMode::Skip] {
            let write =
                keyed_write(parquet_input("s3://bucket/t/existing.parquet"), mode)
                    .with_task_partitioning();

            let decoded = roundtrip(Arc::new(write)).unwrap();
            let write = decoded.downcast_ref::<LakeSoulWriteExec>().unwrap();
            assert!(write.task_partitioning());
            assert_eq!(
                write
                    .insert_only()
                    .expect("validation state preserved")
                    .conflict_mode,
                mode
            );
        }
    }

    /// The embedded historical scan is a physical plan; a malformed payload
    /// must fail at decode instead of producing a worker task that cannot
    /// validate keys.
    #[test]
    fn write_exec_decode_rejects_a_malformed_existing_key_scan() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        for existing_keys in [Vec::new(), vec![0xff, 0xff]] {
            let mut insert = insert_only_proto(
                WriteConflictModeProto::Reject,
                Some(key_schema_proto()),
                "default.write_codec",
            );
            insert.existing_keys = existing_keys;
            let mut proto = write_proto("keyedwrite000001");
            proto.insert_only = Some(insert);
            let err = LakeSoulWriteCodec
                .try_decode(
                    &proto.encode_to_vec(),
                    &inputs,
                    &TaskContext::default(),
                    &DefaultPhysicalProtoConverter {},
                )
                .unwrap_err();
            assert!(err.to_string().contains("existing_keys"), "{err}");
        }
    }

    /// An unknown conflict-mode value must fail loudly: defaulting to reject
    /// or skip would change whether the statement may succeed.
    #[test]
    fn write_exec_decode_rejects_unknown_conflict_modes() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        for value in [-1, 2, 7] {
            let mut insert = insert_only_proto(
                WriteConflictModeProto::Reject,
                Some(key_schema_proto()),
                "default.write_codec",
            );
            insert.conflict_mode = value;
            let mut proto = write_proto("keyedwrite000001");
            proto.insert_only = Some(insert);
            let err = LakeSoulWriteCodec
                .try_decode(
                    &proto.encode_to_vec(),
                    &inputs,
                    &TaskContext::default(),
                    &DefaultPhysicalProtoConverter {},
                )
                .unwrap_err();
            let message = err.to_string();
            assert!(matches!(err, DataFusionError::Internal(_)), "{message}");
            assert!(message.contains("conflict mode"), "{message}");
        }
    }

    /// The key schema types the key comparison; a missing one cannot be
    /// defaulted from the input because a write may project a subset.
    #[test]
    fn write_exec_decode_rejects_missing_key_schema() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        let mut proto = write_proto("keyedwrite000001");
        proto.insert_only = Some(insert_only_proto(
            WriteConflictModeProto::Reject,
            None,
            "default.write_codec",
        ));
        let err = LakeSoulWriteCodec
            .try_decode(
                &proto.encode_to_vec(),
                &inputs,
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(err.to_string().contains("key_schema"), "{err}");
    }

    fn decoded_keyed_write(
        mode: WriteConflictModeProto,
        input: Vec<RecordBatch>,
        existing: Vec<RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        let input = memory_input(input);
        let mut proto = write_proto("keyedwrite000001");
        proto.insert_only = Some(insert_only_proto(
            mode,
            Some(key_schema_proto()),
            "default.write_codec",
        ));
        proto
            .insert_only
            .as_mut()
            .expect("insert-only state")
            .existing_keys = nested_scan_bytes(memory_input(existing));
        LakeSoulWriteCodec
            .try_decode(
                &proto.encode_to_vec(),
                &[input],
                &SessionStateBuilder::new_with_default_features()
                    .build()
                    .task_ctx(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("a keyed write payload decodes")
    }

    fn write_invariants() -> WriteInvariants {
        WriteInvariants::try_new(key_schema().as_ref(), key_schema().as_ref())
            .expect("matching schemas")
    }

    /// Reject mode fails a worker task on a duplicate key inside the statement
    /// and on a key already present in the historical scan embedded in the
    /// payload, with the statement's qualified table name.
    #[tokio::test]
    async fn insert_only_reject_mode_fails_a_conflicting_key_on_a_worker() {
        let table = "default.write_codec".to_string();
        for (input, existing, expected) in [
            (
                vec![id_batch(&[1, 1])],
                vec![id_batch(&[])],
                WriteViolation::DuplicateKey {
                    table: table.clone(),
                },
            ),
            (
                vec![id_batch(&[1])],
                vec![id_batch(&[1])],
                WriteViolation::ExistingKey {
                    table: table.clone(),
                },
            ),
        ] {
            let decoded =
                decoded_keyed_write(WriteConflictModeProto::Reject, input, existing);
            let write = decoded
                .downcast_ref::<LakeSoulWriteExec>()
                .expect("decodes as LakeSoulWriteExec");
            let insert = write.insert_only().expect("validation state decoded");
            let err = insert
                .validate(
                    Arc::clone(write.input()),
                    SessionStateBuilder::new_with_default_features()
                        .build()
                        .task_ctx(),
                    &write_invariants(),
                )
                .await
                .err()
                .expect("a conflicting key fails validation");
            let DataFusionError::External(inner) = err.find_root() else {
                panic!("expected a typed write violation, got {err}");
            };
            assert_eq!(
                inner.as_ref().downcast_ref::<WriteViolation>(),
                Some(&expected)
            );
        }
    }

    /// Skip mode drops both a duplicate row inside the statement and a row
    /// whose key is already committed, leaving only the rows to insert.
    #[tokio::test]
    async fn insert_only_skip_mode_drops_conflicting_rows_on_a_worker() {
        let decoded = decoded_keyed_write(
            WriteConflictModeProto::Skip,
            vec![id_batch(&[1, 2, 2, 3])],
            vec![id_batch(&[3])],
        );
        let write = decoded
            .downcast_ref::<LakeSoulWriteExec>()
            .expect("decodes as LakeSoulWriteExec");
        let insert = write.insert_only().expect("validation state decoded");
        let streams = insert
            .validate(
                Arc::clone(write.input()),
                SessionStateBuilder::new_with_default_features()
                    .build()
                    .task_ctx(),
                &write_invariants(),
            )
            .await
            .expect("skip mode filters instead of failing");
        let mut rows = 0;
        for stream in streams {
            let batches = datafusion::physical_plan::common::collect(stream)
                .await
                .expect("stream collects");
            rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, 2, "only keys 1 and 2 (first occurrence) remain");
    }

    /// The write node is defined against exactly one input, insert-only or not;
    /// a payload with a different child count would drop or misbind part of
    /// the source tree.
    #[test]
    fn write_exec_decode_requires_exactly_one_child() {
        let mut keyed = write_proto("keyedwrite000001");
        keyed.insert_only = Some(insert_only_proto(
            WriteConflictModeProto::Reject,
            Some(key_schema_proto()),
            "default.write_codec",
        ));
        for bytes in [
            write_proto("statement0000001").encode_to_vec(),
            keyed.encode_to_vec(),
        ] {
            for inputs in [
                vec![],
                vec![
                    parquet_input("s3://bucket/t/a.parquet"),
                    parquet_input("s3://bucket/t/b.parquet"),
                ],
            ] {
                let err = LakeSoulWriteCodec
                    .try_decode(
                        &bytes,
                        &inputs,
                        &TaskContext::default(),
                        &DefaultPhysicalProtoConverter {},
                    )
                    .unwrap_err();
                assert!(err.to_string().contains("input child"), "{err}");
            }
        }
    }

    /// The statement id becomes part of every file name, so a tampered plan
    /// must not be able to steer writes outside the table directory.
    #[test]
    fn write_exec_decode_rejects_unsafe_statement_ids() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        let too_long = "a".repeat(MAX_WRITE_ID_LEN + 1);
        for write_id in ["", "..", "a/b", "a b", "a.b", "a_b", too_long.as_str()] {
            let bytes = write_proto(write_id).encode_to_vec();
            let err = LakeSoulWriteCodec
                .try_decode(
                    &bytes,
                    &inputs,
                    &TaskContext::default(),
                    &DefaultPhysicalProtoConverter {},
                )
                .unwrap_err();
            assert!(err.to_string().contains("write_id"), "{write_id:?}: {err}");
        }
    }

    /// Without a table path the writer would build relative file names.
    #[test]
    fn write_exec_decode_rejects_a_table_without_path() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        let mut table_info = (*write_table_info()).clone();
        table_info.table_path.clear();
        let mut proto = write_proto("statement0000001");
        proto.table_info = table_info.encode_to_vec();

        let err = LakeSoulWriteCodec
            .try_decode(
                &proto.encode_to_vec(),
                &inputs,
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(err.to_string().contains("table_path"), "{err}");
    }

    /// A plan from an incompatible build is refused instead of being decoded
    /// with the wrong field interpretation.
    #[test]
    fn write_exec_decode_rejects_other_codec_versions() {
        let inputs = vec![parquet_input("s3://bucket/t/a.parquet")];
        for version in [0, CODEC_VERSION + 1] {
            let mut proto = write_proto("statement0000001");
            proto.codec_version = version;
            let err = LakeSoulWriteCodec
                .try_decode(
                    &proto.encode_to_vec(),
                    &inputs,
                    &TaskContext::default(),
                    &DefaultPhysicalProtoConverter {},
                )
                .unwrap_err();
            let message = err.to_string();
            assert!(matches!(err, DataFusionError::Internal(_)), "{message}");
            assert!(message.contains("codec version"), "{message}");
        }
    }

    #[test]
    fn write_exec_decode_rejects_garbage() {
        let err = LakeSoulWriteCodec
            .try_decode(
                &[0xff, 0xff],
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    /// The write plan has no IO-config field: object-store credentials stay in
    /// the workers' environment, and the table metadata that does cross the
    /// wire must not carry the commit node's options either.
    #[test]
    fn write_plan_carries_only_its_own_configuration() {
        let exec = write_exec("statement0000001", None);
        let mut buf = Vec::new();
        LakeSoulWriteCodec
            .try_encode(exec, &mut buf, &DefaultPhysicalProtoConverter {})
            .expect("encode");
        let encoded = String::from_utf8_lossy(&buf);

        // Positive control: the table path and statement id are what execution
        // needs, so the assertions below cannot pass vacuously.
        assert!(encoded.contains("file:///tmp/write-codec/"), "{encoded}");
        assert!(encoded.contains("statement0000001"), "{encoded}");
        for absent in ["ACCESS-KEY-SENTINEL", "SECRET-KEY-SENTINEL", "s3.invalid"] {
            assert!(!encoded.contains(absent), "plan leaked {absent}: {encoded}");
        }
    }
}
