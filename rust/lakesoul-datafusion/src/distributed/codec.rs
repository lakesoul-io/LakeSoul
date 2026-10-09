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
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::vortex_scan_session;
use lakesoul_io::physical_plan::MergeParquetExec;
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
/// message layout moved. Version 3 added [`VortexScanCodec`] at position 2.
pub const CODEC_VERSION: u32 = 3;

/// Wire format for [`MergeParquetExec`].
///
/// Children are re-attached by the codec driver, so only merge-specific state
/// is encoded. The config subset mirrors exactly what the merge operator reads
/// at execution time (`files`, primary keys, merge operators, and the
/// `options` map carrying the `is_compacted` / `skip_merge_on_read` flags).
/// Object-store credentials are not part of the format; see the module docs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MergeParquetExecProto {
    #[prost(message, optional, tag = "1")]
    pub schema: Option<datafusion_proto::protobuf::Schema>,
    #[prost(string, repeated, tag = "2")]
    pub primary_keys: Vec<String>,
    #[prost(map = "string, string", tag = "3")]
    pub merge_operators: HashMap<String, String>,
    #[prost(map = "string, string", tag = "4")]
    pub default_column_value: HashMap<String, String>,
    #[prost(string, repeated, tag = "5")]
    pub files: Vec<String>,
    #[prost(map = "string, string", tag = "6")]
    pub options: HashMap<String, String>,
    /// [`CODEC_VERSION`] of the encoder; appended rather than numbered first so
    /// the layout stays additively evolvable.
    #[prost(uint32, tag = "7")]
    pub codec_version: u32,
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
            default_column_value: exec
                .default_column_value()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            files: exec.io_config().files_slice().to_vec(),
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

        let mut builder = LakeSoulIOConfigBuilder::new()
            .with_files(proto.files.clone())
            .with_primary_keys(proto.primary_keys.clone());
        for (key, value) in &proto.options {
            builder = builder.with_option(key, value);
        }
        for (field, op) in &proto.merge_operators {
            builder = builder.with_merge_op(field.clone(), op.clone());
        }

        let exec = MergeParquetExec::from_parts(
            schema,
            Arc::new(proto.primary_keys),
            Arc::new(proto.default_column_value.clone()),
            Arc::new(proto.merge_operators.clone()),
            inputs.to_vec(),
            builder.build(),
        );
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
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    pub(crate) fn exec(node: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let codec = composed_codec();
        let buf = PhysicalPlanNode::try_from_physical_plan(node, &codec)?.encode_to_vec();
        let ctx = TaskContext::default();
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
    use datafusion_datasource::PartitionedFile;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::config::OPTION_KEY_IS_COMPACTED;
    fn merge_exec(inputs: Vec<Arc<dyn ExecutionPlan>>) -> MergeParquetExec {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://bucket/t/file-0.parquet"])
            .with_primary_keys(vec!["id".to_string()])
            .with_merge_op("name".to_string(), "UseLast".to_string())
            .with_option(OPTION_KEY_IS_COMPACTED.to_string(), "true".to_string())
            .build();
        MergeParquetExec::from_parts(
            schema,
            Arc::new(vec!["id".to_string()]),
            Arc::new(HashMap::from([("part".to_string(), "p0".to_string())])),
            Arc::new(HashMap::from([("name".to_string(), "UseLast".to_string())])),
            inputs,
            io_config,
        )
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
            merge.default_column_value().get("part").map(String::as_str),
            Some("p0")
        );
        assert!(merge.io_config().is_compacted());
        assert_eq!(
            merge.io_config().files_slice(),
            ["s3://bucket/t/file-0.parquet"]
        );
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
        let exec = MergeParquetExec::from_parts(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            Arc::new(vec!["id".to_string()]),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            vec![],
            io_config,
        );

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
