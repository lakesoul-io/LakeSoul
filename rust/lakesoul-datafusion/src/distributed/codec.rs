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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::physical_plan::MergeParquetExec;
use prost::Message as _;

/// Wire format for [`MergeParquetExec`].
///
/// Children are re-attached by the codec driver, so only merge-specific state
/// is encoded. The config subset mirrors exactly what the merge operator reads
/// at execution time (`files`, primary keys, merge operators, and the
/// `options` map carrying the `is_compacted` / `skip_merge_on_read` flags).
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
}

/// [`PhysicalExtensionCodec`] for LakeSoul execution plan nodes.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulCodec;

/// Composed codec used to (de)serialize worker stage plans: the distributed
/// codec first, LakeSoul's own nodes last.
///
/// Both sides of a cluster must build this list in the same order — decoding
/// is position-addressed, so reordering the list breaks cross-version plans.
pub fn composed_codec() -> ComposedPhysicalExtensionCodec {
    ComposedPhysicalExtensionCodec::new(vec![
        Arc::new(datafusion_distributed::DistributedCodec),
        Arc::new(LakeSoulCodec),
    ])
}

impl LakeSoulCodec {
    fn proto_from_merge_exec(exec: &MergeParquetExec) -> MergeParquetExecProto {
        MergeParquetExecProto {
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
        FileGroup, FileScanConfigBuilder, ParquetSource,
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
