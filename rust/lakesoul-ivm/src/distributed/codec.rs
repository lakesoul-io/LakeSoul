// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan codec for IVM scan nodes.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::common::{exec_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{ScalarUDF, TypeSignature};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{LogicalExprNode, PhysicalExprNode};
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::distributed::descriptor::IvmTableDescriptor;
use crate::distributed::sink::{IvmRefreshSinkExec, IvmSinkSpec};
use crate::runtime::keyed_plan::{
    IvmPkLookupExec, LakeSoulBucketExpr, LakeSoulColumnsBucketExpr, PkLookupKey,
};
use crate::table::{IvmTable, PartitionFiles};

/// Wire message of one [`IvmStreamExec`] task variant.
#[derive(Debug, Serialize, Deserialize)]
struct IvmStreamExecProto {
    table: IvmTableDescriptor,
    groups: Vec<PartitionFiles>,
    filters: Vec<Vec<Vec<u8>>>,
    reader_projection: Option<Vec<u8>>,
    out_schema: Vec<u8>,
    change_filter: Option<String>,
    merge: bool,
}

/// Wire message of one keyed state lookup.
#[derive(Debug, Serialize, Deserialize)]
struct IvmPkLookupExecProto {
    table: IvmTableDescriptor,
    groups_by_bucket: Vec<Vec<PartitionFiles>>,
    key: PkLookupKey,
    bucket_count: usize,
    multi_match: bool,
    emit_unmatched: bool,
}

/// Wire message of one worker write sink.
#[derive(Debug, Serialize, Deserialize)]
struct IvmRefreshSinkExecProto {
    table: IvmTableDescriptor,
    epoch: Option<i64>,
}

/// Wire form of the two custom bucket expressions.
#[derive(Debug, Serialize, Deserialize)]
struct BucketExprProto {
    /// 0 = single key, 1 = composite columns.
    kind: u8,
    bucket_count: u32,
    children: Vec<Vec<u8>>,
}

/// Envelope distinguishing the IVM physical nodes that cross the wire.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
enum IvmExecProto {
    Stream(IvmStreamExecProto),
    PkLookup(IvmPkLookupExecProto),
    Sink(IvmRefreshSinkExecProto),
}

/// Codec for IVM physical nodes.
///
/// The list position of this codec is part of the distributed wire format;
/// see `lakesoul_datafusion::distributed::codec`.
#[derive(Debug, Clone, Default)]
pub struct IvmRefreshCodec;

impl IvmRefreshCodec {
    fn encode_schema(schema: &SchemaRef) -> DFResult<Vec<u8>> {
        serde_json::to_vec(schema.as_ref()).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "encode IVM scan schema: {error}"
            ))
        })
    }

    fn decode_schema(bytes: &[u8]) -> DFResult<SchemaRef> {
        let schema: Schema = serde_json::from_slice(bytes).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "decode IVM scan schema: {error}"
            ))
        })?;
        Ok(Arc::new(schema))
    }

    fn encode_expr(expr: &datafusion::prelude::Expr) -> DFResult<Vec<u8>> {
        let node =
            serialize_expr(expr, &DefaultLogicalExtensionCodec {}).map_err(|error| {
                datafusion::error::DataFusionError::Internal(format!(
                    "encode IVM scan filter: {error}"
                ))
            })?;
        Ok(node.encode_to_vec())
    }

    fn decode_expr(
        bytes: &[u8],
        ctx: &TaskContext,
    ) -> DFResult<datafusion::prelude::Expr> {
        let node = LogicalExprNode::decode(bytes).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "decode IVM scan filter: {error}"
            ))
        })?;
        parse_expr(&node, ctx, &DefaultLogicalExtensionCodec {}).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "decode IVM scan filter expression: {error}"
            ))
        })
    }
}

impl PhysicalExtensionCodec for IvmRefreshCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let proto: IvmExecProto = serde_json::from_slice(buf).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "decode IVM physical node: {error}"
            ))
        })?;
        match proto {
            IvmExecProto::Stream(proto) => decode_stream_exec(proto, inputs, ctx),
            IvmExecProto::PkLookup(proto) => decode_pk_lookup(proto, inputs),
            IvmExecProto::Sink(proto) => decode_sink(proto, inputs),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        if node
            .downcast_ref::<crate::provider::IvmStreamExec>()
            .is_some()
        {
            let proto = stream_exec_proto(&node)?;
            let bytes =
                serde_json::to_vec(&IvmExecProto::Stream(proto)).map_err(|error| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "encode IvmStreamExec: {error}"
                    ))
                })?;
            buf.extend_from_slice(&bytes);
            return Ok(());
        }
        if let Some(exec) = node.downcast_ref::<IvmRefreshSinkExec>() {
            let proto = IvmRefreshSinkExecProto {
                table: exec.table().to_descriptor().map_err(|error| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "encode IvmRefreshSinkExec table: {error}"
                    ))
                })?,
                epoch: exec.epoch(),
            };
            let bytes =
                serde_json::to_vec(&IvmExecProto::Sink(proto)).map_err(|error| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "encode IvmRefreshSinkExec: {error}"
                    ))
                })?;
            buf.extend_from_slice(&bytes);
            return Ok(());
        }
        let Some(exec) = node.downcast_ref::<IvmPkLookupExec>() else {
            return exec_err!("IvmRefreshCodec cannot encode {}", node.name());
        };
        let proto = IvmPkLookupExecProto {
            table: exec.table().to_descriptor().map_err(|error| {
                datafusion::error::DataFusionError::Internal(format!(
                    "encode IvmPkLookupExec table: {error}"
                ))
            })?,
            groups_by_bucket: exec.groups_by_bucket().to_vec(),
            key: exec.key().clone(),
            bucket_count: exec.bucket_count(),
            multi_match: exec.multi_match(),
            emit_unmatched: exec.emit_unmatched(),
        };
        let bytes =
            serde_json::to_vec(&IvmExecProto::PkLookup(proto)).map_err(|error| {
                datafusion::error::DataFusionError::Internal(format!(
                    "encode IvmPkLookupExec: {error}"
                ))
            })?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> DFResult<()> {
        let (kind, children, bucket_count) =
            if let Some(expr) = node.downcast_ref::<LakeSoulBucketExpr>() {
                (0, vec![Arc::clone(expr.key())], expr.bucket_count())
            } else if let Some(expr) = node.downcast_ref::<LakeSoulColumnsBucketExpr>() {
                (1, expr.keys().to_vec(), expr.bucket_count())
            } else {
                return exec_err!("IvmRefreshCodec cannot encode expression {node}");
            };
        let children = children
            .iter()
            .map(|child| {
                ctx.encode_child(child)
                    .map(|node| node.encode_to_vec())
                    .map_err(|error| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "encode IVM bucket expression child: {error}"
                        ))
                    })
            })
            .collect::<DFResult<Vec<_>>>()?;
        let proto = BucketExprProto {
            kind,
            bucket_count,
            children,
        };
        let bytes = serde_json::to_vec(&proto).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "encode IVM bucket expression: {error}"
            ))
        })?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let proto: BucketExprProto = serde_json::from_slice(buf).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "decode IVM bucket expression: {error}"
            ))
        })?;
        let children = proto
            .children
            .iter()
            .map(|bytes| {
                let node =
                    PhysicalExprNode::decode(bytes.as_slice()).map_err(|error| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "decode IVM bucket expression child: {error}"
                        ))
                    })?;
                ctx.decode(&node).map_err(|error| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "decode IVM bucket expression child: {error}"
                    ))
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        match proto.kind {
            0 => {
                let key = children.into_iter().next().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "IVM bucket expression has no key".to_string(),
                    )
                })?;
                Ok(Arc::new(LakeSoulBucketExpr::from_parts(
                    key,
                    proto.bucket_count,
                )))
            }
            1 => Ok(Arc::new(LakeSoulColumnsBucketExpr::from_parts(
                children,
                proto.bucket_count,
            ))),
            kind => internal_err!("unknown IVM bucket expression kind {kind}"),
        }
    }
    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> DFResult<()> {
        if node.name() != crate::runtime::GROUP_ROW_UDF {
            return Ok(());
        }
        if let TypeSignature::Exact(types) = &node.signature().type_signature {
            let encoded = serde_json::to_vec(types).map_err(|error| {
                datafusion::error::DataFusionError::Internal(format!(
                    "encode ivm_group_row types: {error}"
                ))
            })?;
            buf.extend_from_slice(&encoded);
        }
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        if name != crate::runtime::GROUP_ROW_UDF {
            return internal_err!("no IVM UDF codec for {name}");
        }
        let types: Vec<arrow_schema::DataType> =
            serde_json::from_slice(buf).map_err(|error| {
                datafusion::error::DataFusionError::Internal(format!(
                    "decode ivm_group_row types: {error}"
                ))
            })?;
        let udf = crate::runtime::group_row_udf_for_types(types).map_err(|error| {
            datafusion::error::DataFusionError::Internal(format!(
                "rebuild ivm_group_row: {error:?}"
            ))
        })?;
        Ok(Arc::new(udf))
    }
}

fn stream_exec_proto(node: &Arc<dyn ExecutionPlan>) -> DFResult<IvmStreamExecProto> {
    let Some(exec) = node.downcast_ref::<crate::provider::IvmStreamExec>() else {
        return exec_err!("IvmRefreshCodec cannot encode {}", node.name());
    };
    let table = exec.table().to_descriptor().map_err(|error| {
        datafusion::error::DataFusionError::Internal(format!(
            "encode IvmStreamExec table: {error}"
        ))
    })?;
    let filters = exec
        .filters_per_group()
        .iter()
        .map(|group| {
            group
                .iter()
                .map(IvmRefreshCodec::encode_expr)
                .collect::<DFResult<Vec<_>>>()
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(IvmStreamExecProto {
        table,
        groups: exec.groups().to_vec(),
        filters,
        reader_projection: exec
            .reader_projection()
            .map(IvmRefreshCodec::encode_schema)
            .transpose()?,
        out_schema: IvmRefreshCodec::encode_schema(exec.out_schema())?,
        change_filter: exec.change_filter().map(str::to_string),
        merge: exec.merge(),
    })
}

fn decode_stream_exec(
    proto: IvmStreamExecProto,
    inputs: &[Arc<dyn ExecutionPlan>],
    ctx: &TaskContext,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if !inputs.is_empty() {
        return internal_err!("IvmStreamExec does not accept children");
    }
    let table = IvmTable::from_descriptor(&proto.table).map_err(|error| {
        datafusion::error::DataFusionError::Internal(format!(
            "decode IvmStreamExec table: {error}"
        ))
    })?;
    let filters = proto
        .filters
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|expr| IvmRefreshCodec::decode_expr(expr, ctx))
                .collect::<DFResult<Vec<_>>>()
        })
        .collect::<DFResult<Vec<_>>>()?;
    let reader_projection = proto
        .reader_projection
        .as_deref()
        .map(IvmRefreshCodec::decode_schema)
        .transpose()?;
    let out_schema = IvmRefreshCodec::decode_schema(&proto.out_schema)?;
    let groups = proto.groups;
    let file_counts = groups
        .iter()
        .map(|group| group.files.len())
        .collect::<Vec<_>>();
    let partitions = groups.len().max(1);
    Ok(Arc::new(crate::provider::IvmStreamExec::new(
        table,
        Arc::new(groups),
        Arc::new(filters),
        reader_projection,
        out_schema,
        proto.change_filter,
        proto.merge,
        partitions,
        None,
        Arc::new(file_counts),
    )))
}

fn decode_sink(
    proto: IvmRefreshSinkExecProto,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if inputs.len() != 1 {
        return internal_err!("IvmRefreshSinkExec expects exactly one child");
    }
    let table = IvmTable::from_descriptor(&proto.table).map_err(|error| {
        datafusion::error::DataFusionError::Internal(format!(
            "decode IvmRefreshSinkExec table: {error}"
        ))
    })?;
    Ok(Arc::new(IvmRefreshSinkExec::new(
        Arc::clone(&inputs[0]),
        IvmSinkSpec {
            table,
            epoch: proto.epoch,
        },
    )))
}

fn decode_pk_lookup(
    proto: IvmPkLookupExecProto,
    inputs: &[Arc<dyn ExecutionPlan>],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if inputs.len() != 1 {
        return internal_err!("IvmPkLookupExec expects exactly one child");
    }
    let table = IvmTable::from_descriptor(&proto.table).map_err(|error| {
        datafusion::error::DataFusionError::Internal(format!(
            "decode IvmPkLookupExec table: {error}"
        ))
    })?;
    let exec = IvmPkLookupExec::new_with_key(
        Arc::clone(&inputs[0]),
        table,
        proto.groups_by_bucket,
        proto.key,
        proto.bucket_count,
    )?
    .with_flags(proto.multi_match, proto.emit_unmatched);
    Ok(Arc::new(exec))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::{SessionContext, col, lit};
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;

    use super::*;

    fn table(schema: SchemaRef) -> IvmTable {
        IvmTable {
            table_id: "table-id".to_string(),
            table_name: "table".to_string(),
            namespace: "default".to_string(),
            table_path: "file:///tmp/table".to_string(),
            schema,
            primary_keys: vec!["k".to_string()],
            range_partition_columns: Vec::new(),
            internal_columns: Vec::new(),
            bucket_columns: vec!["k".to_string()],
            hash_bucket_num: "4".to_string(),
            cdc_column: Some("rowKinds".to_string()),
            file_format: lakesoul_io::file_format::PhysicalFormat::Vortex,
            allow_fallback: true,
        }
    }

    #[tokio::test]
    async fn a_pk_lookup_round_trips_through_the_codec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(
                crate::runtime::keyed_plan::IVM_BUCKET_COLUMN,
                DataType::UInt32,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1])),
                Arc::new(arrow_array::UInt32Array::from(vec![0])),
            ],
        )
        .unwrap();
        let context = SessionContext::new();
        let input = context
            .read_batch(batch)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let lookup = IvmPkLookupExec::new(
            Arc::clone(&input),
            table(Arc::new(Schema::new(vec![Field::new(
                "k",
                DataType::Int64,
                false,
            )]))),
            vec![vec![]],
            "k",
            1,
        )
        .unwrap()
        .with_multi_match(true);
        let codec = IvmRefreshCodec;
        let converter = DefaultPhysicalProtoConverter {};
        let mut bytes = Vec::new();
        codec
            .try_encode(Arc::new(lookup), &mut bytes, &converter)
            .unwrap();
        let decoded = codec
            .try_decode(&bytes, &[input], &context.task_ctx(), &converter)
            .unwrap();
        let decoded = decoded.downcast_ref::<IvmPkLookupExec>().unwrap();
        assert_eq!(decoded.bucket_count(), 1);
        assert!(decoded.multi_match());
        assert_eq!(decoded.table().table_id, "table-id");
    }

    #[tokio::test]
    async fn a_sink_round_trips_through_the_codec() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let context = SessionContext::new();
        let input = context
            .read_batch(batch)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let sink = IvmRefreshSinkExec::new(
            Arc::clone(&input),
            IvmSinkSpec {
                table: table(schema),
                epoch: Some(7),
            },
        );
        let codec = IvmRefreshCodec;
        let converter = DefaultPhysicalProtoConverter {};
        let mut bytes = Vec::new();
        codec
            .try_encode(Arc::new(sink), &mut bytes, &converter)
            .unwrap();
        let decoded = codec
            .try_decode(&bytes, &[input], &context.task_ctx(), &converter)
            .unwrap();
        let decoded = decoded.downcast_ref::<IvmRefreshSinkExec>().unwrap();
        assert_eq!(decoded.epoch(), Some(7));
        assert_eq!(decoded.table().table_id, "table-id");
    }

    #[test]
    fn an_ivm_scan_round_trips_through_the_codec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let exec = crate::provider::IvmStreamExec::new(
            table(schema.clone()),
            Arc::new(vec![PartitionFiles {
                partition_desc: "-5".to_string(),
                files: vec!["file.vortex".to_string()],
            }]),
            Arc::new(vec![vec![col("k").eq(lit(1i64))]]),
            None,
            schema,
            Some("rowKinds".to_string()),
            true,
            1,
            None,
            Arc::new(vec![1]),
        );
        let codec = IvmRefreshCodec;
        let converter = DefaultPhysicalProtoConverter {};
        let mut bytes = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut bytes, &converter)
            .unwrap();
        let context = SessionContext::new();
        let decoded = codec
            .try_decode(&bytes, &[], &context.task_ctx(), &converter)
            .unwrap();
        let decoded = decoded
            .downcast_ref::<crate::provider::IvmStreamExec>()
            .unwrap();
        assert_eq!(decoded.group_count(), 1);
        assert_eq!(decoded.table().table_id, "table-id");
        assert_eq!(decoded.filters_per_group()[0].len(), 1);
    }
}
