// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Wire, ownership and real two-worker tests for SQL native vector batches.
//! Run only against the dedicated batch test database: the crate test startup
//! clears metadata. Real workers assert probe task ownership and Flight input,
//! rather than inferring distribution from matching results or elapsed time.

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, Float32Builder, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::memory::{
    MemTable, MemoryCatalogProvider, MemorySchemaProvider, MemorySourceConfig,
};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExt, DistributedTaskContext, NetworkBoundaryExt, Worker,
    WorkerPlanRewriteEvent, WorkerPlanRewriteEventResponse, WorkerQueryContext,
    WorkerSessionBuilder,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::MetaDataClient;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;

use crate::cli::CoreArgs;
use crate::distributed::{DistributedOptions, LakeSoulWorkerSessionBuilder};
use crate::lakesoul_table::LakeSoulTable;
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::vector_batch::distributed::{
    NativeBatchDistribution, NativeBatchShardProbeExec, assigned_snapshot,
    build_window_plan, maybe_search_distributed_window, merge_probe_batches,
    preflight_window_plan, probe_schema,
};
use crate::vector_batch::native::{
    NativeBatchHit, NativeBatchOptions, NativeBatchShardDescriptor, NativeBatchSnapshot,
    NativeBatchSnapshotDescriptor,
};

fn descriptor() -> NativeBatchSnapshotDescriptor {
    NativeBatchSnapshotDescriptor {
        table_id: "pinned-table".into(),
        table_path: "file:///tmp/pinned-table".into(),
        column: "vec".into(),
        epoch: 42,
        dim: 8,
        metric: "L2".into(),
        schema: Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    8,
                ),
                false,
            ),
        ]),
        partitions: vec![("p=1".into(), 8), ("p=2".into(), 12)],
        shards: (0..2)
            .flat_map(|partition| {
                (0..4).map(move |bucket| NativeBatchShardDescriptor {
                    index_prefix: format!(
                        "/tmp/pinned-table/_vector_index/vec/p={partition}/{bucket}"
                    ),
                    commit_id: 100 + i64::from(partition * 4 + bucket),
                    generation: 3,
                    version: 7,
                    header: vec![1, 2, 3],
                    segments: Vec::new(),
                    files: vec![format!(
                        "/tmp/pinned-table/p={partition}/data_{bucket}.vortex"
                    )],
                })
            })
            .collect(),
        read_lease_id: Some("parent-read-lease".into()),
    }
}

fn query_source() -> Arc<dyn ExecutionPlan> {
    let mut vectors = arrow::array::FixedSizeListBuilder::new(Float32Builder::new(), 8);
    vectors.values().append_slice(&[0.0; 8]);
    vectors.append(true);
    let vectors = vectors.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("query_index", DataType::UInt32, false),
        Field::new("query_vector", vectors.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::UInt32Array::from(vec![0])),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

#[test]
fn batch_assignment_keeps_bucket_shards_together_and_snapshot_immutable() {
    let descriptor = descriptor();
    let json = serde_json::to_string(&descriptor).unwrap();
    let mut all = HashSet::new();
    for task in 0..2 {
        let subset = assigned_snapshot(&descriptor, task, 2).unwrap();
        assert_eq!(subset.table_id, descriptor.table_id);
        assert_eq!(subset.epoch, 42);
        assert_eq!(subset.partitions, descriptor.partitions);
        assert_eq!(subset.read_lease_id, descriptor.read_lease_id);
        assert_eq!(subset.shards.len(), 4);
        let buckets: BTreeSet<_> = subset
            .shards
            .iter()
            .map(|s| s.index_prefix.rsplit('/').next().unwrap())
            .collect();
        assert_eq!(buckets.len(), 2);
        for shard in subset.shards {
            assert!(all.insert(shard.index_prefix));
        }
    }
    assert_eq!(all.len(), 8);
    assert_eq!(json, serde_json::to_string(&descriptor).unwrap());
    assert!(assigned_snapshot(&descriptor, 2, 2).is_err());
    let mut malformed = descriptor;
    malformed.shards[0].index_prefix = "/tmp/no-bucket".into();
    assert!(assigned_snapshot(&malformed, 0, 2).is_err());
}

#[test]
fn batch_probe_codec_has_a_real_query_child_and_rejects_other_generations() {
    let input = query_source();
    let probe: Arc<dyn ExecutionPlan> = Arc::new(
        NativeBatchShardProbeExec::try_new(
            descriptor(),
            NativeBatchOptions::default(),
            2,
            input.clone(),
        )
        .unwrap(),
    );
    let decoded = crate::distributed::codec::roundtrip::exec(probe.clone()).unwrap();
    let decoded = decoded.downcast_ref::<NativeBatchShardProbeExec>().unwrap();
    assert_eq!(decoded.children().len(), 1);
    assert_eq!(decoded.descriptor().epoch, 42);
    assert_eq!(
        decoded.descriptor().read_lease_id.as_deref(),
        Some("parent-read-lease")
    );
    assert_eq!(decoded.descriptor().shards[3].commit_id, 103);

    let codec = crate::distributed::vector_batch_codec::NativeBatchProbeCodec;
    let mut bytes = Vec::new();
    codec
        .try_encode(probe, &mut bytes, &DefaultPhysicalProtoConverter {})
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value.get("queries").is_none());
    assert!(value.get("object_store_options").is_none());
    value["codec_version"] =
        serde_json::Value::from(crate::distributed::codec::CODEC_VERSION + 1);
    let error = codec
        .try_decode(
            &serde_json::to_vec(&value).unwrap(),
            &[input],
            &TaskContext::default(),
            &DefaultPhysicalProtoConverter {},
        )
        .unwrap_err();
    assert!(error.to_string().contains("codec version"), "{error}");
}

#[test]
fn batch_window_stages_are_shippable_and_encode_only_a_bounded_ipc_child() {
    let queries = vec![vec![0.125; 8]; 256];
    let plan =
        build_window_plan(descriptor(), &queries, NativeBatchOptions::default(), 2)
            .unwrap();
    preflight_window_plan(&plan).unwrap();
    let mut stages = Vec::new();
    plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary() {
            stages.push(boundary.input_stage().task_count());
            assert!(!boundary.input_stage().query_id().is_nil());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(stages, [2, 1]);
    assert!(
        build_window_plan(
            descriptor(),
            &vec![vec![0.0; 8]; 257],
            NativeBatchOptions::default(),
            2
        )
        .is_err()
    );
}

fn hits_batch(
    task: u32,
    values: &[Vec<NativeBatchHit>],
    metrics: [u64; 4],
) -> RecordBatch {
    let mut qids = Vec::new();
    let mut tasks = Vec::new();
    let mut ids = Vec::new();
    let mut distances = Vec::new();
    let mut rerank_blocks = Vec::new();
    let mut cluster_loads = Vec::new();
    let mut shared_block_reuses = Vec::new();
    let mut peak_workers = Vec::new();
    for (query, hits) in values.iter().enumerate() {
        qids.push(Some(query as u32));
        tasks.push(task);
        ids.push(None);
        distances.push(None);
        rerank_blocks.push(0);
        cluster_loads.push(0);
        shared_block_reuses.push(0);
        peak_workers.push(0);
        for hit in hits {
            qids.push(Some(query as u32));
            tasks.push(task);
            ids.push(Some(hit.id));
            distances.push(Some(hit.distance));
            rerank_blocks.push(0);
            cluster_loads.push(0);
            shared_block_reuses.push(0);
            peak_workers.push(0);
        }
    }
    qids.push(None);
    tasks.push(task);
    ids.push(None);
    distances.push(None);
    rerank_blocks.push(metrics[0]);
    cluster_loads.push(metrics[1]);
    shared_block_reuses.push(metrics[2]);
    peak_workers.push(metrics[3]);
    RecordBatch::try_new(
        probe_schema(),
        vec![
            Arc::new(arrow::array::UInt32Array::from(qids)),
            Arc::new(arrow::array::UInt32Array::from(tasks)),
            Arc::new(Int64Array::from(ids)),
            Arc::new(arrow::array::Float64Array::from(distances)),
            Arc::new(arrow::array::UInt64Array::from(rerank_blocks)),
            Arc::new(arrow::array::UInt64Array::from(cluster_loads)),
            Arc::new(arrow::array::UInt64Array::from(shared_block_reuses)),
            Arc::new(arrow::array::UInt64Array::from(peak_workers)),
        ],
    )
    .unwrap()
}

#[test]
fn batch_global_topk_verifies_all_workers_and_sorts_each_query() {
    let first = hits_batch(
        0,
        &[
            vec![
                NativeBatchHit {
                    id: 1,
                    distance: 0.4,
                },
                NativeBatchHit {
                    id: 3,
                    distance: 0.9,
                },
            ],
            Vec::new(),
        ],
        [5, 2, 3, 2],
    );
    let second = hits_batch(
        1,
        &[
            vec![
                NativeBatchHit {
                    id: 2,
                    distance: 0.1,
                },
                NativeBatchHit {
                    id: 1,
                    distance: 0.2,
                },
            ],
            vec![NativeBatchHit {
                id: 5,
                distance: -2.0,
            }],
        ],
        [7, 4, 6, 4],
    );
    let merged = merge_probe_batches(&[first.clone(), second.clone()], 2, 2, 3).unwrap();
    assert_eq!(merged.metrics.rerank_blocks, 12);
    assert_eq!(merged.metrics.cluster_loads, 6);
    assert_eq!(merged.metrics.shared_block_reuses, 9);
    assert_eq!(merged.metrics.peak_workers, 4);
    assert_eq!(
        merged.results[0],
        [
            NativeBatchHit {
                id: 2,
                distance: 0.1
            },
            NativeBatchHit {
                id: 1,
                distance: 0.2
            },
            NativeBatchHit {
                id: 1,
                distance: 0.4
            },
        ]
    );
    assert_eq!(
        merged.results[1],
        [NativeBatchHit {
            id: 5,
            distance: -2.0
        }]
    );
    assert!(
        merge_probe_batches(&[first.clone()], 2, 2, 2)
            .unwrap_err()
            .to_string()
            .contains("every worker")
    );
    let summary_only = first.slice(first.num_rows() - 1, 1);
    assert!(
        merge_probe_batches(&[first.clone(), summary_only, second.clone()], 2, 2, 2)
            .unwrap_err()
            .to_string()
            .contains("duplicate worker telemetry summary")
    );
    let first_without_summary = first.slice(0, first.num_rows() - 1);
    assert!(
        merge_probe_batches(&[first_without_summary, second.clone()], 2, 2, 2)
            .unwrap_err()
            .to_string()
            .contains("every worker reported telemetry")
    );
    assert!(
        merge_probe_batches(&[first.clone(), first], 2, 2, 2)
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
}

#[derive(Debug, Clone)]
struct ProbeSeen {
    worker: usize,
    task: usize,
    prefixes: Vec<String>,
    flight_child: bool,
}
#[derive(Debug, Clone)]
struct RecordingBuilder {
    worker: usize,
    seen: Arc<Mutex<Vec<ProbeSeen>>>,
    reject_probe: bool,
    rejected: Arc<AtomicUsize>,
}

#[async_trait]
impl WorkerSessionBuilder for RecordingBuilder {
    async fn build_session_state(
        &self,
        context: WorkerQueryContext,
    ) -> Result<SessionState> {
        let state = LakeSoulWorkerSessionBuilder
            .build_session_state(context)
            .await?;
        let recorder = self.clone();
        Ok(SessionStateBuilder::new_from_existing(state)
            .with_distributed_worker_plan_rewrite_handler(
                move |event: WorkerPlanRewriteEvent<'_>| {
                    let task = event
                        .session_config
                        .get_extension::<DistributedTaskContext>()
                        .unwrap();
                    event.plan.apply(|node| {
                        if let Some(probe) =
                            node.downcast_ref::<NativeBatchShardProbeExec>()
                        {
                            if recorder.reject_probe {
                                recorder.rejected.fetch_add(1, Ordering::Relaxed);
                                return Err(DataFusionError::Execution(
                                    "injected native batch probe worker failure".into(),
                                ));
                            }
                            let assigned = assigned_snapshot(
                                probe.descriptor(),
                                task.task_index,
                                task.task_count,
                            )?;
                            let flight_child = probe.children()[0].is_network_boundary();
                            recorder.seen.lock().unwrap().push(ProbeSeen {
                                worker: recorder.worker,
                                task: task.task_index,
                                prefixes: assigned
                                    .shards
                                    .into_iter()
                                    .map(|s| s.index_prefix)
                                    .collect(),
                                flight_child,
                            });
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    Ok(WorkerPlanRewriteEventResponse::new(event.plan))
                },
            )
            .build())
    }
}

async fn workers(
    reject_probe: bool,
) -> (
    Vec<String>,
    JoinSet<()>,
    Arc<Mutex<Vec<ProbeSeen>>>,
    Arc<AtomicUsize>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let rejected = Arc::new(AtomicUsize::new(0));
    let mut urls = Vec::new();
    let mut workers = JoinSet::new();
    for index in 0..2 {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        urls.push(format!(
            "http://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ));
        let worker = Worker::from_session_builder(RecordingBuilder {
            worker: index,
            seen: seen.clone(),
            reject_probe,
            rejected: rejected.clone(),
        })
        .with_version(crate::distributed::DISTRIBUTED_PROTOCOL_VERSION);
        workers.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
    }
    (urls, workers, seen, rejected)
}

async fn seed(
    metric: &str,
) -> (
    Arc<MetaDataClient>,
    String,
    Arc<SessionContext>,
    Arc<NativeBatchSnapshot>,
    Vec<Vec<f32>>,
) {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let name = format!("native_batch_remote_{}", uuid::Uuid::new_v4().simple());
    let mut vectors = arrow::array::FixedSizeListBuilder::new(Float32Builder::new(), 8);
    let mut query = Vec::new();
    for row in 0..128 {
        let values: Vec<_> = (0..8)
            .map(|column| ((row * 31 + column * 17 + 9) % 127) as f32 / 31.0 - 2.0)
            .collect();
        if row < 4 {
            query.push(values.clone());
        }
        vectors.values().append_slice(&values);
        vectors.append(true);
    }
    let vectors = vectors.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vec", vectors.data_type().clone(), false),
    ]));
    let config = LakeSoulIOConfigBuilder::new()
        .with_schema(schema.clone())
        .with_primary_keys(vec!["id".into()])
        .with_hash_bucket_num("4")
        .build();
    let index = crate::vector_index::VectorIndexTableConfig {
        column: "vec".into(),
        params: crate::vector_index::VectorIndexParams {
            dim: 8,
            nlist: 4,
            total_bits: 7,
            metric: metric.into(),
            rotator_type: "FhtKac".into(),
            seed: 42,
            use_faster_config: true,
        },
        management: crate::index::IndexManagementConfig {
            rebuild_mode: "auto".into(),
            max_delta_ratio: 1.0,
            gc_enabled: true,
            gc_grace_seconds: 3600,
            gc_keep_generations: 1,
        },
    };
    crate::tests::create_table_with_vector_index(client.clone(), &name, config, &[index])
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..128)),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    LakeSoulTable::for_name(&name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    let context =
        crate::create_lakesoul_session_ctx(client.clone(), &CoreArgs::default()).unwrap();
    let provider = context
        .table_provider(format!("lakesoul.default.{name}"))
        .await
        .unwrap();
    let provider = provider
        .downcast_ref::<crate::datasource::table_provider::LakeSoulTableProvider>()
        .unwrap();
    let snapshot =
        NativeBatchSnapshot::prepare(provider, &context.state(), "vec", metric)
            .await
            .unwrap();
    assert_eq!(snapshot.descriptor().shards.len(), 4);
    (client, name, context, snapshot, query)
}

async fn compare_sql_windows(
    local: &SessionContext,
    remote: &SessionContext,
    name: &str,
    queries: &[Vec<f32>],
) {
    let mut vectors = arrow::array::FixedSizeListBuilder::new(Float32Builder::new(), 8);
    for index in 0..270 {
        vectors
            .values()
            .append_slice(&queries[index % queries.len()]);
        vectors.append(true);
    }
    let vectors = vectors.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("qid", DataType::Int64, false),
        Field::new("qvec", vectors.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values((1000..1270).rev())),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let source = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let memory_schema = Arc::new(MemorySchemaProvider::new());
    memory_schema
        .register_table("batch_input_queries".into(), source)
        .unwrap();
    let memory_catalog = Arc::new(MemoryCatalogProvider::new());
    memory_catalog
        .register_schema("public", memory_schema)
        .unwrap();
    local.register_catalog("query_input", memory_catalog.clone());
    remote.register_catalog("query_input", memory_catalog);
    let sql = format!(
        "SELECT * FROM lakesoul_vector_search_batch_from_table('lakesoul.default.{name}', 'vec', 'query_input.public.batch_input_queries', 'qid', 'qvec', 3, 4, 'L2', 'index') ORDER BY query_id, rank"
    );
    let expected = local.sql(&sql).await.unwrap().collect().await.unwrap();
    let actual = tokio::time::timeout(Duration::from_secs(30), async {
        remote.sql(&sql).await.unwrap().collect().await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(
        arrow::util::pretty::pretty_format_batches(&actual)
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&expected)
            .unwrap()
            .to_string()
    );
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 810);
}

async fn cleanup(client: &MetaDataClient, name: &str) {
    client.drop_table(name, "default").await.unwrap();
    let root = std::env::current_dir().unwrap().join("default").join(name);
    let intended = std::env::current_dir().unwrap().join("default");
    assert!(root.starts_with(&intended) && root.file_name().unwrap() == name);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn batch_distributed_real_workers_match_local_index_exact_and_pinned_drop() {
    crate::tests::distributed_tests::run_distributed_test(async {
        let (urls, mut worker_jobs, seen, _) = workers(false).await;
        for metric in ["L2", "IP"] {
            let (client, name, local, snapshot, queries) = seed(metric).await;
            let factory =
                LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())
                    .unwrap()
                    .with_distributed(DistributedOptions::static_workers(urls.clone()));
            let context = factory
                .create_session(&LakeSoulSessionOptions::default())
                .unwrap();
            for exact in [false, true] {
                let options = NativeBatchOptions {
                    top_k: 3,
                    nprobe: 4,
                    exact,
                    exact_overfetch: 3,
                    ..Default::default()
                };
                let expected = snapshot
                    .search(&queries, &options, Arc::new(AtomicBool::new(false)))
                    .await
                    .unwrap();
                let actual = tokio::time::timeout(
                    Duration::from_secs(30),
                    maybe_search_distributed_window(
                        snapshot.clone(),
                        &queries,
                        &options,
                        context.task_ctx(),
                        Arc::new(AtomicBool::new(false)),
                    ),
                )
                .await
                .unwrap()
                .unwrap()
                .unwrap();
                assert_eq!(actual, expected, "metric={metric} exact={exact}");
            }
            if metric == "L2" {
                let before = seen.lock().unwrap().len();
                compare_sql_windows(&local, &context, &name, &queries).await;
                assert!(
                    seen.lock().unwrap().len() - before >= 4,
                    "two bounded SQL windows must each reach both workers"
                );
            }
            let before = seen.lock().unwrap().len();
            // Drop commits after the parent snapshot was pinned. Worker decode
            // must use the historical descriptor protected by its parent lease.
            LakeSoulTable::for_name(&name)
                .await
                .unwrap()
                .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
                    columns: Some(vec!["vec".into()]),
                    purge_files: false,
                    ..Default::default()
                })
                .await
                .unwrap();
            let options = NativeBatchOptions {
                top_k: 3,
                nprobe: 4,
                ..Default::default()
            };
            let expected = snapshot
                .search(&queries, &options, Arc::new(AtomicBool::new(false)))
                .await
                .unwrap();
            let actual = tokio::time::timeout(
                Duration::from_secs(30),
                maybe_search_distributed_window(
                    snapshot.clone(),
                    &queries,
                    &options,
                    context.task_ctx(),
                    Arc::new(AtomicBool::new(false)),
                ),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(actual, expected);
            let latest = seen.lock().unwrap()[before..].to_vec();
            assert_eq!(latest.len(), 2);
            assert_eq!(
                latest
                    .iter()
                    .map(|s| s.worker)
                    .collect::<BTreeSet<_>>()
                    .len(),
                2
            );
            assert_eq!(
                latest.iter().map(|s| s.task).collect::<BTreeSet<_>>(),
                BTreeSet::from([0, 1])
            );
            assert!(
                latest
                    .iter()
                    .all(|s| s.flight_child && s.prefixes.len() == 2)
            );
            let a: HashSet<_> = latest[0].prefixes.iter().collect();
            let b: HashSet<_> = latest[1].prefixes.iter().collect();
            assert!(a.is_disjoint(&b));
            drop(snapshot);
            drop(context);
            drop(factory);
            cleanup(&client, &name).await;
        }
        worker_jobs.abort_all();
        Ok(())
    });
}

#[derive(Debug)]
struct UnavailableResolver;
impl datafusion_distributed::WorkerResolver for UnavailableResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>> {
        Err(DataFusionError::Execution(
            "injected batch resolver unavailable".into(),
        ))
    }
}

#[test]
fn batch_distributed_fallback_is_limited_to_predispatch_failures() {
    crate::tests::distributed_tests::run_distributed_test(async {
        let (client, name, local, snapshot, queries) = seed("L2").await;
        let options = NativeBatchOptions {
            top_k: 3,
            nprobe: 4,
            ..Default::default()
        };
        let local_result = maybe_search_distributed_window(
            snapshot.clone(),
            &queries,
            &options,
            local.task_ctx(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        assert!(local_result.is_none());
        let config = SessionConfig::new()
            .with_extension(Arc::new(NativeBatchDistribution {
                fallback_to_local: true,
            }))
            .with_distributed_worker_resolver(
                crate::distributed::WorkerSnapshot::default(),
            );
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(local.runtime_env())
                .with_session_config(config.clone()),
        );
        assert!(
            maybe_search_distributed_window(
                snapshot.clone(),
                &queries,
                &options,
                context,
                Arc::new(AtomicBool::new(false))
            )
            .await
            .unwrap()
            .is_none()
        );
        let broken = config
            .clone()
            .with_distributed_worker_resolver(UnavailableResolver);
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(local.runtime_env())
                .with_session_config(broken),
        );
        let error = maybe_search_distributed_window(
            snapshot.clone(),
            &queries,
            &options,
            context,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("resolver unavailable"));
        // A too-large window is rejected before a network request. An unused
        // URL proves fallback did not merely catch a worker connection error.
        let config = config.with_distributed_worker_resolver(
            crate::distributed::WorkerSnapshot::new(vec![
                url::Url::parse("http://127.0.0.1:1").unwrap(),
            ]),
        );
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(local.runtime_env())
                .with_session_config(config.clone()),
        );
        assert!(
            maybe_search_distributed_window(
                snapshot.clone(),
                &vec![queries[0].clone(); 257],
                &options,
                context,
                Arc::new(AtomicBool::new(false))
            )
            .await
            .unwrap()
            .is_none()
        );
        let config = config.with_extension(Arc::new(NativeBatchDistribution {
            fallback_to_local: false,
        }));
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(local.runtime_env())
                .with_session_config(config),
        );
        let error = maybe_search_distributed_window(
            snapshot.clone(),
            &vec![queries[0].clone(); 257],
            &options,
            context,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("one native query window"));
        drop(snapshot);
        drop(local);
        cleanup(&client, &name).await;
        Ok(())
    });
}

#[test]
fn batch_distributed_worker_failure_never_silently_reruns_locally() {
    crate::tests::distributed_tests::run_distributed_test(async {
        let (client, name, _, snapshot, queries) = seed("L2").await;
        let (urls, mut worker_jobs, _, rejected) = workers(true).await;
        let mut distributed = DistributedOptions::static_workers(urls);
        distributed.fallback_to_local = true;
        let factory = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())
            .unwrap()
            .with_distributed(distributed);
        let context = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            maybe_search_distributed_window(
                snapshot.clone(),
                &queries,
                &NativeBatchOptions {
                    top_k: 3,
                    nprobe: 4,
                    ..Default::default()
                },
                context.task_ctx(),
                Arc::new(AtomicBool::new(false)),
            ),
        )
        .await
        .unwrap();
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("injected native batch probe worker failure"),
            "{error}"
        );
        assert!(rejected.load(Ordering::Relaxed) > 0);
        drop(snapshot);
        drop(context);
        drop(factory);
        worker_jobs.abort_all();
        cleanup(&client, &name).await;
        Ok(())
    });
}
