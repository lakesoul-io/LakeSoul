// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, FixedSizeListBuilder, Float32Builder, Float64Array, Int32Array,
    Int64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use lakesoul_metadata::MetaDataClient;

use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::lakesoul_table::LakeSoulTable;
use crate::vector_batch::native::{NativeBatchOptions, NativeBatchSnapshot};

struct Fixture {
    client: Arc<MetaDataClient>,
    name: String,
    context: Arc<SessionContext>,
    provider: Arc<dyn datafusion::catalog::TableProvider>,
    vectors: Vec<Vec<f32>>,
}

impl Fixture {
    async fn new(metric: &str) -> Self {
        Self::with_partition_scoped_ids(metric, false).await
    }
    async fn with_partition_scoped_ids(metric: &str, duplicate_ids: bool) -> Self {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let name = format!("native_batch_test_{}", uuid::Uuid::new_v4().simple());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    8,
                ),
                false,
            ),
        ]));
        let configs = crate::vector_index::parse_vector_index_columns(Some(&format!(
            r#"[{{"column":"vec","dim":8,"nlist":4,"total_bits":7,"metric":"{metric}"}}]"#
        )))
        .unwrap();
        crate::tests::create_partitioned_vector_table(
            client.clone(),
            &name,
            schema.clone(),
            &configs,
        )
        .await
        .unwrap();
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(17);
        let vectors: Vec<Vec<f32>> = (0..64)
            .map(|_| (0..8).map(|_| rng.random_range(-1.0f32..1.0)).collect())
            .collect();
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 8);
        for vector in &vectors {
            builder.values().append_slice(vector);
            builder.append(true);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..64).map(|id| if duplicate_ids { id / 2 } else { id }),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    (0..64).map(|i| (i % 2) as i32),
                )),
                Arc::new(builder.finish()),
            ],
        )
        .unwrap();
        LakeSoulTable::for_name(&name)
            .await
            .unwrap()
            .execute_upsert(batch)
            .await
            .unwrap();
        let context = crate::create_lakesoul_session_ctx(
            client.clone(),
            &crate::cli::CoreArgs {
                worker_threads: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let provider = context
            .catalog("lakesoul")
            .unwrap()
            .schema("default")
            .unwrap()
            .table(&name)
            .await
            .unwrap()
            .unwrap();
        Self {
            client,
            name,
            context,
            provider,
            vectors,
        }
    }
    fn provider(&self) -> &LakeSoulTableProvider {
        self.provider
            .downcast_ref::<LakeSoulTableProvider>()
            .unwrap()
    }
    async fn cleanup(self) {
        self.client.drop_table(&self.name, "default").await.unwrap();
        let root = std::env::current_dir().unwrap().join("default");
        let target = root.join(&self.name);
        assert!(self.name.starts_with("native_batch_test_"));
        assert_eq!(target.parent(), Some(root.as_path()));
        if target.exists() {
            std::fs::remove_dir_all(target).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_batch_snapshot_partitions_exact_scores_and_sql() {
    for metric in ["L2", "IP"] {
        let fixture = Fixture::new(metric).await;
        let snapshot = NativeBatchSnapshot::prepare(
            fixture.provider(),
            &fixture.context.state(),
            "vec",
            metric,
        )
        .await
        .unwrap();
        assert_eq!(
            snapshot.descriptor().shards.len(),
            2,
            "same bucket ID in two range partitions must not merge"
        );
        let query = fixture.vectors[7].clone();
        let queries = vec![query.clone(); 16];
        let options = NativeBatchOptions {
            top_k: 64,
            nprobe: 4,
            exact: true,
            exact_overfetch: 1,
            parallelism: 2,
            ..Default::default()
        };
        let results = snapshot
            .search(&queries, &options, Arc::new(AtomicBool::new(false)))
            .await
            .unwrap();
        let mut expected: Vec<_> = fixture
            .vectors
            .iter()
            .enumerate()
            .map(|(id, v)| {
                let d = if metric == "L2" {
                    v.iter()
                        .zip(&query)
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum::<f32>()
                        .sqrt()
                } else {
                    -v.iter().zip(&query).map(|(a, b)| a * b).sum::<f32>()
                };
                (id as i64, d as f64)
            })
            .collect();
        expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        for result in &results {
            assert_eq!(result.len(), 64);
            for (actual, (id, distance)) in result.iter().zip(&expected) {
                assert_eq!(actual.id, *id);
                assert!(
                    (actual.distance - distance).abs() < 1e-5,
                    "{metric}: {} != {distance}",
                    actual.distance
                );
            }
        }
        let blocks = snapshot.metrics.rerank_blocks.load(Ordering::Relaxed);
        assert!(
            blocks < (queries.len() * 64) as u64,
            "rerank must share physical blocks between queries"
        );
        let json = serde_json::to_string(&queries[..2]).unwrap();
        let sql = format!(
            "SELECT * FROM lakesoul_vector_search_batch('lakesoul.default.{}','vec','{}',64,4,'{}','exact',1) ORDER BY query_id,rank",
            fixture.name, json, metric
        );
        let batches = fixture
            .context
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            128
        );
        for batch in batches {
            let ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let distances = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let ranks = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let rank = (ranks.value(row) - 1) as usize;
                assert_eq!(ids.value(row), expected[rank].0);
                assert!((distances.value(row) - expected[rank].1).abs() < 1e-5);
            }
        }
        drop(snapshot);
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_batch_read_lease_preserves_query_across_drop() {
    let fixture = Fixture::new("L2").await;
    let snapshot = NativeBatchSnapshot::prepare(
        fixture.provider(),
        &fixture.context.state(),
        "vec",
        "L2",
    )
    .await
    .unwrap();
    let query = vec![fixture.vectors[1].clone(), fixture.vectors[2].clone()];
    let options = NativeBatchOptions {
        top_k: 5,
        nprobe: 4,
        parallelism: 2,
        ..Default::default()
    };
    let before = snapshot
        .search(&query, &options, Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    let report = crate::vector_index::drop_vector_index(
        &fixture.client,
        &fixture.name,
        "default",
        &HashMap::new(),
        crate::vector_index::DropVectorIndexOptions {
            wait_for_cleanup: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(report.cleanup, "deferred");
    let after = snapshot
        .search(&query, &options, Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "existing statement must retain immutable index objects"
    );
    assert!(
        NativeBatchSnapshot::prepare(
            fixture.provider(),
            &fixture.context.state(),
            "vec",
            "L2"
        )
        .await
        .is_err(),
        "new statement cannot use logically dropped index"
    );
    let catalog = fixture.client.vector_index_catalog();
    let prefix = snapshot.descriptor().shards[0].index_prefix.clone();
    assert!(catalog.has_live_read_leases(&prefix).await.unwrap());
    drop(snapshot);
    tokio::time::timeout(Duration::from_secs(10), async {
        while catalog.has_live_read_leases(&prefix).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_batch_keeps_same_id_from_distinct_range_partitions() {
    let fixture = Fixture::with_partition_scoped_ids("L2", true).await;
    let snapshot = NativeBatchSnapshot::prepare(
        fixture.provider(),
        &fixture.context.state(),
        "vec",
        "L2",
    )
    .await
    .unwrap();
    for exact in [false, true] {
        let options = NativeBatchOptions {
            top_k: 64,
            nprobe: 4,
            exact,
            exact_overfetch: 1,
            parallelism: 2,
            ..Default::default()
        };
        let results = snapshot
            .search(
                &[fixture.vectors[1].clone()],
                &options,
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
        assert_eq!(
            results[0].len(),
            64,
            "partition-scoped rows must not collapse to 32 IDs"
        );
        let mut counts = HashMap::new();
        for hit in &results[0] {
            *counts.entry(hit.id).or_insert(0) += 1;
        }
        assert_eq!(counts.len(), 32);
        assert!(counts.values().all(|count| *count == 2));
        if exact {
            let mut expected: Vec<_> = fixture
                .vectors
                .iter()
                .enumerate()
                .map(|(row, vector)| {
                    let d = vector
                        .iter()
                        .zip(&fixture.vectors[1])
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum::<f32>()
                        .sqrt() as f64;
                    ((row / 2) as i64, d)
                })
                .collect();
            expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            for (hit, (id, d)) in results[0].iter().zip(expected) {
                assert_eq!(hit.id, id);
                assert!((hit.distance - d).abs() < 1e-5);
            }
        }
    }
    drop(snapshot);
    fixture.cleanup().await;
}

use std::collections::HashMap;
