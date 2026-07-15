// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: glove-200d vectors → build index → search → verify recall.
//!
//! Prerequisites:
//!   python3 python/tests/vector/prepare_data.py
//!
//! Usage:
//!   cargo test -p lakesoul-io --test vector_e2e_test -- --nocapture

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_vector::{ManifestStore, Metric, VectorIndexConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const TEST_DATA_DIR: &str = "/tmp/lakesoul_test/glove200d";
const DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../python/tests/vector/data");
const DIM: usize = 200;

/// Helper: read .fvecs file into Vec<Vec<f32>>
fn read_fvecs(path: &str, n: Option<usize>) -> Vec<Vec<f32>> {
    use std::io::Seek;
    let mut f = std::fs::File::open(path).unwrap();
    let f_len = f.metadata().unwrap().len() as usize;
    let mut read_one = || -> Option<Vec<f32>> {
        let mut dim_buf = [0u8; 4];
        f.read_exact(&mut dim_buf).ok()?;
        let dim = i32::from_le_bytes(dim_buf) as usize;
        if dim == 0 || dim > 10000 {
            return None;
        }
        let mut vec = vec![0.0f32; dim];
        let mut buf = vec![0u8; dim * 4];
        f.read_exact(&mut buf).ok()?;
        for (j, chunk) in buf.chunks_exact(4).enumerate() {
            vec[j] = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        Some(vec)
    };
    let mut vectors = Vec::new();
    while vectors.len() < n.unwrap_or(usize::MAX) {
        match read_one() {
            Some(v) => vectors.push(v),
            None => break,
        }
    }
    vectors
}

/// Helper: read .ivecs for ground truth
fn read_ivecs(path: &str, n: Option<usize>) -> Vec<Vec<i32>> {
    let mut f = std::fs::File::open(path).unwrap();
    let mut results = Vec::new();
    loop {
        let mut k_buf = [0u8; 4];
        if f.read_exact(&mut k_buf).is_err() {
            break;
        }
        let k = i32::from_le_bytes(k_buf) as usize;
        if k == 0 || k > 10000 {
            break;
        }
        let mut ids = vec![0i32; k];
        let mut buf = vec![0u8; k * 4];
        f.read_exact(&mut buf).unwrap();
        for (j, chunk) in buf.chunks_exact(4).enumerate() {
            ids[j] = i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        results.push(ids);
        if results.len() >= n.unwrap_or(usize::MAX) {
            break;
        }
    }
    results
}

fn compute_recall(predicted: &[u64], ground_truth: &[i32], k: usize) -> f64 {
    let gt_set: std::collections::HashSet<u64> =
        ground_truth.iter().take(k).map(|&x| x as u64).collect();
    let hits = predicted.iter().filter(|&&id| gt_set.contains(&id)).count();
    hits as f64 / k as f64
}

/// Test: build index from parquet, search, verify recall.
///
/// Uses a small subset (500 train, 5 test) for speed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires prepare_data.py to be run first"]
async fn test_glove_e2e_build_and_search() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let index_prefix = "_vector_index/vec/-5/0/";

    let config = VectorIndexConfig {
        column_name: "vec".to_string(),
        dim: DIM,
        nlist: 32,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
    };

    // Build index from parquet file
    let parquet_path = format!("{}/data/train_10k.parquet", TEST_DATA_DIR);
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config.clone(),
        vec![format!("file://{}", parquet_path)],
        "id".to_string(),
        HashMap::new(),
        None,
    );
    builder.build().await.unwrap();
    println!("Index built successfully at {}", index_prefix);

    // Search: load index, query, check recall
    let queries = read_fvecs(&format!("{}/test_5.fvecs", DATA_DIR), Some(5));
    let ground_truth = read_ivecs(
        // groundtruth.ivecs not bundled; this test is #[ignore]
        "/home/chenxu/program/opensource/rabitq-rs/data/glove-200d/processed/groundtruth.ivecs",
        Some(5),
    );

    let mstore = ManifestStore::new(store, index_prefix.to_string());
    let index = lakesoul_vector::IvfRabitqIndex::load_from_v4(&mstore)
        .await
        // print the error
        .unwrap_or_else(|e| panic!("load_from_v4 failed: {:?}", e));
    let params = lakesoul_vector::SearchParams::new(10, 8);

    for (i, query) in queries.iter().enumerate() {
        let results = index.search(query, params).unwrap();
        let pred_ids: Vec<u64> = results.iter().map(|r| r.id).collect();
        let recall10 = compute_recall(&pred_ids, &ground_truth[i], 10);
        println!(
            "Query {}: recall@10={:.2}, top-5 IDs={:?}",
            i,
            recall10,
            pred_ids.iter().take(5).collect::<Vec<_>>()
        );
        assert!(recall10 > 0.0, "Recall@10 should be > 0 for query {}", i);
    }
    println!("All searches passed recall check");
}

/// Quick test: verify LakeSoulReader can read our parquet schema
#[tokio::test]
async fn test_read_parquet_schema() {
    use futures::StreamExt;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::reader::LakeSoulReader;

    let parquet_path = format!("{}/data/train_10k.parquet", TEST_DATA_DIR);
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![format!("file://{}", parquet_path)])
        .with_batch_size(10)
        .build();
    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();
    let batch = reader.next_rb().await.unwrap().unwrap();
    println!("Batch schema: {:?}", batch.schema());
    println!(
        "Columns: {:?}",
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
    assert!(
        batch.schema().column_with_name("id").is_some(),
        "id column should exist"
    );
    assert!(
        batch.schema().column_with_name("vec").is_some(),
        "vec column should exist"
    );
    println!(
        "First row: id={}, vec_len={}",
        batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .value(0),
        batch
            .column_by_name("vec")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap()
            .value_length(),
    );
}

/// Quick test: build and verify files on disk
#[tokio::test]
async fn test_build_and_list_files() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let index_prefix = "_vector_index/vec/-5/0/";

    let config = VectorIndexConfig {
        column_name: "vec".to_string(),
        dim: DIM,
        nlist: 4,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
    };
    let parquet_path = format!("{}/data/train_10k.parquet", TEST_DATA_DIR);
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        vec![format!("file://{}", parquet_path)],
        "id".to_string(),
        HashMap::new(),
        None,
    );
    builder.build().await.unwrap();

    // List files
    use std::process::Command;
    let output = Command::new("find")
        .arg(tmp.path())
        .arg("-type")
        .arg("f")
        .output()
        .unwrap();
    println!(
        "Files under temp dir:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );

    // Try load
    let mstore = ManifestStore::new(store, index_prefix.to_string());
    match lakesoul_vector::IvfRabitqIndex::load_from_v4(&mstore).await {
        Ok(idx) => println!("Load succeeded: {} vectors", idx.len()),
        Err(e) => println!("Load failed: {:?}", e),
    }
}

/// Test: LakeSoulReader with vector_search options — full filter injection path.
///
/// Verifies that LakeSoulReader::start() → inject_vector_search_filter →
/// search_matching_shards → IDs injected as filter → filtered results.
#[tokio::test]
async fn test_reader_with_vector_search() {
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
        OPTION_KEY_VECTOR_SEARCH_NPROBE, OPTION_KEY_VECTOR_SEARCH_QUERY,
        OPTION_KEY_VECTOR_SEARCH_TOP_K,
    };
    use lakesoul_io::reader::LakeSoulReader;
    use std::io::Read;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let parquet_path = format!("{}/part-abc_0000.parquet", tmp_path);
    let index_prefix = "_vector_index/vec/-5/0/";
    let pk_col = "id";
    let vec_col = "vec";

    // 1. Read a subset of vectors and write parquet
    let train_vecs = read_fvecs(&format!("{}/train_700.fvecs", DATA_DIR), Some(300));
    let test_vecs = read_fvecs(&format!("{}/test_5.fvecs", DATA_DIR), Some(5));
    let n_train = train_vecs.len();
    let dim = train_vecs[0].len();

    // Write parquet
    {
        use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(pk_col, arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new(
                vec_col,
                arrow_schema::DataType::FixedSizeList(
                    std::sync::Arc::new(arrow_schema::Field::new(
                        "item",
                        arrow_schema::DataType::Float32,
                        true,
                    )),
                    dim as i32,
                ),
                false,
            ),
        ]));
        let ids = UInt64Array::from((0..n_train as u64).collect::<Vec<_>>());
        let flat: Vec<f32> = train_vecs.iter().flatten().copied().collect();
        let vec_arr = FixedSizeListArray::new(
            std::sync::Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            )),
            dim as i32,
            std::sync::Arc::new(Float32Array::from(flat)),
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(ids), std::sync::Arc::new(vec_arr)],
        )
        .unwrap();
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // 2. Build vector index
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let config = VectorIndexConfig {
        column_name: vec_col.to_string(),
        dim,
        nlist: 8,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
    };
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        vec![format!("file://{}", parquet_path)],
        pk_col.to_string(),
        HashMap::new(),
        Some(format!("file://{}", tmp_path)),
    );
    builder.build().await.unwrap();
    println!("Index built");

    // 3. Read via LakeSoulReader with vector search
    let query_vec = &test_vecs[0];
    let query_str: String = query_vec
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let read_schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new(pk_col, arrow_schema::DataType::UInt64, false),
        arrow_schema::Field::new(
            vec_col,
            arrow_schema::DataType::FixedSizeList(
                std::sync::Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Float32,
                    true,
                )),
                dim as i32,
            ),
            false,
        ),
    ]));

    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![format!("file://{}", parquet_path)])
        .with_prefix(format!("file://{}", tmp_path))
        .with_schema(read_schema)
        .with_primary_keys(vec![pk_col.to_string()])
        .with_batch_size(8192)
        .with_thread_num(2)
        .with_option(OPTION_KEY_VECTOR_SEARCH_COLUMN, vec_col)
        .with_option(OPTION_KEY_VECTOR_SEARCH_QUERY, query_str)
        .with_option(OPTION_KEY_VECTOR_SEARCH_TOP_K, "3")
        .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, "4")
        .with_option("skip_merge_on_read", "true")
        .with_option("file_filter_pushdown", "false")
        .build();

    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();

    let mut total_rows = 0usize;
    let mut all_ids: Vec<u64> = Vec::new();
    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.unwrap();
        total_rows += batch.num_rows();
        let ids = batch.column_by_name(pk_col).unwrap();
        let ids_arr = ids
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        all_ids.extend(ids_arr.values());
    }

    println!("Reader returned {} rows, IDs={:?}", total_rows, all_ids);

    assert!(total_rows > 0, "Vector search reader should return rows");
    assert!(total_rows <= 3, "Should return at most top_k=3 rows");
    for id in &all_ids {
        assert!(*id < n_train as u64, "ID {} out of range", id);
    }
    println!("✓ LakeSoulReader + vector search test PASSED");
}
