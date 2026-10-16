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

use std::io::Read;
use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_common::misc::SecretMap;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_vector::{
    IndexHeader, IndexStore, IvfRabitqIndex, Metric, VectorIndexConfig,
};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const TEST_DATA_DIR: &str = "/tmp/lakesoul_test/glove200d";
const DATA_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../python/tests/vector/data"
);
const DIM: usize = 200;

/// Helper: read .fvecs file into Vec<Vec<f32>>
fn read_fvecs(path: &str, n: Option<usize>) -> Vec<Vec<f32>> {
    let mut f = std::fs::File::open(path).unwrap();
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
        for (j, chunk) in buf.as_chunks::<4>().0.iter().enumerate() {
            vec[j] = f32::from_le_bytes(*chunk);
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
        for (j, chunk) in buf.as_chunks::<4>().0.iter().enumerate() {
            ids[j] = i32::from_le_bytes(*chunk);
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

/// Decode the `id: UInt64` arrow-Row index keys these tests build.
fn decode_u64_ids(keys: &[lakesoul_vector::IndexKey]) -> Vec<u64> {
    use lakesoul_io::index::key::{KeyCodec, KeyLayout};
    if keys.is_empty() {
        return Vec::new();
    }
    let codec = KeyCodec::new(KeyLayout::new(
        vec!["id".to_string()],
        vec![arrow_schema::DataType::UInt64],
    ))
    .unwrap();
    let arrays = codec.decode(keys).unwrap();
    let values = arrays[0]
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
    (0..values.len()).map(|i| values.value(i)).collect()
}

/// Test: build index from parquet, search, verify recall.
///
/// Uses a small subset (500 train, 5 test) for speed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires prepare_data.py to be run first"]
async fn test_glove_e2e_build_and_search() {
    let store = Arc::new(LocalFileSystem::new());
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
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };

    // Build index from parquet file
    let parquet_path = format!("{}/data/train_10k.parquet", TEST_DATA_DIR);
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config.clone(),
        vec![format!("file://{}", parquet_path)],
        vec!["id".to_string()],
        SecretMap::new(),
        None,
    );
    let outcome = builder.build().await.unwrap();
    println!("Index built successfully at {}", index_prefix);

    // Search: load index, query, check recall
    let queries = read_fvecs(&format!("{}/test_5.fvecs", DATA_DIR), Some(5));
    let ground_truth = read_ivecs(
        // groundtruth.ivecs not bundled; this test is #[ignore]
        "/home/chenxu/program/opensource/rabitq-rs/data/glove-200d/processed/groundtruth.ivecs",
        Some(5),
    );

    let istore = IndexStore::new(store, outcome.index_prefix.clone());
    let header = IndexHeader::deserialize(&outcome.header).unwrap();
    let index =
        IvfRabitqIndex::load_from_segments(&istore, &header, &outcome.new_segments)
            .await
            .unwrap_or_else(|e| panic!("load_from_segments failed: {:?}", e));
    let params = lakesoul_vector::SearchParams::new(10, 8);

    for (i, query) in queries.iter().enumerate() {
        let results = index.search(query, params).unwrap();
        let keys: Vec<_> = results.iter().map(|r| r.id.clone()).collect();
        let pred_ids: Vec<u64> = decode_u64_ids(&keys);
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
#[ignore = "requires prepare_data.py to be run first"]
async fn test_read_parquet_schema() {
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
#[ignore = "requires prepare_data.py to be run first"]
async fn test_build_and_list_files() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new());

    let config = VectorIndexConfig {
        column_name: "vec".to_string(),
        dim: DIM,
        nlist: 4,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };
    let parquet_path = format!("{}/data/train_10k.parquet", TEST_DATA_DIR);
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        vec![format!("file://{}", parquet_path)],
        vec!["id".to_string()],
        SecretMap::new(),
        None,
    );
    let outcome = builder.build().await.unwrap();

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
    let istore = IndexStore::new(store, outcome.index_prefix.clone());
    let header = IndexHeader::deserialize(&outcome.header).unwrap();
    match IvfRabitqIndex::load_from_segments(&istore, &header, &outcome.new_segments)
        .await
    {
        Ok(idx) => println!("Load succeeded: {} vectors", idx.len()),
        Err(e) => println!("Load failed: {:?}", e),
    }
}

/// Test: LakeSoulReader with vector_search options — full filter injection path.
///
/// Verifies that LakeSoulReader::start() → inject_index_search_filters →
/// search_matching_shards → IDs injected as filter → filtered results.
#[tokio::test]
async fn test_reader_with_vector_search() {
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
        OPTION_KEY_VECTOR_SEARCH_NPROBE, OPTION_KEY_VECTOR_SEARCH_QUERY,
        OPTION_KEY_VECTOR_SEARCH_TOP_K,
    };
    use lakesoul_io::reader::LakeSoulReader;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let parquet_path = format!("{}/part-abc_0000.parquet", tmp_path);
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
    let store = Arc::new(LocalFileSystem::new());
    let config = VectorIndexConfig {
        column_name: vec_col.to_string(),
        dim,
        nlist: 8,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        vec![format!("file://{}", parquet_path)],
        vec![pk_col.to_string()],
        SecretMap::new(),
        Some(format!("file://{}", tmp_path)),
    );
    let outcome = builder.build().await.unwrap();
    let resolved = ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: outcome.index_prefix.clone(),
        commit_id: 1,
        generation: 1,
        version: 1,
        header: outcome.header.clone(),
        segments: serde_json::to_value(&outcome.new_segments).unwrap(),
    };
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
        .with_resolved_index_shards(vec![resolved])
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

/// The caller may declare the reserved index-distance column up front (as the
/// Daft scan does): the reader strips it from the scan plan, attaches the
/// sqrt-ed L2 estimate per row and returns exactly the declared schema.
#[tokio::test]
async fn test_reader_exposes_index_distance_column() {
    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
        OPTION_KEY_VECTOR_SEARCH_NPROBE, OPTION_KEY_VECTOR_SEARCH_ORDER_BY,
        OPTION_KEY_VECTOR_SEARCH_PARALLELISM, OPTION_KEY_VECTOR_SEARCH_QUERY,
        OPTION_KEY_VECTOR_SEARCH_TOP_K,
    };
    use lakesoul_io::reader::LakeSoulReader;
    use lakesoul_io::vector::score::VECTOR_DISTANCE_FIELD;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let parquet_path = format!("{}/part-dist_0000.parquet", tmp_path);
    let pk_col = "id";
    let vec_col = "vec";

    let train_vecs = read_fvecs(&format!("{}/train_700.fvecs", DATA_DIR), Some(300));
    let test_vecs = read_fvecs(&format!("{}/test_5.fvecs", DATA_DIR), Some(5));
    let n_train = train_vecs.len();
    let dim = train_vecs[0].len();

    {
        use arrow_array::{FixedSizeListArray, RecordBatch, UInt64Array};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new(pk_col, DataType::UInt64, false),
            Field::new(
                vec_col,
                DataType::FixedSizeList(
                    std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                false,
            ),
        ]));
        let ids = UInt64Array::from((0..n_train as u64).collect::<Vec<_>>());
        let flat: Vec<f32> = train_vecs.iter().flatten().copied().collect();
        let vec_arr = FixedSizeListArray::new(
            std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
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

    let store = Arc::new(LocalFileSystem::new());
    let config = VectorIndexConfig {
        column_name: vec_col.to_string(),
        dim,
        nlist: 8,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        vec![format!("file://{}", parquet_path)],
        vec![pk_col.to_string()],
        SecretMap::new(),
        Some(format!("file://{}", tmp_path)),
    );
    let outcome = builder.build().await.unwrap();
    let resolved = ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: outcome.index_prefix.clone(),
        commit_id: 1,
        generation: 1,
        version: 1,
        header: outcome.header.clone(),
        segments: serde_json::to_value(&outcome.new_segments).unwrap(),
    };

    let query_vec = &test_vecs[0];
    let query_str: String = query_vec
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // The caller declares the reserved field up front: the scan plan must drop
    // it (it is not a file column) and the output must carry it in place.
    let read_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new(pk_col, DataType::UInt64, false),
        Field::new(
            vec_col,
            DataType::FixedSizeList(
                std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
        Field::new(VECTOR_DISTANCE_FIELD, DataType::Float32, false),
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
        .with_option(OPTION_KEY_VECTOR_SEARCH_TOP_K, "5")
        .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, "4")
        .with_option(OPTION_KEY_VECTOR_SEARCH_ORDER_BY, "true")
        .with_option(OPTION_KEY_VECTOR_SEARCH_PARALLELISM, "2")
        .with_option("skip_merge_on_read", "true")
        .with_option("file_filter_pushdown", "false")
        .with_resolved_index_shards(vec![resolved])
        .build();

    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();

    let mut rows: Vec<(u64, f32)> = Vec::new();
    let mut output_fields: Option<Vec<String>> = None;
    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.unwrap();
        output_fields.get_or_insert_with(|| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        });
        let ids = batch
            .column_by_name(pk_col)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        let distances = batch
            .column_by_name(VECTOR_DISTANCE_FIELD)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), distances.value(row)));
        }
    }
    assert!(!rows.is_empty(), "the search must return candidates");
    assert_eq!(
        output_fields.expect("at least one batch"),
        vec![
            pk_col.to_string(),
            vec_col.to_string(),
            VECTOR_DISTANCE_FIELD.to_string()
        ],
        "the declared reserved column must be filled in place"
    );

    // Every attached distance is the (sqrt-ed) L2 estimate of that row: close
    // to the exact distance and never larger than the worst returned row.
    for (id, distance) in &rows {
        assert!(
            distance.is_finite() && *distance >= 0.0,
            "bad distance {distance}"
        );
        let exact: f32 = train_vecs[*id as usize]
            .iter()
            .zip(query_vec)
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        let exact = exact.sqrt();
        assert!(
            (*distance - exact).abs() <= 0.35 * exact.max(1.0),
            "index distance {distance} too far from exact {exact} for id {id}"
        );
    }
}

/// Two indexed vector columns can be searched in one scan: the candidate sets
/// are intersected and the reserved distance column carries the weighted sum
/// of the per-column index distances (no vector column is read).
#[tokio::test]
async fn test_reader_fuses_multiple_vector_columns() {
    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_ORDER_BY,
        OPTION_KEY_VECTOR_SEARCH_REQUESTS,
    };
    use lakesoul_io::reader::LakeSoulReader;
    use lakesoul_io::vector::score::VECTOR_DISTANCE_FIELD;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let parquet_path = format!("{}/part-fuse_0000.parquet", tmp_path);
    let pk_col = "id";

    let train_vecs = read_fvecs(&format!("{}/train_700.fvecs", DATA_DIR), Some(40));
    let test_vecs = read_fvecs(&format!("{}/test_5.fvecs", DATA_DIR), Some(5));
    let n_train = train_vecs.len();
    let dim = train_vecs[0].len();

    {
        use arrow_array::{FixedSizeListArray, RecordBatch, UInt64Array};
        let list_type = || {
            DataType::FixedSizeList(
                std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            )
        };
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new(pk_col, DataType::UInt64, false),
            Field::new("vec_a", list_type(), false),
            Field::new("vec_b", list_type(), false),
        ]));
        let ids = UInt64Array::from((0..n_train as u64).collect::<Vec<_>>());
        let flat_a: Vec<f32> = train_vecs.iter().flatten().copied().collect();
        // Column b holds the same vectors: both indexes then return the same
        // candidate set for the same query, which makes the fusion checkable.
        let flat_b = flat_a.clone();
        let make_list = |values: Vec<f32>| {
            FixedSizeListArray::new(
                std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
                std::sync::Arc::new(Float32Array::from(values)),
                None,
            )
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(ids),
                std::sync::Arc::new(make_list(flat_a)),
                std::sync::Arc::new(make_list(flat_b)),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let build_index = |column: &str| {
        let store = Arc::new(LocalFileSystem::new());
        let config = VectorIndexConfig {
            column_name: column.to_string(),
            dim,
            nlist: 4,
            total_bits: 7,
            metric: Metric::L2,
            rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
            seed: 42,
            use_faster_config: true,
            rebuild_mode: "auto".to_string(),
            max_delta_ratio: 1.0,
        };
        VectorShardIndexBuilder::new(
            store,
            config,
            vec![format!("file://{}", parquet_path)],
            vec![pk_col.to_string()],
            SecretMap::new(),
            Some(format!("file://{}", tmp_path)),
        )
    };
    let outcome_a = build_index("vec_a").build().await.unwrap();
    let outcome_b = build_index("vec_b").build().await.unwrap();
    let resolved =
        |outcome: &lakesoul_io::vector::builder::ShardBuildOutcome| ResolvedIndex {
            kind: IndexKind::Vector,
            index_prefix: outcome.index_prefix.clone(),
            commit_id: 1,
            generation: 1,
            version: 1,
            header: outcome.header.clone(),
            segments: serde_json::to_value(&outcome.new_segments).unwrap(),
        };

    let query = &test_vecs[0];
    let query_str: String = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    // Same query for both columns, weights 0.5 / 0.25: the fused distance is
    // 0.75 x the exact L2 distance while the vector columns are never read.
    let requests = format!(
        r#"[{{"column":"vec_a","query":"{query_str}","top_k":10,"weight":0.5,"metric":"L2"}},
            {{"column":"vec_b","query":"{query_str}","top_k":10,"weight":0.25,"metric":"L2"}}]"#
    );

    let read_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new(pk_col, DataType::UInt64, false),
        Field::new(VECTOR_DISTANCE_FIELD, DataType::Float32, false),
    ]));
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![format!("file://{}", parquet_path)])
        .with_prefix(format!("file://{}", tmp_path))
        .with_schema(read_schema)
        .with_primary_keys(vec![pk_col.to_string()])
        .with_batch_size(8192)
        .with_thread_num(2)
        .with_option(OPTION_KEY_VECTOR_SEARCH_REQUESTS, requests)
        .with_option(OPTION_KEY_VECTOR_SEARCH_ORDER_BY, "true")
        .with_option("skip_merge_on_read", "true")
        .with_option("file_filter_pushdown", "false")
        .with_resolved_index_shards(vec![resolved(&outcome_a), resolved(&outcome_b)])
        .build();

    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();

    let mut rows: Vec<(u64, f32)> = Vec::new();
    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.unwrap();
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(
            names,
            vec![pk_col.to_string(), VECTOR_DISTANCE_FIELD.to_string()],
            "the fused column replaces the declared reserved field"
        );
        let ids = batch
            .column_by_name(pk_col)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        let distances = batch
            .column_by_name(VECTOR_DISTANCE_FIELD)
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), distances.value(row)));
        }
    }
    assert!(!rows.is_empty(), "the fused search must return candidates");
    assert!(
        rows.len() <= 10,
        "each request caps at 10 candidates: {rows:?}"
    );

    for (id, fused) in &rows {
        let exact: f32 = train_vecs[*id as usize]
            .iter()
            .zip(query)
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        let expected = 0.75 * exact.sqrt();
        assert!(
            (*fused - expected).abs() <= 0.35 * expected.max(1.0),
            "fused distance {fused} too far from 0.75 x exact {expected} for id {id}"
        );
    }
}

/// Write one parquet part with `ids` and their vectors.
fn write_vectors_parquet(
    path: &str,
    ids: std::ops::Range<u64>,
    vector_of: impl Fn(u64) -> Vec<f32>,
    dim: usize,
    pk_col: &str,
    vec_col: &str,
) {
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};

    let item = std::sync::Arc::new(arrow_schema::Field::new(
        "item",
        arrow_schema::DataType::Float32,
        true,
    ));
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new(pk_col, arrow_schema::DataType::UInt64, false),
        arrow_schema::Field::new(
            vec_col,
            arrow_schema::DataType::FixedSizeList(item.clone(), dim as i32),
            false,
        ),
    ]));
    let ids: Vec<u64> = ids.collect();
    let flat: Vec<f32> = ids.iter().flat_map(|id| vector_of(*id)).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(UInt64Array::from(ids)),
            std::sync::Arc::new(FixedSizeListArray::new(
                item,
                dim as i32,
                std::sync::Arc::new(Float32Array::from(flat)),
                None,
            )),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A query must not lose rows from files the index does not cover yet.
///
/// The index is built after each commit, so a commit whose build was deferred
/// (or failed) leaves newer files outside the index.  Narrowing the scan to
/// the index's own keys alone would silently drop those rows; the search has
/// to score the uncovered files exactly as well.
#[tokio::test]
async fn test_reader_searches_files_the_index_does_not_cover() {
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
        OPTION_KEY_VECTOR_SEARCH_NPROBE, OPTION_KEY_VECTOR_SEARCH_QUERY,
        OPTION_KEY_VECTOR_SEARCH_TOP_K,
    };
    use lakesoul_io::reader::LakeSoulReader;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let pk_col = "id";
    let vec_col = "vec";
    let dim = 4usize;

    // `covered` (ids 0..20) is indexed; `pending` (ids 20..40) was written
    // after the index was built.  Only `pending` holds the query's nearest
    // row (id 20, at distance zero), and every pending row is closer than
    // every covered one.
    let covered_path = format!("{}/part-abc_0000.parquet", tmp_path);
    let pending_path = format!("{}/part-def_0000.parquet", tmp_path);
    write_vectors_parquet(
        &covered_path,
        0..20,
        |id| vec![1.0, 0.0, 0.0, id as f32 * 0.001],
        dim,
        pk_col,
        vec_col,
    );
    write_vectors_parquet(
        &pending_path,
        20..40,
        |id| vec![0.0, 1.0, 0.0, (id - 20) as f32 * 0.01],
        dim,
        pk_col,
        vec_col,
    );

    // The index only knows the committed `covered` file.
    let store = Arc::new(LocalFileSystem::new());
    let config = VectorIndexConfig {
        column_name: vec_col.to_string(),
        dim,
        nlist: 4,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };
    let outcome = VectorShardIndexBuilder::new(
        store,
        config,
        vec![format!("file://{}", covered_path)],
        vec![pk_col.to_string()],
        SecretMap::new(),
        Some(format!("file://{}", tmp_path)),
    )
    .build()
    .await
    .unwrap();
    let resolved = ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: outcome.index_prefix.clone(),
        commit_id: 1,
        generation: 1,
        version: 1,
        header: outcome.header.clone(),
        segments: serde_json::to_value(&outcome.new_segments).unwrap(),
    };

    let query = [0.0f32, 1.0, 0.0, 0.0];
    let query_str: String = query
        .iter()
        .map(|value| value.to_string())
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
        .with_files(vec![
            format!("file://{}", covered_path),
            format!("file://{}", pending_path),
        ])
        .with_prefix(format!("file://{}", tmp_path))
        .with_schema(read_schema)
        .with_primary_keys(vec![pk_col.to_string()])
        .with_batch_size(8192)
        .with_thread_num(2)
        .with_option(OPTION_KEY_VECTOR_SEARCH_COLUMN, vec_col)
        .with_option(OPTION_KEY_VECTOR_SEARCH_QUERY, query_str)
        .with_option(OPTION_KEY_VECTOR_SEARCH_TOP_K, "5")
        .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, "8")
        .with_option("skip_merge_on_read", "true")
        .with_option("file_filter_pushdown", "false")
        .with_resolved_index_shards(vec![resolved])
        .build();

    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();
    let mut ids: Vec<u64> = Vec::new();
    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.unwrap();
        let column = batch
            .column_by_name(pk_col)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        ids.extend(column.values());
    }
    ids.sort_unstable();

    // The candidates are the union of the index's hits and the exactly
    // scored rows of the file the index does not cover; the caller's global
    // sort + limit then picks the true top-k, which all live in that file.
    assert_eq!(
        ids.len(),
        10,
        "5 index candidates + 5 tail candidates: {ids:?}"
    );
    for id in 20..25 {
        assert!(
            ids.contains(&id),
            "row {id} from an unindexed file must be searched too: {ids:?}"
        );
    }
}

/// Write one parquet part with `ids` and a scalar column, no vector column:
/// a sparse update (the shape a write takes when it omits an indexed column).
fn write_sparse_parquet(path: &str, ids: std::ops::Range<u64>, pk_col: &str) {
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};

    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new(pk_col, arrow_schema::DataType::UInt64, false),
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, true),
    ]));
    let ids: Vec<u64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|id| *id as i64 * 10).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(UInt64Array::from(ids)),
            std::sync::Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A file the index does not cover and that does not carry the vector column
/// (a sparse update) has nothing to score: the tail search must skip it
/// instead of failing on the projection.
#[tokio::test]
async fn test_tail_skips_uncovered_files_without_the_vector_column() {
    use lakesoul_io::config::{
        LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
        OPTION_KEY_VECTOR_SEARCH_NPROBE, OPTION_KEY_VECTOR_SEARCH_QUERY,
        OPTION_KEY_VECTOR_SEARCH_TOP_K,
    };
    use lakesoul_io::reader::LakeSoulReader;

    let tmp = TempDir::new().unwrap();
    let tmp_path = tmp.path().to_str().unwrap().to_string();
    let pk_col = "id";
    let vec_col = "vec";
    let dim = 4usize;

    let covered_path = format!("{}/part-abc_0000.parquet", tmp_path);
    let sparse_path = format!("{}/part-def_0000.parquet", tmp_path);
    write_vectors_parquet(
        &covered_path,
        0..4,
        |id| vec![0.0, 1.0, 0.0, id as f32 * 0.001],
        dim,
        pk_col,
        vec_col,
    );
    write_sparse_parquet(&sparse_path, 4..8, pk_col);

    let store = Arc::new(LocalFileSystem::new());
    let config = VectorIndexConfig {
        column_name: vec_col.to_string(),
        dim,
        nlist: 2,
        total_bits: 7,
        metric: Metric::L2,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };
    let outcome = VectorShardIndexBuilder::new(
        store,
        config,
        vec![format!("file://{}", covered_path)],
        vec![pk_col.to_string()],
        SecretMap::new(),
        Some(format!("file://{}", tmp_path)),
    )
    .build()
    .await
    .unwrap();
    let resolved = ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: outcome.index_prefix.clone(),
        commit_id: 1,
        generation: 1,
        version: 1,
        header: outcome.header.clone(),
        segments: serde_json::to_value(&outcome.new_segments).unwrap(),
    };

    let query = [0.0f32, 1.0, 0.0, 0.0];
    let query_str: String = query
        .iter()
        .map(|value| value.to_string())
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
            true,
        ),
    ]));
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![
            format!("file://{}", covered_path),
            format!("file://{}", sparse_path),
        ])
        .with_prefix(format!("file://{}", tmp_path))
        .with_schema(read_schema)
        .with_primary_keys(vec![pk_col.to_string()])
        .with_batch_size(8192)
        .with_thread_num(2)
        .with_option(OPTION_KEY_VECTOR_SEARCH_COLUMN, vec_col)
        .with_option(OPTION_KEY_VECTOR_SEARCH_QUERY, query_str)
        .with_option(OPTION_KEY_VECTOR_SEARCH_TOP_K, "2")
        .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, "4")
        .with_option("skip_merge_on_read", "true")
        .with_option("file_filter_pushdown", "false")
        .with_resolved_index_shards(vec![resolved])
        .build();

    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();
    let mut ids: Vec<u64> = Vec::new();
    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.unwrap();
        let column = batch
            .column_by_name(pk_col)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        ids.extend(column.values());
    }
    ids.sort_unstable();
    // The uncovered sparse file is skipped without an error; the candidates
    // are the index's own (the sparse rows carry no vector at all).
    assert_eq!(ids, vec![0, 1], "the sparse file must be skipped: {ids:?}");
}
