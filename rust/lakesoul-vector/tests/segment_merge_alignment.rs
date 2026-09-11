// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
//! Regression: merged base+delta segments must stay position-aligned.
//!
//! Each segment pads its final FastScan batch to 32 vectors, so merging a
//! base segment whose size is not a multiple of 32 with a delta segment must
//! re-pack the batches; raw concatenation used to make every delta vector
//! unreachable (and could evict true neighbours with garbage scores).

use std::sync::Arc;

use lakesoul_vector::{
    IdAndVecBatch, IvfRabitqBuilder, IvfRabitqIndex, ManifestStore, Metric, RotatorType,
    SearchParams,
};
use object_store::memory::InMemory;

#[tokio::test]
async fn merged_segments_keep_delta_vectors_searchable() {
    let dim = 4usize;
    let nlist = 1usize;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let mstore = ManifestStore::new(store, "align".to_string());

    // base: 5 vectors (5 % 32 != 0)
    let base_vectors: Vec<f32> = vec![
        0.0, 0.0, 0.0, 0.0, // id 0
        1.0, 0.0, 0.0, 0.0, // id 1
        0.0, 1.0, 0.0, 0.0, // id 2
        0.0, 0.0, 1.0, 0.0, // id 3
        0.0, 0.0, 0.0, 1.0, // id 4
    ];
    let mut builder = IvfRabitqBuilder::new(
        dim,
        nlist,
        7,
        Metric::L2,
        RotatorType::FhtKacRotator,
        42,
        true,
    );
    builder
        .insert_batch(IdAndVecBatch {
            ids: (0..5).collect(),
            vectors: base_vectors.clone(),
        })
        .unwrap();
    let stream = vec![IdAndVecBatch {
        ids: (0..5).collect(),
        vectors: base_vectors.clone(),
    }];
    let index = builder
        .build(|| futures::stream::iter(stream.clone().into_iter()))
        .await
        .unwrap();
    index.save_to_v4(&mstore).await.unwrap();

    // delta: one new vector, far from the base ones
    let new_vec = vec![5.0, 5.0, 5.0, 5.0];
    let mut builder = IvfRabitqBuilder::load(
        &mstore,
        dim,
        nlist,
        7,
        Metric::L2,
        RotatorType::FhtKacRotator,
        42,
        true,
    )
    .await
    .unwrap();
    builder
        .insert_batch(IdAndVecBatch {
            ids: vec![100],
            vectors: new_vec.clone(),
        })
        .unwrap();
    builder.flush(&mstore).await.unwrap();

    let loaded = IvfRabitqIndex::load_from_v4(&mstore).await.unwrap();
    assert_eq!(loaded.len(), 6, "merged vector count");

    // Query exactly the new vector: it must be the top-1 with distance ~0.
    let results = loaded.search(&new_vec, SearchParams::new(3, 1)).unwrap();
    let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
    println!("query=new_vec results: {:?}", results);
    assert_eq!(ids.first(), Some(&100), "new vector must be found");

    // Query each base vector and verify self-retrieval.
    for (i, v) in base_vectors.chunks_exact(dim).enumerate() {
        let results = loaded.search(v, SearchParams::new(3, 1)).unwrap();
        let top: Vec<u64> = results.iter().map(|r| r.id).collect();
        println!("query=base[{i}] results: {:?}", results);
        assert_eq!(
            top.first(),
            Some(&(i as u64)),
            "base vector {i} must be found"
        );
    }
}
