// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow::array::RecordBatch;
use std::sync::Arc;
use tracing::debug;

use lakesoul_metadata::MetaDataClient;

mod hash_tests;
mod insert_tests;
mod upsert_tests;
// mod compaction_tests;
// mod streaming_tests;
#[cfg(feature = "ci")]
mod integration_tests;

#[cfg(feature = "ci")]
mod benchmarks;

mod catalog_tests;

// in cargo test, this executed only once
#[ctor::ctor]
fn init() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client = Arc::new(MetaDataClient::from_env().await.unwrap());
            client.meta_cleanup().await.unwrap();
            debug!("clean metadata");
        })
}

fn assert_batches_eq(table_name: &str, expected: &[&str], results: &[RecordBatch]) {
    // let expected_lines: Vec<String> =
    //         expected.iter().map(|&s| s.into()).collect();
    let (schema, remain) = expected.split_at(3);
    let (expected, end) = remain.split_at(remain.len() - 1);
    let mut expected = Vec::from(expected);

    expected.sort();

    let expected_lines = [schema, &expected, end].concat();

    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(results)
        .unwrap()
        .to_string();

    let actual_lines: Vec<&str> = formatted.trim().lines().collect();
    let (schema, remain) = actual_lines.split_at(3);
    let (result, end) = remain.split_at(remain.len() - 1);
    let mut result = Vec::from(result);

    result.sort();

    let result = [schema, &result, end].concat();

    assert_eq!(
        expected_lines, result,
        "\n\n{}\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        table_name, expected_lines, result
    );
}
