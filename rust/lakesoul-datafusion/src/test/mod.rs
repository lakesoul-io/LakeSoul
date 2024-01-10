// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use lakesoul_metadata::MetaDataClient;

mod insert_tests;
mod upsert_tests;
mod hash_tests;
// mod compaction_tests;
// mod streaming_tests;

#[ctor::ctor]
fn init() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client = Arc::new(MetaDataClient::from_env().await.unwrap());
            client.meta_cleanup().await.unwrap();
            println!("clean metadata");
        })
}
