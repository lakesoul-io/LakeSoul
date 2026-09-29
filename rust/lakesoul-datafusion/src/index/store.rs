// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Shared object-store plumbing for index builds and GC.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_io::object_store_metrics::MonitoredObjectStore;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

use crate::Result;

/// Build an object store for an index from the table's data files.
pub fn store_for_files(
    first_file: &str,
    object_store_options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>> {
    if first_file.starts_with("s3://") || first_file.starts_with("s3a://") {
        let store = lakesoul_io::object_store::create_s3_store_from_options(
            object_store_options,
        )?;
        Ok(Arc::new(MonitoredObjectStore::new(Arc::new(store), "s3")))
    } else {
        Ok(Arc::new(MonitoredObjectStore::new(
            Arc::new(LocalFileSystem::new()),
            "local",
        )))
    }
}
