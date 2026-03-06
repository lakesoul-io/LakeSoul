// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
use std::sync::LazyLock;

use crate::install_module;
use lakesoul_metadata::{
    PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY, PooledClient, SECONDARY_URL_ENV_KEY,
    SECONDARY_URL_PROP_KEY, execute_query, pg_config_from_env,
};
use pyo3::{exceptions::PyValueError, prelude::*};

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_metadata")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._metadata", &m)?;
    m.add_function(wrap_pyfunction!(exec_query, &m)?)?;
    Ok(())
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .max_blocking_threads(8)
        .build()
        .unwrap()
});

static CLIENT: LazyLock<PooledClient> = LazyLock::new(|| {
    let config = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY).unwrap();
    let secondary_config = pg_config_from_env(SECONDARY_URL_PROP_KEY, SECONDARY_URL_ENV_KEY).ok();
    RUNTIME
        .block_on(PooledClient::try_new(config, secondary_config))
        .unwrap()
});

#[pyfunction]
// multiply copy data
fn exec_query(query_type: i32, joined_string: String) -> PyResult<Vec<u8>> {
    let client = &*CLIENT;
    RUNTIME.block_on(async {
        execute_query(client, query_type, joined_string)
            .await
            .map_err(|e| PyValueError::new_err(e.to_string()))
    })
}
