// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
use std::sync::LazyLock;

use crate::install_module;
use lakesoul_metadata::{
    PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY, PooledClient, SECONDARY_URL_ENV_KEY,
    SECONDARY_URL_PROP_KEY, execute_query, pg_config_from_env,
};
use proto::proto::entity::TableInfo;
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_metadata")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._metadata", &m)?;
    m.add_function(wrap_pyfunction!(exec_query, &m)?)?;
    m.add_function(wrap_pyfunction!(commit_data_files, &m)?)?;
    m.add_function(wrap_pyfunction!(create_table, &m)?)?;
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

#[pyfunction]
fn commit_data_files(
    py: Python,
    table_name: String,
    namespace: String,
    files: Vec<(String, String, u64, Vec<String>)>,
) -> PyResult<()> {
    let files = files
        .into_iter()
        .map(|(partition_desc, path, size, existing_columns)| {
            Ok(DataFileInfo {
                partition_desc,
                path,
                file_op: "add".to_string(),
                size: i64::try_from(size)
                    .map_err(|_| PyValueError::new_err("file size exceeds i64::MAX"))?,
                file_exist_cols: existing_columns.join(","),
                ..Default::default()
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    py.detach(move || {
        let client = META_CLIENT
            .as_ref()
            .map_err(|error| PyRuntimeError::new_err(error.clone()))?;
        RUNTIME
            .block_on(client.commit_data_files(&table_name, &namespace, files))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}

#[pyfunction]
#[pyo3(signature = (
    table_name,
    namespace,
    table_path,
    table_schema,
    properties = String::from("{}"),
    partitions = String::from(";"),
    domain = String::from("public"),
))]
fn create_table(
    py: Python,
    table_name: String,
    namespace: String,
    table_path: String,
    table_schema: String,
    properties: String,
    partitions: String,
    domain: String,
) -> PyResult<()> {
    let table_info = TableInfo {
        table_id: uuid::Uuid::new_v4().to_string(),
        table_namespace: namespace,
        table_name,
        table_path,
        table_schema,
        properties,
        partitions,
        domain,
    };
    py.detach(move || {
        let client = META_CLIENT
            .as_ref()
            .map_err(|error| PyRuntimeError::new_err(error.clone()))?;
        RUNTIME
            .block_on(client.create_table(table_info))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}
