// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors

use std::{env, sync::LazyLock};

use crate::install_module;
use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_metadata::{MetaDataClient, transfusion::DataFileInfo, utils::qualify_path};
use proto::proto::entity::TableInfo;
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_metadata")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._metadata", &m)?;
    m.add_function(wrap_pyfunction!(exec_query, &m)?)?;
    m.add_function(wrap_pyfunction!(commit_data_files, &m)?)?;
    m.add_function(wrap_pyfunction!(create_table, &m)?)?;
    m.add_function(wrap_pyfunction!(drop_table, &m)?)?;
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

static META_CLIENT: LazyLock<Result<MetaDataClient, String>> = LazyLock::new(|| {
    RUNTIME
        .block_on(MetaDataClient::from_env())
        .map_err(|error| error.to_string())
});

#[pyfunction]
// multiply copy data
fn exec_query(py: Python, query_type: i32, joined_string: String) -> PyResult<Vec<u8>> {
    py.detach(move || {
        let client = META_CLIENT
            .as_ref()
            .map_err(|error| PyRuntimeError::new_err(error.clone()))?;
        RUNTIME
            .block_on(client.execute_query_raw(query_type, joined_string))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
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

#[pyfunction(name = "_create_table")]
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
    table_schema: PyArrowType<Schema>,
    properties: String, // json
    partitions: String, //  hash;range
    domain: String,
) -> PyResult<()> {
    // stupid design
    // let schema_json = lakesoul_common::ser::arrow_java::schema_to_metadata_str(&table_schema.0);
    let schema_json = serde_json::to_string(&table_schema.0).unwrap();
    let table_path = if table_path.is_empty() {
        format!(
            "file://{}/{}/{}",
            env::current_dir().unwrap().to_str().unwrap(),
            namespace,
            table_name,
        )
    } else {
        // hdfs is not checked
        qualify_path(&table_path)
            .map_err(|_| PyRuntimeError::new_err(String::from("unable to qualify path")))?
    };
    let table_info = TableInfo {
        table_id: uuid::Uuid::new_v4().to_string(),
        table_namespace: namespace,
        table_name,
        table_path,
        table_schema: schema_json,
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

#[pyfunction(name = "drop_table")]
#[pyo3(signature = (
    table_name,
    namespace = String::from("default"),
))]
fn drop_table(py: Python, table_name: String, namespace: String) -> PyResult<()> {
    py.detach(move || {
        let client = META_CLIENT
            .as_ref()
            .map_err(|error| PyRuntimeError::new_err(error.clone()))?;
        RUNTIME
            .block_on(client.drop_table(&table_name, &namespace))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}
