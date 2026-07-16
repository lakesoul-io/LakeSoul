// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors

use std::env;
use std::sync::LazyLock;

use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_metadata::{MetaDataClient, transfusion::DataFileInfo, utils::qualify_path};
use proto::proto::entity::{CommitOp, TableInfo};
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};

use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_metadata")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._metadata", &m)?;
    m.add_class::<NativeMetadataClient>()?;
    Ok(())
}

// this is a global tokio runtime only used for metadata operations
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(std::thread::available_parallelism().unwrap().get())
        .max_blocking_threads(8)
        .build()
        .unwrap()
});

#[pyclass(module = "lakesoul._lib._metadata", name = "_NativeMetadataClient")]
pub struct NativeMetadataClient {
    client: MetaDataClient,
}

#[pymethods]
impl NativeMetadataClient {
    #[new]
    #[pyo3(signature = (config, secondary_config = None, max_retry = 3))]
    fn new(
        py: Python,
        config: String,
        secondary_config: Option<String>,
        max_retry: usize,
    ) -> PyResult<Self> {
        let client = py
            .detach(move || {
                RUNTIME.block_on(MetaDataClient::from_config_and_max_retry(
                    config,
                    secondary_config,
                    max_retry,
                ))
            })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self { client })
    }

    #[staticmethod]
    fn from_env(py: Python) -> PyResult<Self> {
        let client = py
            .detach(|| RUNTIME.block_on(MetaDataClient::from_env()))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self { client })
    }

    fn exec_query(&self, py: Python, query_type: i32, joined_string: String) -> PyResult<Vec<u8>> {
        py.detach(|| {
            RUNTIME
                .block_on(self.client.execute_query_raw(query_type, joined_string))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    fn commit_data_files(
        &self,
        py: Python,
        table_name: String,
        namespace: String,
        files: Vec<(String, String, u64, Vec<String>)>,
    ) -> PyResult<()> {
        let files = py_files_to_data_file_info(files)?;
        py.detach(|| {
            RUNTIME
                .block_on(self.client.commit_data_files_with_commit_op(
                    &table_name,
                    &namespace,
                    files,
                    CommitOp::AppendCommit,
                ))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

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
        &self,
        py: Python,
        table_name: String,
        namespace: String,
        table_path: String,
        table_schema: PyArrowType<Schema>,
        properties: String,
        partitions: String,
        domain: String,
    ) -> PyResult<()> {
        let table_info = build_table_info(
            table_name,
            namespace,
            table_path,
            table_schema,
            properties,
            partitions,
            domain,
        )?;
        py.detach(|| {
            RUNTIME
                .block_on(self.client.create_table(table_info))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    #[pyo3(signature = (table_name, namespace = String::from("default")))]
    fn drop_table(&self, py: Python, table_name: String, namespace: String) -> PyResult<()> {
        py.detach(|| {
            RUNTIME
                .block_on(self.client.drop_table(&table_name, &namespace))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }
}

fn py_files_to_data_file_info(
    files: Vec<(String, String, u64, Vec<String>)>,
) -> PyResult<Vec<DataFileInfo>> {
    files
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
        .collect()
}

fn build_table_info(
    table_name: String,
    namespace: String,
    table_path: String,
    table_schema: PyArrowType<Schema>,
    properties: String,
    partitions: String,
    domain: String,
) -> PyResult<TableInfo> {
    let (schema_json, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        lakesoul_common::ser::arrow_java::schema_to_metadata_parts(&table_schema.0);
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
    Ok(TableInfo {
        table_id: uuid::Uuid::new_v4().to_string(),
        table_namespace: namespace,
        table_name,
        table_path,
        table_schema: schema_json,
        table_schema_arrow_ipc,
        table_schema_arrow_ipc_json_hash,
        properties,
        partitions,
        domain,
    })
}
