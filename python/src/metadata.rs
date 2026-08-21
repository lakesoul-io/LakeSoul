// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors

use std::env;
use std::sync::LazyLock;

use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_metadata::{
    LakeSoulMetaDataError, MetaDataClient, transfusion::DataFileInfo, utils::qualify_path,
};
use lakesoul_metadata_proto::entity::{CommitOp, PartitionInfo, TableInfo, Uuid};
use pyo3::{
    create_exception,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};

create_exception!(
    lakesoul._lib._metadata,
    LakeSoulError,
    PyRuntimeError,
    "Base exception for LakeSoul runtime failures."
);
create_exception!(
    lakesoul._lib._metadata,
    MetadataError,
    LakeSoulError,
    "Base exception for LakeSoul metadata failures."
);
create_exception!(
    lakesoul._lib._metadata,
    TableNotFoundError,
    MetadataError,
    "The requested LakeSoul table does not exist."
);
create_exception!(
    lakesoul._lib._metadata,
    NamespaceNotFoundError,
    MetadataError,
    "The requested LakeSoul namespace does not exist."
);
create_exception!(
    lakesoul._lib._metadata,
    AlreadyExistsError,
    MetadataError,
    "The requested LakeSoul metadata object already exists."
);
create_exception!(
    lakesoul._lib._metadata,
    MetadataUnavailableError,
    MetadataError,
    "The LakeSoul metadata service is unavailable."
);
create_exception!(
    lakesoul._lib._metadata,
    PermissionDeniedError,
    MetadataError,
    "The metadata operation was denied."
);
create_exception!(
    lakesoul._lib._metadata,
    InvalidMetadataError,
    MetadataError,
    "LakeSoul metadata is invalid or cannot be decoded."
);
use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_metadata")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._metadata", &m)?;
    m.add("LakeSoulError", py.get_type::<LakeSoulError>())?;
    m.add("MetadataError", py.get_type::<MetadataError>())?;
    m.add("TableNotFoundError", py.get_type::<TableNotFoundError>())?;
    m.add(
        "NamespaceNotFoundError",
        py.get_type::<NamespaceNotFoundError>(),
    )?;
    m.add("AlreadyExistsError", py.get_type::<AlreadyExistsError>())?;
    m.add(
        "MetadataUnavailableError",
        py.get_type::<MetadataUnavailableError>(),
    )?;
    m.add(
        "PermissionDeniedError",
        py.get_type::<PermissionDeniedError>(),
    )?;
    m.add(
        "InvalidMetadataError",
        py.get_type::<InvalidMetadataError>(),
    )?;
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

#[derive(Clone, Copy)]
enum MissingObject {
    Table,
    Namespace,
    Unknown,
}

fn metadata_error_to_py(error: LakeSoulMetaDataError, missing: MissingObject) -> PyErr {
    let message = error.to_string();
    match error {
        LakeSoulMetaDataError::NotFound(_) => match missing {
            MissingObject::Table => TableNotFoundError::new_err(message),
            MissingObject::Namespace => NamespaceNotFoundError::new_err(message),
            MissingObject::Unknown => MetadataError::new_err(message),
        },
        LakeSoulMetaDataError::PostgresError(error) => {
            let sqlstate = error.code().map(|code| code.code());
            match sqlstate {
                Some("23505") => AlreadyExistsError::new_err(message),
                Some("28000" | "28P01" | "42501") => PermissionDeniedError::new_err(message),
                Some(
                    "08000" | "08001" | "08003" | "08004" | "08006" | "08007" | "08P01" | "3D000"
                    | "57P01" | "57P02" | "57P03",
                ) => MetadataUnavailableError::new_err(message),
                _ if error.as_db_error().is_none() || error.is_closed() => {
                    MetadataUnavailableError::new_err(message)
                }
                _ => MetadataError::new_err(message),
            }
        }
        LakeSoulMetaDataError::PostgresPoolError(_) | LakeSoulMetaDataError::IoError(_) => {
            MetadataUnavailableError::new_err(message)
        }
        LakeSoulMetaDataError::SerdeJsonError(_)
        | LakeSoulMetaDataError::ParseIntError(_)
        | LakeSoulMetaDataError::ParseUrlError(_)
        | LakeSoulMetaDataError::UuidError(_)
        | LakeSoulMetaDataError::ProstDecodeError(_)
        | LakeSoulMetaDataError::ProstEncodeError(_) => InvalidMetadataError::new_err(message),
        LakeSoulMetaDataError::Internal(_) | LakeSoulMetaDataError::Other(_) => {
            MetadataError::new_err(message)
        }
    }
}

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
            .map_err(|error| metadata_error_to_py(error, MissingObject::Unknown))?;
        Ok(Self { client })
    }

    #[staticmethod]
    fn from_env(py: Python) -> PyResult<Self> {
        let client = py
            .detach(|| RUNTIME.block_on(MetaDataClient::from_env()))
            .map_err(|error| metadata_error_to_py(error, MissingObject::Unknown))?;
        Ok(Self { client })
    }

    fn exec_query(&self, py: Python, query_type: i32, joined_string: String) -> PyResult<Vec<u8>> {
        py.detach(|| RUNTIME.block_on(self.client.execute_query_raw(query_type, joined_string)))
            .map_err(|error| metadata_error_to_py(error, MissingObject::Unknown))
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
            RUNTIME.block_on(self.client.commit_data_files_with_commit_op(
                &table_name,
                &namespace,
                files,
                CommitOp::AppendCommit,
            ))
        })
        .map_err(|error| metadata_error_to_py(error, MissingObject::Table))
    }

    fn get_data_files_of_single_partition(
        &self,
        py: Python,
        table_id: String,
        partition_desc: String,
        snapshot: Vec<(u64, u64)>,
    ) -> PyResult<Vec<String>> {
        let partition_info = PartitionInfo {
            table_id,
            partition_desc,
            snapshot: snapshot
                .into_iter()
                .map(|(high, low)| Uuid { high, low })
                .collect(),
            ..Default::default()
        };
        py.detach(|| {
            RUNTIME.block_on(
                self.client
                    .get_data_files_of_single_partition(&partition_info),
            )
        })
        .map_err(|error| metadata_error_to_py(error, MissingObject::Table))
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
        py.detach(|| RUNTIME.block_on(self.client.create_table(table_info)))
            .map_err(|error| metadata_error_to_py(error, MissingObject::Namespace))
    }

    #[pyo3(signature = (table_name, namespace = String::from("default")))]
    fn drop_table(&self, py: Python, table_name: String, namespace: String) -> PyResult<()> {
        py.detach(|| RUNTIME.block_on(self.client.drop_table(&table_name, &namespace)))
            .map_err(|error| metadata_error_to_py(error, MissingObject::Table))
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
