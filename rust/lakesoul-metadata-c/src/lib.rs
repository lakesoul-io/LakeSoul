// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The C API for the [`lakesoul-metadata`] crate.

#![allow(clippy::not_unsafe_ptr_arg_deref)]
extern crate core;
#[macro_use]
extern crate tracing;

use std::collections::HashMap;
use std::ffi::{c_char, c_uchar, CStr, CString};
use std::io::Write;
use std::ptr::{null, null_mut, NonNull};

use prost::bytes::BufMut;
use prost::Message;

use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::transfusion::SplitDesc;
use lakesoul_metadata::{Builder, MetaDataClient, PooledClient, Runtime};
use proto::proto::entity;

#[allow(non_camel_case_types)]
pub type c_size_t = usize;
#[allow(non_camel_case_types)]
pub type c_ptrdiff_t = isize;

/// Opaque wrapper for the result of a function call.
#[repr(C)]
pub struct CResult<OpaqueT> {
    ptr: *mut OpaqueT,
    err: *const c_char,
}

impl<OpaqueT> CResult<OpaqueT> {
    pub fn new<T>(obj: T) -> Self {
        CResult {
            ptr: convert_to_opaque_raw::<T, OpaqueT>(obj),
            err: null(),
        }
    }

    pub fn error(err_msg: &str) -> Self {
        CResult {
            ptr: null_mut(),
            err: CString::new(err_msg).unwrap().into_raw(),
        }
    }

    pub fn free<T>(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                // Invoking `std::mem::drop` with a value that implements `Copy` does nothing
                drop(from_opaque::<OpaqueT, T>(NonNull::new_unchecked(self.ptr)));
            }
            if !self.err.is_null() {
                drop(CString::from_raw(self.err as *mut c_char));
            }
        }
    }
}

/// The callback function with bool result and error string.
pub type ResultCallback = extern "C" fn(bool, *const c_char);

/// The callback function with i32 result and error string.
pub type IntegerResultCallBack = extern "C" fn(i32, *const c_char);

// pub type DataResultCallback = extern "C" fn(bool, *const c_char, *const c_void);

/// Call the callback function with bool result and error string.
fn call_result_callback<T>(callback: extern "C" fn(T, *const c_char), status: T, err: *const c_char) {
    callback(status, err);
    // release error string
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

fn _call_integer_result_callback(callback: IntegerResultCallBack, status: i32, err: *const c_char) {
    // release error string
    callback(status, err);
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

/// The opaque type for the [`PooledClient`].
#[repr(C)]
pub struct TokioPostgresClient {
    private: [u8; 0],
}

/// The opaque type for the [`Runtime`].
#[repr(C)]
pub struct TokioRuntime {
    private: [u8; 0],
}

/// The opaque type for the bytes result.
#[repr(C)]
pub struct BytesResult {
    private: [u8; 0],
}

/// Convert the opaque type to the raw pointer.
fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

/// Convert the opaque type to the [`NonNull`] pointer.
fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

/// Convert the [`NonNull`] pointer to the opaque type.
fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

/// Convert the [`NonNull`] pointer to the object.
fn from_nonnull<T>(obj: NonNull<T>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr()) }
}

/// Convert the pointer to the string.
fn string_from_ptr(ptr: *const c_char) -> String {
    unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() }
}

/// Execute the insert Data Access Object.
#[no_mangle]
pub extern "C" fn execute_insert(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    insert_type: i32,
    addr: c_ptrdiff_t,
    len: i32,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut() };

    let raw_parts = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };
    let wrapper = entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();
    let result = runtime.block_on(async { lakesoul_metadata::execute_insert(client, insert_type, wrapper).await });
    match result {
        Ok(count) => call_result_callback(callback, count, null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

/// Execute the update Data Access Object.
#[no_mangle]
pub extern "C" fn execute_update(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut() };

    let result = runtime.block_on(async {
        lakesoul_metadata::execute_update(client, update_type, string_from_ptr(joined_string)).await
    });
    match result {
        Ok(count) => call_result_callback(callback, count, null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

/// Execute the query scalar Data Access Object.
#[no_mangle]
pub extern "C" fn execute_query_scalar(
    callback: extern "C" fn(*const c_char, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut() };

    let result = runtime.block_on(async {
        lakesoul_metadata::execute_query_scalar(client, update_type, string_from_ptr(joined_string)).await
    });
    let (result, err): (*mut c_char, *const c_char) = match result {
        Ok(Some(result)) => (CString::new(result.as_str()).unwrap().into_raw(), null()),
        Ok(None) => (CString::new("").unwrap().into_raw(), null()),
        Err(e) => (
            CString::new("").unwrap().into_raw(),
            CString::new(e.to_string().as_str()).unwrap().into_raw(),
        ),
    };
    call_result_callback(callback, result, err);
    unsafe {
        let _ = CString::from_raw(result);
    }
}

/// Execute the query Data Access Object.
#[no_mangle]
pub extern "C" fn execute_query(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    query_type: i32,
    joined_string: *const c_char,
) -> NonNull<CResult<BytesResult>> {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref() };

    let result = runtime
        .block_on(async { lakesoul_metadata::execute_query(client, query_type, string_from_ptr(joined_string)).await });
    match result {
        Ok(u8_vec) => {
            let len = u8_vec.len();
            call_result_callback(callback, len as i32, null());
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(u8_vec))
        }
        Err(e) => {
            call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw());
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

/// Export the bytes result.
#[no_mangle]
pub extern "C" fn export_bytes_result(
    callback: extern "C" fn(bool, *const c_char),
    bytes: NonNull<CResult<BytesResult>>,
    len: i32,
    addr: c_ptrdiff_t,
) {
    let len = len as usize;
    let bytes = unsafe { NonNull::new_unchecked(bytes.as_ref().ptr as *mut Vec<c_uchar>).as_mut() };

    if bytes.len() != len {
        call_result_callback(
            callback,
            false,
            CString::new("Size of buffer and result mismatch at export_bytes_result.")
                .unwrap()
                .into_raw(),
        );
        return;
    }
    bytes.push(0u8);
    bytes.shrink_to_fit();

    let dst = unsafe { std::slice::from_raw_parts_mut(addr as *mut u8, len + 1) };
    let mut writer = dst.writer();
    let _ = writer.write_all(bytes.as_slice());

    call_result_callback(callback, true, null());
}

/// Free the bytes result.
#[no_mangle]
pub extern "C" fn free_bytes_result(bytes: NonNull<CResult<BytesResult>>) {
    from_nonnull(bytes).free::<Vec<u8>>();
}

/// Clean the metadata for test.
#[no_mangle]
pub extern "C" fn clean_meta_for_test(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref() };
    let result = runtime.block_on(async { lakesoul_metadata::clean_meta_for_test(client).await });
    match result {
        Ok(count) => call_result_callback(callback, count, null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

/// Create the tokio runtime.
#[no_mangle]
pub extern "C" fn create_tokio_runtime() -> NonNull<CResult<TokioRuntime>> {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .max_blocking_threads(8)
        .build()
        .unwrap();
    convert_to_nonnull(CResult::<TokioRuntime>::new(runtime))
}

/// Free the tokio runtime.
#[no_mangle]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<CResult<TokioRuntime>>) {
    from_nonnull(runtime).free::<Runtime>();
}

/// Create the tokio postgres client.
#[no_mangle]
pub extern "C" fn create_tokio_postgres_client(
    callback: extern "C" fn(bool, *const c_char),
    config: *const c_char,
    runtime: NonNull<CResult<TokioRuntime>>,
) -> NonNull<CResult<TokioPostgresClient>> {
    let config = string_from_ptr(config);
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };

    let result = runtime.block_on(async { lakesoul_metadata::create_connection(config).await });

    let result = match result {
        Ok(client) => {
            call_result_callback(callback, true, null());
            CResult::<TokioPostgresClient>::new(client)
        }
        Err(e) => {
            call_result_callback(
                callback,
                false,
                CString::new(e.to_string().as_str()).unwrap().into_raw(),
            );
            CResult::<TokioPostgresClient>::error(format!("{}", e).as_str())
        }
    };
    convert_to_nonnull(result)
}

/// Free the tokio postgres client.
#[no_mangle]
pub extern "C" fn free_tokio_postgres_client(client: NonNull<CResult<TokioPostgresClient>>) {
    from_nonnull(client).free::<PooledClient>();
}

/// Create the lakesoul metadata client.
#[no_mangle]
pub extern "C" fn create_lakesoul_metadata_client() -> NonNull<CResult<MetaDataClient>> {
    let client = MetaDataClient::from_env();
    convert_to_nonnull(CResult::<MetaDataClient>::new(client))
}

/// Free the lakesoul metadata client.
#[no_mangle]
pub extern "C" fn free_lakesoul_metadata_client(client: NonNull<CResult<MetaDataClient>>) {
    from_nonnull(client).free::<MetaDataClient>();
}

/// # Safety
/// check nothing
fn c_char2str<'a>(ptr: *const c_char) -> &'a str {
    unsafe {
        let c_str = CStr::from_ptr(ptr);
        c_str.to_str().unwrap()
    }
}

/// USE: JNR
/// return split(partition) desc array in json format by table_name, namespace , filter(WIP)
#[no_mangle]
pub extern "C" fn create_split_desc_array(
    callback: ResultCallback,
    client: NonNull<CResult<TokioPostgresClient>>,
    runtime: NonNull<CResult<TokioRuntime>>,
    table_name: *const c_char,
    namespace: *const c_char,
) -> *mut c_char {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref() };
    let table_name = c_char2str(table_name);
    let namespace = c_char2str(namespace);
    let result: Result<*mut c_char, LakeSoulMetaDataError> = runtime.block_on(async {
        let ret = lakesoul_metadata::transfusion::split_desc_array(client, table_name, namespace).await?;
        let v = serde_json::to_vec(&ret)?;
        Ok(CString::new(v)
            .map_err(|e| LakeSoulMetaDataError::Internal(e.to_string()))?
            .into_raw())
    });

    let (ret, status, e) = match result {
        Ok(ptr) => (ptr, true, null()),
        Err(e) => (
            null_mut(),
            false,
            CString::new(e.to_string()).unwrap().into_raw() as *const c_char,
        ),
    };
    call_result_callback(callback, status, e);
    ret
}

/// # Safety
/// caller should keep it safe
#[no_mangle]
pub unsafe extern "C" fn free_split_desc_array(json: *mut c_char) {
    free_c_string(json)
}

#[no_mangle]
pub extern "C" fn debug(callback: extern "C" fn(bool, *const c_char)) -> *mut c_char {
    debug!("in debug");
    let x = vec![
        SplitDesc {
            file_paths: vec!["hello jnr".into()],
            primary_keys: vec![],
            partition_desc: HashMap::new(),
            table_schema: "".to_string(),
        };
        1
    ];
    let array = lakesoul_metadata::transfusion::SplitDescArray(x);
    let json_vec = serde_json::to_vec(&array).unwrap();
    let c_string = CString::new(json_vec).unwrap();
    let x = CString::new("oops").unwrap().into_raw();
    callback(false, x);
    unsafe {
        let _s = CString::from_raw(x);
    }
    c_string.into_raw()
}

/// # Safety
/// c_string should be valid
#[no_mangle]
pub unsafe extern "C" fn free_c_string(c_string: *mut c_char) {
    unsafe {
        // only check ptr is not null
        if c_string.is_null() {
            debug!("early return due to null ptr");
            return;
        }
        debug!("free c string start");
        let _s = CString::from_raw(c_string);
        debug!("free c string finished");
    }
}

/// init a global logger for rust code
/// now use RUST_LOG=LEVEL to activate
#[no_mangle]
pub extern "C" fn rust_logger_init() {
    // TODO add logger format
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    match tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_timer(timer)
        .try_init()
    {
        Ok(_) => {}
        Err(e) => {
            eprintln!("failed to initialize tracing subscriber: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rust_logger_init;

    #[test]
    fn log_test() {
        rust_logger_init();
        rust_logger_init();
        error!("rust logger activate");
        info!("rust logger activate");
        warn!("rust logger activate");
        debug!("rust logger activate");
        trace!("rust logger activate");
    }
}
