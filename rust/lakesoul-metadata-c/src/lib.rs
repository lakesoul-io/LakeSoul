// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![feature(c_size_t)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
extern crate core;

use core::ffi::c_ptrdiff_t;
use std::collections::HashMap;
use std::ffi::{c_char, c_uchar, CStr, CString};
use std::io::Write;
use std::ptr::{null, null_mut, NonNull};

use log::debug;
use prost::bytes::BufMut;
use prost::Message;

use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::transfusion::SplitDesc;
use lakesoul_metadata::{Builder, Client, MetaDataClient, PreparedStatementMap, Runtime};
use proto::proto::entity;

#[repr(C)]
pub struct CResult<OpaqueT> {
    ptr: *mut OpaqueT,
    err: *const c_char,
}

impl<OpaqueT> CResult<OpaqueT> {
    pub fn new<T>(obj: T) -> Self {
        CResult {
            ptr: convert_to_opaque_raw::<T, OpaqueT>(obj),
            err: std::ptr::null(),
        }
    }

    pub fn error(err_msg: &str) -> Self {
        CResult {
            ptr: std::ptr::null_mut(),
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

pub type ResultCallback = extern "C" fn(bool, *const c_char);

pub type IntegerResultCallBack = extern "C" fn(i32, *const c_char);

// pub type DataResultCallback = extern "C" fn(bool, *const c_char, *const c_void);

/// for jnr
/// can use as_ptr instead of into_raw?
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

// #[repr(C)]
// struct CVoid {
//     data: *const c_void,
// }

// unsafe impl Send for CVoid {}

// unsafe impl Sync for CVoid {}

#[repr(C)]
pub struct PreparedStatement {
    private: [u8; 0],
}

#[repr(C)]
pub struct TokioPostgresClient {
    private: [u8; 0],
}

#[repr(C)]
pub struct TokioRuntime {
    private: [u8; 0],
}

#[repr(C)]
pub struct BytesResult {
    private: [u8; 0],
}

fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

fn from_nonnull<T>(obj: NonNull<T>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr()) }
}

fn string_from_ptr(ptr: *const c_char) -> String {
    unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() }
}

#[no_mangle]
pub extern "C" fn execute_insert(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    prepared: NonNull<CResult<PreparedStatement>>,
    insert_type: i32,
    addr: c_ptrdiff_t,
    len: i32,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut() };
    let prepared = unsafe { NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut() };

    let raw_parts = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };
    let wrapper = entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();
    let result =
        runtime.block_on(async { lakesoul_metadata::execute_insert(client, prepared, insert_type, wrapper).await });
    match result {
        Ok(count) => call_result_callback(callback, count, std::ptr::null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

#[no_mangle]
pub extern "C" fn execute_update(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    prepared: NonNull<CResult<PreparedStatement>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut() };
    let prepared = unsafe { NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut() };

    let result = runtime.block_on(async {
        lakesoul_metadata::execute_update(client, prepared, update_type, string_from_ptr(joined_string)).await
    });
    match result {
        Ok(count) => call_result_callback(callback, count, std::ptr::null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

#[no_mangle]
pub extern "C" fn execute_query_scalar(
    callback: extern "C" fn(*const c_char, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    prepared: NonNull<CResult<PreparedStatement>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut() };
    let prepared = unsafe { NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut() };

    let result = runtime.block_on(async {
        lakesoul_metadata::execute_query_scalar(client, prepared, update_type, string_from_ptr(joined_string)).await
    });
    let (result, err): (*mut c_char, *const c_char) = match result {
        Ok(Some(result)) => (CString::new(result.as_str()).unwrap().into_raw(), std::ptr::null()),
        Ok(None) => (CString::new("").unwrap().into_raw(), std::ptr::null()),
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

#[no_mangle]
pub extern "C" fn execute_query(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    prepared: NonNull<CResult<PreparedStatement>>,
    query_type: i32,
    joined_string: *const c_char,
) -> NonNull<CResult<BytesResult>> {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_ref() };
    let prepared = unsafe { NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut() };

    let result = runtime.block_on(async {
        lakesoul_metadata::execute_query(client, prepared, query_type, string_from_ptr(joined_string)).await
    });
    match result {
        Ok(u8_vec) => {
            let len = u8_vec.len();
            call_result_callback(callback, len as i32, std::ptr::null());
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(u8_vec))
        }
        Err(e) => {
            call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw());
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

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

    call_result_callback(callback, true, std::ptr::null());
}

#[no_mangle]
pub extern "C" fn free_bytes_result(bytes: NonNull<CResult<BytesResult>>) {
    from_nonnull(bytes).free::<Vec<u8>>();
}

#[no_mangle]
pub extern "C" fn clean_meta_for_test(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
) {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_ref() };
    let result = runtime.block_on(async { lakesoul_metadata::clean_meta_for_test(client).await });
    match result {
        Ok(count) => call_result_callback(callback, count, std::ptr::null()),
        Err(e) => call_result_callback(callback, -1, CString::new(e.to_string().as_str()).unwrap().into_raw()),
    }
}

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

#[no_mangle]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<CResult<TokioRuntime>>) {
    from_nonnull(runtime).free::<Runtime>();
}

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
            call_result_callback(callback, true, std::ptr::null());
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

#[no_mangle]
pub extern "C" fn free_tokio_postgres_client(client: NonNull<CResult<TokioPostgresClient>>) {
    from_nonnull(client).free::<Client>();
}

#[no_mangle]
pub extern "C" fn create_prepared_statement() -> NonNull<CResult<PreparedStatement>> {
    let prepared = PreparedStatementMap::new();
    convert_to_nonnull(CResult::<PreparedStatement>::new(prepared))
}

#[no_mangle]
pub extern "C" fn free_prepared_statement(prepared: NonNull<CResult<PreparedStatement>>) {
    from_nonnull(prepared).free::<PreparedStatementMap>();
}

#[no_mangle]
pub extern "C" fn create_lakesoul_metadata_client() -> NonNull<CResult<MetaDataClient>> {
    let client = MetaDataClient::from_env();
    convert_to_nonnull(CResult::<MetaDataClient>::new(client))
}

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
    prepared: NonNull<CResult<PreparedStatement>>,
    runtime: NonNull<CResult<TokioRuntime>>,
    table_name: *const c_char,
    namespace: *const c_char,
) -> *mut c_char {
    let runtime = unsafe { NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref() };
    let client = unsafe { NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_ref() };
    let prepared = unsafe { NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut() };
    let table_name = c_char2str(table_name);
    let namespace = c_char2str(namespace);
    let result: Result<*mut c_char, LakeSoulMetaDataError> = runtime.block_on(async {
        let ret = lakesoul_metadata::transfusion::split_desc_array(client, prepared, table_name, namespace).await?;
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
/// TODO use tokio::tracing
/// TODO add logger format
#[no_mangle]
pub extern "C" fn rust_logger_init() {
    let _ = env_logger::try_init();
}
