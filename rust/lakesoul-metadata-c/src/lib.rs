// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The C API for the [`lakesoul-metadata`] crate.

#![allow(clippy::not_unsafe_ptr_arg_deref)]
extern crate core;
#[macro_use]
extern crate tracing;

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_uchar};
use std::io::Write;
use std::panic::{self, AssertUnwindSafe};
use std::ptr::{NonNull, null, null_mut};

use lakesoul_common::ffi::{
    CResult, catch_unwind_cresult, convert_to_nonnull, from_nonnull, into_c_string,
    log_panic_and_extract_message,
};
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::transfusion::SplitDesc;
use lakesoul_metadata::{
    Builder, Claims, JwtServer, MetaDataClient, PooledClient, Runtime,
};
use lakesoul_metadata_proto::entity;
use prost::Message;
use prost::bytes::BufMut;
use tracing_subscriber::EnvFilter;

#[allow(non_camel_case_types)]
pub type c_size_t = usize;
#[allow(non_camel_case_types)]
pub type c_ptrdiff_t = isize;

/// Return the LakeSoul Core version.
///
/// The returned pointer remains valid for the lifetime of the process and must not be freed.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_metadata_version() -> *const c_char {
    lakesoul_build_info::VERSION_NUL.as_ptr().cast()
}

/// Return the LakeSoul native build identity.
///
/// The returned pointer remains valid for the lifetime of the process and must not be freed.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_metadata_build_info() -> *const c_char {
    lakesoul_build_info::BUILD_INFO_NUL.as_ptr().cast()
}

/// The callback function with bool result and error string.
pub type ResultCallback = extern "C" fn(bool, *const c_char);

/// The callback function with i32 result and error string.
pub type IntegerResultCallBack = extern "C" fn(i32, *const c_char);

/// Call the callback function with a generic status and error string.
fn call_result_callback<T>(
    callback: extern "C" fn(T, *const c_char),
    status: T,
    err: *const c_char,
) {
    callback(status, err);
    // release error string
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

/// Convert the pointer to the string.
fn string_from_ptr(ptr: *const c_char) -> String {
    unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() }
}

fn string_from_nullable_ptr(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        unsafe { Some(CStr::from_ptr(ptr).to_str().unwrap().to_string()) }
    }
}

/// # Safety
/// check nothing
fn c_char2str<'a>(ptr: *const c_char) -> &'a str {
    unsafe {
        let c_str = CStr::from_ptr(ptr);
        c_str.to_str().unwrap()
    }
}

/// Execute the insert Data Access Object.
#[unsafe(no_mangle)]
pub extern "C" fn execute_insert(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    insert_type: i32,
    addr: c_ptrdiff_t,
    len: i32,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut()
        };

        let raw_parts =
            unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };
        let wrapper =
            entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();
        let result = runtime.block_on(async {
            lakesoul_metadata::execute_insert(client, insert_type, wrapper).await
        });
        match result {
            Ok(count) => call_result_callback(callback, count, null()),
            Err(e) => call_result_callback(callback, -1, into_c_string(e.to_string())),
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, -1, into_c_string(msg));
    }
}

/// Execute the update Data Access Object.
#[unsafe(no_mangle)]
pub extern "C" fn execute_update(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut()
        };

        let result = runtime.block_on(async {
            lakesoul_metadata::execute_update(
                client,
                update_type,
                string_from_ptr(joined_string),
            )
            .await
        });
        match result {
            Ok(count) => call_result_callback(callback, count, null()),
            Err(e) => call_result_callback(callback, -1, into_c_string(e.to_string())),
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, -1, into_c_string(msg));
    }
}

/// Execute the query scalar Data Access Object.
#[unsafe(no_mangle)]
pub extern "C" fn execute_query_scalar(
    callback: extern "C" fn(*const c_char, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    update_type: i32,
    joined_string: *const c_char,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_mut()
        };

        let result = runtime.block_on(async {
            lakesoul_metadata::execute_query_scalar(
                client,
                update_type,
                string_from_ptr(joined_string),
            )
            .await
        });
        let (result, err): (*mut c_char, *const c_char) = match result {
            Ok(Some(result)) => (into_c_string(result), null()),
            Ok(None) => (into_c_string(""), null()),
            Err(e) => (into_c_string(""), into_c_string(e.to_string())),
        };
        call_result_callback(callback, result, err);
        unsafe {
            let _ = CString::from_raw(result);
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        let err = into_c_string(msg);
        let empty = into_c_string("");
        call_result_callback(callback, empty, err);
        unsafe {
            let _ = CString::from_raw(empty);
        }
    }
}

/// Execute the query Data Access Object.
#[unsafe(no_mangle)]
pub extern "C" fn execute_query(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
    query_type: i32,
    joined_string: *const c_char,
) -> NonNull<CResult<BytesResult>> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref()
        };

        let result = runtime.block_on(async {
            lakesoul_metadata::execute_query(
                client,
                query_type,
                string_from_ptr(joined_string),
            )
            .await
        });
        match result {
            Ok(u8_vec) => {
                let len = u8_vec.len();
                call_result_callback(callback, len as i32, null());
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(u8_vec))
            }
            Err(e) => {
                call_result_callback(callback, -1, into_c_string(e.to_string()));
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
            }
        }
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            call_result_callback(callback, -1, into_c_string(msg));
            catch_unwind_cresult(|| CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

/// Export the bytes result.
#[unsafe(no_mangle)]
pub extern "C" fn export_bytes_result(
    callback: extern "C" fn(bool, *const c_char),
    bytes: NonNull<CResult<BytesResult>>,
    len: i32,
    addr: c_ptrdiff_t,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let len = len as usize;
        let bytes = unsafe {
            NonNull::new_unchecked(bytes.as_ref().ptr as *mut Vec<c_uchar>).as_mut()
        };

        if bytes.len() != len {
            call_result_callback(
                callback,
                false,
                into_c_string(
                    "Size of buffer and result mismatch at export_bytes_result.",
                ),
            );
            return;
        }
        bytes.push(0u8);
        bytes.shrink_to_fit();

        let dst = unsafe { std::slice::from_raw_parts_mut(addr as *mut u8, len + 1) };
        let mut writer = dst.writer();
        let _ = writer.write_all(bytes.as_slice());

        call_result_callback(callback, true, null());
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, false, into_c_string(msg));
    }
}

/// Free the bytes result.
#[unsafe(no_mangle)]
pub extern "C" fn free_bytes_result(bytes: NonNull<CResult<BytesResult>>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(bytes).free::<Vec<u8>>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Clean the metadata for test.
#[unsafe(no_mangle)]
pub extern "C" fn clean_meta_for_test(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<CResult<TokioRuntime>>,
    client: NonNull<CResult<TokioPostgresClient>>,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref()
        };
        let result = runtime
            .block_on(async { lakesoul_metadata::clean_meta_for_test(client).await });
        match result {
            Ok(count) => call_result_callback(callback, count, null()),
            Err(e) => call_result_callback(callback, -1, into_c_string(e.to_string())),
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, -1, into_c_string(msg));
    }
}

/// Create the tokio runtime.
#[unsafe(no_mangle)]
pub extern "C" fn create_tokio_runtime() -> NonNull<CResult<TokioRuntime>> {
    catch_unwind_cresult(|| {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .max_blocking_threads(8)
            .build()
            .unwrap();
        CResult::<TokioRuntime>::new(runtime)
    })
}

/// Free the tokio runtime.
#[unsafe(no_mangle)]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<CResult<TokioRuntime>>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(runtime).free::<Runtime>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Create the tokio postgres client.
#[unsafe(no_mangle)]
pub extern "C" fn create_tokio_postgres_client(
    callback: extern "C" fn(bool, *const c_char),
    config: *const c_char,
    secondary_config: *const c_char,
    runtime: NonNull<CResult<TokioRuntime>>,
) -> NonNull<CResult<TokioPostgresClient>> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let config = string_from_ptr(config);
        let secondary_config = string_from_nullable_ptr(secondary_config);
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };

        let result = runtime.block_on(async {
            lakesoul_metadata::create_connection(config, secondary_config).await
        });

        match result {
            Ok(client) => {
                call_result_callback(callback, true, null());
                CResult::<TokioPostgresClient>::new(client)
            }
            Err(e) => {
                call_result_callback(callback, false, into_c_string(e.to_string()));
                CResult::<TokioPostgresClient>::error(e.to_string())
            }
        }
    }));
    match result {
        Ok(r) => convert_to_nonnull(r),
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            call_result_callback(callback, false, into_c_string(msg.clone()));
            catch_unwind_cresult(|| CResult::<TokioPostgresClient>::error(msg))
        }
    }
}

/// Free the tokio postgres client.
#[unsafe(no_mangle)]
pub extern "C" fn free_tokio_postgres_client(
    client: NonNull<CResult<TokioPostgresClient>>,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(client).free::<PooledClient>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Create the lakesoul metadata client.
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_metadata_client() -> NonNull<CResult<MetaDataClient>> {
    catch_unwind_cresult(|| {
        let client = MetaDataClient::from_env();
        CResult::<MetaDataClient>::new(client)
    })
}

/// Free the lakesoul metadata client.
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_metadata_client(
    client: NonNull<CResult<MetaDataClient>>,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(client).free::<MetaDataClient>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Encode the token from `Claims`.
///
/// # Parameters
/// - `callback`: The callback function to call.
///   - When the first parameter is `""`, the second parameter is the err string.
///   - When the first parameter is not `""`, it represents the token string and the second parameter is `null`.
/// - `claims_json`: The claims json string.
/// - `secret`: The secret string.
#[unsafe(no_mangle)]
pub extern "C" fn encode_token_from_claims(
    callback: extern "C" fn(*const c_char, *const c_char),
    claims_json: *const c_char,
    secret: *const c_char,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let claims_json_str = c_char2str(claims_json);
        let secret_str = c_char2str(secret);

        let claims: Claims = match serde_json::from_str(claims_json_str) {
            Ok(c) => c,
            Err(e) => {
                let status = into_c_string("");
                let err = into_c_string(e.to_string());
                call_result_callback(callback, status, err);
                // release `status`, and the `err` has been released by the `call_result_callback`
                unsafe {
                    let _ = CString::from_raw(status);
                }
                return;
            }
        };

        let (result, err): (*mut c_char, *const c_char) =
            match JwtServer::new(secret_str).create_token(&claims) {
                Ok(token) => (into_c_string(token), null()),
                Err(e) => (into_c_string(""), into_c_string(e.to_string())),
            };

        call_result_callback(callback, result, err);

        // release `result`, and the `err` has been released by the `call_result_callback`
        unsafe {
            let _ = CString::from_raw(result);
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        let err = into_c_string(msg);
        let empty = into_c_string("");
        call_result_callback(callback, empty, err);
        unsafe {
            let _ = CString::from_raw(empty);
        }
    }
}

/// Decode the token to `Claims`.
///
/// # Parameters
/// - `callback`: The callback function to call.
///   - When the first parameter is `""`, the second parameter is the err string.
///   - When the first parameter is not `""`, it represents the claims json string and the second parameter is `null`.
/// - `token`: The token string.
/// - `secret`: The secret string.
#[unsafe(no_mangle)]
pub extern "C" fn decode_token_to_claims(
    callback: extern "C" fn(*const c_char, *const c_char),
    token: *const c_char,
    secret: *const c_char,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let token_str = c_char2str(token);
        let secret_str = c_char2str(secret);

        let (result, err): (*mut c_char, *const c_char) =
            match JwtServer::new(secret_str).decode_token(token_str) {
                Ok(claims) => (
                    into_c_string(serde_json::to_string(&claims).unwrap()),
                    null(),
                ),
                Err(e) => (into_c_string(""), into_c_string(e.to_string())),
            };

        call_result_callback(callback, result, err);

        // release `result`, and the `err` has been released by the `call_result_callback`
        unsafe {
            let _ = CString::from_raw(result);
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        let err = into_c_string(msg);
        let empty = into_c_string("");
        call_result_callback(callback, empty, err);
        unsafe {
            let _ = CString::from_raw(empty);
        }
    }
}

/// USE: JNR
/// return split(partition) desc array in json format by table_name, namespace , filter(WIP)
#[unsafe(no_mangle)]
pub extern "C" fn create_split_desc_array(
    callback: ResultCallback,
    client: NonNull<CResult<TokioPostgresClient>>,
    runtime: NonNull<CResult<TokioRuntime>>,
    table_name: *const c_char,
    namespace: *const c_char,
) -> *mut c_char {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let runtime = unsafe {
            NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()
        };
        let client = unsafe {
            NonNull::new_unchecked(client.as_ref().ptr as *mut PooledClient).as_ref()
        };
        let table_name = c_char2str(table_name);
        let namespace = c_char2str(namespace);
        let result: Result<*mut c_char, LakeSoulMetaDataError> =
            runtime.block_on(async {
                let ret = lakesoul_metadata::transfusion::split_desc_array(
                    client, table_name, namespace,
                )
                .await?;
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
                into_c_string(e.to_string()) as *const c_char,
            ),
        };
        call_result_callback(callback, status, e);
        ret
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            call_result_callback(callback, false, into_c_string(msg));
            null_mut()
        }
    }
}

/// # Safety
/// caller should keep it safe
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_split_desc_array(json: *mut c_char) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe { free_c_string(json) }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn debug(callback: extern "C" fn(bool, *const c_char)) -> *mut c_char {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
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
        let x = into_c_string("oops");
        callback(false, x);
        unsafe {
            let _s = CString::from_raw(x);
        }
        c_string.into_raw()
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            let err = into_c_string(msg);
            callback(false, err);
            unsafe {
                let _s = CString::from_raw(err);
            }
            into_c_string("{}") // return empty JSON array on panic
        }
    }
}

/// # Safety
/// c_string should be valid
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_c_string(c_string: *mut c_char) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        // only check ptr is not null
        if c_string.is_null() {
            debug!("early return due to null ptr");
            return;
        }
        debug!("free c string start");
        let _s = CString::from_raw(c_string);
        debug!("free c string finished");
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// init a global logger for rust code
/// now use RUST_LOG=LEVEL to activate
#[unsafe(no_mangle)]
pub extern "C" fn rust_logger_init() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
        match tracing_subscriber::fmt()
            .with_timer(timer)
            .with_target(false)
            .with_ansi(false)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_env_filter(EnvFilter::from_default_env())
            .try_init()
        {
            Ok(_) => {}
            Err(e) => {
                if !e
                    .to_string()
                    .contains("a global default trace dispatcher has already been set")
                {
                    let msg = format!("Failed to initialize tracing subscriber {:?}", e);
                    eprintln!("{}", msg);
                    panic!("{}", msg)
                }
            }
        }
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
        eprintln!("rust_logger_init panicked: {}", _msg);
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
