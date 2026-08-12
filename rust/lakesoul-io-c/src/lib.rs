// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
// SPDX-License-Identifier: Apache-2.0

//! The C API for the [`lakesoul-io`] crate

extern crate core;
#[macro_use]
extern crate tracing;

use std::ffi::{CStr, CString, c_char, c_int, c_uchar, c_void};
use std::io::Write;
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

use bytes::BufMut;

use arrow_array::RecordBatch;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::{Array, StructArray};
use arrow_schema::{Schema, SchemaRef};
use datafusion_substrait::substrait::proto::Plan;
// Re-export FFI types so cbindgen picks them up from this crate
pub use lakesoul_common::ffi::{CResult, CStatus};

use lakesoul_common::ffi::{
    catch_unwind_cresult, catch_unwind_cstatus, convert_to_nonnull, convert_to_opaque,
    from_nonnull, from_opaque, into_c_string, log_panic_and_extract_message,
};
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::helpers;
use lakesoul_io::helpers::transform::{
    normalize_record_batch_for_java, normalize_schema_for_java,
};
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::writer::SyncSendableMutableLakeSoulWriter;
use lakesoul_metadata_proto::entity;
use prost::Message;
use std::panic::{self, AssertUnwindSafe};

use rootcause::Report;
use tokio::runtime::{Builder, Runtime};
use tracing_subscriber::EnvFilter;

#[allow(non_camel_case_types)]
pub type c_size_t = usize;
#[allow(non_camel_case_types)]
pub type c_ptrdiff_t = isize;

/// Return the LakeSoul Core version.
///
/// The returned pointer remains valid for the lifetime of the process and must not be freed.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_io_version() -> *const c_char {
    lakesoul_build_info::VERSION_NUL.as_ptr().cast()
}

/// The opaque builder of the IO config
#[repr(C)]
pub struct IOConfigBuilder {
    private: [u8; 0],
}

/// The opaque IO config
#[repr(C)]
pub struct IOConfig {
    private: [u8; 0],
}

/// The opaque reader
#[repr(C)]
pub struct Reader {
    private: [u8; 0],
}

/// The opaque writer
#[repr(C)]
pub struct Writer {
    private: [u8; 0],
}

/// The opaque bytes result
#[repr(C)]
pub struct BytesResult {
    private: [u8; 0],
}

/// Catch a panic in a builder closure, returning null on failure.
/// Unlike returning a fresh default builder, this alerts the caller
/// that the operation failed — preventing silent config loss and leaks.
fn catch_null_on_panic<F>(f: F) -> *mut IOConfigBuilder
where
    F: FnOnce() -> NonNull<IOConfigBuilder>,
{
    panic::catch_unwind(AssertUnwindSafe(f))
        .map(|p| p.as_ptr())
        .unwrap_or_else(|payload| {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null_mut()
        })
}

/// Create a new [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub extern "C" fn new_lakesoul_io_config_builder() -> *mut IOConfigBuilder {
    catch_null_on_panic(|| convert_to_opaque(LakeSoulIOConfigBuilder::new()))
}

/// Set the prefix of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `prefix` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_with_prefix(
    builder: NonNull<IOConfigBuilder>,
    prefix: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        let prefix = unsafe { CStr::from_ptr(prefix).to_str().unwrap().to_string() };
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_prefix(prefix),
        )
    })
}

/// Add a single file to the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `file` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_file(
    builder: NonNull<IOConfigBuilder>,
    file: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        let file = unsafe { CStr::from_ptr(file).to_str().unwrap().to_string() };
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_file(file),
        )
    })
}

/// Add a single column to the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `column` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
#[allow(deprecated)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        let column = unsafe { CStr::from_ptr(column).to_str().unwrap().to_string() };
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_column(column),
        )
    })
}

/// Add a single aux sort column to the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `column` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_aux_sort_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        let column = unsafe { CStr::from_ptr(column).to_str().unwrap().to_string() };
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_aux_sort_column(column),
        )
    })
}

/// Add a filter to the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `filter` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_filter(
    builder: NonNull<IOConfigBuilder>,
    filter: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        let filter = unsafe { CStr::from_ptr(filter).to_str().unwrap().to_string() };
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_filter_str(filter),
        )
    })
}

/// Add a filter to the IO config from a protobuf
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `proto_addr` must be a valid pointer with `len` bytes available
///
/// panic on invalid protobuf
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_filter_proto(
    builder: NonNull<IOConfigBuilder>,
    proto_addr: c_ptrdiff_t,
    len: i32,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        debug!("proto_addr: {:#x}, len:{}", proto_addr, len);
        let dst: &mut [u8] =
            unsafe { slice::from_raw_parts_mut(proto_addr as *mut u8, len as usize) };
        let plan = Plan::decode(&*dst).unwrap();
        debug!("{:#?}", plan);
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_filter_proto(plan),
        )
    })
}

/// Set the schema of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `schema_addr` must be a valid pointer to an [`FFI_ArrowSchema`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_addr as *mut FFI_ArrowSchema);
        let schema = Schema::try_from(&ffi_schema).unwrap();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_schema(Arc::new(schema)),
        )
    })
}

/// Set the partition schema of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `schema_addr` must be a valid pointer to an [`FFI_ArrowSchema`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_partition_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_addr as *mut FFI_ArrowSchema);
        let schema = Schema::try_from(&ffi_schema).unwrap();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_partition_schema(Arc::new(schema)),
        )
    })
}

/// Set the thread number of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_thread_num(
    builder: NonNull<IOConfigBuilder>,
    thread_num: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_thread_num(thread_num),
        )
    })
}

/// Set whether to use dynamic partition of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_dynamic_partition(
    builder: NonNull<IOConfigBuilder>,
    enable: bool,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .set_dynamic_partition(enable),
        )
    })
}

/// Set whether to infer the schema of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_inferring_schema(
    builder: NonNull<IOConfigBuilder>,
    enable: bool,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .set_inferring_schema(enable),
        )
    })
}

/// Set the batch size of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `batch_size` must be a valid batch size.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_batch_size(
    builder: NonNull<IOConfigBuilder>,
    batch_size: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_batch_size(batch_size),
        )
    })
}

/// Set the max row group size of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_max_row_group_size(
    builder: NonNull<IOConfigBuilder>,
    max_row_group_size: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_max_row_group_size(max_row_group_size),
        )
    })
}

/// Set the max row group num values of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_max_row_group_num_values(
    builder: NonNull<IOConfigBuilder>,
    max_row_group_num_values: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_max_row_group_num_values(max_row_group_num_values),
        )
    })
}

/// Set the buffer size of the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_buffer_size(
    builder: NonNull<IOConfigBuilder>,
    buffer_size: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_prefetch_size(buffer_size),
        )
    })
}

/// Set the hash bucket number of the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_hash_bucket_num(
    builder: NonNull<IOConfigBuilder>,
    hash_bucket_num: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_hash_bucket_num(hash_bucket_num.to_string()),
        )
    })
}

/// Set the object store option of the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `key` and `value` must be valid pointers to c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_object_store_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_object_store_option(key, value),
        )
    })
}

/// Add a option to the IO config.
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `key` and `value` must be valid pointers to c-strings (null-terminated strings)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_option(key, value),
        )
    })
}

/// Add a files to the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `files` must be a valid pointer to an array of pointers to c-strings (null-terminated strings) with `file_num` elements.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_files(
    builder: NonNull<IOConfigBuilder>,
    files: *const *const c_char,
    file_num: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let files = slice::from_raw_parts(files, file_num);
        let files: Vec<_> = files
            .iter()
            .map(|p| CStr::from_ptr(*p))
            .map(|c_str| c_str.to_str().unwrap())
            .map(|str| str.to_string())
            .collect();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_files(files),
        )
    })
}

/// Add a single primary key to the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `pk` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_primary_key(
    builder: NonNull<IOConfigBuilder>,
    pk: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let pk = CStr::from_ptr(pk).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_primary_key(pk),
        )
    })
}

/// Add a single range partition to the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `col` must be a valid pointer to a c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_range_partition(
    builder: NonNull<IOConfigBuilder>,
    col: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let col = CStr::from_ptr(col).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_range_partition(col),
        )
    })
}

/// Add a merge operation to the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `field` and `merge_op` must be valid pointers to c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_merge_op(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    merge_op: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let merge_op = CStr::from_ptr(merge_op).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_merge_op(field, merge_op),
        )
    })
}

/// Add collection of primary keys to the IO config
///
/// # Safety
///
/// * `builder` must be a valid pointer to an [`IOConfigBuilder`]
/// * `pks` must be a valid pointer to an array of pointers to c-string (null-terminated string) with `pk_num` elements
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_primary_keys(
    builder: NonNull<IOConfigBuilder>,
    pks: *const *const c_char,
    pk_num: c_size_t,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let pks = slice::from_raw_parts(pks, pk_num);
        let pks: Vec<_> = pks
            .iter()
            .map(|p| CStr::from_ptr(*p))
            .map(|c_str| c_str.to_str().unwrap())
            .map(|str| str.to_string())
            .collect();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_primary_keys(pks),
        )
    })
}

/// Set the default column value of the IO config.
///
/// # Safety
///
/// * `builder` must be a valid pointer to a [`IOConfigBuilder`] struct
/// * `field` and `value` must be valid pointers to c-string (null-terminated string)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_default_column_value(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    value: *const c_char,
) -> *mut IOConfigBuilder {
    catch_null_on_panic(|| unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_default_column_value(field, value),
        )
    })
}

/// Create a new [`IOConfig`] from the [`IOConfigBuilder`]
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_io_config_from_builder(
    builder: NonNull<IOConfigBuilder>,
) -> *mut IOConfig {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).build(),
        )
    }));
    match result {
        Ok(r) => r.as_ptr(),
        Err(payload) => {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null_mut()
        }
    }
}

/// Create a new [`SyncSendableMutableLakeSoulReader`] from the [`IOConfig`]
/// and return a [`Reader`] wrapped in [`CResult`]
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_reader_from_config(
    config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Reader>> {
    catch_unwind_cresult(|| {
        let config: LakeSoulIOConfig = from_opaque(config);
        let runtime: Runtime = from_opaque(runtime);
        match LakeSoulReader::new(config) {
            Ok(reader) => CResult::<Reader>::new(SyncSendableMutableLakeSoulReader::new(
                reader, runtime,
            )),
            Err(e) => CResult::<Reader>::error(e.to_string()),
        }
    })
}

/// Create a new [`SyncSendableMutableLakeSoulReader`] from the [`IOConfig`] with global runtime
/// and return a [`Reader`] wrapped in [`CResult`]
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_reader_from_config_with_global_runtime(
    config: NonNull<IOConfig>,
) -> NonNull<CResult<Reader>> {
    catch_unwind_cresult(|| {
        let config: LakeSoulIOConfig = from_opaque(config);
        match LakeSoulReader::new(config) {
            Ok(reader) => CResult::<Reader>::new(
                SyncSendableMutableLakeSoulReader::new_with_global_runtime(reader),
            ),
            Err(e) => CResult::<Reader>::error(e.to_string()),
        }
    })
}

/// Check if the [`Reader`] is created successfully.
#[unsafe(no_mangle)]
pub extern "C" fn check_reader_created(
    reader: NonNull<CResult<Reader>>,
) -> *const c_char {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        if let Some(err) = reader.as_ref().err.as_ref() {
            err as *const c_char
        } else {
            std::ptr::null()
        }
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null()
        }
    }
}

/// The callback function with bool result and error string
pub type ResultCallback = extern "C" fn(bool, *const c_char);
/// The callback function with bool result, error string and data pointer
pub type DataResultCallback = extern "C" fn(bool, *const c_char, *const c_void);

/// Function to call the callback function with bool result and error string
fn call_result_callback(callback: ResultCallback, status: bool, err: *const c_char) {
    callback(status, err);
    // release error string
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

/// Function to call the callback function with bool result, error string and data pointer.
fn call_data_result_callback(
    callback: DataResultCallback,
    status: bool,
    err: *const c_char,
    data: Cvoid,
) {
    callback(status, err, data.data);
    if !err.is_null() {
        unsafe {
            // release error string
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

/// The callback function with i32 result and error string.
pub type I32ResultCallback = extern "C" fn(i32, *const c_char);
/// The callback function with i32 result, error string and data pointer.
pub type I32DataResultCallback = extern "C" fn(i32, *const c_char, *const c_void);

/// Function to call the callback function with i32 result and error string.
fn call_i32_result_callback(
    callback: I32ResultCallback,
    status: i32,
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

/// Function to call the callback function with i32 result, error string and data pointer.
fn call_i32_data_result_callback(
    callback: I32DataResultCallback,
    status: i32,
    err: *const c_char,
    data: Cvoid,
) {
    callback(status, err, data.data);
    // release error string
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`]
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * return a [`CStatus`] struct with the result of the operation
#[unsafe(no_mangle)]
pub unsafe extern "C" fn start_reader(
    reader: NonNull<CResult<Reader>>,
) -> NonNull<CStatus> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let mut reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        reader.as_mut().start_blocked()
    }));
    match result {
        Ok(Ok(_)) => convert_to_nonnull(CStatus::new(0)),
        Ok(Err(e)) => convert_to_nonnull(CStatus::error(e.to_string(), -1)),
        Err(panic_payload) => {
            let msg = log_panic_and_extract_message(panic_payload);
            convert_to_nonnull(CStatus::error(msg, -1))
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`] with data
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * `data` must be a valid pointer to the data to be passed to the reader
/// * `callback` must be a safe function pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn start_reader_with_data(
    reader: NonNull<CResult<Reader>>,
    data: *const c_void,
    callback: DataResultCallback,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let mut reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let data = Cvoid { data };
        let result = reader.as_mut().start_blocked();
        match result {
            Ok(_) => call_data_result_callback(callback, true, std::ptr::null(), data),
            Err(e) => call_data_result_callback(
                callback,
                false,
                into_c_string(e.to_string()),
                data,
            ),
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_data_result_callback(callback, false, into_c_string(msg), Cvoid { data });
    }
}

fn export_record_batch_for_java(
    batch: RecordBatch,
    array_addr: c_ptrdiff_t,
    schema_addr: Option<c_ptrdiff_t>,
) -> std::result::Result<i32, String> {
    let batch = normalize_record_batch_for_java(batch).map_err(|e| e.to_string())?;
    let rows = batch.num_rows() as i32;
    let batch: Arc<StructArray> = Arc::new(batch.into());
    let schema = schema_addr
        .map(|_| FFI_ArrowSchema::try_from(batch.data_type()))
        .transpose()
        .map_err(|e| e.to_string())?;

    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
    unsafe {
        (&ffi_array as *const FFI_ArrowArray)
            .copy_to(array_addr as *mut FFI_ArrowArray, 1);
    }
    std::mem::forget(ffi_array);

    if let (Some(schema_addr), Some(schema)) = (schema_addr, schema) {
        unsafe {
            (&schema as *const FFI_ArrowSchema)
                .copy_to(schema_addr as *mut FFI_ArrowSchema, 1);
        }
        std::mem::forget(schema);
    }

    Ok(rows)
}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`]
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * `schema_addr` must be a valid pointer to the schema address
/// * `array_addr` must be a valid pointer to the array address
/// * `callback` must be a safe function pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn next_record_batch(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: I32ResultCallback,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let f = move |rb: Option<Result<RecordBatch, Report>>| match rb {
            None => {
                call_i32_result_callback(callback, 0, std::ptr::null());
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_i32_result_callback(callback, -1, into_c_string(e.to_string()));
                }
                Ok(rb) => {
                    match export_record_batch_for_java(rb, array_addr, Some(schema_addr))
                    {
                        Ok(rows) => {
                            call_i32_result_callback(callback, rows, std::ptr::null());
                        }
                        Err(e) => {
                            call_i32_result_callback(callback, -1, into_c_string(e));
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_i32_result_callback(callback, -1, into_c_string(msg));
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_blocked`] of the [`Reader`]
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * `array_addr` must be a valid pointer to the array address
/// * return a [`CStatus`] struct with the result of the operation
#[unsafe(no_mangle)]
pub unsafe extern "C" fn next_record_batch_blocked(
    reader: NonNull<CResult<Reader>>,
    array_addr: c_ptrdiff_t,
) -> NonNull<CStatus> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let result = reader.as_ref().next_rb_blocked();
        match result {
            None => (0, std::ptr::null()),
            Some(rb_result) => match rb_result {
                Err(e) => (-1, into_c_string(e.to_string()) as *const c_char),
                Ok(rb) => match export_record_batch_for_java(rb, array_addr, None) {
                    Ok(rows) => (rows, std::ptr::null()),
                    Err(e) => (-1, into_c_string(e) as *const c_char),
                },
            },
        }
    }));
    let (status, err): (c_int, *const c_char) = match result {
        Ok(inner) => inner,
        Err(panic_payload) => {
            let msg = log_panic_and_extract_message(panic_payload);
            (-1, into_c_string(msg))
        }
    };
    convert_to_nonnull(CStatus { status, err })
}

// accept a callback with arbitrary user data pointer

struct Cvoid {
    data: *const c_void,
}

unsafe impl Send for Cvoid {}

unsafe impl Sync for Cvoid {}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`]
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * `schema_addr` must be a valid pointer to the schema address
/// * `array_addr` must be a valid pointer to the array address
/// * `data` must be a valid pointer
/// * `callback` must be a valid callback function
#[unsafe(no_mangle)]
pub unsafe extern "C" fn next_record_batch_with_data(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    data: *const c_void,
    callback: I32DataResultCallback,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let data = Cvoid { data };
        let f = move |rb: Option<Result<RecordBatch, Report>>| match rb {
            None => {
                call_i32_data_result_callback(callback, 0, std::ptr::null(), data);
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_i32_data_result_callback(
                        callback,
                        -1,
                        into_c_string(e.to_string()),
                        data,
                    );
                }
                Ok(rb) => {
                    match export_record_batch_for_java(rb, array_addr, Some(schema_addr))
                    {
                        Ok(rows) => {
                            call_i32_data_result_callback(
                                callback,
                                rows,
                                std::ptr::null(),
                                data,
                            );
                        }
                        Err(e) => {
                            call_i32_data_result_callback(
                                callback,
                                -1,
                                into_c_string(e),
                                data,
                            );
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_i32_data_result_callback(callback, -1, into_c_string(msg), Cvoid { data });
    }
}

/// Export the schema of the [`Reader`].
///
/// # Safety
///
/// * `reader` must be a valid pointer to a [`CResult<Reader>`] struct
/// * `schema_addr` must be a valid pointer to the schema address
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_reader_get_schema(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let schema = reader
            .as_ref()
            .get_schema()
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let schema = normalize_schema_for_java(schema.as_ref());
        let schema_addr = schema_addr as *mut FFI_ArrowSchema;
        let _ = FFI_ArrowSchema::try_from(schema.as_ref()).map(|s| {
            std::ptr::write_unaligned(schema_addr, s);
        });
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Free the [`Reader`].
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_reader(reader: NonNull<CResult<Reader>>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(reader).free::<SyncSendableMutableLakeSoulReader>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Free the [`Writer`].
///
/// for writer this is called when writer is failed to create
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_writer(writer: NonNull<CResult<Writer>>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(writer).free::<SyncSendableMutableLakeSoulWriter>();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Free the [`IOConfigBuiler`].
///
/// for writer this is called when writer is failed to create
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_io_config_builder(builder: NonNull<IOConfigBuilder>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder);
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Free the [`IOConfig`].
///
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_io_config(io_config: NonNull<IOConfig>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = from_opaque::<IOConfig, LakeSoulIOConfig>(io_config);
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Free the [`TokioRuntimeBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn free_tokio_runtime_builder(builder: NonNull<TokioRuntimeBuilder>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// runtime is usually moved to create reader/writer,
/// so you don't need to free it unless it's used independently
///
/// # Safety
///
/// * `runtime` must be a valid pointer to a [`CResult<TokioRuntime>`] struct
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_tokio_runtime(runtime: NonNull<TokioRuntime>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = from_opaque::<TokioRuntime, Runtime>(runtime);
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// free the [`CStatus`].
///
/// # Safety
///
/// * `status` must be a valid pointer to a [`CStatus`] struct
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_c_status(status: NonNull<CStatus>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(status).free();
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
    }
}

/// Create a new [`SyncSendableMutableLakeSoulWriter`] from the [`IOConfig`] and return a [`Writer`] wrapped in [`CResult`].
///
/// # Safety
///
/// * `io_config` must be a valid pointer to an [`IOConfig`] struct
/// * `runtime` must be a valid pointer to a [`TokioRuntime`] struct
#[unsafe(no_mangle)]
pub unsafe extern "C" fn create_lakesoul_writer_from_config(
    io_config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Writer>> {
    catch_unwind_cresult(|| {
        let io_config: LakeSoulIOConfig = from_opaque(io_config);
        let runtime: Runtime = from_opaque(runtime);
        match SyncSendableMutableLakeSoulWriter::from_io_config(io_config, runtime) {
            Ok(writer) => CResult::<Writer>::new(writer),
            Err(e) => CResult::<Writer>::error(e.to_string()),
        }
    })
}

/// Check if the [`Writer`] was created successfully.
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
#[unsafe(no_mangle)]
pub unsafe extern "C" fn check_writer_created(
    mut writer: NonNull<CResult<Writer>>,
) -> NonNull<CStatus> {
    catch_unwind_cstatus(|| unsafe {
        if writer.as_ref().err.is_null() {
            CStatus::new(0)
        } else {
            // take ownership of the error string
            let s = CString::from_raw(writer.as_ref().err.cast_mut());
            writer.as_mut().err = std::ptr::null();
            CStatus::error(s, -1)
        }
    })
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] with callback.
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
/// * `schema_addr` must be a valid pointer to the schema address
/// * `array_addr` must be a valid pointer to the array address
/// * `callback` must be a valid pointer to a [`ResultCallback`] function
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_record_batch(
    writer: NonNull<CResult<Writer>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: ResultCallback,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let writer = NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut();
        let ffi_array = FFI_ArrowArray::from_raw(array_addr as *mut FFI_ArrowArray);
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_addr as *mut FFI_ArrowSchema);
        let result_fn = move || {
            let mut array_data = from_ffi(ffi_array, &ffi_schema)?;
            array_data.align_buffers();
            #[cfg(debug_assertions)]
            {
                array_data.validate_full()?;
            }
            let struct_array = StructArray::from(array_data);
            let rb = RecordBatch::from(struct_array);
            writer.write_batch(rb)?;
            Ok(())
        };
        let result: lakesoul_io::Result<()> = result_fn();
        match result {
            Ok(_) => call_result_callback(callback, true, std::ptr::null()),
            Err(e) => {
                call_result_callback(callback, false, into_c_string(format!("{}", e)))
            }
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, false, into_c_string(msg));
    }
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode.
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
/// * `schema_addr` must be a valid pointer to the schema address
/// * `array_addr` must be a valid pointer to the array address
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_record_batch_blocked(
    writer: NonNull<CResult<Writer>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
) -> NonNull<CStatus> {
    catch_unwind_cstatus(|| unsafe {
        let writer = NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut();

        let ffi_array = FFI_ArrowArray::from_raw(array_addr as *mut FFI_ArrowArray);
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_addr as *mut FFI_ArrowSchema);

        let result_fn = move || {
            let mut array_data = from_ffi(ffi_array, &ffi_schema)?;
            array_data.align_buffers();
            #[cfg(debug_assertions)]
            {
                array_data.validate_full()?;
            }
            let struct_array = StructArray::from(array_data);
            let rb = RecordBatch::from(struct_array);
            writer.write_batch(rb)?;
            Ok(())
        };
        let result: lakesoul_io::Result<()> = result_fn();
        match result {
            Ok(_) => CStatus::new(0),
            Err(e) => CStatus::error(e.to_string(), -1),
        }
    })
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode,
/// record batch is read from ipc protocol.
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
/// * `ipc_addr` must be a valid pointer to the ipc address
/// * `len` must be the length of the ipc data
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_record_batch_ipc_blocked(
    writer: NonNull<CResult<Writer>>,
    ipc_addr: c_ptrdiff_t,
    len: i64,
) -> NonNull<CStatus> {
    catch_unwind_cstatus(|| unsafe {
        let writer = NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut();

        let raw_parts = std::slice::from_raw_parts(ipc_addr as *const u8, len as usize);

        let reader = std::io::Cursor::new(raw_parts);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(reader, None).unwrap();
        let mut row_count = 0;
        loop {
            if reader.is_finished() {
                break;
            }
            match reader.next().transpose() {
                Ok(Some(batch)) => {
                    let num_rows = batch.num_rows();
                    match writer.write_batch(batch) {
                        Ok(_) => row_count += num_rows,
                        Err(e) => {
                            return CStatus::error(e.to_string(), -1);
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    return CStatus::error(e.to_string(), -1);
                }
            }
        }
        CStatus::new(row_count as c_int)
    })
}

/// Export the byte result to ffi side
///
/// # Safety
///
/// * `callback` must be a valid function pointer
/// * `bytes` must be a valid pointer to a [`CResult<BytesResult>`] struct
/// * `len` must be the length of the byte result
/// * `addr` must be a valid pointer to the byte result
#[unsafe(no_mangle)]
pub unsafe extern "C" fn export_bytes_result(
    callback: extern "C" fn(bool, *const c_char),
    bytes: NonNull<CResult<BytesResult>>,
    len: i32,
    addr: c_ptrdiff_t,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let len = len as usize;

        let mut c_result = from_nonnull(bytes);
        let inner_ptr = c_result.ptr;

        let mut bytes =
            from_opaque::<BytesResult, Vec<c_uchar>>(NonNull::new_unchecked(inner_ptr));
        c_result.ptr = std::ptr::null_mut::<BytesResult>();
        c_result.free::<BytesResult>();

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

        let dst = std::slice::from_raw_parts_mut(addr as *mut u8, len + 1);
        let mut writer = dst.writer();
        let _ = writer.write_all(bytes.as_slice());

        call_result_callback(callback, true, std::ptr::null());
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, false, into_c_string(msg));
    }
}

/// Flush and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`].
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
/// * `callback` must be a valid function pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn flush_and_close_writer(
    writer: NonNull<CResult<Writer>>,
    callback: I32ResultCallback,
) -> NonNull<CResult<BytesResult>> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let mut c_result = from_nonnull(writer);
        let inner_ptr = c_result.ptr;
        let writer = from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(
            NonNull::new_unchecked(inner_ptr),
        );
        let result = writer.flush_and_close();
        c_result.ptr = std::ptr::null_mut::<Writer>();
        c_result.free::<Writer>();
        match result {
            Ok(bytes) => {
                call_i32_result_callback(callback, bytes.len() as i32, std::ptr::null());
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(bytes))
            }
            Err(e) => {
                call_i32_result_callback(callback, -1, into_c_string(e.to_string()));
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
            }
        }
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            call_i32_result_callback(callback, -1, into_c_string(msg));
            catch_unwind_cresult(|| CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

/// Abort and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`],
/// when encountering an external error.
///
/// # Safety
///
/// * `writer` must be a valid pointer to a [`CResult<Writer>`] struct
/// * `callback` must be a valid function pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn abort_and_close_writer(
    writer: NonNull<CResult<Writer>>,
    callback: ResultCallback,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let mut c_result = from_nonnull(writer);
        let inner_ptr = c_result.ptr;
        let writer = from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(
            NonNull::new_unchecked(inner_ptr),
        );
        let result = writer.abort_and_close();
        c_result.ptr = std::ptr::null_mut::<Writer>();
        c_result.free::<Writer>();
        match result {
            Ok(_) => call_result_callback(callback, true, std::ptr::null()),
            Err(e) => call_result_callback(callback, false, into_c_string(e.to_string())),
        }
    }));
    if let Err(payload) = result {
        let msg = log_panic_and_extract_message(payload);
        call_result_callback(callback, false, into_c_string(msg));
    }
}

/// The opaque type for the [`TokioRuntimeBuilder`].
#[repr(C)]
pub struct TokioRuntimeBuilder {
    private: [u8; 0],
}

/// The opaque type for the [`TokioRuntime`].
#[repr(C)]
pub struct TokioRuntime {
    private: [u8; 0],
}

/// Create a new [`TokioRuntimeBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn new_tokio_runtime_builder() -> *mut TokioRuntimeBuilder {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        builder.worker_threads(2);
        builder.max_blocking_threads(8);
        convert_to_opaque(builder)
    }));
    match result {
        Ok(r) => r.as_ptr(),
        Err(payload) => {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null_mut()
        }
    }
}

/// Set the number of threads of the [`TokioRuntimeBuilder`].
///
/// # Safety
///
/// * `builder` must be a valid pointer to a [`TokioRuntimeBuilder`] struct
/// * `thread_num` must be a valid number of threads
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tokio_runtime_builder_set_thread_num(
    builder: NonNull<TokioRuntimeBuilder>,
    thread_num: c_size_t,
) -> *mut TokioRuntimeBuilder {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
        builder.worker_threads(thread_num);
        convert_to_opaque(builder)
    }));
    match result {
        Ok(r) => r.as_ptr(),
        Err(payload) => {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null_mut()
        }
    }
}

/// Create a new [`TokioRuntime`] from the [`TokioRuntimeBuilder`].
///
/// # Safety
///
/// * `builder` must be a valid pointer to a [`TokioRuntimeBuilder`] struct
#[unsafe(no_mangle)]
pub unsafe extern "C" fn create_tokio_runtime_from_builder(
    builder: NonNull<TokioRuntimeBuilder>,
) -> *mut TokioRuntime {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
        let runtime = builder.build().unwrap();
        convert_to_opaque(runtime)
    }));
    match result {
        Ok(r) => r.as_ptr(),
        Err(payload) => {
            let _msg = log_panic_and_extract_message(payload);
            std::ptr::null_mut()
        }
    }
}

/// Apply the partition filter to the [`entity::JniWrapper`] and return the [`BytesResult`] wrapped in [`CResult`].
///
/// # Safety
///
/// * `callback` must be a valid function pointer
/// * `len` must be a valid length of the [`entity::JniWrapper`] bytes
/// * `jni_wrapper_addr` must be a valid pointer to the [`entity::JniWrapper`] bytes
/// * `schema_addr` must be a valid pointer to an [`FFI_ArrowSchema`] struct
/// * `filter_len` must be a valid length of the filter bytes
/// * `filter_addr` must be a valid pointer to the filter bytes
#[unsafe(no_mangle)]
pub unsafe extern "C" fn apply_partition_filter(
    callback: extern "C" fn(i32, *const c_char),
    len: i32,
    jni_wrapper_addr: c_ptrdiff_t,
    schema_addr: c_ptrdiff_t,
    filter_len: i32,
    filter_addr: c_ptrdiff_t,
) -> NonNull<CResult<BytesResult>> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        let raw_parts =
            std::slice::from_raw_parts(jni_wrapper_addr as *const u8, len as usize);
        let wrapper =
            entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();

        let dst = slice::from_raw_parts(filter_addr as *const u8, filter_len as usize);
        let filter = Plan::decode(dst).unwrap();

        let ffi_schema = FFI_ArrowSchema::from_raw(schema_addr as *mut FFI_ArrowSchema);
        let schema = SchemaRef::from(Schema::try_from(&ffi_schema).unwrap());

        let filtered_partition = helpers::apply_partition_filter(wrapper, schema, filter);

        match filtered_partition {
            Ok(wrapper) => {
                let u8_vec = wrapper.encode_to_vec();
                let len = u8_vec.len();
                call_i32_result_callback(callback, len as i32, std::ptr::null());
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(u8_vec))
            }
            Err(e) => {
                call_i32_result_callback(callback, -1, into_c_string(e.to_string()));
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
            }
        }
    }));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            call_i32_result_callback(callback, -1, into_c_string(msg));
            catch_unwind_cresult(|| CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

/// Free the [`BytesResult`].
#[unsafe(no_mangle)]
pub extern "C" fn free_bytes_result(bytes: NonNull<CResult<BytesResult>>) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        from_nonnull(bytes).free::<Vec<u8>>();
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
            .with_thread_names(true)
            .with_ansi(false)
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
                    let msg = format!("Failed to initialize tracing subscriber {}", e);
                    eprintln!("{}", msg);
                    // do nothing
                }
            }
        }
    }));
    if let Err(payload) = result {
        let _msg = log_panic_and_extract_message(payload);
        eprintln!("rust_logger_init panicked: {}", _msg);
    }
}
