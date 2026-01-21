// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
// SPDX-License-Identifier: Apache-2.0

//! The C API for the [`lakesoul-io`] crate.

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
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::helpers;
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::writer::SyncSendableMutableLakeSoulWriter;
use prost::Message;
use proto::proto::entity;
use rootcause::Report;
use tokio::runtime::{Builder, Runtime};
use tracing_subscriber::EnvFilter;

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
            err: std::ptr::null(),
        }
    }

    pub fn error<T: Into<Vec<u8>>>(err_msg: T) -> Self {
        CResult {
            ptr: std::ptr::null_mut(),
            err: CString::new(err_msg).unwrap().into_raw(),
        }
    }

    pub fn free<T>(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                drop(from_opaque::<OpaqueT, T>(NonNull::new_unchecked(self.ptr)));
            }
            if !self.err.is_null() {
                drop(CString::from_raw(self.err as *mut c_char));
            }
        }
    }
}

/// Convert the object to a raw opaque pointer.
fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

/// Convert the object to a [`NonNull`] opaque pointer.
fn convert_to_opaque<F, T>(obj: F) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut T) }
}

/// Convert the [`NonNull`] opaque pointer to the object.
fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

/// Convert the object to a [`NonNull`] opaque pointer.
fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

/// Convert the [`NonNull`] opaque pointer to the object.
fn from_nonnull<T>(obj: NonNull<T>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr()) }
}

// C interface for lakesoul native io

// opaque types to pass as raw pointers

/// The opaque builder of the IO config.
#[repr(C)]
pub struct IOConfigBuilder {
    private: [u8; 0],
}

/// The opaque IO config.
#[repr(C)]
pub struct IOConfig {
    private: [u8; 0],
}

/// The opaque reader.
#[repr(C)]
pub struct Reader {
    private: [u8; 0],
}

/// The opaque writer.
#[repr(C)]
pub struct Writer {
    private: [u8; 0],
}

/// The opaque bytes result.
#[repr(C)]
pub struct BytesResult {
    private: [u8; 0],
}

/// Create a new [`IOConfigBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn new_lakesoul_io_config_builder() -> NonNull<IOConfigBuilder> {
    convert_to_opaque(LakeSoulIOConfigBuilder::new())
}

/// Set the prefix of the IO config.
/// # Safety
///
/// `prefix` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_with_prefix(
    builder: NonNull<IOConfigBuilder>,
    prefix: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let prefix = CStr::from_ptr(prefix).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_prefix(prefix),
        )
    }
}

/// Add a single file to the IO config.
/// # Safety
///
/// `file` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_file(
    builder: NonNull<IOConfigBuilder>,
    file: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let file = CStr::from_ptr(file).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_file(file),
        )
    }
}

/// Add a single column to the IO config.
/// # Safety
///
/// `column` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
#[allow(deprecated)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_column(column),
        )
    }
}

/// Add a single aux sort column to the IO config.
/// # Safety
///
/// `column` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_aux_sort_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_aux_sort_column(column),
        )
    }
}

/// Add a filter to the IO config.
/// # Safety
///
/// `filter` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_filter(
    builder: NonNull<IOConfigBuilder>,
    filter: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let filter = CStr::from_ptr(filter).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_filter_str(filter),
        )
    }
}

/// Add a filter to the IO config from a protobuf.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_add_filter_proto(
    builder: NonNull<IOConfigBuilder>,
    proto_addr: c_ptrdiff_t,
    len: i32,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        debug!("proto_addr: {:#x}, len:{}", proto_addr, len);
        let dst: &mut [u8] =
            slice::from_raw_parts_mut(proto_addr as *mut u8, len as usize);
        let plan = Plan::decode(&*dst).unwrap();
        debug!("{:#?}", plan);
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_filter_proto(plan),
        )
    }
}

/// Set the schema of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let ffi_schema = schema_addr as *mut FFI_ArrowSchema;
        let schema_data = std::ptr::replace(ffi_schema, FFI_ArrowSchema::empty());
        let schema = Schema::try_from(&schema_data).unwrap();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_schema(Arc::new(schema)),
        )
    }
}

/// Set the partition schema of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_partition_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let ffi_schema = schema_addr as *mut FFI_ArrowSchema;
        let schema_data = std::ptr::replace(ffi_schema, FFI_ArrowSchema::empty());
        let schema = Schema::try_from(&schema_data).unwrap();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_partition_schema(Arc::new(schema)),
        )
    }
}

/// Set the thread number of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_thread_num(
    builder: NonNull<IOConfigBuilder>,
    thread_num: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_thread_num(thread_num),
    )
}

/// Set whether to use dynamic partition of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_dynamic_partition(
    builder: NonNull<IOConfigBuilder>,
    enable: bool,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .set_dynamic_partition(enable),
    )
}

/// Set whether to infer the schema of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_inferring_schema(
    builder: NonNull<IOConfigBuilder>,
    enable: bool,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .set_inferring_schema(enable),
    )
}

/// Set the batch size of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_batch_size(
    builder: NonNull<IOConfigBuilder>,
    batch_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_batch_size(batch_size),
    )
}

/// Set the max row group size of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_max_row_group_size(
    builder: NonNull<IOConfigBuilder>,
    max_row_group_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_max_row_group_size(max_row_group_size),
    )
}

/// Set the max row group num values of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_max_row_group_num_values(
    builder: NonNull<IOConfigBuilder>,
    max_row_group_num_values: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_max_row_group_num_values(max_row_group_num_values),
    )
}

/// Set the buffer size of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_buffer_size(
    builder: NonNull<IOConfigBuilder>,
    buffer_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_prefetch_size(buffer_size),
    )
}

/// Set the hash bucket number of the IO config.
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_config_builder_set_hash_bucket_num(
    builder: NonNull<IOConfigBuilder>,
    hash_bucket_num: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
            .with_hash_bucket_num(hash_bucket_num.to_string()),
    )
}

/// Set the object store option of the IO config.
/// # Safety
///
/// `key` and `value` must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_object_store_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_object_store_option(key, value),
        )
    }
}

/// Add a option to the IO config.
/// # Safety
///
/// `key` and `value` must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_option(key, value),
        )
    }
}

/// Add a files to the IO config.
/// # Safety
///
/// `files` must be a valid pointer to an array of pointers to null-terminated strings.
/// Elements must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_files(
    builder: NonNull<IOConfigBuilder>,
    files: *const *const c_char,
    file_num: c_size_t,
) -> NonNull<IOConfigBuilder> {
    unsafe {
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
    }
}

/// Add a single primary key to the IO config.
/// # Safety
///
/// `pk` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_primary_key(
    builder: NonNull<IOConfigBuilder>,
    pk: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let pk = CStr::from_ptr(pk).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_primary_key(pk),
        )
    }
}

/// Add a single range partition to the IO config.
/// # Safety
///
/// `col` must be a valid pointer to a null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_single_range_partition(
    builder: NonNull<IOConfigBuilder>,
    col: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let col = CStr::from_ptr(col).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_range_partition(col),
        )
    }
}

/// Add a merge operation to the IO config.
/// # Safety
///
/// `field` and `merge_op` must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_merge_op(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    merge_op: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let merge_op = CStr::from_ptr(merge_op).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_merge_op(field, merge_op),
        )
    }
}

/// Add collection of primary keys to the IO config.
/// # Safety
///
/// `pks` must be a valid pointer to an array of pointers to null-terminated strings.
/// Elements must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_add_primary_keys(
    builder: NonNull<IOConfigBuilder>,
    pks: *const *const c_char,
    pk_num: c_size_t,
) -> NonNull<IOConfigBuilder> {
    unsafe {
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
    }
}

/// Set the default column value of the IO config.
/// # Safety
///
/// `field` and `value` must be valid pointers to null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lakesoul_config_builder_set_default_column_value(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder)
                .with_default_column_value(field, value),
        )
    }
}

// C interface for reader

/// Create a new [`IOConfig`] from the [`IOConfigBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_io_config_from_builder(
    builder: NonNull<IOConfigBuilder>,
) -> NonNull<IOConfig> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).build(),
    )
}

/// Create a new [`SyncSendableMutableLakeSoulReader`] from the [`IOConfig`] and return a [`Reader`] wrapped in [`CResult`].
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_reader_from_config(
    config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Reader>> {
    let config: LakeSoulIOConfig = from_opaque(config);
    let runtime: Runtime = from_opaque(runtime);
    let result = match LakeSoulReader::new(config) {
        Ok(reader) => CResult::<Reader>::new(SyncSendableMutableLakeSoulReader::new(
            reader, runtime,
        )),
        Err(e) => CResult::<Reader>::error(e.to_string()),
    };
    convert_to_nonnull(result)
}

/// Check if the [`Reader`] is created successfully.
#[unsafe(no_mangle)]
pub extern "C" fn check_reader_created(
    reader: NonNull<CResult<Reader>>,
) -> *const c_char {
    unsafe {
        if let Some(err) = reader.as_ref().err.as_ref() {
            err as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

/// The callback function with bool result and error string.
pub type ResultCallback = extern "C" fn(bool, *const c_char);
/// The callback function with bool result, error string and data pointer.
pub type DataResultCallback = extern "C" fn(bool, *const c_char, *const c_void);

/// Function to call the callback function with bool result and error string.
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
    // release error string
    callback(status, err, data.data);
    if !err.is_null() {
        unsafe {
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

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`].
#[unsafe(no_mangle)]
pub extern "C" fn start_reader(
    reader: NonNull<CResult<Reader>>,
    callback: ResultCallback,
) {
    unsafe {
        let mut reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let result = reader.as_mut().start_blocked();
        match result {
            Ok(_) => call_result_callback(callback, true, std::ptr::null()),
            Err(e) => call_result_callback(
                callback,
                false,
                CString::new(e.to_string()).unwrap().into_raw(),
            ),
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::start_blocked`] of the [`Reader`] with data.
#[unsafe(no_mangle)]
pub extern "C" fn start_reader_with_data(
    reader: NonNull<CResult<Reader>>,
    data: *const c_void,
    callback: DataResultCallback,
) {
    unsafe {
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
                CString::new(e.to_string()).unwrap().into_raw(),
                data,
            ),
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`].
#[unsafe(no_mangle)]
pub extern "C" fn next_record_batch(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: I32ResultCallback,
) {
    unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let f = move |rb: Option<Result<RecordBatch, Report>>| match rb {
            None => {
                call_i32_result_callback(callback, 0, std::ptr::null());
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_i32_result_callback(
                        callback,
                        -1,
                        CString::new(e.to_string()).unwrap().into_raw(),
                    );
                }
                Ok(rb) => {
                    let rows = rb.num_rows() as i32;
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
                    (&ffi_array as *const FFI_ArrowArray)
                        .copy_to(array_addr as *mut FFI_ArrowArray, 1);
                    std::mem::forget(ffi_array);
                    let schema_result = FFI_ArrowSchema::try_from(batch.data_type());
                    match schema_result {
                        Ok(schema) => {
                            (&schema as *const FFI_ArrowSchema)
                                .copy_to(schema_addr as *mut FFI_ArrowSchema, 1);
                            std::mem::forget(schema);
                            call_i32_result_callback(callback, rows, std::ptr::null());
                        }
                        Err(e) => {
                            call_i32_result_callback(
                                callback,
                                -1,
                                CString::new(e.to_string()).unwrap().into_raw(),
                            );
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }
}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_blocked`] of the [`Reader`].
/// # Safety
///
/// `count` must be a valid pointer to an integer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn next_record_batch_blocked(
    reader: NonNull<CResult<Reader>>,
    array_addr: c_ptrdiff_t,
    count: *mut c_int,
) -> *const c_char {
    unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let result = reader.as_ref().next_rb_blocked();
        match result {
            None => {
                *count = 0;
                std::ptr::null()
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    *count = -1;
                    CString::new(e.to_string()).unwrap().into_raw()
                }
                Ok(rb) => {
                    let rows = rb.num_rows() as i32;
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
                    (&ffi_array as *const FFI_ArrowArray)
                        .copy_to(array_addr as *mut FFI_ArrowArray, 1);
                    std::mem::forget(ffi_array);
                    *count = rows;
                    std::ptr::null()
                }
            },
        }
    }
}

// accept a callback with arbitrary user data pointer

struct Cvoid {
    data: *const c_void,
}

unsafe impl Send for Cvoid {}

unsafe impl Sync for Cvoid {}

/// Call [`SyncSendableMutableLakeSoulReader::next_rb_callback`] of the [`Reader`].
/// # Safety
///
/// `data` must be a valid pointer
#[unsafe(no_mangle)]
pub extern "C" fn next_record_batch_with_data(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    data: *const c_void,
    callback: I32DataResultCallback,
) {
    unsafe {
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
                        CString::new(e.to_string()).unwrap().into_raw(),
                        data,
                    );
                }
                Ok(rb) => {
                    let rows = rb.num_rows() as i32;
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
                    (&ffi_array as *const FFI_ArrowArray)
                        .copy_to(array_addr as *mut FFI_ArrowArray, 1);
                    std::mem::forget(ffi_array);
                    let schema_result = FFI_ArrowSchema::try_from(batch.data_type());
                    match schema_result {
                        Ok(schema) => {
                            (&schema as *const FFI_ArrowSchema)
                                .copy_to(schema_addr as *mut FFI_ArrowSchema, 1);
                            std::mem::forget(schema);
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
                                CString::new(e.to_string()).unwrap().into_raw(),
                                data,
                            );
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }
}

/// Export the schema of the [`Reader`].
#[unsafe(no_mangle)]
pub extern "C" fn lakesoul_reader_get_schema(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
) {
    unsafe {
        let reader = NonNull::new_unchecked(
            reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader,
        );
        let schema = reader
            .as_ref()
            .get_schema()
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let schema_addr = schema_addr as *mut FFI_ArrowSchema;
        let _ = FFI_ArrowSchema::try_from(schema.as_ref()).map(|s| {
            std::ptr::write_unaligned(schema_addr, s);
        });
    }
}

/// Free the [`Reader`].
#[unsafe(no_mangle)]
pub extern "C" fn free_lakesoul_reader(reader: NonNull<CResult<Reader>>) {
    from_nonnull(reader).free::<SyncSendableMutableLakeSoulReader>();
}

// C interface for writer

/// Create a new [`SyncSendableMutableLakeSoulWriter`] from the [`IOConfig`] and return a [`Writer`] wrapped in [`CResult`].
#[unsafe(no_mangle)]
pub extern "C" fn create_lakesoul_writer_from_config(
    io_config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Writer>> {
    let io_config: LakeSoulIOConfig = from_opaque(io_config);
    let runtime: Runtime = from_opaque(runtime);
    let result =
        match SyncSendableMutableLakeSoulWriter::from_io_config(io_config, runtime) {
            Ok(writer) => CResult::<Writer>::new(writer),
            Err(e) => CResult::<Writer>::error(format!("{}", e).as_str()),
        };
    convert_to_nonnull(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn check_writer_created(
    writer: NonNull<CResult<Reader>>,
) -> *const c_char {
    unsafe {
        if let Some(err) = writer.as_ref().err.as_ref() {
            err as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] with callback.
#[unsafe(no_mangle)]
pub extern "C" fn write_record_batch(
    writer: NonNull<CResult<Writer>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: ResultCallback,
) {
    unsafe {
        let writer = NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut();
        let mut ffi_array = FFI_ArrowArray::empty();
        (array_addr as *mut FFI_ArrowArray)
            .copy_to(&mut ffi_array as *mut FFI_ArrowArray, 1);
        let mut ffi_schema = FFI_ArrowSchema::empty();
        (schema_addr as *mut FFI_ArrowSchema)
            .copy_to(&mut ffi_schema as *mut FFI_ArrowSchema, 1);
        let result_fn = move || {
            let mut array_data = from_ffi(ffi_array, &ffi_schema)?;
            array_data.align_buffers();
            let struct_array = StructArray::from(array_data);
            let rb = RecordBatch::from(struct_array);
            writer.write_batch(rb)?;
            Ok(())
        };
        let result: lakesoul_io::Result<()> = result_fn();
        match result {
            Ok(_) => call_result_callback(callback, true, std::ptr::null()),
            Err(e) => call_result_callback(
                callback,
                false,
                CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
            ),
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode.
#[unsafe(no_mangle)]
pub extern "C" fn write_record_batch_blocked(
    writer: NonNull<CResult<Writer>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
) -> *const c_char {
    unsafe {
        let writer = NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut();
        let mut ffi_array = FFI_ArrowArray::empty();
        (array_addr as *mut FFI_ArrowArray)
            .copy_to(&mut ffi_array as *mut FFI_ArrowArray, 1);
        let mut ffi_schema = FFI_ArrowSchema::empty();
        (schema_addr as *mut FFI_ArrowSchema)
            .copy_to(&mut ffi_schema as *mut FFI_ArrowSchema, 1);
        let result_fn = move || {
            let mut array_data = from_ffi(ffi_array, &ffi_schema)?;
            array_data.align_buffers();
            let struct_array = StructArray::from(array_data);
            let rb = RecordBatch::from(struct_array);
            writer.write_batch(rb)?;
            Ok(())
        };
        let result: lakesoul_io::Result<()> = result_fn();
        match result {
            Ok(_) => std::ptr::null(),
            Err(e) => CString::new(e.to_string()).unwrap().into_raw(),
        }
    }
}

/// Call [`SyncSendableMutableLakeSoulWriter::write_batch`] of the [`Writer`] by blocking mode, record batch is read from ipc protocol.
#[unsafe(no_mangle)]
pub extern "C" fn write_record_batch_ipc_blocked(
    writer: NonNull<CResult<Writer>>,
    ipc_addr: c_ptrdiff_t,
    len: i64,
) -> *const c_char {
    let writer = unsafe {
        NonNull::new_unchecked(
            writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter,
        )
        .as_mut()
    };
    let raw_parts =
        unsafe { std::slice::from_raw_parts(ipc_addr as *const u8, len as usize) };

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
                        return CString::new(e.to_string()).unwrap().into_raw();
                    }
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                return CString::new(e.to_string()).unwrap().into_raw();
            }
        }
    }
    CString::new(format!("Number of rows: {}", row_count))
        .unwrap()
        .into_raw()
}

/// Export the byte result of the [`Writer`].
#[unsafe(no_mangle)]
pub extern "C" fn export_bytes_result(
    callback: extern "C" fn(bool, *const c_char),
    bytes: NonNull<CResult<BytesResult>>,
    len: i32,
    addr: c_ptrdiff_t,
) {
    let len = len as usize;
    let bytes = unsafe {
        NonNull::new_unchecked(bytes.as_ref().ptr as *mut Vec<c_uchar>).as_mut()
    };

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

/// Flush and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`].
#[unsafe(no_mangle)]
pub extern "C" fn flush_and_close_writer(
    writer: NonNull<CResult<Writer>>,
    callback: I32ResultCallback,
) -> NonNull<CResult<BytesResult>> {
    unsafe {
        let writer = from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(
            NonNull::new_unchecked(writer.as_ref().ptr),
        );
        let result = writer.flush_and_close();
        match result {
            Ok(bytes) => {
                call_i32_result_callback(callback, bytes.len() as i32, std::ptr::null());
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(bytes))
            }
            Err(e) => {
                call_i32_result_callback(
                    callback,
                    -1,
                    CString::new(e.to_string()).unwrap().into_raw(),
                );
                convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
            }
        }
    }
}

/// Abort and close the [`Writer`] and return the [`BytesResult`] wrapped in [`CResult`], when encountering an external error.
#[unsafe(no_mangle)]
pub extern "C" fn abort_and_close_writer(
    writer: NonNull<CResult<Writer>>,
    callback: ResultCallback,
) {
    unsafe {
        let writer = from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(
            NonNull::new_unchecked(writer.as_ref().ptr),
        );
        let result = writer.abort_and_close();
        match result {
            Ok(_) => call_result_callback(callback, true, std::ptr::null()),
            Err(e) => call_result_callback(
                callback,
                false,
                CString::new(e.to_string()).unwrap().into_raw(),
            ),
        }
    }
}

// C interface for tokio::runtime

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
pub extern "C" fn new_tokio_runtime_builder() -> NonNull<TokioRuntimeBuilder> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    builder.worker_threads(2);
    builder.max_blocking_threads(8);
    convert_to_opaque(builder)
}

/// Set the number of threads of the [`TokioRuntimeBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn tokio_runtime_builder_set_thread_num(
    builder: NonNull<TokioRuntimeBuilder>,
    thread_num: c_size_t,
) -> NonNull<TokioRuntimeBuilder> {
    let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    builder.worker_threads(thread_num);
    convert_to_opaque(builder)
}

/// Create a new [`TokioRuntime`] from the [`TokioRuntimeBuilder`].
#[unsafe(no_mangle)]
pub extern "C" fn create_tokio_runtime_from_builder(
    builder: NonNull<TokioRuntimeBuilder>,
) -> NonNull<TokioRuntime> {
    let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    let runtime = builder.build().unwrap();
    convert_to_opaque(runtime)
}

// runtime is usually moved to create reader/writer,
// so you don't need to free it unless it's used independently
#[unsafe(no_mangle)]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<CResult<TokioRuntime>>) {
    from_nonnull(runtime).free::<Runtime>();
}

/// Apply the partition filter to the [`entity::JniWrapper`] and return the [`BytesResult`] wrapped in [`CResult`].
#[unsafe(no_mangle)]
pub extern "C" fn apply_partition_filter(
    callback: extern "C" fn(i32, *const c_char),
    len: i32,
    jni_wrapper_addr: c_ptrdiff_t,
    schema_addr: c_ptrdiff_t,
    filter_len: i32,
    filter_addr: c_ptrdiff_t,
) -> NonNull<CResult<BytesResult>> {
    let raw_parts = unsafe {
        std::slice::from_raw_parts(jni_wrapper_addr as *const u8, len as usize)
    };
    let wrapper =
        entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();

    let dst =
        unsafe { slice::from_raw_parts(filter_addr as *const u8, filter_len as usize) };
    let filter = Plan::decode(dst).unwrap();

    let ffi_schema = schema_addr as *mut FFI_ArrowSchema;
    let schema_data = unsafe { std::ptr::replace(ffi_schema, FFI_ArrowSchema::empty()) };
    let schema = SchemaRef::from(Schema::try_from(&schema_data).unwrap());

    let filtered_partition = helpers::apply_partition_filter(wrapper, schema, filter);

    match filtered_partition {
        Ok(wrapper) => {
            let u8_vec = wrapper.encode_to_vec();
            let len = u8_vec.len();
            call_i32_result_callback(callback, len as i32, std::ptr::null());
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(u8_vec))
        }
        Err(e) => {
            call_i32_result_callback(
                callback,
                -1,
                CString::new(e.to_string().as_str()).unwrap().into_raw(),
            );
            convert_to_nonnull(CResult::<BytesResult>::new::<Vec<u8>>(vec![]))
        }
    }
}

/// Free the [`BytesResult`].
#[unsafe(no_mangle)]
pub extern "C" fn free_bytes_result(bytes: NonNull<CResult<BytesResult>>) {
    from_nonnull(bytes).free::<Vec<u8>>();
}

/// init a global logger for rust code
/// now use RUST_LOG=LEVEL to activate
#[unsafe(no_mangle)]
pub extern "C" fn rust_logger_init() {
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
                let msg = format!("Failed to initialize tracing subscriber {:?}", e);
                eprintln!("{}", msg);
                panic!("{}", msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(static_mut_refs)]
    use std::ffi::{CStr, CString};
    use std::os::raw::c_char;
    use std::ptr::NonNull;
    use std::sync::{Condvar, Mutex};

    use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

    use crate::{
        IOConfigBuilder, create_lakesoul_io_config_from_builder,
        create_lakesoul_reader_from_config, create_lakesoul_writer_from_config,
        flush_and_close_writer, free_lakesoul_reader,
        lakesoul_config_builder_add_single_file,
        lakesoul_config_builder_add_single_primary_key,
        lakesoul_config_builder_set_batch_size,
        lakesoul_config_builder_set_max_row_group_size,
        lakesoul_config_builder_set_object_store_option,
        lakesoul_config_builder_set_schema, lakesoul_config_builder_set_thread_num,
        lakesoul_reader_get_schema, next_record_batch, rust_logger_init, start_reader,
        tokio_runtime_builder_set_thread_num, write_record_batch,
    };

    fn set_object_store_kv(
        builder: NonNull<IOConfigBuilder>,
        key: &str,
        value: &str,
    ) -> NonNull<IOConfigBuilder> {
        unsafe {
            lakesoul_config_builder_set_object_store_option(
                builder,
                CString::from_vec_unchecked(Vec::from(key)).as_ptr() as *const c_char,
                CString::from_vec_unchecked(Vec::from(value)).as_ptr() as *const c_char,
            )
        }
    }

    fn add_pk(builder: NonNull<IOConfigBuilder>, pk: &str) -> NonNull<IOConfigBuilder> {
        unsafe {
            lakesoul_config_builder_add_single_primary_key(
                builder,
                CString::from_vec_unchecked(Vec::from(pk)).as_ptr() as *const c_char,
            )
        }
    }

    static mut READER_FINISHED: bool = false;
    static mut READER_FAILED: Option<String> = None;

    static mut CALL_BACK_CV: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());

    #[unsafe(no_mangle)]
    pub extern "C" fn reader_callback(status: bool, err: *const c_char) {
        unsafe {
            let mut reader_called = CALL_BACK_CV.0.lock().unwrap();
            if !status {
                if let Some(e) = err.as_ref() {
                    READER_FAILED = Some(
                        CStr::from_ptr(e as *const c_char)
                            .to_str()
                            .unwrap()
                            .to_string(),
                    )
                }
                READER_FINISHED = true;
            }
            *reader_called = true;
            CALL_BACK_CV.1.notify_one();
        }
    }

    static mut CALL_BACK_I32_CV: (Mutex<i32>, Condvar) = (Mutex::new(-1), Condvar::new());

    #[unsafe(no_mangle)]
    pub extern "C" fn reader_i32_callback(status: i32, err: *const c_char) {
        unsafe {
            let mut reader_called = CALL_BACK_I32_CV.0.lock().unwrap();
            if status > 0 {
                if let Some(e) = err.as_ref() {
                    READER_FAILED = Some(
                        CStr::from_ptr(e as *const c_char)
                            .to_str()
                            .unwrap()
                            .to_string(),
                    )
                }
                READER_FINISHED = true;
            }
            *reader_called = status;
            CALL_BACK_I32_CV.1.notify_one();
        }
    }

    fn wait_callback() {
        unsafe {
            let mut called = CALL_BACK_CV.0.lock().unwrap();
            while !*called {
                called = CALL_BACK_CV.1.wait(called).unwrap();
            }
        }
    }

    static mut WRITER_FINISHED: bool = false;
    static mut WRITER_FAILED: Option<String> = None;

    #[unsafe(no_mangle)]
    pub extern "C" fn writer_callback(status: i32, err: *const c_char) {
        unsafe {
            let mut writer_called = CALL_BACK_CV.0.lock().unwrap();
            if status < 0 {
                if let Some(e) = err.as_ref() {
                    WRITER_FAILED = Some(
                        CStr::from_ptr(e as *const c_char)
                            .to_str()
                            .unwrap()
                            .to_string(),
                    )
                }
                WRITER_FINISHED = true;
            }
            *writer_called = true;
            CALL_BACK_CV.1.notify_one();
        }
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn result_callback(status: bool, err: *const c_char) {
        unsafe {
            let mut writer_called = CALL_BACK_CV.0.lock().unwrap();
            if !status {
                if let Some(e) = err.as_ref() {
                    WRITER_FAILED = Some(
                        CStr::from_ptr(e as *const c_char)
                            .to_str()
                            .unwrap()
                            .to_string(),
                    )
                }
                WRITER_FINISHED = true;
            }
            *writer_called = true;
            CALL_BACK_CV.1.notify_one();
        }
    }

    #[test]
    fn test_native_read_write() {
        let mut reader_config_builder = crate::new_lakesoul_io_config_builder();
        reader_config_builder =
            lakesoul_config_builder_set_batch_size(reader_config_builder, 8192);
        reader_config_builder =
            lakesoul_config_builder_set_thread_num(reader_config_builder, 2);
        unsafe {
            reader_config_builder = lakesoul_config_builder_add_single_file(
                reader_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.access.key",
            "minioadmin1",
        );
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.secret.key",
            "minioadmin1",
        );
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.endpoint",
            "http://localhost:9000",
        );

        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let reader_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let reader_config = create_lakesoul_io_config_from_builder(reader_config_builder);
        let reader = create_lakesoul_reader_from_config(reader_config, reader_runtime);

        unsafe {
            if let Some(err) = reader.as_ref().err.as_ref() {
                assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char)
                        .to_str()
                        .unwrap()
                        .to_string()
                )
            }
        }

        start_reader(reader, reader_callback);
        unsafe {
            assert!(!READER_FINISHED, "{:?}", READER_FAILED.as_ref());
        }

        let schema_ffi = FFI_ArrowSchema::empty();
        lakesoul_reader_get_schema(reader, std::ptr::addr_of!(schema_ffi) as isize);

        let mut writer_config_builder = crate::new_lakesoul_io_config_builder();
        writer_config_builder =
            lakesoul_config_builder_set_batch_size(writer_config_builder, 8192);
        writer_config_builder =
            lakesoul_config_builder_set_thread_num(writer_config_builder, 2);
        writer_config_builder =
            lakesoul_config_builder_set_max_row_group_size(writer_config_builder, 250000);
        writer_config_builder = lakesoul_config_builder_set_schema(
            writer_config_builder,
            std::ptr::addr_of!(schema_ffi) as isize,
        );
        unsafe {
            writer_config_builder = lakesoul_config_builder_add_single_file(
                writer_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_c.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.access.key",
            "minioadmin1",
        );
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.secret.key",
            "minioadmin1",
        );
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.endpoint",
            "http://localhost:9000",
        );
        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let writer_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let writer_config = create_lakesoul_io_config_from_builder(writer_config_builder);
        let writer = create_lakesoul_writer_from_config(writer_config, writer_runtime);
        unsafe {
            if let Some(err) = writer.as_ref().err.as_ref() {
                assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char)
                        .to_str()
                        .unwrap()
                        .to_string()
                )
            }
        }

        loop {
            unsafe {
                let mut called = CALL_BACK_CV.0.lock().unwrap();
                *called = false;
            }
            let array_ptr = FFI_ArrowArray::empty();
            let schema_ptr = FFI_ArrowSchema::empty();

            next_record_batch(
                reader,
                std::ptr::addr_of!(schema_ptr) as isize,
                std::ptr::addr_of!(array_ptr) as isize,
                reader_i32_callback,
            );
            wait_callback();

            unsafe {
                if READER_FINISHED {
                    if let Some(err) = READER_FAILED.as_ref() {
                        assert!(false, "Reader failed {}", err);
                    }
                    break;
                }
            }

            unsafe {
                let mut called = CALL_BACK_CV.0.lock().unwrap();
                *called = false;
            }
            write_record_batch(
                writer,
                std::ptr::addr_of!(schema_ptr) as isize,
                std::ptr::addr_of!(array_ptr) as isize,
                result_callback,
            );
            wait_callback();

            unsafe {
                if WRITER_FINISHED {
                    if let Some(err) = WRITER_FAILED.as_ref() {
                        assert!(false, "Writer {}", err);
                    }
                    break;
                }
            }
        }

        flush_and_close_writer(writer, writer_callback);
        free_lakesoul_reader(reader);
    }

    #[test]
    fn test_native_read_sort_write() {
        let mut reader_config_builder = crate::new_lakesoul_io_config_builder();
        reader_config_builder =
            lakesoul_config_builder_set_batch_size(reader_config_builder, 8192);
        reader_config_builder =
            lakesoul_config_builder_set_thread_num(reader_config_builder, 2);
        unsafe {
            reader_config_builder = lakesoul_config_builder_add_single_file(
                reader_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.access.key",
            "minioadmin1",
        );
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.secret.key",
            "minioadmin1",
        );
        reader_config_builder = set_object_store_kv(
            reader_config_builder,
            "fs.s3a.endpoint",
            "http://localhost:9000",
        );

        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let reader_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let reader_config = create_lakesoul_io_config_from_builder(reader_config_builder);
        let reader = create_lakesoul_reader_from_config(reader_config, reader_runtime);

        unsafe {
            if let Some(err) = reader.as_ref().err.as_ref() {
                assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char)
                        .to_str()
                        .unwrap()
                        .to_string()
                )
            }
        }

        start_reader(reader, reader_callback);
        unsafe {
            assert!(!READER_FINISHED, "{:?}", READER_FAILED.as_ref());
        }

        let schema_ffi = FFI_ArrowSchema::empty();
        lakesoul_reader_get_schema(reader, std::ptr::addr_of!(schema_ffi) as isize);

        let mut writer_config_builder = crate::new_lakesoul_io_config_builder();
        writer_config_builder =
            lakesoul_config_builder_set_batch_size(writer_config_builder, 8192);
        writer_config_builder =
            lakesoul_config_builder_set_thread_num(writer_config_builder, 2);
        writer_config_builder =
            lakesoul_config_builder_set_max_row_group_size(writer_config_builder, 250000);
        writer_config_builder = lakesoul_config_builder_set_schema(
            writer_config_builder,
            std::ptr::addr_of!(schema_ffi) as isize,
        );
        writer_config_builder = add_pk(writer_config_builder, "str2");
        writer_config_builder = add_pk(writer_config_builder, "str3");
        writer_config_builder = add_pk(writer_config_builder, "int2");
        writer_config_builder = add_pk(writer_config_builder, "int6");
        unsafe {
            writer_config_builder = lakesoul_config_builder_add_single_file(
                writer_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_sorted_c.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.access.key",
            "minioadmin1",
        );
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.secret.key",
            "minioadmin1",
        );
        writer_config_builder = set_object_store_kv(
            writer_config_builder,
            "fs.s3a.endpoint",
            "http://localhost:9000",
        );
        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let writer_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let writer_config = create_lakesoul_io_config_from_builder(writer_config_builder);
        let writer = create_lakesoul_writer_from_config(writer_config, writer_runtime);
        unsafe {
            if let Some(err) = writer.as_ref().err.as_ref() {
                assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char)
                        .to_str()
                        .unwrap()
                        .to_string()
                )
            }
        }

        loop {
            unsafe {
                let mut called = CALL_BACK_CV.0.lock().unwrap();
                *called = false;
            }
            let array_ptr = FFI_ArrowArray::empty();
            let schema_ptr = FFI_ArrowSchema::empty();

            next_record_batch(
                reader,
                std::ptr::addr_of!(schema_ptr) as isize,
                std::ptr::addr_of!(array_ptr) as isize,
                reader_i32_callback,
            );
            wait_callback();

            unsafe {
                if READER_FINISHED {
                    if let Some(err) = READER_FAILED.as_ref() {
                        assert!(false, "Reader failed {}", err);
                    }
                    break;
                }
            }

            unsafe {
                let mut called = CALL_BACK_CV.0.lock().unwrap();
                *called = false;
            }
            write_record_batch(
                writer,
                std::ptr::addr_of!(schema_ptr) as isize,
                std::ptr::addr_of!(array_ptr) as isize,
                result_callback,
            );
            wait_callback();

            unsafe {
                if WRITER_FINISHED {
                    if let Some(err) = WRITER_FAILED.as_ref() {
                        assert!(false, "Writer {}", err);
                    }
                    break;
                }
            }
        }

        flush_and_close_writer(writer, writer_callback);
        free_lakesoul_reader(reader);
    }

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
