// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![feature(c_size_t)]
extern crate core;

use core::ffi::{c_ptrdiff_t, c_size_t};
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

pub use arrow::array::StructArray;
use arrow::array::{Array, ArrayData};
use arrow::datatypes::Schema;
use arrow::ffi::ArrowArray;
pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use tokio::runtime::{Builder, Runtime};

use lakesoul_io::lakesoul_reader::{LakeSoulReader, RecordBatch, Result, SyncSendableMutableLakeSoulReader};
use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;

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
                drop(from_opaque::<OpaqueT, T>(NonNull::new_unchecked(self.ptr)));
            }
            if !self.err.is_null() {
                drop(CString::from_raw(self.err as *mut c_char));
            }
        }
    }
}

fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

fn convert_to_opaque<F, T>(obj: F) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut T) }
}

fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

fn from_nonnull<T>(obj: NonNull<T>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr()) }
}

// C interface for lakesoul native io

// opaque types to pass as raw pointers
#[repr(C)]
pub struct IOConfigBuilder {
    private: [u8; 0],
}

#[repr(C)]
pub struct IOConfig {
    private: [u8; 0],
}

#[repr(C)]
pub struct Reader {
    private: [u8; 0],
}

#[repr(C)]
pub struct Writer {
    private: [u8; 0],
}

#[no_mangle]
pub extern "C" fn new_lakesoul_io_config_builder() -> NonNull<IOConfigBuilder> {
    convert_to_opaque(LakeSoulIOConfigBuilder::new())
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_file(
    builder: NonNull<IOConfigBuilder>,
    file: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let file = CStr::from_ptr(file).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_file(file))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_column(column))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_aux_sort_column(
    builder: NonNull<IOConfigBuilder>,
    column: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_aux_sort_column(column))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_filter(
    builder: NonNull<IOConfigBuilder>,
    filter: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let filter = CStr::from_ptr(filter).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_filter_str(filter))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let ffi_schema = schema_addr as *mut FFI_ArrowSchema;
        let schema_data = std::ptr::replace(ffi_schema, FFI_ArrowSchema::empty());
        let schema = Schema::try_from(&schema_data).unwrap();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_schema(Arc::new(schema)),
        )
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_thread_num(
    builder: NonNull<IOConfigBuilder>,
    thread_num: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_thread_num(thread_num))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_batch_size(
    builder: NonNull<IOConfigBuilder>,
    batch_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_batch_size(batch_size))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_max_row_group_size(
    builder: NonNull<IOConfigBuilder>,
    max_row_group_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(
        from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_max_row_group_size(max_row_group_size),
    )
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_buffer_size(
    builder: NonNull<IOConfigBuilder>,
    buffer_size: c_size_t,
) -> NonNull<IOConfigBuilder> {
    convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_prefetch_size(buffer_size))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_object_store_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_object_store_option(key, value),
        )
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_files(
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
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_files(files))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_primary_key(
    builder: NonNull<IOConfigBuilder>,
    pk: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let pk = CStr::from_ptr(pk).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_primary_key(pk))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_merge_op(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    merge_op: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let merge_op = CStr::from_ptr(merge_op).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_merge_op(field, merge_op),
        )
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_primary_keys(
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
        convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_primary_keys(pks))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_default_column_value(
    builder: NonNull<IOConfigBuilder>,
    field: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder> {
    unsafe {
        let field = CStr::from_ptr(field).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(
            from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).with_default_column_value(field, value),
        )
    }
}

// C interface for reader

#[no_mangle]
pub extern "C" fn create_lakesoul_io_config_from_builder(builder: NonNull<IOConfigBuilder>) -> NonNull<IOConfig> {
    convert_to_opaque(from_opaque::<IOConfigBuilder, LakeSoulIOConfigBuilder>(builder).build())
}

#[no_mangle]
pub extern "C" fn create_lakesoul_reader_from_config(
    config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Reader>> {
    let config: LakeSoulIOConfig = from_opaque(config);
    let runtime: Runtime = from_opaque(runtime);
    let result = match LakeSoulReader::new(config) {
        Ok(reader) => CResult::<Reader>::new(SyncSendableMutableLakeSoulReader::new(reader, runtime)),
        Err(e) => CResult::<Reader>::error(format!("{}", e).as_str()),
    };
    convert_to_nonnull(result)
}

#[no_mangle]
pub extern "C" fn check_reader_created(reader: NonNull<CResult<Reader>>) -> *const c_char {
    unsafe {
        if let Some(err) = reader.as_ref().err.as_ref() {
            err as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

pub type ResultCallback = extern "C" fn(bool, *const c_char);
pub type DataResultCallback = extern "C" fn(bool, *const c_char, *const c_void);

fn call_result_callback(callback: ResultCallback, status: bool, err: *const c_char) {
    callback(status, err);
    // release error string
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

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

pub type I32ResultCallback = extern "C" fn(i32, *const c_char);
pub type I32DataResultCallback = extern "C" fn(i32, *const c_char, *const c_void);

fn call_i32_result_callback(callback: I32ResultCallback, status: i32, err: *const c_char) {
    callback(status, err);
    // release error string
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

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

#[no_mangle]
pub extern "C" fn start_reader(reader: NonNull<CResult<Reader>>, callback: ResultCallback) {
    unsafe {
        let mut reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let result = reader.as_mut().start_blocked();
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

#[no_mangle]
pub extern "C" fn start_reader_with_data(
    reader: NonNull<CResult<Reader>>,
    data: *const c_void,
    callback: DataResultCallback,
) {
    unsafe {
        let mut reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let data = Cvoid {data};
        let result = reader.as_mut().start_blocked();
        match result {
            Ok(_) => call_data_result_callback(callback, true, std::ptr::null(), data),
            Err(e) => call_data_result_callback(
                callback,
                false,
                CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                data,
            ),
        }
    }
}

#[no_mangle]
pub extern "C" fn next_record_batch(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: I32ResultCallback,
) {
    unsafe {
        let reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let f = move |rb: Option<Result<RecordBatch>>| match rb {
            None => {
                call_i32_result_callback(callback, 0, std::ptr::null());
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_i32_result_callback(
                        callback,
                        -1,
                        CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                    );
                }
                Ok(rb) => {
                    let rows = rb.num_rows() as i32;
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
                    (&ffi_array as *const FFI_ArrowArray).copy_to(array_addr as *mut FFI_ArrowArray, 1);
                    std::mem::forget(ffi_array);
                    let schema_result = FFI_ArrowSchema::try_from(batch.data_type());
                    match schema_result {
                        Ok(schema) => {
                            (&schema as *const FFI_ArrowSchema).copy_to(schema_addr as *mut FFI_ArrowSchema, 1);
                            std::mem::forget(schema);
                            call_i32_result_callback(callback, rows, std::ptr::null());
                        }
                        Err(e) => {
                            call_i32_result_callback(
                                callback,
                                -1,
                                CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                            );
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }
}

// accept a callback with arbitrary user data pointer

struct Cvoid {
    data: *const c_void,
}
unsafe impl Send for Cvoid {}
unsafe impl Sync for Cvoid {}

#[no_mangle]
pub extern "C" fn next_record_batch_with_data(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    data: *const c_void,
    callback: I32DataResultCallback,
) {
    unsafe {
        let reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let data = Cvoid {data};
        let f = move |rb: Option<Result<RecordBatch>>| match rb {
            None => {
                call_i32_data_result_callback(callback, 0, std::ptr::null(), data);
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_i32_data_result_callback(
                        callback,
                        -1,
                        CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                        data,
                    );
                }
                Ok(rb) => {
                    let rows = rb.num_rows() as i32;
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let ffi_array = FFI_ArrowArray::new(&batch.to_data());
                    (&ffi_array as *const FFI_ArrowArray).copy_to(array_addr as *mut FFI_ArrowArray, 1);
                    std::mem::forget(ffi_array);
                    let schema_result = FFI_ArrowSchema::try_from(batch.data_type());
                    match schema_result {
                        Ok(schema) => {
                            (&schema as *const FFI_ArrowSchema).copy_to(schema_addr as *mut FFI_ArrowSchema, 1);
                            std::mem::forget(schema);
                            call_i32_data_result_callback(callback, rows, std::ptr::null(), data);
                        }
                        Err(e) => {
                            call_i32_data_result_callback(
                                callback,
                                -1,
                                CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
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

#[no_mangle]
pub extern "C" fn lakesoul_reader_get_schema(reader: NonNull<CResult<Reader>>, schema_addr: c_ptrdiff_t) {
    unsafe {
        let reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
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

#[no_mangle]
pub extern "C" fn free_lakesoul_reader(reader: NonNull<CResult<Reader>>) {
    from_nonnull(reader).free::<SyncSendableMutableLakeSoulReader>();
}

// C interface for writer
#[no_mangle]
pub extern "C" fn create_lakesoul_writer_from_config(
    config: NonNull<IOConfig>,
    runtime: NonNull<TokioRuntime>,
) -> NonNull<CResult<Writer>> {
    let config: LakeSoulIOConfig = from_opaque(config);
    let runtime: Runtime = from_opaque(runtime);
    let result = match SyncSendableMutableLakeSoulWriter::try_new(config, runtime) {
        Ok(writer) => CResult::<Writer>::new(writer),
        Err(e) => CResult::<Writer>::error(format!("{}", e).as_str()),
    };
    convert_to_nonnull(result)
}

#[no_mangle]
pub extern "C" fn check_writer_created(writer: NonNull<CResult<Reader>>) -> *const c_char {
    unsafe {
        if let Some(err) = writer.as_ref().err.as_ref() {
            err as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub extern "C" fn write_record_batch(
    writer: NonNull<CResult<Writer>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: ResultCallback,
) {
    unsafe {
        let writer = NonNull::new_unchecked(writer.as_ref().ptr as *mut SyncSendableMutableLakeSoulWriter);
        let mut ffi_array = FFI_ArrowArray::empty();
        (array_addr as *mut FFI_ArrowArray).copy_to(&mut ffi_array as *mut FFI_ArrowArray, 1);
        let mut ffi_schema = FFI_ArrowSchema::empty();
        (schema_addr as *mut FFI_ArrowSchema).copy_to(&mut ffi_schema as *mut FFI_ArrowSchema, 1);
        let array = ArrowArray::new(ffi_array, ffi_schema);
        let result_fn = move || {
            let array_data = ArrayData::try_from(array)?;
            let struct_array = StructArray::from(array_data);
            let rb = RecordBatch::from(struct_array);
            writer.as_ref().write_batch(rb)?;
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

// consumes the writer pointer
// this writer cannot be used again
#[no_mangle]
pub extern "C" fn flush_and_close_writer(writer: NonNull<CResult<Writer>>, callback: ResultCallback) {
    unsafe {
        let writer =
            from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(NonNull::new_unchecked(writer.as_ref().ptr));
        let result = writer.flush_and_close();
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

// consumes the writer pointer
// this writer cannot be used again
#[no_mangle]
pub extern "C" fn abort_and_close_writer(writer: NonNull<CResult<Writer>>, callback: ResultCallback) {
    unsafe {
        let writer =
            from_opaque::<Writer, SyncSendableMutableLakeSoulWriter>(NonNull::new_unchecked(writer.as_ref().ptr));
        let result = writer.abort_and_close();
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

// C interface for tokio::runtime

// opaque types to pass as raw pointers
#[repr(C)]
pub struct TokioRuntimeBuilder {
    private: [u8; 0],
}

#[repr(C)]
pub struct TokioRuntime {
    private: [u8; 0],
}

#[no_mangle]
pub extern "C" fn new_tokio_runtime_builder() -> NonNull<TokioRuntimeBuilder> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    builder.worker_threads(2);
    builder.max_blocking_threads(8);
    convert_to_opaque(builder)
}

#[no_mangle]
pub extern "C" fn tokio_runtime_builder_set_thread_num(
    builder: NonNull<TokioRuntimeBuilder>,
    thread_num: c_size_t,
) -> NonNull<TokioRuntimeBuilder> {
    let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    builder.worker_threads(thread_num);
    convert_to_opaque(builder)
}

#[no_mangle]
pub extern "C" fn create_tokio_runtime_from_builder(builder: NonNull<TokioRuntimeBuilder>) -> NonNull<TokioRuntime> {
    let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    let runtime = builder.build().unwrap();
    convert_to_opaque(runtime)
}

// runtime is usually moved to create reader/writer
// so you don't need to free it unless it's used independently
#[no_mangle]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<CResult<TokioRuntime>>) {
    from_nonnull(runtime).free::<Runtime>();
}

#[cfg(test)]
mod tests {
    use crate::{
        create_lakesoul_io_config_from_builder, create_lakesoul_reader_from_config, create_lakesoul_writer_from_config,
        flush_and_close_writer, free_lakesoul_reader, lakesoul_config_builder_add_single_file,
        lakesoul_config_builder_add_single_primary_key, lakesoul_config_builder_set_batch_size,
        lakesoul_config_builder_set_max_row_group_size, lakesoul_config_builder_set_object_store_option,
        lakesoul_config_builder_set_schema, lakesoul_config_builder_set_thread_num, lakesoul_reader_get_schema,
        next_record_batch, start_reader, tokio_runtime_builder_set_thread_num, write_record_batch, IOConfigBuilder,
    };
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use core::ffi::c_ptrdiff_t;
    use std::ffi::{CStr, CString};
    use std::os::raw::c_char;
    use std::ptr::NonNull;
    use std::sync::{Condvar, Mutex};

    fn set_object_store_kv(builder: NonNull<IOConfigBuilder>, key: &str, value: &str) -> NonNull<IOConfigBuilder> {
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

    #[no_mangle]
    pub extern "C" fn reader_callback(status: bool, err: *const c_char) {
        unsafe {
            let mut reader_called = CALL_BACK_CV.0.lock().unwrap();
            if !status {
                match err.as_ref() {
                    Some(e) => READER_FAILED = Some(CStr::from_ptr(e as *const c_char).to_str().unwrap().to_string()),
                    None => {}
                }
                READER_FINISHED = true;
            }
            *reader_called = true;
            CALL_BACK_CV.1.notify_one();
        }
    }

    static mut CALL_BACK_I32_CV: (Mutex<i32>, Condvar) = (Mutex::new(-1), Condvar::new());
    #[no_mangle]
    pub extern "C" fn reader_i32_callback(status: i32, err: *const c_char) {
        unsafe {
            let mut reader_called = CALL_BACK_I32_CV.0.lock().unwrap();
            if status > 0 {
                match err.as_ref() {
                    Some(e) => READER_FAILED = Some(CStr::from_ptr(e as *const c_char).to_str().unwrap().to_string()),
                    None => {}
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

    #[no_mangle]
    pub extern "C" fn writer_callback(status: bool, err: *const c_char) {
        unsafe {
            let mut writer_called = CALL_BACK_CV.0.lock().unwrap();
            if !status {
                match err.as_ref() {
                    Some(e) => WRITER_FAILED = Some(CStr::from_ptr(e as *const c_char).to_str().unwrap().to_string()),
                    None => {}
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
        reader_config_builder = lakesoul_config_builder_set_batch_size(reader_config_builder, 8192);
        reader_config_builder = lakesoul_config_builder_set_thread_num(reader_config_builder, 2);
        unsafe {
            reader_config_builder = lakesoul_config_builder_add_single_file(
                reader_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.access.key", "minioadmin1");
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.secret.key", "minioadmin1");
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.endpoint", "http://localhost:9000");

        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let reader_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let reader_config = create_lakesoul_io_config_from_builder(reader_config_builder);
        let reader = create_lakesoul_reader_from_config(reader_config, reader_runtime);

        unsafe {
            match reader.as_ref().err.as_ref() {
                Some(err) => assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char).to_str().unwrap().to_string()
                ),
                None => {}
            }
        }

        start_reader(reader, reader_callback.clone());
        unsafe {
            assert_eq!(READER_FINISHED, false, "{:?}", READER_FAILED.as_ref());
        }

        let schema_ffi = FFI_ArrowSchema::empty();
        lakesoul_reader_get_schema(reader, std::ptr::addr_of!(schema_ffi) as c_ptrdiff_t);

        let mut writer_config_builder = crate::new_lakesoul_io_config_builder();
        writer_config_builder = lakesoul_config_builder_set_batch_size(writer_config_builder, 8192);
        writer_config_builder = lakesoul_config_builder_set_thread_num(writer_config_builder, 2);
        writer_config_builder = lakesoul_config_builder_set_max_row_group_size(writer_config_builder, 250000);
        writer_config_builder =
            lakesoul_config_builder_set_schema(writer_config_builder, std::ptr::addr_of!(schema_ffi) as c_ptrdiff_t);
        unsafe {
            writer_config_builder = lakesoul_config_builder_add_single_file(
                writer_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_c.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.access.key", "minioadmin1");
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.secret.key", "minioadmin1");
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.endpoint", "http://localhost:9000");
        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let writer_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let writer_config = create_lakesoul_io_config_from_builder(writer_config_builder);
        let writer = create_lakesoul_writer_from_config(writer_config, writer_runtime);
        unsafe {
            match writer.as_ref().err.as_ref() {
                Some(err) => assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char).to_str().unwrap().to_string()
                ),
                None => {}
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
                std::ptr::addr_of!(schema_ptr) as c_ptrdiff_t,
                std::ptr::addr_of!(array_ptr) as c_ptrdiff_t,
                reader_i32_callback.clone(),
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
                std::ptr::addr_of!(schema_ptr) as c_ptrdiff_t,
                std::ptr::addr_of!(array_ptr) as c_ptrdiff_t,
                writer_callback.clone(),
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
        reader_config_builder = lakesoul_config_builder_set_batch_size(reader_config_builder, 8192);
        reader_config_builder = lakesoul_config_builder_set_thread_num(reader_config_builder, 2);
        unsafe {
            reader_config_builder = lakesoul_config_builder_add_single_file(
                reader_config_builder,
                CString::from_vec_unchecked(Vec::from(
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet",
                ))
                .as_ptr() as *const c_char,
            );
        }
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.access.key", "minioadmin1");
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.secret.key", "minioadmin1");
        reader_config_builder = set_object_store_kv(reader_config_builder, "fs.s3a.endpoint", "http://localhost:9000");

        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let reader_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let reader_config = create_lakesoul_io_config_from_builder(reader_config_builder);
        let reader = create_lakesoul_reader_from_config(reader_config, reader_runtime);

        unsafe {
            match reader.as_ref().err.as_ref() {
                Some(err) => assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char).to_str().unwrap().to_string()
                ),
                None => {}
            }
        }

        start_reader(reader, reader_callback.clone());
        unsafe {
            assert_eq!(READER_FINISHED, false, "{:?}", READER_FAILED.as_ref());
        }

        let schema_ffi = FFI_ArrowSchema::empty();
        lakesoul_reader_get_schema(reader, std::ptr::addr_of!(schema_ffi) as c_ptrdiff_t);

        let mut writer_config_builder = crate::new_lakesoul_io_config_builder();
        writer_config_builder = lakesoul_config_builder_set_batch_size(writer_config_builder, 8192);
        writer_config_builder = lakesoul_config_builder_set_thread_num(writer_config_builder, 2);
        writer_config_builder = lakesoul_config_builder_set_max_row_group_size(writer_config_builder, 250000);
        writer_config_builder =
            lakesoul_config_builder_set_schema(writer_config_builder, std::ptr::addr_of!(schema_ffi) as c_ptrdiff_t);
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
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.access.key", "minioadmin1");
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.secret.key", "minioadmin1");
        writer_config_builder = set_object_store_kv(writer_config_builder, "fs.s3a.endpoint", "http://localhost:9000");
        let mut runtime_builder = crate::new_tokio_runtime_builder();
        runtime_builder = tokio_runtime_builder_set_thread_num(runtime_builder, 4);
        let writer_runtime = crate::create_tokio_runtime_from_builder(runtime_builder);
        let writer_config = create_lakesoul_io_config_from_builder(writer_config_builder);
        let writer = create_lakesoul_writer_from_config(writer_config, writer_runtime);
        unsafe {
            match writer.as_ref().err.as_ref() {
                Some(err) => assert!(
                    false,
                    "{}",
                    CStr::from_ptr(err as *const c_char).to_str().unwrap().to_string()
                ),
                None => {}
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
                std::ptr::addr_of!(schema_ptr) as c_ptrdiff_t,
                std::ptr::addr_of!(array_ptr) as c_ptrdiff_t,
                reader_i32_callback.clone(),
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
                std::ptr::addr_of!(schema_ptr) as c_ptrdiff_t,
                std::ptr::addr_of!(array_ptr) as c_ptrdiff_t,
                writer_callback.clone(),
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
}
