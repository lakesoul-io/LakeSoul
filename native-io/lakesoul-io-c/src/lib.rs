#![feature(c_size_t)]
extern crate core;

use core::ffi::{c_ptrdiff_t, c_size_t};
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

use lakesoul_io::lakesoul_reader::{
    export_array_into_raw, ArrowResult, FFI_ArrowArray, FFI_ArrowSchema, LakeSoulReader, LakeSoulReaderConfig,
    LakeSoulReaderConfigBuilder, RecordBatch, StructArray, SyncSendableMutableLakeSoulReader,
};

// opaque types to pass as raw pointers
#[repr(C)]
pub struct ReaderConfigBuilder {
    private: [u8; 0],
}

#[repr(C)]
pub struct ReaderConfig {
    private: [u8; 0],
}

#[repr(C)]
pub struct Reader {
    private: [u8; 0],
}

#[repr(C)]
pub struct Result<OpaqueT> {
    ptr: *mut OpaqueT,
    err: *const c_char,
}

impl<OpaqueT> Result<OpaqueT> {
    pub fn new<T>(obj: T) -> Self {
        Result {
            ptr: convert_to_opaque_raw::<T, OpaqueT>(obj),
            err: std::ptr::null(),
        }
    }

    pub fn error(err_msg: &str) -> Self {
        Result {
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

#[no_mangle]
pub extern "C" fn new_lakesoul_reader_config_builder() -> NonNull<ReaderConfigBuilder> {
    convert_to_opaque(LakeSoulReaderConfigBuilder::new())
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_file(
    builder: NonNull<ReaderConfigBuilder>,
    file: *const c_char
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        println!("[From Rust][lakesoul_config_builder_add_single_file], file={}", CStr::from_ptr(file).to_str().unwrap().to_string());
        let file = CStr::from_ptr(file).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_file(file))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_column(
    builder: NonNull<ReaderConfigBuilder>,
    column: *const c_char
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        println!("[From Rust][lakesoul_config_builder_add_single_column], col={}", CStr::from_ptr(column).to_str().unwrap().to_string());
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_column(column))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_thread_num(
    builder: NonNull<ReaderConfigBuilder>,
    thread_num: c_size_t,
) -> NonNull<ReaderConfigBuilder> {
    println!("Setting thread_num={} for lakesoul_config_builder", thread_num);
    convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_thread_num(thread_num))
}



#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_file(
    builder: NonNull<ReaderConfigBuilder>,
    files: *const *const c_char,
    file_num: c_size_t,
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        let files = slice::from_raw_parts(files, file_num as usize);
        let files: Vec<_> = files
            .iter()
            .map(|p| CStr::from_ptr(*p))
            .map(|c_str| c_str.to_str().unwrap())
            .map(|str| str.to_string())
            .collect();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_files(files))
    }
}

#[no_mangle]
pub extern "C" fn create_lakesoul_reader_config_from_builder(
    builder: NonNull<ReaderConfigBuilder>,
) -> NonNull<ReaderConfig> {
    convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).build())
}

#[no_mangle]
pub extern "C" fn create_lakesoul_reader_from_config(config: NonNull<ReaderConfig>) -> NonNull<Result<Reader>> {
    let config: LakeSoulReaderConfig = from_opaque(config);
    let reader = LakeSoulReader::new(config);
    let result = match reader {
        Ok(reader) => Result::<Reader>::new(SyncSendableMutableLakeSoulReader::new(reader)),
        Err(e) => Result::<Reader>::error(format!("{}", e).as_str()),
    };
    convert_to_nonnull(result)
}

pub type ResultCallback = extern "C" fn(bool, *const c_char) -> c_void;

fn call_result_callback(callback: ResultCallback, status: bool, err: *const c_char) {
    println!("[From Rust][call_result_callback], status={}", status);
    callback(status, err);
    if !err.is_null() {
        unsafe {
            let _ = CString::from_raw(err as *mut c_char);
        }
    }
}

#[no_mangle]
pub extern "C" fn start_reader(reader: NonNull<Result<Reader>>, callback: ResultCallback) {
    unsafe {
        let reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let result = reader.as_ref().start_blocked();
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
pub extern "C" fn next_record_batch(
    reader: NonNull<Result<Reader>>,
    schema_addr: c_ptrdiff_t,
    array_addr: c_ptrdiff_t,
    callback: ResultCallback,
) {
    unsafe {
        let reader = NonNull::new_unchecked(reader.as_ref().ptr as *mut SyncSendableMutableLakeSoulReader);
        let f = move |rb: Option<ArrowResult<RecordBatch>>| match rb {
            None => {
                call_result_callback(callback, false, std::ptr::null());
            }
            Some(rb_result) => match rb_result {
                Err(e) => {
                    call_result_callback(
                        callback,
                        false,
                        CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                    );
                }
                Ok(rb) => {
                    println!("[From Rust][next_record_batch]rb.num_rows()={}", rb.num_rows());
                    let batch: Arc<StructArray> = Arc::new(rb.into());
                    let result = export_array_into_raw(
                        batch,
                        array_addr as *mut FFI_ArrowArray,
                        schema_addr as *mut FFI_ArrowSchema,
                    );
                    match result {
                        Ok(()) => {
                            call_result_callback(callback, true, std::ptr::null());
                        }
                        Err(e) => {
                            call_result_callback(
                                callback,
                                false,
                                CString::new(format!("{}", e).as_str()).unwrap().into_raw(),
                            );
                        }
                    }
                }
            },
        };
        reader.as_ref().next_rb_callback(Box::new(f));
        // try_load_ffi_ptr(schema_addr, array_addr, String::from("[end of next_record_batch]"));
    }
}

#[no_mangle]
pub extern "C" fn free_lakesoul_reader(mut reader: NonNull<Result<Reader>>) {
    unsafe {
        reader.as_mut().free::<SyncSendableMutableLakeSoulReader>();
    }
}
