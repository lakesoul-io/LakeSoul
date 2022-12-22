#![feature(c_size_t)]
extern crate core;

use core::ffi::{c_ptrdiff_t, c_size_t};
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

pub use arrow::array::{export_array_into_raw, StructArray};
pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use tokio::runtime::{Builder, Runtime};

use lakesoul_io::lakesoul_reader::{
    ArrowResult, LakeSoulReader, LakeSoulReaderConfig,
    LakeSoulReaderConfigBuilder, RecordBatch, SyncSendableMutableLakeSoulReader,
};

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

//jni for lakesoul_io::lakesoul_reader 

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
        // println!("[From Rust][lakesoul_config_builder_add_single_file], file={}", CStr::from_ptr(file).to_str().unwrap().to_string());
        let file = CStr::from_ptr(file).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_file(file))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_single_column(
    builder: NonNull<ReaderConfigBuilder>,
    column: *const c_char,
    datatype: *const c_char,
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        // println!("[From Rust][lakesoul_config_builder_add_single_column], col={}", CStr::from_ptr(column).to_str().unwrap().to_string());
        let column = CStr::from_ptr(column).to_str().unwrap().to_string();
        let datatype = CStr::from_ptr(datatype).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_column(column, datatype))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_add_filter(
    builder: NonNull<ReaderConfigBuilder>,
    filter: *const c_char
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        //println!("[JNI][Rust][lakesoul_config_builder_add_filter], col={}", CStr::from_ptr(filter).to_str().unwrap().to_string());
        let filter = CStr::from_ptr(filter).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_filter_str(filter))
    }
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_thread_num(
    builder: NonNull<ReaderConfigBuilder>,
    thread_num: c_size_t,
) -> NonNull<ReaderConfigBuilder> {
    // println!("Setting thread_num={} for lakesoul_config_builder", thread_num);
    convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_thread_num(thread_num))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_batch_size(
    builder: NonNull<ReaderConfigBuilder>,
    batch_size: c_size_t,
) -> NonNull<ReaderConfigBuilder> {
    // println!("Setting batch_size={} for lakesoul_config_builder", batch_size);
    convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_batch_size(batch_size))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_buffer_size(
    builder: NonNull<ReaderConfigBuilder>,
    buffer_size: c_size_t,
) -> NonNull<ReaderConfigBuilder> {
    // println!("Setting batch_size={} for lakesoul_config_builder", batch_size);
    convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_buffer_size(buffer_size))
}

#[no_mangle]
pub extern "C" fn lakesoul_config_builder_set_object_store_option(
    builder: NonNull<ReaderConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> NonNull<ReaderConfigBuilder> {
    unsafe {
        // println!("Setting object_store_option:{}={}", CStr::from_ptr(key).to_str().unwrap().to_string(), CStr::from_ptr(value).to_str().unwrap().to_string());
        let key = CStr::from_ptr(key).to_str().unwrap().to_string();
        let value = CStr::from_ptr(value).to_str().unwrap().to_string();
        convert_to_opaque(from_opaque::<ReaderConfigBuilder, LakeSoulReaderConfigBuilder>(builder).with_object_store_option(key, value))
    }
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
pub extern "C" fn create_lakesoul_reader_from_config(
    config: NonNull<ReaderConfig>,
    runtime: NonNull<TokioRuntime>
) -> NonNull<Result<Reader>> {
    let config: LakeSoulReaderConfig = from_opaque(config);
    let runtime: Runtime = from_opaque(runtime);
    let result = match LakeSoulReader::new(config) {
        Ok(reader) => unsafe{
            Result::<Reader>::new(SyncSendableMutableLakeSoulReader::new(reader, runtime))
        }
        Err(e) => Result::<Reader>::error(format!("{}", e).as_str()),
    };
    convert_to_nonnull(result)
    
}

pub type ResultCallback = extern "C" fn(bool, *const c_char) -> c_void;

fn call_result_callback(callback: ResultCallback, status: bool, err: *const c_char) {
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
        let f = move |rb: Option<ArrowResult<RecordBatch>>| {
            match rb {
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
                        // println!("[From Rust][next_record_batch]rb.num_rows()={}", rb.num_rows());
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
            }
        };
        reader.as_ref().next_rb_callback(Box::new(f));
    }
}

#[no_mangle]
pub extern "C" fn free_lakesoul_reader(mut reader: NonNull<Result<Reader>>) {
    unsafe {
        reader.as_mut().free::<SyncSendableMutableLakeSoulReader>();
    }
}

//jni for tokio::runtime 

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
    convert_to_opaque(Builder::new_multi_thread())
}


#[no_mangle]
pub extern "C" fn tokio_runtime_builder_set_thread_num(
    builder: NonNull<TokioRuntimeBuilder>,
    thread_num: c_size_t,
) -> NonNull<TokioRuntimeBuilder> {
    // println!("Setting thread_num={} for tokio_runtime_builder", thread_num);
    convert_to_opaque(from_opaque::<TokioRuntimeBuilder, Builder>(builder).worker_threads(thread_num))
}


#[no_mangle]
pub extern "C" fn create_tokio_runtime_from_builder(
    builder: NonNull<TokioRuntimeBuilder>,
) -> NonNull<TokioRuntime> {
    let mut builder = from_opaque::<TokioRuntimeBuilder, Builder>(builder);
    let runtime = builder.worker_threads(1).enable_all().build().unwrap();
    let ret = convert_to_opaque(runtime);
    ret
}


#[no_mangle]
pub extern "C" fn free_tokio_runtime(mut runtime: NonNull<Result<TokioRuntime>>) {
    unsafe {
        // runtime.as_mut().free::<Runtime>();
    }
}

#[cfg(test)]
mod tests {

    use tokio::runtime::{Builder, Runtime};


    #[test]
    fn test_native_call() {
        let builder = Builder::new_multi_thread().worker_threads(1);
        // println!("{:?}", builder);
        let configBuilder = crate::new_lakesoul_reader_config_builder();
        let runtimbeBuilder = crate::new_tokio_runtime_builder();
        // let runtimbeBuilder = crate::tokio_runtime_builder_set_thread_num(runtimbeBuilder, 1);
        crate::create_tokio_runtime_from_builder(runtimbeBuilder);
    }
}