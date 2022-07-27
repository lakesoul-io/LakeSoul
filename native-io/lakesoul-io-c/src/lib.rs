#![feature(c_size_t)]
extern crate core;

use core::ffi::c_size_t;
use std::ffi::{c_char, CStr};
use std::ptr::NonNull;
use std::slice;

use lakesoul_io::lakesoul_reader::{LakeSoulReader, LakeSoulReaderConfig, LakeSoulReaderConfigBuilder};

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

fn convert_to_opaque<F, T>(obj: F) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut T) }
}

fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

#[no_mangle]
pub extern "C" fn new_lakesoul_reader_config_builder() -> NonNull<ReaderConfigBuilder> {
    convert_to_opaque(LakeSoulReaderConfigBuilder::new())
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
) -> NonNull<Reader> {
    let config: LakeSoulReaderConfig = from_opaque(config);
    let reader = LakeSoulReader::new(config);
}