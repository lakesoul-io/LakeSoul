// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![feature(c_size_t)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
extern crate core;

use core::ffi::c_ptrdiff_t;
use std::ptr::NonNull;
use std::ffi::{c_char, c_uchar, CString, CStr};

use lakesoul_metadata::{Runtime, Builder, Client, PreparedStatementMap};
use proto::proto::entity;
use prost::Message;

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
    unsafe {
        CStr::from_ptr(ptr).to_str().unwrap().to_string()
    }
}

pub type ResultCallback<T> = extern "C" fn(T, *const c_char);

#[no_mangle]
pub extern "C" fn execute_insert(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<Result<TokioRuntime>>,
    client: NonNull<Result<TokioPostgresClient>>,
    prepared: NonNull<Result<PreparedStatement>>,
    insert_type: i32,
    addr: c_ptrdiff_t,
    len: i32,
) {
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    let client = unsafe {NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut()};
    let prepared = unsafe {NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut()};

    let raw_parts = unsafe {std::slice::from_raw_parts(addr as *const u8, len as usize)};
    let wrapper = entity::JniWrapper::decode(prost::bytes::Bytes::from(raw_parts)).unwrap();
    let result = lakesoul_metadata::execute_insert(
        runtime,
        client,
        prepared,
        insert_type,
        wrapper
    );
    match result {
        Ok(count) => callback(count, CString::new("").unwrap().into_raw()),
        Err(e) => callback(-1, CString::new(e.to_string().as_str()).unwrap().into_raw())
    }
}

#[no_mangle]
pub extern "C" fn execute_update(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<Result<TokioRuntime>>,
    client: NonNull<Result<TokioPostgresClient>>,
    prepared: NonNull<Result<PreparedStatement>>,
    update_type: i32,
    joined_string: *const c_char
) {
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    let client = unsafe {NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut()};
    let prepared = unsafe {NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut()};

    let result = lakesoul_metadata::execute_update(
        runtime,
        client,
        prepared,
        update_type,
        string_from_ptr(joined_string),
    );
    match result {
        Ok(count) => callback(count, CString::new("").unwrap().into_raw()),
        Err(e) => callback(-1, CString::new(e.to_string().as_str()).unwrap().into_raw())
    }
}

#[no_mangle]
pub extern "C" fn execute_query_scalar(
    callback: extern "C" fn(*const c_char, *const c_char),
    runtime: NonNull<Result<TokioRuntime>>,
    client: NonNull<Result<TokioPostgresClient>>,
    prepared: NonNull<Result<PreparedStatement>>,
    update_type: i32,
    joined_string: *const c_char
) {
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    let client = unsafe {NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_mut()};
    let prepared = unsafe {NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut()};

    let result = lakesoul_metadata::execute_query_scalar(
        runtime,
        client,
        prepared,
        update_type,
        string_from_ptr(joined_string),
    );
    match result {
        Ok(Some(result)) => callback(CString::new(result.as_str()).unwrap().into_raw(), CString::new("").unwrap().into_raw()),
        Ok(None) => callback(CString::new("").unwrap().into_raw(), CString::new("").unwrap().into_raw()),
        Err(e) => callback(CString::new("").unwrap().into_raw(), CString::new(e.to_string().as_str()).unwrap().into_raw())
    }
}



#[no_mangle]
pub extern "C" fn execute_query(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<Result<TokioRuntime>>,
    client: NonNull<Result<TokioPostgresClient>>,
    prepared: NonNull<Result<PreparedStatement>>,
    query_type: i32,
    joined_string: *const c_char,
    addr: c_ptrdiff_t,
){
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    let client = unsafe {NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_ref()};
    let prepared = unsafe {NonNull::new_unchecked(prepared.as_ref().ptr as *mut PreparedStatementMap).as_mut()};
    
    let result = lakesoul_metadata::execute_query(
        runtime,
        client,
        prepared,
        query_type,
        string_from_ptr(joined_string),
    );
    match result {
        Ok(u8_vec) => {
            let addr = addr as *mut  c_uchar;
            let _ = u8_vec
                .iter()
                .enumerate()
                .map(
                    |(idx, byte)| 
                    unsafe{std::ptr::write::<c_uchar>(addr.wrapping_add(idx), *byte)})
                .collect::<Vec<_>>();
            let len = u8_vec.len();
            unsafe{std::ptr::write::<c_uchar>(addr.wrapping_add(len), 0u8)}
            callback( len as i32, CString::new("").unwrap().into_raw());
        }
        Err(e) => {
            callback(0, CString::new(e.to_string().as_str()).unwrap().into_raw());
        }
    }
}

#[no_mangle]
pub extern "C" fn clean_meta_for_test(
    callback: extern "C" fn(i32, *const c_char),
    runtime: NonNull<Result<TokioRuntime>>,
    client: NonNull<Result<TokioPostgresClient>>,
) {
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    let client = unsafe {NonNull::new_unchecked(client.as_ref().ptr as *mut Client).as_ref()};
    let result = lakesoul_metadata::clean_meta_for_test(
        runtime,
        client);
    match result {
        Ok(count) => callback(count, CString::new("").unwrap().into_raw()),
        Err(e) => callback(-1, CString::new(e.to_string().as_str()).unwrap().into_raw())
    }    
}

#[no_mangle]
pub extern "C" fn create_tokio_runtime() -> NonNull<Result<TokioRuntime>> {
    let runtime =  Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .max_blocking_threads(8)
        .build()
        .unwrap();
    convert_to_nonnull(Result::<TokioRuntime>::new(runtime))
}

#[no_mangle]
pub extern "C" fn free_tokio_runtime(runtime: NonNull<Result<TokioRuntime>>) {
    from_nonnull(runtime).free::<Runtime>();
}

#[no_mangle]
pub extern "C" fn create_tokio_postgres_client(
    callback: extern "C" fn(bool, *const c_char),
    config: *const c_char,
    runtime: NonNull<Result<TokioRuntime>>,
) -> NonNull<Result<TokioPostgresClient>> {
    let config = string_from_ptr(config); 
    let runtime = unsafe {NonNull::new_unchecked(runtime.as_ref().ptr as *mut Runtime).as_ref()};
    
     let result = match lakesoul_metadata::create_connection(runtime, config) {
        Ok(client) => {
            callback(true, CString::new("").unwrap().into_raw());
            Result::<TokioPostgresClient>::new(client)
        }
        Err(e) => {
            callback(false, CString::new(e.to_string().as_str()).unwrap().into_raw());
            Result::<TokioPostgresClient>::error(format!("{}", e).as_str())
        }
    };
    convert_to_nonnull(result)
}

#[no_mangle]
pub extern "C" fn free_tokio_postgres_client(client: NonNull<Result<TokioPostgresClient>>) {
    from_nonnull(client).free::<Client>();
}

#[no_mangle]
pub extern "C" fn create_prepared_statement() -> NonNull<Result<PreparedStatement>> {
    let prepared = PreparedStatementMap::new();
    convert_to_nonnull(Result::<PreparedStatement>::new(prepared))
}

#[no_mangle]
pub extern "C" fn free_prepared_statement(prepared: NonNull<Result<PreparedStatement>>) {
    from_nonnull(prepared).free::<PreparedStatementMap>();
}