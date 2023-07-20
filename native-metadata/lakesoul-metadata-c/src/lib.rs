// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![feature(c_size_t)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
extern crate core;

use core::ffi::c_ptrdiff_t;
use std::ptr::NonNull;
use std::ffi::{c_char, c_uchar, CString, CStr};

use lakesoul_metadata::RuntimeClient;
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
pub struct ClientPointer {
    private: [u8; 0],
}

fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

fn convert_to_opaque<F, T>(obj: F) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut T) }
}

fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

fn string_from_ptr(ptr: *const c_char) -> String {
    unsafe {
        CStr::from_ptr(ptr).to_str().unwrap().to_string()
    }
}

pub type ResultCallback<T> = extern "C" fn(T, *const c_char);

#[no_mangle]
pub extern "C" fn namespace(
    bytes: *const c_uchar,
    len: i32,
) {
    println!("rust::namespace");
    let a = unsafe {std::slice::from_raw_parts(bytes, len as usize)};
    println!("{:?}", entity::Namespace::decode(prost::bytes::Bytes::from(a)).unwrap());
}

#[no_mangle]
pub extern "C" fn create_native_client(
    callback: extern "C" fn(bool, *const c_char),
    config: *const c_char,
) -> NonNull<ClientPointer> {
    let config = string_from_ptr(config); 
    let result = lakesoul_metadata::create_connection(config);
    match result {
        Ok(client) => {
            callback(true, CString::new("").unwrap().into_raw());
            convert_to_opaque::<RuntimeClient, ClientPointer>(client)
        }
        Err(e) => {
            callback(false, CString::new(e.to_string().as_str()).unwrap().into_raw());
            convert_to_nonnull(ClientPointer { private: [] })
        }
    }
}

#[no_mangle]
pub extern "C" fn execute_query(
    callback: extern "C" fn(i32, *const c_char),
    query_type: *const c_char,
    joined_string: *const c_char,
    delim: *const c_char,
    addr: c_ptrdiff_t,
){
    let result = lakesoul_metadata::execute_query(
        string_from_ptr(query_type),
        string_from_ptr(joined_string),
        string_from_ptr(delim),
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
            let len = u8_vec.len() as i32;
            callback( len, CString::new("").unwrap().into_raw());
        }
        Err(e) => {
            callback(0, CString::new(e.to_string().as_str()).unwrap().into_raw());
        }
    }
}