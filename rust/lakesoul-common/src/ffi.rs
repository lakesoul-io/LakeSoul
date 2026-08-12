/// Common FFI utilities shared across LakeSoul C API crates.
use std::any::Any;
use std::ffi::{CString, c_char, c_int};
use std::panic::{self, AssertUnwindSafe};
use std::ptr::NonNull;

/// Opaque wrapper for the result of a function call
/// containing a pointer to a type and an error msg
#[repr(C)]
pub struct CResult<OpaqueT> {
    pub ptr: *mut OpaqueT,
    pub err: *const c_char,
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
            err: into_c_string(err_msg),
        }
    }

    pub fn free<T>(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                drop(from_opaque::<OpaqueT, T>(NonNull::new_unchecked(self.ptr)));
                self.ptr = std::ptr::null_mut();
            }
            if !self.err.is_null() {
                drop(CString::from_raw(self.err as *mut c_char));
                self.err = std::ptr::null();
            }
        }
    }
}

/// Opaque wrapper for the result of a function call
/// containing a status and an error msg
#[repr(C)]
pub struct CStatus {
    pub err: *const c_char,
    pub status: c_int,
}

impl CStatus {
    pub fn new(status: c_int) -> Self {
        CStatus {
            err: std::ptr::null(),
            status,
        }
    }

    pub fn error<T: Into<Vec<u8>>>(err_msg: T, status: c_int) -> Self {
        CStatus {
            err: into_c_string(err_msg),
            status,
        }
    }

    pub fn free(&mut self) {
        unsafe {
            if !self.err.is_null() {
                drop(CString::from_raw(self.err as *mut c_char));
            }
        }
    }
}

/// Convert an error message into a raw C string pointer.
/// Returns `*mut c_char` so it can be used directly with `CString::from_raw`.
/// Interior null bytes are stripped to avoid `CString::new` panicking.
pub fn into_c_string(msg: impl Into<Vec<u8>>) -> *mut c_char {
    let mut bytes: Vec<u8> = msg.into();
    let had_null = bytes.contains(&0);
    if had_null {
        tracing::error!(
            "Error message contained interior null bytes (will be stripped): {}",
            String::from_utf8_lossy(&bytes).escape_default()
        );
    }
    bytes.retain(|&b| b != 0);
    match CString::new(bytes) {
        Ok(cstr) => cstr.into_raw(),
        Err(e) => {
            tracing::error!(
                "Failed to create CString from error message (after stripping null bytes): {}",
                e
            );
            CString::new("unknown error (CString conversion failed)")
                .unwrap()
                .into_raw()
        }
    }
}

/// Extract a human-readable message from a panic payload.
pub fn log_panic_and_extract_message(panic_payload: Box<dyn Any + Send>) -> String {
    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    };
    tracing::error!(
        "LakeSoul native FFI caught a panic: {}. \
         This is a bug in the Rust code, please report.",
        msg
    );
    msg
}

/// Catch a panic in a closure that returns `CResult<T>`, converting the panic
/// into an error `CResult<T>`.
pub fn catch_unwind_cresult<T, F: FnOnce() -> CResult<T>>(f: F) -> NonNull<CResult<T>> {
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => convert_to_nonnull(r),
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            convert_to_nonnull(CResult::<T>::error(msg))
        }
    }
}

/// Catch a panic in a closure that returns `CStatus`, converting the panic
/// into an error `CStatus`.
pub fn catch_unwind_cstatus<F: FnOnce() -> CStatus>(f: F) -> NonNull<CStatus> {
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => convert_to_nonnull(r),
        Err(payload) => {
            let msg = log_panic_and_extract_message(payload);
            convert_to_nonnull(CStatus::error(msg, -1))
        }
    }
}

/// Convert the object to a raw opaque pointer
pub fn convert_to_opaque_raw<F, T>(obj: F) -> *mut T {
    Box::into_raw(Box::new(obj)) as *mut T
}

/// Convert the object to a [`NonNull`] opaque pointer
pub fn convert_to_opaque<F, T>(obj: F) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut T) }
}

/// Convert the [`NonNull`] opaque pointer to the object
pub fn from_opaque<F, T>(obj: NonNull<F>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr() as *mut T) }
}

/// Convert the object to a [`NonNull`] opaque pointer
pub fn convert_to_nonnull<T>(obj: T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj))) }
}

/// Convert the [`NonNull`] opaque pointer to the object
pub fn from_nonnull<T>(obj: NonNull<T>) -> T {
    unsafe { *Box::from_raw(obj.as_ptr()) }
}
