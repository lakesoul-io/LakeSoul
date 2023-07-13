#![feature(c_size_t)]
extern crate core;

use std::ptr::NonNull;

#[repr(C)]
pub struct Holder {
    private: [u8; 0],
}


#[no_mangle]
pub extern "C" fn do_something() -> NonNull<Holder> {
    let obj = "a".to_owned();
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(obj)) as *mut Holder) }
}
