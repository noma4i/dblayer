use std::ffi::c_void;
use std::mem::{transmute, ManuallyDrop};

use uniffi::{ForeignFutureCallback, ForeignFutureResult, RustBuffer};

#[repr(C)]
struct Answer<R> {
    return_value: R,
    call_status: Status,
}

#[repr(C)]
struct Status {
    code: i8,
    error_buf: ManuallyDrop<RustBuffer>,
}

fn complete<R>(complete: *const c_void, handle: u64, return_value: R, code: i8, error_buf: RustBuffer) {
    let call_status = Status { code, error_buf: ManuallyDrop::new(error_buf) };
    let answer = ManuallyDrop::new(Answer { return_value, call_status });
    let result = unsafe { std::ptr::read(&*answer as *const Answer<R> as *const ForeignFutureResult<R>) };
    let complete = unsafe { transmute::<*const c_void, ForeignFutureCallback<R>>(complete) };
    complete(handle, result)
}

// JNA hands a struct passed by value to the native call as a pointer into its own call frame; scalars and RustBuffer arguments take the safe path.
#[no_mangle]
pub extern "C" fn dblayer_complete_foreign_future_buffer(callback: *const c_void, handle: u64, return_value: RustBuffer, code: i8, error_buf: RustBuffer) {
    complete(callback, handle, return_value, code, error_buf)
}

#[no_mangle]
pub extern "C" fn dblayer_complete_foreign_future_unit(callback: *const c_void, handle: u64, code: i8, error_buf: RustBuffer) {
    complete(callback, handle, (), code, error_buf)
}

#[cfg(test)]
#[path = "foreign_future_tests.rs"]
mod foreign_future_tests;
