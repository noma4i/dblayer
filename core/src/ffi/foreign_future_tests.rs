use std::ffi::c_void;
use std::sync::OnceLock;

use uniffi::{foreign_async_call, ForeignFutureCallback, Lower, RustBuffer};
use crate::UniFfiTag;

use super::*;

fn runtime() -> tokio::runtime::Runtime { tokio::runtime::Builder::new_current_thread().build().unwrap() }

fn start<T: uniffi::LiftReturn<UniFfiTag> + 'static>() -> (std::pin::Pin<Box<dyn std::future::Future<Output = T>>>, ForeignFutureCallback<T::ReturnType>, u64)
where
    T::ReturnType: 'static,
{
    let slot = std::sync::Arc::new(OnceLock::new());
    let seen = slot.clone();
    let mut future: std::pin::Pin<Box<dyn std::future::Future<Output = T>>> = Box::pin(foreign_async_call::<_, T, UniFfiTag>(move |callback, data, _| {
        seen.set((callback, data)).ok().unwrap();
    }));
    let waker = futures_noop();
    let mut context = std::task::Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut context).is_pending());
    let (callback, data) = *slot.get().unwrap();
    (future, callback, data)
}

fn futures_noop() -> std::task::Waker {
    struct Noop;
    impl std::task::Wake for Noop {
        fn wake(self: std::sync::Arc<Self>) {}
    }
    std::task::Waker::from(std::sync::Arc::new(Noop))
}

#[test]
fn a_string_answer_reaches_the_future_through_scalar_arguments() {
    let (future, callback, data) = start::<String>();
    dblayer_complete_foreign_future_buffer(callback as *const c_void, data, <String as Lower<UniFfiTag>>::lower("answer".to_string()), 0, RustBuffer::new());
    assert_eq!(runtime().block_on(future), "answer");
}

#[test]
fn a_unit_answer_reaches_the_future_through_scalar_arguments() {
    let (future, callback, data) = start::<()>();
    dblayer_complete_foreign_future_unit(callback as *const c_void, data, 0, RustBuffer::new());
    runtime().block_on(future);
}
