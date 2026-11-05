//! The lease type that the read and write owners take in their ports. It lives in `ffi/admission.rs` until the old stack is gone.

pub use crate::ffi::admission::Admitted;

pub(crate) mod authority;
