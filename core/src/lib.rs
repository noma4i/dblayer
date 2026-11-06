#![cfg_attr(test, allow(clippy::disallowed_methods))]
uniffi::setup_scaffolding!();

pub mod identity;
pub mod pacing;
pub mod transport;
pub mod value;
pub mod schema;
pub mod write;
pub mod store;
pub mod read;
pub mod upload;
pub mod ffi;
#[cfg(test)]
mod check;
#[cfg(test)]
pub(crate) mod schema_fixtures;
#[cfg(test)]
mod spec;
pub mod runtime;
pub mod owners;
pub(crate) mod lock;
mod blocking;
mod lifecycle;
mod task;

#[cfg(test)]
mod lib_tests;
#[cfg(test)]
mod test_wait;
