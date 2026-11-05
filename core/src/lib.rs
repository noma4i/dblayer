#![cfg_attr(test, allow(clippy::disallowed_methods))]
uniffi::setup_scaffolding!();

pub mod identity;
pub mod pacing;
pub mod transport;
pub mod value;
pub mod schema;
pub mod ddl;
pub mod nodes;
pub mod operation;
pub mod syncstate;
pub mod write;
pub mod commit;
pub mod store;
pub mod query;
pub mod read;
pub mod db;
pub mod files;
pub mod sync;
pub mod upload;
pub mod ffi;
#[cfg(test)]
mod check;
pub mod runtime;
mod blocking;
mod lifecycle;
mod task;

#[cfg(test)]
mod lib_tests;
#[cfg(test)]
mod test_wait;
