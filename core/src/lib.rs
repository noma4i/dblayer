#![cfg_attr(test, allow(clippy::disallowed_methods))]
uniffi::setup_scaffolding!();

pub(crate) mod cloud;
pub mod identity;
pub mod transport;
pub mod value;
pub mod schema;
pub mod store;
pub mod ffi;
#[cfg(test)]
mod check;
#[cfg(test)]
pub(crate) mod schema_fixtures;
#[cfg(test)]
mod spec;
pub(crate) mod server_contract;
pub mod runtime;
pub mod owners;
pub(crate) mod lock;
mod blocking;
mod task;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod test_wait;
