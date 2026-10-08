uniffi::setup_scaffolding!();

pub mod identity;
pub mod pacing;
pub mod transport;
pub mod value;
pub mod schema;
pub mod ddl;
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

#[uniffi::export]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
