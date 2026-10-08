mod derive;
mod info;
mod table;

pub use derive::{fold_entries, SyncEntry};
pub use info::{FailureKind, OperationFailure, SyncInfo, SyncStamp, SyncState};
pub use table::{operation_keys, read_sync, rebuild_sync, recompute_sync, SyncKey};

#[cfg(test)]
mod derive_tests;
#[cfg(test)]
mod table_tests;
#[cfg(test)]
mod upload_tests;
