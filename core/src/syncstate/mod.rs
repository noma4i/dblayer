mod derive;
mod info;
mod table;

pub use derive::{fold_entries, SyncEntry};
pub use info::{FailureKind, OperationFailure, SyncInfo, SyncState};
pub use table::{clear_sync, operation_keys, read_sync, rebuild_sync, rebuild_sync_with, recompute_sync, recompute_sync_with, SyncKey};

#[cfg(test)]
mod derive_tests;
#[cfg(test)]
mod table_tests;
#[cfg(test)]
mod upload_tests;
