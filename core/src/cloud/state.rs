#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum CloudState {
    Idle,
    Pending { changes: u64 },
    Syncing,
    Offline,
    NeedsAuthorization,
    QuotaExceeded,
    NeedsInitialMerge { rows: u64 },
    Failed { detail: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MergeSource {
    UseLocal,
    UseCloud,
}
