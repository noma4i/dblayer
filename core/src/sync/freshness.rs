use std::time::Duration;

use crate::pacing::FreshnessTable;

pub(crate) use crate::pacing::parse_duration;

use super::error::SyncError;

pub(crate) fn stale_window(table: &FreshnessTable, name: &str) -> Result<Duration, SyncError> {
    table
        .resolve(name)
        .ok()
        .or_else(|| parse_duration(name))
        .ok_or_else(|| SyncError::UnknownFreshness(name.to_string()))
}
