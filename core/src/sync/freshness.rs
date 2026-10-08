use std::time::Duration;

pub(crate) use crate::pacing::parse_duration;

use super::error::SyncError;

pub(crate) fn stale_window(name: &str) -> Result<Duration, SyncError> {
    parse_duration(name).ok_or_else(|| SyncError::UnknownFreshness(name.to_string()))
}
