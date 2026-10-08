use std::time::Duration;

use crate::pacing::FreshnessTable;

use super::error::SyncError;

pub(crate) fn parse_duration(text: &str) -> Option<Duration> {
    let (count, unit) = text.split_once('.')?;
    let count: u64 = count.parse().ok()?;
    let seconds = match unit.trim_end_matches('s') {
        "second" => 1,
        "minute" => 60,
        "hour" => 3600,
        "day" => 86_400,
        _ => return None,
    };
    Some(Duration::from_secs(count * seconds))
}

pub(crate) fn stale_window(table: &FreshnessTable, name: &str) -> Result<Duration, SyncError> {
    table
        .resolve(name)
        .ok()
        .or_else(|| parse_duration(name))
        .ok_or_else(|| SyncError::UnknownFreshness(name.to_string()))
}
