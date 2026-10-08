use std::time::Duration;

pub fn selected_policy<'a>(is_empty_result: bool, landed: &'a str, empty: Option<&'a str>) -> &'a str {
    match empty {
        Some(empty) if is_empty_result => empty,
        _ => landed,
    }
}

pub fn is_fresh(landed_at: Duration, now: Duration, window: Duration) -> bool {
    now.saturating_sub(landed_at) < window
}

pub fn expires_at(landed_at: Duration, window: Duration) -> Duration {
    landed_at + window
}
