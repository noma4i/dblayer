use std::time::Duration;

pub fn parse_duration(text: &str) -> Option<Duration> {
    let (count, unit) = text.split_once('.')?;
    let count: u64 = count.parse().ok()?;
    let milliseconds = match unit.trim_end_matches('s') {
        "millisecond" => 1,
        "second" => 1000,
        "minute" => 60_000,
        "hour" => 3_600_000,
        "day" => 86_400_000,
        _ => return None,
    };
    Some(Duration::from_millis(count * milliseconds))
}
