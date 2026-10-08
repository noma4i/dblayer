use std::time::Duration;

pub fn parse_duration(text: &str) -> Option<Duration> {
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
