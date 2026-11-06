use serde_json::Value;

use super::state::UploadScenario;

pub fn scenario_for(total: u64, threshold: u64) -> UploadScenario {
    if total >= threshold {
        UploadScenario::Multipart
    } else {
        UploadScenario::Single
    }
}

pub fn part_count(total: u64, part_size: u64) -> u32 {
    u32::try_from(total.div_ceil(part_size.max(1)).max(1)).unwrap_or(u32::MAX)
}

pub fn part_range(total: u64, part_size: u64, number: u32) -> (u64, u64) {
    let offset = (u64::from(number) - 1) * part_size;
    (offset, part_size.min(total.saturating_sub(offset)))
}

pub fn bytes_sent(info: &super::state::UploadInfo, size: u64) -> u64 {
    match info.status {
        super::state::UploadStatus::Sending | super::state::UploadStatus::Done => size,
        _ if info.total_bytes > 1 => (1..=info.sent_bytes).map(|number| part_range(size, super::config::UploadConfig::default().part_size, u32::try_from(number).unwrap_or(u32::MAX)).1).sum(),
        _ => 0,
    }
}

pub fn mime_of(name: &str) -> &'static str {
    let extension = name.rsplit_once('.').map(|(_, extension)| extension.to_ascii_lowercase()).unwrap_or_default();
    match extension.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "heic" => "image/heic",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "m4a" => "audio/mp4",
        "mp3" => "audio/mpeg",
        "aac" => "audio/aac",
        "wav" => "audio/wav",
        "pdf" => "application/pdf",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
}

pub fn parse_headers(value: Option<&Value>) -> Result<Vec<(String, String)>, String> {
    let invalid = || "upload headers must contain string names and values".to_string();
    match value {
        Some(Value::Object(map)) => map.iter().map(|(name, value)| Ok((name.clone(), value.as_str().ok_or_else(invalid)?.to_string()))).collect(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                let name = item.get("name").or_else(|| item.get("key")).and_then(Value::as_str).ok_or_else(invalid)?;
                let value = item.get("value").and_then(Value::as_str).ok_or_else(invalid)?;
                Ok((name.to_string(), value.to_string()))
            })
            .collect(),
        Some(Value::String(text)) => {
            let decoded = serde_json::from_str(text).map_err(|error| format!("invalid upload headers JSON: {error}"))?;
            match decoded { Value::Object(_) | Value::Array(_) => parse_headers(Some(&decoded)), _ => Err(invalid()) }
        }
        None | Some(Value::Null) => Ok(Vec::new()),
        _ => Err(invalid()),
    }
}
