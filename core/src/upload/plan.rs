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

pub fn parse_headers(value: Option<&Value>) -> Vec<(String, String)> {
    match value {
        Some(Value::Object(map)) => map.iter().filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_string()))).collect(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| {
                let name = item.get("name").or_else(|| item.get("key"))?.as_str()?;
                Some((name.to_string(), item.get("value")?.as_str()?.to_string()))
            })
            .collect(),
        _ => Vec::new(),
    }
}
