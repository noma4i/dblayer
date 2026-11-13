const TECHNICAL: [&str; 8] = ["major_brand", "minor_version", "compatible_brands", "handler_name", "vendor_id", "encoder", "©too", "tsse"];

pub fn video_metadata_to_strip(keys: Vec<String>) -> Vec<String> {
    keys.into_iter()
        .filter(|key| {
            let name = key_name(key);
            !name.is_empty() && !TECHNICAL.contains(&name.as_str())
        })
        .collect()
}

fn key_name(key: &str) -> String {
    let lower = key.trim().to_lowercase();
    match lower.split_once('/') {
        Some((scheme, name)) if scheme.chars().count() <= 4 => name.to_string(),
        _ => lower,
    }
}
