use super::*;

fn id(value: &Value) -> bool {
    value.as_str().is_some_and(|id| !id.is_empty() && id.len() <= 64 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
}

pub(super) fn valid(body: &Value, server: bool) -> bool {
    let Some(fields) = body.as_object() else { return false };
    if !body["n"].is_u64() { return false; }
    let (required, optional): (&[&str], &[&str]) = match (server, body["t"].as_str()) {
        (false, Some("subscribe")) => (&["id", "query"], &["variables", "operationName"]),
        (false, Some("unsubscribe")) => (&["id"], &[]),
        (false, Some("ping")) | (true, Some("pong")) => (&["ts"], &[]),
        (false, Some("close")) => (&[], &[]),
        (true, Some("ready")) => (&["connectionId", "challengeDigest"], &[]),
        (true, Some("subscribed" | "complete")) => (&["id"], &[]),
        (true, Some("next")) => (&["id", "result"], &[]),
        (true, Some("error")) => (&["code", "retryable"], &["id"]),
        (true, Some("closing")) => (&["code", "reason"], &[]),
        _ => return false,
    };
    if !required.iter().all(|name| fields.contains_key(*name)) { return false; }
    fields.iter().all(|(name, value)| {
        if matches!(name.as_str(), "n" | "t") { return true; }
        if !required.contains(&name.as_str()) && !optional.contains(&name.as_str()) { return false; }
        if optional.contains(&name.as_str()) && value.is_null() { return true; }
        match name.as_str() {
            "id" => id(value),
            "query" | "operationName" => value.is_string(),
            "variables" | "result" => value.is_object(),
            "ts" => value.is_i64() || value.is_u64(),
            "retryable" => value.is_boolean(),
            "connectionId" => value.as_str().is_some_and(|text| text.len() == 36 && uuid::Uuid::parse_str(text).is_ok()),
            "challengeDigest" => value.as_str().is_some_and(|text| text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit())),
            "reason" => value.as_str().is_some_and(|text| ["LOGGED_OUT", "ACCOUNT_LOCKED", "SERVER_RESTART", "SUPERSEDED", "RECONNECT"].contains(&text)),
            "code" if body["t"] == "closing" => value.as_u64().is_some_and(|code| [1000, 1001, 1009, 4400, 4401, 4403, 4408, 4409, 4429, 4430, 4500, 4503].contains(&code)),
            "code" => value.as_str().is_some_and(|text| ["INVALID_INPUT", "NOT_FOUND", "OPERATION_NOT_ALLOWED", "TOO_MANY_SUBSCRIPTIONS", "DUPLICATE_ID", "RATE_LIMITED", "QUERY_TOO_COMPLEX", "QUERY_TOO_DEEP", "INTERNAL"].contains(&text)),
            _ => false,
        }
    })
}
