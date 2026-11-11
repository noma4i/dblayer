use serde_json::{json, Value};

use crate::check::value::Observation;

/// `freshness expires_in: 5.minutes` of the stand schema (docs/models.md): the same for an empty page.
pub const LIFETIME_MS: i64 = 300_000;
/// `paginate size: 20` of the stand schema.
pub const PAGE_SIZE: u64 = 20;
/// Coordinator answers H11, C3: the retry limit counts sends, the first one included. Network 3 sends, server 2 sends, one counter per load, the limit is the one of the class of the current refusal.
pub const NETWORK_SENDS: u32 = 3;
pub const SERVER_SENDS: u32 = 2;

/// Coordinator answer H12: the pause before the retry n is at most `1 s * 2^(n-1)`, never above 30 s (the jitter only shortens it).
pub fn pause_ceiling_ms(attempt: u32) -> u64 {
    (1_000u64 << attempt.saturating_sub(1).min(5)).min(30_000)
}

/// How the stand shows the session request of `login`.
pub const SESSION_OPERATION: &str = "session/api/auth/social";

pub fn error(kind: &str) -> Observation {
    Observation::new("error", json!({ "kind": kind }))
}

/// The call name a load reports under.
pub fn tag(mut item: Observation, call: &str) -> Observation {
    match &mut item.value {
        Value::Object(fields) => {
            fields.insert("call".into(), call.into());
        }
        other => *other = json!({ "call": call }),
    }
    item
}

pub fn request(id: &str) -> Observation {
    Observation::new("request", json!({ "id": id, "kind": "http", "purpose": "standard" }))
}

pub fn sent(id: &str, operation: &str, variables: Value) -> Observation {
    Observation::new("sent", json!({ "id": id, "operation": operation, "variables": variables }))
}

pub fn replied(id: &str, how: &str) -> Observation {
    Observation::new("replied", json!({ "id": id, "how": how }))
}

pub fn session() -> Observation {
    Observation::new("session", json!({ "operation": "social", "answer": { "nextStep": "authenticated" } }))
}
