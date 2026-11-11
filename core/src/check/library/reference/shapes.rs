use serde_json::{json, Value};

use crate::check::value::Observation;

/// `freshness expires_in: 5.minutes` of the stand schema (docs/models.md): the same for an empty page.
pub const LIFETIME_MS: i64 = 300_000;
/// `paginate size: 20` of the stand schema.
pub const PAGE_SIZE: u64 = 20;
/// docs/server-sync.md "Retries": a plain load repeats 3 times after a network failure and 2 times after a temporary server failure.
pub const NETWORK_RETRIES: u32 = 3;
pub const SERVER_RETRIES: u32 = 2;
/// docs/server-sync.md "Retries": the delay ceilings in ms, the last one repeats.
pub const BACKOFF_CEILINGS: [u64; 6] = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000];

/// How the stand shows the session request of `login`.
pub const SESSION_OPERATION: &str = "session/social";

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
