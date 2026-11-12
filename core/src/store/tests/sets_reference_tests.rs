use std::cmp::Ordering;

use serde_json::{json, Value};

use crate::store::Store;

#[derive(Clone, Debug, PartialEq, Default)]
pub(super) struct Sets {
    pub id: Option<String>,
    pub at: Option<i64>,
    pub seq: Option<i64>,
    pub body: Option<String>,
}

fn numeric(id: &str) -> Option<u64> {
    let plain = !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()) && (id.len() == 1 || !id.starts_with('0'));
    if plain { id.parse().ok() } else { None }
}

pub(super) fn by_id(a: &str, b: &str) -> Ordering {
    match (numeric(a), numeric(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.as_bytes().cmp(b.as_bytes()),
    }
}

fn milliseconds(text: &str) -> Option<i64> {
    text.rsplit(':').next()?.trim_end_matches('Z').parse::<f64>().ok().map(|seconds| (seconds * 1000.0).round() as i64)
}

pub(super) fn milliseconds_of(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_str).and_then(milliseconds)
}

pub(super) fn stored_sets(db: &Store) -> Sets {
    stored_sets_of(db, "c1")
}

pub(super) fn stored_sets_of(db: &Store, chat: &str) -> Sets {
    let row = db.read(&["chats"], |reader| reader.point("chats", chat)).unwrap().unwrap();
    let text = |key: &str| row.get(key).and_then(Value::as_str).map(str::to_string);
    Sets { id: text("lastMessageId"), at: milliseconds_of(row.get("lastMessageAt")), seq: row.get("lastSequenceNumber").and_then(Value::as_i64), body: text("lastMessageBody") }
}

pub(super) fn red_of(owner: &dyn Owner, seed: u64, length: usize) -> Option<String> {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    runtime.block_on(async {
        let divergence = find(owner, 0, &[seed], length).await?;
        let small = minimize(owner, divergence).await;
        let inputs: Vec<String> = small.inputs.iter().map(ToString::to_string).collect();
        let DivergenceKind::Property { name, message } = &small.fault else { return Some(format!("seed {seed}: observations diverged")) };
        Some(format!("seed {seed}: {} steps [{}] {name}: {message}", inputs.len(), inputs.join(", ")))
    })
}
