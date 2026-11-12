use std::cmp::Ordering;

use serde_json::{json, Value};

use crate::check::explore::{find, minimize};
use crate::check::model::{DivergenceKind, Owner};
use crate::store::Store;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Child {
    pub id: String,
    pub at: Option<i64>,
    pub seq: Option<i64>,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Server {
    pub id: Option<String>,
    pub at: Option<i64>,
    pub seq: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(super) struct Sets {
    pub id: Option<String>,
    pub at: Option<i64>,
    pub seq: Option<i64>,
    pub body: Option<String>,
}

impl Child {
    pub(super) fn temp(&self) -> bool {
        self.id.starts_with("temp-")
    }
}

impl Sets {
    pub(super) fn to_json(&self) -> Value {
        json!({"lastMessageId": self.id, "lastMessageAt": self.at, "lastSequenceNumber": self.seq, "lastMessageBody": self.body})
    }
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

pub(super) fn order(a: &Child, b: &Child) -> Ordering {
    a.temp().cmp(&b.temp()).then(a.seq.cmp(&b.seq)).then(a.at.cmp(&b.at)).then_with(|| by_id(&a.id, &b.id))
}

pub(super) fn newest(children: &[Child]) -> Option<&Child> {
    children.iter().max_by(|a, b| order(a, b))
}

fn from_child(child: &Child) -> Sets {
    Sets { id: Some(child.id.clone()), at: child.at, seq: child.seq, body: Some(child.body.clone()) }
}

fn from_server(server: &Server) -> Sets {
    Sets { id: server.id.clone(), at: server.at, seq: server.seq, body: None }
}

fn child_beats_server(child: &Child, server: &Server) -> bool {
    match (child.seq, server.seq, child.at, server.at) {
        (Some(child), Some(server), _, _) => child > server,
        (_, _, Some(child), Some(server)) => child > server,
        _ => false,
    }
}

/// P-DATA-59: the value of the fields `sets:` of a parent from the server value of the parent and the stored children. A field the winner lacks comes from the other side.
pub(super) fn derive(server: Option<&Server>, children: &[Child]) -> Sets {
    let Some(newest) = newest(children) else { return server.map(from_server).unwrap_or_default() };
    let won = match server {
        None => from_child(newest),
        Some(server) if server.id.as_deref() == Some(newest.id.as_str()) || (!newest.temp() && !child_beats_server(newest, server)) => {
            return Sets { id: server.id.clone().or_else(|| Some(newest.id.clone())), at: server.at.or(newest.at), seq: server.seq.or(newest.seq), body: Some(newest.body.clone()) };
        }
        Some(server) => Sets { at: newest.at.or(server.at), seq: newest.seq.or(server.seq), ..from_child(newest) },
    };
    if !newest.temp() {
        return won;
    }
    let confirmed: Vec<Child> = children.iter().filter(|child| !child.temp()).cloned().collect();
    let behind = derive(server, &confirmed);
    Sets { at: won.at.max(behind.at), seq: won.seq.max(behind.seq), ..won }
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

pub(super) fn parent_revision(db: &Store) -> i64 {
    crate::store::fixtures::count(db, "SELECT revision FROM row_chats WHERE id = 'c1'")
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
