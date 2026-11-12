use std::cmp::Ordering;

use serde_json::{json, Value};

use crate::check::explore::{find, minimize};
use crate::check::model::{DivergenceKind, Owner};
use crate::store::Store;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Child {
    pub id: String,
    pub at: i64,
    pub seq: Option<i64>,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Server {
    pub id: String,
    pub at: i64,
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

fn by_id(a: &str, b: &str) -> Ordering {
    match (a.parse::<i64>(), b.parse::<i64>()) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

pub(super) fn order(a: &Child, b: &Child) -> Ordering {
    a.temp().cmp(&b.temp()).then(a.seq.cmp(&b.seq)).then(a.at.cmp(&b.at)).then_with(|| by_id(&a.id, &b.id))
}

pub(super) fn newest(children: &[Child]) -> Option<&Child> {
    children.iter().max_by(|a, b| order(a, b))
}

fn from_child(child: &Child) -> Sets {
    Sets { id: Some(child.id.clone()), at: Some(child.at), seq: child.seq, body: Some(child.body.clone()) }
}

fn from_server(server: &Server) -> Sets {
    Sets { id: Some(server.id.clone()), at: Some(server.at), seq: server.seq, body: None }
}

fn child_beats_server(child: &Child, server: &Server) -> bool {
    match (child.seq, server.seq) {
        (Some(child), Some(server)) => child > server,
        _ => child.at > server.at,
    }
}

/// P-DATA-59: the value of the fields `sets:` of a parent from the server value of the parent and the stored children.
pub(super) fn derive(server: Option<&Server>, children: &[Child]) -> Sets {
    let Some(newest) = newest(children) else { return server.map(from_server).unwrap_or_default() };
    let won = match server {
        None => from_child(newest),
        Some(server) if server.id == newest.id || (!newest.temp() && !child_beats_server(newest, server)) => {
            return Sets { body: Some(newest.body.clone()), ..from_server(server) };
        }
        Some(server) => Sets { seq: newest.seq.or(server.seq), ..from_child(newest) },
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
    let row = db.read(&["chats"], |reader| reader.point("chats", "c1")).unwrap().unwrap();
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

#[test]
fn data_newer_reference_orders_the_children() {
    let child = |id: &str, at: i64, seq: Option<i64>| Child { id: id.into(), at, seq, body: id.into() };
    let ranked = |mut children: Vec<Child>| {
        children.sort_by(|a, b| order(b, a));
        children.into_iter().map(|child| child.id).collect::<Vec<_>>()
    };
    let cases = [
        ("a temporary id first whatever the times", vec![child("m9", 9, Some(9)), child("temp-1", 0, None)], vec!["temp-1", "m9"]),
        ("the first pair, an absent value is the oldest", vec![child("m1", 9, None), child("m2", 1, Some(1))], vec!["m2", "m1"]),
        ("the second pair when the first is equal", vec![child("m1", 1, Some(2)), child("m2", 3, Some(2))], vec!["m2", "m1"]),
        ("numeric ids as numbers", vec![child("9", 1, None), child("10", 1, None)], vec!["10", "9"]),
        ("text ids as text", vec![child("m2", 1, None), child("m10", 1, None)], vec!["m2", "m10"]),
    ];
    for (name, children, expected) in cases {
        assert_eq!(ranked(children), expected, "{name}");
    }
}
