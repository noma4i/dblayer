use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::check::explore::seeded;
use crate::check::model::{Owner, Property, RefModel, Sut};
use crate::check::value::{Input, Observation, Step};
use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreOp};
use crate::schema::RowSource;
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::data_sets_tests::{intent_creating, write};
use super::sets_reference_tests::{derive, parent_revision, red_of, stored_sets, Child, Server, Sets};

const EQUALS_F: &str = "P-DATA-103 equals F";
const SAME_REVISION: &str = "P-DATA-56 same revision";
const SEEDS: u64 = 24;
const STEPS: usize = 90;
const IDS: [&str; 4] = ["9", "10", "m1", "m2"];
const POINTERS: [&str; 7] = ["9", "10", "m1", "m2", "r1", "r2", "x7"];
const PATHS: [&str; 3] = ["event", "response", "page"];

fn alphabet() -> Vec<Input> {
    let mut inputs = Vec::new();
    for path in PATHS {
        for id in IDS {
            for (at, seq, body) in [1, 3].into_iter().flat_map(|at| [0, 1, 2].into_iter().flat_map(move |seq| ["x", "y"].map(move |body| (at, seq, body)))) {
                inputs.push(Input::new("child").with("path", path).with("id", id).with("at", at as u64).with("seq", seq as u64).with("body", body));
            }
        }
        for pointer in POINTERS {
            for (at, seq) in [(2, 1), (2, 2), (5, 1), (5, 2)] {
                inputs.push(Input::new("parent").with("path", path).with("pointer", pointer).with("at", at as u64).with("seq", seq as u64));
            }
        }
        inputs.push(Input::new("bare").with("path", path));
    }
    for op in 1..=2u64 {
        for device in [0, 9u64] {
            inputs.push(Input::new("temp").with("op", op).with("device", device));
        }
        for (at, seq) in [(1, 0), (3, 2), (3, 0)] {
            inputs.push(Input::new("confirm").with("op", op).with("at", at as u64).with("seq", seq as u64));
        }
        inputs.push(Input::new("rollback").with("op", op));
    }
    for id in IDS.into_iter().chain(["r1", "r2"]) {
        inputs.push(Input::new("remove").with("id", id));
    }
    inputs.extend((0..6u64).map(|back| Input::new("repeat").with("back", back)));
    inputs
}

#[derive(Clone, PartialEq)]
enum Op {
    Open,
    Done,
}

struct Landing {
    store: Arc<Store>,
    server: Option<Server>,
    children: BTreeMap<String, Child>,
    ops: BTreeMap<u64, Op>,
    history: Vec<Input>,
}

fn number(input: &Input, key: &str) -> i64 {
    input.number(key).unwrap() as i64
}

fn optional(seq: i64) -> Option<i64> {
    (seq != 0).then_some(seq)
}

fn child_row(id: &str, at: i64, seq: Option<i64>, body: &str) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": "c1", "body": body, "createdAt": at, "sequenceNumber": seq}).to_string() }
}

impl Landing {
    fn deliver(&self, path: &str, model: &str, row: RowValue) {
        match path {
            "event" => {
                let event = json!({"op": "upsert", "id": row.row_id, "row": serde_json::from_str::<Value>(&row.values).unwrap()}).to_string();
                write(&self.store, BatchAuthor::Feed, vec![(model.into(), event)], Vec::new());
            }
            "response" => write(&self.store, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: model.into(), rows: vec![row] }]),
            _ => {
                let key = WindowKey::nodes(model, &[row.row_id.as_str()]);
                let op = StoreOp::Window { source: RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row], related: Vec::new(), total: None, base_revision: self.store.revision_seq().unwrap() };
                write(&self.store, BatchAuthor::Window, Vec::new(), vec![op]);
            }
        }
    }

    fn perform(&mut self, input: &Input) {
        let text = |key: &str| input.text(key).unwrap().to_string();
        match input.name.as_str() {
            "child" => {
                let (id, at, seq) = (text("id"), number(input, "at"), optional(number(input, "seq")));
                let body = format!("{}{}", text("body"), id);
                self.deliver(&text("path"), "messages", child_row(&id, at, seq, &body));
                self.children.insert(id.clone(), Child { id, at, seq, body });
            }
            "parent" => {
                let (pointer, at, seq) = (text("pointer"), number(input, "at"), number(input, "seq"));
                let values = json!({"id": "c1", "title": "t", "lastMessageId": pointer, "lastMessageAt": at, "lastSequenceNumber": seq});
                self.deliver(&text("path"), "chats", RowValue { row_id: "c1".into(), values: values.to_string() });
                self.server = Some(Server { id: pointer, at, seq: Some(seq) });
            }
            "bare" => self.deliver(&text("path"), "chats", RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t"}).to_string() }),
            "temp" => {
                let (op, device) = (input.number("op").unwrap(), number(input, "device"));
                let id = format!("temp-{op}");
                let row = child_row(&id, device, None, &format!("t{op}"));
                match self.ops.get(&op) {
                    None => intent_creating(&self.store, op, &row),
                    Some(Op::Open) => write(&self.store, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![row] }]),
                    Some(Op::Done) => return,
                }
                self.ops.insert(op, Op::Open);
                self.children.insert(id.clone(), Child { id, at: device, seq: None, body: format!("t{op}") });
            }
            "confirm" => {
                let op = input.number("op").unwrap();
                if self.ops.get(&op) != Some(&Op::Open) {
                    return;
                }
                let (temp, real) = (format!("temp-{op}"), format!("r{op}"));
                let (at, seq) = (number(input, "at"), optional(number(input, "seq")));
                let ops = vec![
                    StoreOp::Swap { model: "messages".into(), temp: temp.clone(), real: real.clone() },
                    StoreOp::Rows { model: "messages".into(), rows: vec![child_row(&real, at, seq, &format!("s{op}"))] },
                    StoreOp::RemoveIntent { op_id: op },
                ];
                write(&self.store, BatchAuthor::Outbox, Vec::new(), ops);
                self.children.remove(&temp);
                self.children.insert(real.clone(), Child { id: real, at, seq, body: format!("s{op}") });
                self.ops.insert(op, Op::Done);
            }
            "rollback" => {
                let op = input.number("op").unwrap();
                if self.ops.get(&op) != Some(&Op::Open) {
                    return;
                }
                write(&self.store, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: op }, StoreOp::RemoveIntent { op_id: op }]);
                self.children.remove(&format!("temp-{op}"));
                self.ops.insert(op, Op::Done);
            }
            "remove" => {
                let id = text("id");
                if !self.children.get(&id).is_some_and(|child| !child.temp()) {
                    return;
                }
                write(&self.store, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec![id.clone()] }]);
                self.children.remove(&id);
            }
            other => panic!("no input {other}"),
        }
    }

    fn wanted(&self) -> Sets {
        derive(self.server.as_ref(), &self.children.values().cloned().collect::<Vec<_>>())
    }
}

#[async_trait]
impl Sut for Landing {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        let (revision, children, wanted) = (parent_revision(&self.store), self.children.clone(), self.wanted());
        if input.name == "repeat" {
            if let Some(again) = self.history.len().checked_sub(1 + number(input, "back") as usize % self.history.len().max(1)).map(|at| self.history[at].clone()) {
                self.perform(&again);
            }
        } else {
            self.perform(input);
            self.history.push(input.clone());
        }
        let now = self.wanted();
        vec![
            Observation::new("sets", json!({"want": now.to_json(), "got": stored_sets(&self.store).to_json()})),
            Observation::new("revision", json!({"before": revision, "after": parent_revision(&self.store), "children_same": children == self.children, "sets_same": wanted == now})),
        ]
    }
}

struct SetsOwner;

#[async_trait]
impl Owner for SetsOwner {
    fn name(&self) -> &'static str {
        "sets"
    }

    fn alphabet(&self) -> Vec<Input> {
        alphabet()
    }

    fn model(&self) -> Option<Box<dyn RefModel>> {
        None
    }

    async fn real(&self) -> Box<dyn Sut> {
        let store = Arc::new(Store::open_models(None, all_specs(), None, Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_millis(1_000_000)))).unwrap());
        put(&store, "chats", json!({"id": "c1", "title": "t"}));
        Box::new(Landing { store, server: None, children: BTreeMap::new(), ops: BTreeMap::new(), history: Vec::new() })
    }

    fn properties(&self) -> Vec<Property> {
        vec![Property { name: EQUALS_F, check: equals_f }, Property { name: SAME_REVISION, check: same_revision }]
    }
}

fn last(steps: &[Step], kind: &str) -> Value {
    steps.last().unwrap().observed.iter().find(|observation| observation.kind == kind).unwrap().value.clone()
}

fn equals_f(steps: &[Step]) -> Result<(), String> {
    let sets = last(steps, "sets");
    let (want, got) = (sets["want"].as_object().unwrap(), sets["got"].as_object().unwrap());
    let lines: Vec<String> = want.iter().filter(|(field, value)| got[*field] != **value).map(|(field, value)| format!("field {field}: want {value} got {}", got[field])).collect();
    if lines.is_empty() { Ok(()) } else { Err(lines.join(", ")) }
}

fn same_revision(steps: &[Step]) -> Result<(), String> {
    let revision = last(steps, "revision");
    if revision["children_same"] == json!(true) && revision["sets_same"] == json!(true) && revision["before"] != revision["after"] {
        return Err(format!("revision of the parent moved {} -> {} while the fields and the children stayed", revision["before"], revision["after"]));
    }
    Ok(())
}

fn red() -> Vec<String> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = (1..=SEEDS).map(|seed| scope.spawn(move || red_of(&SetsOwner, seed * 7919, STEPS))).collect();
        handles.into_iter().filter_map(|handle| handle.join().unwrap()).collect()
    })
}

/// P-DATA-103, P-DATA-56: after every step the fields of the parent are `F` of the server value and the stored children, and a step that leaves `F` and the children alone leaves the revision alone.
#[test]
fn data_sets_property_equals_the_function() {
    let found = red();
    assert!(found.is_empty(), "cells against P-DATA-103:\n{}", found.join("\n"));
}

/// The generator reaches every kind of step and the reference sees a pending child, a server value and a destroy.
#[test]
fn data_sets_property_generator_reaches_the_cells() {
    let inputs = seeded(&alphabet(), 7919, 400);
    let kinds: std::collections::BTreeSet<&str> = inputs.iter().map(|input| input.name.as_str()).collect();
    assert_eq!(kinds.len(), 8, "{kinds:?}");
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let reached = runtime.block_on(async {
        let mut real = SetsOwner.real().await;
        let mut seen = std::collections::BTreeSet::new();
        for input in &inputs {
            let observed = real.apply(input).await;
            let want = &observed[0].value["want"];
            seen.insert(want["lastMessageId"].as_str().map(|id| if id.starts_with("temp-") { "temp" } else if id.starts_with('r') { "confirmed" } else { "plain" }));
        }
        seen
    });
    assert!(reached.contains(&Some("temp")) && reached.contains(&Some("confirmed")) && reached.contains(&Some("plain")), "{reached:?}");
}
