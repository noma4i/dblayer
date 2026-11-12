use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::check::explore::seeded;
use crate::check::model::{Owner, Property, RefModel, Sut};
use crate::check::value::{Input, Observation, Step};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::data_sets_tests::{deliver, intent_creating, intent_hiding, write};
use super::sets_reference_tests::{derive, is_red, parent_revision, red_of, stored_sets, Child, Server, Sets};

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
            for (at, seq, body) in [0, 1, 3].into_iter().flat_map(|at| [0, 1, 2].into_iter().flat_map(move |seq| ["x", "y"].map(move |body| (at, seq, body)))) {
                inputs.push(Input::new("child").with("path", path).with("id", id).with("at", at as u64).with("seq", seq as u64).with("body", body));
            }
        }
        for pointer in POINTERS {
            for (at, seq) in [(2, 1), (2, 2), (5, 1), (5, 2)] {
                inputs.push(Input::new("parent").with("path", path).with("pointer", pointer).with("at", at as u64).with("seq", seq as u64));
            }
        }
        inputs.push(Input::new("bare").with("path", path));
        for pointer in POINTERS {
            inputs.extend((0..3u64).map(|shape| Input::new("partial").with("path", path).with("pointer", pointer).with("shape", shape)));
        }
        for id in IDS.into_iter().chain(["r1", "r2"]) {
            inputs.push(Input::new("move").with("path", path).with("id", id));
        }
    }
    for op in 3..=4u64 {
        for (id, at, seq) in IDS.into_iter().flat_map(|id| [(2, 1), (9, 3)].map(move |(at, seq)| (id, at, seq))) {
            inputs.push(Input::new("edit").with("op", op).with("id", id).with("at", at as u64).with("seq", seq as u64));
        }
    }
    for op in 5..=6u64 {
        inputs.extend(IDS.into_iter().map(|id| Input::new("hide").with("op", op).with("id", id)));
    }
    for op in 1..=2u64 {
        for device in [0, 9u64] {
            inputs.push(Input::new("temp").with("op", op).with("device", device));
        }
        for (at, seq) in [(1, 0), (3, 2), (3, 0)] {
            inputs.push(Input::new("confirm").with("op", op).with("at", at as u64).with("seq", seq as u64));
        }
    }
    inputs.extend((1..=6u64).map(|op| Input::new("rollback").with("op", op)));
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
    held: BTreeMap<u64, (String, Child)>,
    tombstones: BTreeSet<String>,
    parent_rows: u32,
    reached: BTreeSet<&'static str>,
    history: Vec<Input>,
}

fn number(input: &Input, key: &str) -> i64 {
    input.number(key).unwrap() as i64
}

fn optional(seq: i64) -> Option<i64> {
    (seq != 0).then_some(seq)
}

fn child_row(id: &str, chat: &str, at: Option<i64>, seq: Option<i64>, body: &str) -> RowValue {
    RowValue { row_id: id.into(), values: json!({"id": id, "chatId": chat, "body": body, "createdAt": at, "sequenceNumber": seq}).to_string() }
}

impl Landing {
    fn deliver(&self, path: &str, model: &str, row: RowValue) {
        deliver(&self.store, 1, path, model, row, Vec::new());
    }

    fn locked(&self, id: &str) -> bool {
        self.held.values().any(|(held, _)| held == id)
    }

    fn perform(&mut self, input: &Input) {
        let text = |key: &str| input.text(key).unwrap().to_string();
        match input.name.as_str() {
            "child" => {
                let (path, id, at, seq) = (text("path"), text("id"), optional(number(input, "at")), optional(number(input, "seq")));
                if self.locked(&id) {
                    return;
                }
                let body = format!("{}{}", text("body"), id);
                self.deliver(&path, "messages", child_row(&id, "c1", at, seq, &body));
                if self.tombstones.contains(&id) {
                    if path == "response" {
                        self.reached.insert("tombstone refuses the answer");
                        return;
                    }
                    self.reached.insert("event or page lifts the tombstone");
                    self.tombstones.remove(&id);
                }
                self.children.insert(id.clone(), Child { id, at, seq, body });
            }
            "parent" | "partial" => {
                let pointer = text("pointer");
                let partial = input.name == "partial";
                if self.tombstones.contains(&pointer) || (partial && self.parent_rows > 0) {
                    return;
                }
                let shape = if partial { number(input, "shape") } else { 3 };
                let (at, seq) = if partial { ((shape == 1).then_some(2), (shape == 2).then_some(1)) } else { (Some(number(input, "at")), Some(number(input, "seq"))) };
                let mut values = json!({"id": "c1", "title": "t", "lastMessageId": pointer});
                for (field, value) in [("lastMessageAt", at), ("lastSequenceNumber", seq)] {
                    if let Some(value) = value {
                        values[field] = json!(value);
                    }
                }
                self.deliver(&text("path"), "chats", RowValue { row_id: "c1".into(), values: values.to_string() });
                self.server = Some(Server { id: Some(pointer), at, seq });
                self.parent_rows += 1;
                if partial {
                    self.reached.insert("server row with part of the fields");
                }
            }
            "bare" => self.deliver(&text("path"), "chats", RowValue { row_id: "c1".into(), values: json!({"id": "c1", "title": "t"}).to_string() }),
            "move" => {
                let id = text("id");
                let Some(child) = self.children.get(&id).filter(|child| !child.temp() && !self.locked(&id)).cloned() else { return };
                self.deliver(&text("path"), "messages", child_row(&id, "c2", child.at, child.seq, &child.body));
                self.children.remove(&id);
                self.reached.insert("child moved to another parent");
            }
            "temp" => {
                let (op, device) = (input.number("op").unwrap(), number(input, "device"));
                let id = format!("temp-{op}");
                let row = child_row(&id, "c1", Some(device), None, &format!("t{op}"));
                match self.ops.get(&op) {
                    None => intent_creating(&self.store, op, &row),
                    Some(Op::Open) => write(&self.store, BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "messages".into(), rows: vec![row] }]),
                    Some(Op::Done) => return,
                }
                self.ops.insert(op, Op::Open);
                self.children.insert(id.clone(), Child { id, at: Some(device), seq: None, body: format!("t{op}") });
            }
            "confirm" => {
                let op = input.number("op").unwrap();
                if self.ops.get(&op) != Some(&Op::Open) {
                    return;
                }
                let (temp, real) = (format!("temp-{op}"), format!("r{op}"));
                let (at, seq) = (Some(number(input, "at")), optional(number(input, "seq")));
                let ops = vec![
                    StoreOp::Swap { model: "messages".into(), temp: temp.clone(), real: real.clone() },
                    StoreOp::Rows { model: "messages".into(), rows: vec![child_row(&real, "c1", at, seq, &format!("s{op}"))] },
                    StoreOp::RemoveIntent { op_id: op },
                ];
                write(&self.store, BatchAuthor::Outbox, Vec::new(), ops);
                self.children.remove(&temp);
                self.children.insert(real.clone(), Child { id: real, at, seq, body: format!("s{op}") });
                self.ops.insert(op, Op::Done);
            }
            "edit" | "hide" => {
                let (op, id) = (input.number("op").unwrap(), text("id"));
                let Some(base) = self.children.get(&id).filter(|child| !child.temp() && !self.locked(&id)).cloned() else { return };
                if self.ops.contains_key(&op) {
                    return;
                }
                if input.name == "edit" {
                    let (at, seq) = (number(input, "at"), number(input, "seq"));
                    let body = format!("e{op}");
                    intent_creating(&self.store, op, &child_row(&id, "c1", Some(at), Some(seq), &body));
                    self.children.insert(id.clone(), Child { id: id.clone(), at: Some(at), seq: Some(seq), body });
                } else {
                    intent_hiding(&self.store, op, "messages", &id);
                    self.children.remove(&id);
                }
                self.reached.insert(if input.name == "edit" { "open edit of a confirmed child" } else { "open hide of a confirmed child" });
                self.held.insert(op, (id, base));
                self.ops.insert(op, Op::Open);
            }
            "rollback" => {
                let op = input.number("op").unwrap();
                if self.ops.get(&op) != Some(&Op::Open) {
                    return;
                }
                write(&self.store, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RollBack { op_id: op }, StoreOp::RemoveIntent { op_id: op }]);
                match self.held.remove(&op) {
                    Some((id, base)) => {
                        self.children.insert(id, base);
                        self.reached.insert("rollback of an edit or a hide");
                    }
                    None => {
                        self.children.remove(&format!("temp-{op}"));
                    }
                }
                self.ops.insert(op, Op::Done);
            }
            "remove" => {
                let id = text("id");
                if !self.children.get(&id).is_some_and(|child| !child.temp()) || self.locked(&id) {
                    return;
                }
                write(&self.store, BatchAuthor::Outbox, Vec::new(), vec![StoreOp::RemoveRows { model: "messages".into(), ids: vec![id.clone()] }]);
                self.children.remove(&id);
                self.tombstones.insert(id.clone());
                if self.server.as_ref().is_some_and(|server| server.id.as_deref() == Some(id.as_str())) {
                    self.server = None;
                    self.reached.insert("destroy of the child the server value names");
                }
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
        let mut performed = input.name.clone();
        if input.name == "repeat" {
            if let Some(again) = self.history.len().checked_sub(1 + number(input, "back") as usize % self.history.len().max(1)).map(|at| self.history[at].clone()) {
                performed = again.name.clone();
                self.perform(&again);
            }
        } else {
            self.perform(input);
            self.history.push(input.clone());
        }
        let parent_row = ["parent", "partial", "bare"].contains(&performed.as_str());
        let now = self.wanted();
        vec![
            Observation::new("sets", json!({"want": now.to_json(), "got": stored_sets(&self.store).to_json()})),
            Observation::new("revision", json!({"before": revision, "after": parent_revision(&self.store), "children_same": children == self.children, "sets_same": wanted == now, "parent_row": parent_row})),
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
        Box::new(self.landing().await)
    }

    fn properties(&self) -> Vec<Property> {
        vec![Property { name: EQUALS_F, check: equals_f }, Property { name: SAME_REVISION, check: same_revision }]
    }
}

impl SetsOwner {
    async fn landing(&self) -> Landing {
        let store = Arc::new(Store::open_models(None, all_specs(), None, Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_millis(1_000_000)))).unwrap());
        put(&store, "chats", json!({"id": "c1", "title": "t"}));
        put(&store, "chats", json!({"id": "c2", "title": "t"}));
        Landing { store, server: None, children: BTreeMap::new(), ops: BTreeMap::new(), held: BTreeMap::new(), tombstones: BTreeSet::new(), parent_rows: 0, reached: BTreeSet::new(), history: Vec::new() }
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
    if revision["parent_row"] == json!(true) && revision["children_same"] == json!(true) && revision["sets_same"] == json!(true) && revision["before"] != revision["after"] {
        return Err(format!("revision of the parent moved {} -> {} while the fields and the children stayed", revision["before"], revision["after"]));
    }
    Ok(())
}

fn red() -> Vec<String> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = (1..=SEEDS).map(|seed| scope.spawn(move || is_red(&SetsOwner, seed * 7919, STEPS).then_some(seed * 7919))).collect();
        let seeds: Vec<u64> = handles.into_iter().filter_map(crate::test_wait::join_within).collect();
        let first = seeds.first().and_then(|seed| red_of(&SetsOwner, *seed, STEPS));
        first.into_iter().chain(seeds.iter().skip(1).map(|seed| format!("seed {seed}: red"))).collect()
    })
}

/// P-DATA-103, P-DATA-56: after every step the fields of the parent are `F` of the server value and the stored children, and a row of the parent that leaves `F` alone leaves the revision alone.
#[test]
fn data_sets_property_equals_the_function() {
    let found = red();
    assert!(found.is_empty(), "cells against P-DATA-103:\n{}", found.join("\n"));
}

#[test]
fn data_sets_property_generator_reaches_the_cells() {
    let cells = [
        "server row with part of the fields",
        "child moved to another parent",
        "open edit of a confirmed child",
        "open hide of a confirmed child",
        "rollback of an edit or a hide",
        "destroy of the child the server value names",
        "tombstone refuses the answer",
        "event or page lifts the tombstone",
    ];
    let reached: BTreeSet<&'static str> = std::thread::scope(|scope| {
        let handles: Vec<_> = (1..=SEEDS)
            .map(|seed| {
                scope.spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
                    runtime.block_on(async {
                        let mut landing = SetsOwner.landing().await;
                        for input in &seeded(&alphabet(), seed * 7919, STEPS) {
                            landing.apply(input).await;
                        }
                        landing.reached
                    })
                })
            })
            .collect();
        handles.into_iter().flat_map(crate::test_wait::join_within).collect()
    });
    let missing: Vec<&str> = cells.into_iter().filter(|cell| !reached.contains(cell)).collect();
    assert!(missing.is_empty(), "the traces of the property never reach: {missing:?}");
}
