use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::types::ValueRef;
use serde_json::{json, Map, Value};

use crate::check::explore::seeded;
use crate::check::model::{Owner, Property, RefModel, Sut};
use crate::check::value::{Input, Observation, Step};
use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::runtime::ManualClock;
use crate::schema::RowSource;
use crate::store::fixtures::snapshot_specs;
use crate::store::Store;

use super::data_sets_tests::deliver;

pub(super) const LANDS: &str = "P-DATA-104 lands";

#[derive(Clone, Copy)]
pub(super) struct Case {
    pub name: &'static str,
    pub sets: bool,
    pub partial: bool,
    pub related: bool,
    pub windows: bool,
    pub order: bool,
    pub tombstone: bool,
}

const NONE: Case = Case { name: "", sets: false, partial: false, related: false, windows: false, order: false, tombstone: false };

pub(super) const CASES: [Case; 6] = [
    Case { name: "sets_full_rows", sets: true, ..NONE },
    Case { name: "sets_partial_projection", sets: true, partial: true, ..NONE },
    Case { name: "sets_related_parent", sets: true, related: true, ..NONE },
    Case { name: "window_members", windows: true, ..NONE },
    Case { name: "window_order", windows: true, order: true, ..NONE },
    Case { name: "tombstone", tombstone: true, ..NONE },
];

struct Row {
    model: &'static str,
    id: &'static str,
    values: Map<String, Value>,
}

fn optional(model: &str) -> &'static [&'static str] {
    match model {
        "chats" => &["title", "lastMessageId", "lastMessageAt"],
        "messages" => &["body", "createdAt", "reactions"],
        _ => &["text"],
    }
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn rows(seed: u64) -> Vec<Row> {
    let (pointer, at) = [("m2", 7), ("m9", 9), ("m2", 5)][(seed % 3) as usize];
    let message = |id: &'static str, at: i64, reactions: Value| Row { model: "messages", id, values: object(json!({"id": id, "chatId": "c1", "body": id, "createdAt": at, "reactions": reactions})) };
    let post = |id: &'static str, group: &str| Row { model: "posts", id, values: object(json!({"id": id, "text": id, "groupId": group})) };
    vec![
        Row { model: "chats", id: "c1", values: object(json!({"id": "c1", "title": "t", "lastMessageId": pointer, "lastMessageAt": at})) },
        message("m1", 1, json!([{}])),
        message("m2", 5, json!([])),
        message("m3", 3, json!([{}, {}])),
        post("p1", "g1"),
        post("p2", "g2"),
        post("p3", "g1"),
    ]
}

fn windows() -> [(&'static str, Value, &'static [&'static str]); 4] {
    [("pager", json!({}), &["p1", "p2", "p3"]), ("evt", json!({}), &["p1", "p2", "p3"]), ("cols", json!({"groupId": "g1"}), &["p1", "p3"]), ("cols", json!({"groupId": "g2"}), &["p2"])]
}

fn field_sets(model: &str, partial: bool, seed: u64) -> Vec<Vec<&'static str>> {
    if model == "chats" {
        let all = vec!["title", "lastMessageId", "lastMessageAt"];
        return if partial { vec![all, vec!["lastMessageId", "lastMessageAt"], vec!["title"], Vec::new()] } else { vec![all] };
    }
    let all = optional(model).to_vec();
    let mut sets = vec![all.clone()];
    if partial {
        let picks: Vec<Input> = (0..all.len() as u64).map(|n| Input::new("drop").with("n", n)).collect();
        let dropped = seeded(&picks, seed, 1)[0].number("n").unwrap() as usize;
        sets.push(all.iter().enumerate().filter(|(index, _)| *index != dropped).map(|(_, field)| *field).collect());
        sets.push(Vec::new());
    }
    sets
}

fn projected(row: &Row, fields: &[Value]) -> Map<String, Value> {
    let keep: Vec<&str> = fields.iter().filter_map(Value::as_str).collect();
    row.values.iter().filter(|(key, _)| !optional(row.model).contains(&key.as_str()) || keep.contains(&key.as_str())).map(|(key, value)| (key.clone(), value.clone())).collect()
}

fn alphabet(case: Case, seed: u64) -> Vec<Input> {
    let all = rows(seed);
    let mut inputs = Vec::new();
    let models: Vec<&str> = [(case.sets, vec!["chats", "messages"]), (case.windows || case.tombstone, vec!["posts"])].into_iter().filter(|(on, _)| *on).flat_map(|(_, models)| models).collect();
    for row in all.iter().filter(|row| models.contains(&row.model)) {
        for fields in field_sets(row.model, case.partial, seed) {
            let fields = json!(fields);
            for path in ["event", "response", "node"] {
                inputs.push(Input::new("deliver").with("path", path).with("model", row.model).with("id", row.id).with("fields", fields.clone()));
            }
            if case.related && row.model == "messages" {
                for chat in field_sets("chats", true, seed) {
                    inputs.push(Input::new("deliver").with("path", "page").with("model", row.model).with("id", row.id).with("fields", fields.clone()).with("related", json!(chat)));
                }
            }
        }
    }
    if case.windows {
        for (index, (name, params, _)) in windows().iter().enumerate() {
            inputs.push(Input::new("deliver").with("path", "page").with("window", *name).with("params", params.clone()).with("index", index as u64));
        }
    }
    if case.tombstone {
        inputs.push(Input::new("deliver").with("path", "delete").with("model", "posts").with("id", "p9"));
    }
    inputs
}

pub(super) struct World {
    case: Case,
    seed: u64,
    alphabet: Vec<Input>,
}

fn open() -> Arc<Store> {
    Arc::new(Store::open_models(None, snapshot_specs(), None, Arc::new(ManualClock::new(Duration::from_millis(1_000_000)))).unwrap())
}

fn value_of(cell: ValueRef<'_>) -> Value {
    match cell {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(number) => json!(number),
        ValueRef::Real(number) => json!(number),
        ValueRef::Text(text) => json!(String::from_utf8_lossy(text)),
        ValueRef::Blob(bytes) => json!(bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
    }
}

fn is_revision(column: &str) -> bool {
    ["revision", "born_rev", "operation_seq", "seq"].contains(&column) || column.starts_with("rev__")
}

fn dump(store: &Store, order: bool) -> Value {
    let guard = store.lock();
    let connection = &guard.connection;
    let mut names = connection.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND ((name LIKE 'row\\_%' ESCAPE '\\' AND name != 'row_alias') OR name IN ('scope_member', 'tombstone', 'scope_revision')) ORDER BY name").unwrap();
    let tables: Vec<String> = names.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    let mut out = Map::new();
    for table in tables {
        if table == "scope_revision" {
            continue;
        }
        let mut statement = connection.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
        let columns: Vec<String> = statement.column_names().iter().map(ToString::to_string).collect();
        let keep: Vec<bool> = columns.iter().map(|column| !(is_revision(column) || (!order && (column == "order_key" || column == "place")))).collect();
        let mut found: Vec<Value> = statement
            .query_map([], |row| {
                let mut cells = Map::new();
                for (index, column) in columns.iter().enumerate().filter(|(index, _)| keep[*index]) {
                    cells.insert(column.clone(), value_of(row.get_ref(index)?));
                }
                Ok(Value::Object(cells))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        found.sort_by_key(ToString::to_string);
        out.insert(table, Value::Array(found));
    }
    Value::Object(out)
}

fn row_value(row: &Row, fields: &[Value]) -> RowValue {
    RowValue { row_id: row.id.into(), values: Value::Object(projected(row, fields)).to_string() }
}

struct Lander {
    store: Arc<Store>,
    batch: u64,
}

impl Lander {
    fn commit(&mut self, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
        self.batch += 1;
        let batch = StoreBatch { author, id: self.batch, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
        self.store.write_batch(&batch, self.store.files(), None).unwrap();
    }

    fn window(&mut self, source: RowSource, key: WindowKey, roots: Vec<RowValue>, related: Vec<(String, RowValue)>) {
        let op = StoreOp::Window { source, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related, total: None, base_revision: self.store.revision_seq().unwrap() };
        self.commit(BatchAuthor::Window, Vec::new(), vec![op]);
    }

    fn deliver(&mut self, seed: u64, input: &Input) {
        let all = rows(seed);
        let path = input.text("path").unwrap();
        if let Some(window) = input.text("window") {
            let (_, params, ids) = &windows()[input.number("index").unwrap() as usize];
            let roots = ids.iter().map(|id| row_value(all.iter().find(|row| row.id == *id).unwrap(), &[json!("text")])).collect();
            let key = WindowKey::relation("posts", window, params.as_object().unwrap());
            return self.window(RowSource::Page, key, roots, Vec::new());
        }
        let model = input.text("model").unwrap();
        let id = input.text("id").unwrap();
        if path == "delete" {
            let event = json!({"op": "delete", "id": id}).to_string();
            return self.commit(BatchAuthor::Feed, vec![(model.into(), event)], Vec::new());
        }
        let row = all.iter().find(|row| row.id == id).unwrap();
        let fields = input.args["fields"].as_array().unwrap();
        let chat = all.iter().find(|row| row.id == "c1").unwrap();
        let related = input.args.get("related").and_then(Value::as_array).map(|chat_fields| vec![("chats".to_string(), row_value(chat, chat_fields))]).unwrap_or_default();
        self.batch += 1;
        deliver(&self.store, self.batch, path, model, row_value(row, fields), related);
    }
}

impl World {
    fn informative(&self, landed: &BTreeSet<usize>) -> bool {
        let full = |input: &Input| input.args.get("fields").and_then(Value::as_array).is_some_and(|fields| fields.len() == optional(input.text("model").unwrap()).len());
        self.alphabet.iter().filter(|input| input.text("model").is_some() && input.text("path") != Some("delete")).all(|row| {
            landed.iter().any(|index| {
                let seen = &self.alphabet[*index];
                seen.text("model") == row.text("model") && seen.text("id") == row.text("id") && full(seen)
            })
        })
    }

    fn canonical(&self, landed: &BTreeSet<usize>) -> Value {
        let mut lander = Lander { store: open(), batch: 0 };
        landed.iter().for_each(|index| lander.deliver(self.seed, &self.alphabet[*index]));
        dump(&lander.store, self.case.order)
    }
}

impl World {
    /// Independent of the order of landing: the rows by the reader, the members of the landed pages, the tombstone of a delivered delete.
    fn snapshot_diff(&self, store: &Store, landed: &BTreeSet<usize>) -> Vec<String> {
        let mut lines = Vec::new();
        let state = dump(store, false);
        let table = |name: &str| -> Vec<Value> { state.get(name).and_then(Value::as_array).cloned().unwrap_or_default() };
        let mut expected: BTreeMap<&str, usize> = BTreeMap::new();
        for index in landed.iter().filter(|index| self.alphabet[**index].text("window").is_some()) {
            for id in windows()[self.alphabet[*index].number("index").unwrap() as usize].2 {
                *expected.entry(id).or_default() += 1;
            }
        }
        for (id, count) in expected {
            let held = table("scope_member").iter().filter(|member| member["row_id"] == json!(id)).count();
            if held < count {
                lines.push(format!("scope_member {id}: landed pages give {count} members, store {held}"));
            }
        }
        let delivered = landed.iter().any(|index| self.alphabet[*index].text("path") == Some("delete"));
        if self.case.tombstone && table("tombstone").len() != usize::from(delivered) {
            lines.push(format!("tombstone: delete delivered {delivered}, store {}", table("tombstone").len()));
        }
        lines
    }
}

struct Landing {
    world: Arc<World>,
    lander: Lander,
    landed: BTreeSet<usize>,
    canonical: BTreeMap<BTreeSet<usize>, Value>,
}

#[async_trait]
impl Sut for Landing {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        self.lander.deliver(self.world.seed, input);
        let index = self.world.alphabet.iter().position(|known| known == input).unwrap();
        self.landed.insert(index);
        let world = self.world.clone();
        let landed = self.landed.clone();
        let store = &self.lander.store;
        let informative = world.informative(&landed);
        let mut observations = vec![Observation::new("state", dump(store, world.case.order))];
        if informative {
            observations.push(Observation::new("snapshot_diff", json!(world.snapshot_diff(store, &landed))));
            observations.push(Observation::new("canonical", self.canonical.entry(landed.clone()).or_insert_with(|| world.canonical(&landed)).clone()));
        }
        observations
    }
}

pub(super) struct SnapshotOwner(pub Arc<World>);

impl SnapshotOwner {
    pub(super) fn new(case: Case, seed: u64) -> Self {
        Self(Arc::new(World { case, seed, alphabet: alphabet(case, seed) }))
    }
}

#[async_trait]
impl Owner for SnapshotOwner {
    fn name(&self) -> &'static str {
        "snapshot"
    }

    fn alphabet(&self) -> Vec<Input> {
        self.0.alphabet.clone()
    }

    fn model(&self) -> Option<Box<dyn RefModel>> {
        None
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(Landing { world: self.0.clone(), lander: Lander { store: open(), batch: 0 }, landed: BTreeSet::new(), canonical: BTreeMap::new() })
    }

    fn properties(&self) -> Vec<Property> {
        vec![Property { name: LANDS, check: lands }]
    }
}

fn observed<'a>(step: &'a Step, kind: &str) -> Option<&'a Value> {
    step.observed.iter().find(|observation| observation.kind == kind).map(|observation| &observation.value)
}

fn lands(steps: &[Step]) -> Result<(), String> {
    let last = steps.last().unwrap();
    let Some(canonical) = observed(last, "canonical") else { return Ok(()) };
    let diff: Vec<&str> = observed(last, "snapshot_diff").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
    if !diff.is_empty() {
        return Err(diff.join(" | "));
    }
    differ(canonical, observed(last, "state").unwrap(), "canonical", "got")
}

fn differ(want: &Value, got: &Value, want_name: &str, got_name: &str) -> Result<(), String> {
    let (want, got) = (want.as_object().unwrap(), got.as_object().unwrap());
    let mut lines = Vec::new();
    for table in want.keys().chain(got.keys()).collect::<BTreeSet<_>>() {
        let side = |tables: &Map<String, Value>| -> BTreeMap<String, Vec<Map<String, Value>>> {
            let mut grouped: BTreeMap<String, Vec<Map<String, Value>>> = BTreeMap::new();
            for row in tables.get(table).and_then(Value::as_array).into_iter().flatten() {
                let row = row.as_object().unwrap();
                let key = row.get("id").map_or_else(|| Value::Object(row.clone()).to_string(), ToString::to_string);
                grouped.entry(key).or_default().push(row.clone());
            }
            grouped
        };
        let (left, right) = (side(want), side(got));
        for key in left.keys().chain(right.keys()).collect::<BTreeSet<_>>() {
            match (left.get(key), right.get(key)) {
                (Some(a), Some(b)) if a == b => {}
                (Some(a), Some(b)) if a.len() == 1 && b.len() == 1 => {
                    for column in a[0].keys().chain(b[0].keys()).collect::<BTreeSet<_>>() {
                        if a[0].get(column) != b[0].get(column) {
                            lines.push(format!("{table} {key} field {column}: {want_name} {} {got_name} {}", a[0].get(column).unwrap_or(&Value::Null), b[0].get(column).unwrap_or(&Value::Null)));
                        }
                    }
                }
                (a, b) => lines.push(format!("{table} {key}: {want_name} {a:?} {got_name} {b:?}")),
            }
        }
    }
    if lines.is_empty() { Ok(()) } else { Err(lines.join(" | ")) }
}
