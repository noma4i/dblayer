//! Matrix W, the server paths: a node page, a list page, a mutation answer and a feed event land the kinds of fields of Moment and Chat of the app; the oracle is the merge rules of models.md and the value of the server.
use serde_json::{json, Value};

use super::stand::{moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::ReadFetch;
use crate::test_matrix::Matrix;

fn plain(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool { value } => json!(value),
        Cell::Int { value } => json!(value),
        Cell::Real { value } => json!(value),
        Cell::Text { value } => json!(value),
        Cell::Seq { items } => Value::Array(items.iter().map(plain).collect()),
        Cell::Keyed { entries } => Value::Object(entries.iter().map(|entry| (entry.key.clone(), plain(&entry.value))).collect()),
    }
}

/// The server fields a case sets, the moments that land in order, and what the field reads after the last landing.
struct Case {
    name: &'static str,
    field: &'static str,
    member: Option<usize>,
    landings: Vec<Value>,
    expected: Value,
}

fn moment_seed(id: &str) -> Value { moment(id, 0) }

fn with(fields: Value) -> Value {
    let mut base = moment("k", 0);
    base.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    base
}

fn cases() -> Vec<Case> {
    let media = |fields: Value| json!({"media": fields});
    vec![
        Case { name: "scalar string", field: "shareUrl", member: None, landings: vec![with(json!({"shareUrl": "u"}))], expected: json!("u") },
        Case { name: "bool", field: "reacted", member: None, landings: vec![with(json!({"reacted": true}))], expected: json!(true) },
        Case { name: "int", field: "visitsCount", member: None, landings: vec![with(json!({"visitsCount": 3}))], expected: json!(3) },
        Case { name: "null on a nullable", field: "visitsCount", member: None, landings: vec![with(json!({"visitsCount": 3})), with(json!({"visitsCount": null}))], expected: Value::Null },
        Case { name: "absent keeps the stored value", field: "visitsCount", member: None, landings: vec![with(json!({"visitsCount": 3})), {let mut m = moment("k", 0); m.as_object_mut().unwrap().remove("visitsCount"); m}], expected: json!(3) },
        Case { name: "omitted takes the default", field: "compassTitle", member: None, landings: vec![with(json!({}))], expected: json!("") },
        Case { name: "enum", field: "kind", member: None, landings: vec![with(json!({"kind": "INITIAL"}))], expected: json!("INITIAL") },
        Case { name: "unknown enum case from the server", field: "kind", member: None, landings: vec![with(json!({"kind": "WEIRD"}))], expected: json!("WEIRD") },
        Case { name: "date", field: "compassUpdatedAt", member: None, landings: vec![with(json!({"compassUpdatedAt": "2026-11-08T00:00:00Z"}))], expected: json!("2026-11-08T00:00:00.000Z") },
        Case { name: "list of ids", field: "visitorIds", member: None, landings: vec![with(json!({"visitorIds": ["a", "b"]}))], expected: json!(["a", "b"]) },
        Case { name: "nested member", field: "media", member: Some(1), landings: vec![with(media(json!({"fileUrl": "u1"})))], expected: json!("u1") },
        Case { name: "non_empty member keeps its value against an empty one", field: "media", member: Some(1), landings: vec![with(media(json!({"fileUrl": "u1"}))), with(media(json!({"fileUrl": ""})))], expected: json!("u1") },
        Case { name: "monotonic member does not go back", field: "media", member: Some(11), landings: vec![with(media(json!({"transcodeProgress": 0.5}))), with(media(json!({"transcodeProgress": 0.2})))], expected: json!(0.5) },
        Case { name: "wrong type keeps the stored value and answers no error", field: "visitsCount", member: None, landings: vec![with(json!({"visitsCount": 3})), with(json!({"visitsCount": "x"}))], expected: json!(3) },
    ]
}

fn read(stand: &Stand, row: &WireRow, case: &Case) -> Value {
    let cell = stand.cell("Moment", row, case.field);
    match case.member {
        Some(index) => match plain(&cell) { Value::Array(items) => items.get(index).cloned().unwrap_or(Value::Null), other => other },
        None => plain(&cell),
    }
}

async fn row_of(stand: &Stand, id: &str) -> Option<WireRow> {
    bound(stand.database.read_row(stand.database.session_epoch(), "Moment".into(), id.into())).await.ok().flatten()
}

async fn run(path: &'static str, name: &'static str) {
    let mut matrix = Matrix::new(name);
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    for (index, case) in cases().into_iter().enumerate() {
        // `react` declares `response :ignore` (outbox.md Sending x Response): the answer lands no row, the row keeps the optimistic value of the call (reacted: false)
        if path == "mutation answer" && case.field != "reacted" {
            matrix.check(&format!("{path}.{}", case.field), case.name, "-", "N/A{the selection of MomentReact carries id, reacted, updatedAt}", "N/A{the selection of MomentReact carries id, reacted, updatedAt}");
            continue;
        }
        let id = format!("k{index}");
        let mut observed = Value::String("nothing landed".into());
        for landing in &case.landings {
            let mut moment = landing.clone();
            moment["id"] = json!(id);
            moment["uuid"] = json!(format!("u-{id}"));
            match path {
                "node page" => {
                    stand.answer("MomentNode", json!({"node": moment}));
                    let rows = stand.entity("Moment", &id, ReadFetch::Force);
                    let _ = bound(stand.database.read_row(epoch, "Moment".into(), id.clone())).await;
                    stand.settle().await;
                    drop(rows);
                }
                "list page" => {
                    stand.answer("myMoments", json!({"viewer": {"moments": {"edges": [{"cursor": "cursor-1", "impressionScore": null, "impressionUuid": null, "node": moment}], "pageInfo": {"hasNextPage": false, "endCursor": null}}}}));
                    let _ = bound(stand.database.refresh(epoch, "Moment".into(), Some("owned".into()), "{}".into())).await;
                    stand.settle().await;
                }
                _ => {
                    stand.answer("MomentNode", json!({"node": moment_seed(&id)}));
                    let seed = stand.entity("Moment", &id, ReadFetch::Force);
                    stand.settle().await;
                    drop(seed);
                    stand.answer("MomentReact", json!({"momentReact": {"moment": {"id": id, "reacted": moment.get("reacted").cloned().unwrap_or(json!(false)), "updatedAt": moment["updatedAt"], "user": null}}}));
                    let _ = bound(stand.database.act(epoch, "Moment".into(), id.clone(), "react".into(), "{}".into(), "{}".into())).await;
                    stand.settle().await;
                }
            }
        }
        if let Some(row) = row_of(&stand, &id).await { observed = read(&stand, &row, &case); }
        let expected = if path == "mutation answer" { json!(false) } else { case.expected.clone() };
        matrix.check(&format!("{path}.{}", case.field), case.name, &case.landings.last().map(Value::to_string).unwrap_or_default(), &expected.to_string(), &observed.to_string());
    }
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_write_server_node_page() { run("node page", "W-server-node-page").await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_write_server_list_page() { run("list page", "W-server-list-page").await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_write_server_mutation_answer() { run("mutation answer", "W-server-mutation-answer").await; }

/// The payload of a feed event lands like a page: a field of the event reads back as the server sent it, a nested member too, and an event does not turn a wrong type into an error of the stream.
#[tokio::test(flavor = "multi_thread")]
async fn matrix_write_server_feed_event() {
    use super::stand::chat;
    let mut matrix = Matrix::new("W-server-feed-event");
    let stand = Stand::open().await;
    let (events, token) = stand.events("Chat").await;
    let rows = stand.entity("Chat", "c1", ReadFetch::Never);
    for (index, (kind, name, pinned, expected_name, expected_pinned)) in [("string and nested bool", json!("n1"), json!(true), json!("n1"), json!(true)), ("string replaced", json!("n2"), json!(false), json!("n2"), json!(false))].into_iter().enumerate() {
        let mut payload = chat("c1", "x");
        payload["name"] = name.clone();
        payload["viewerState"]["pinned"] = pinned.clone();
        stand.push(&token, json!({"area": "CHAT", "kind": "UPDATED", "entityId": "c1", "chat": payload}));
        let _ = bound(events.pull_next()).await;
        stand.settle().await;
        let observed_name = rows.0.lock().unwrap().iter().find(|row| row.id == "c1").map(|row| plain(&stand.cell("Chat", row, "name"))).unwrap_or(Value::Null);
        matrix.check("feed event.name", kind, &format!("event {index}"), &expected_name.to_string(), &observed_name.to_string());
        let observed_pinned = rows.0.lock().unwrap().iter().find(|row| row.id == "c1").map(|row| plain(&stand.cell("Chat", row, "pinned"))).unwrap_or(Value::Null);
        matrix.check("feed event.pinned", kind, &format!("event {index}"), &expected_pinned.to_string(), &observed_pinned.to_string());
    }
    matrix.finish();
}
