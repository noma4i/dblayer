use super::*;
use crate::store::Record;
use crate::identity::RowId;
use crate::schema::{resolve_types, EphemeralSpec, FieldSpec, ModelSpec};
use crate::value::sync::SyncInfo;
use crate::value::ScalarKind;
use serde_json::{json, Value};
use std::sync::Arc;

fn typed(mut field: FieldSpec, name: &str, children: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.to_string());
    field.fields = children;
    field
}

fn spec() -> ModelSpec {
    let item = vec![
        FieldSpec::scalar("title", ScalarKind::Str),
        FieldSpec::scalar("price", ScalarKind::Num).nullable(),
        FieldSpec::list("tags", ScalarKind::Str),
        typed(FieldSpec::object("size"), "WireSize", vec![FieldSpec::scalar("w", ScalarKind::Int), FieldSpec::scalar("h", ScalarKind::Int)]),
    ];
    let node = vec![FieldSpec::scalar("name", ScalarKind::Str), typed(FieldSpec::array("children"), "WireNode", vec![])];
    let mut owner = FieldSpec::scalar("ownerId", ScalarKind::Id);
    owner.from = Some("owner_id".to_string());
    let mut spec = ModelSpec::new(
        "wires",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str),
            FieldSpec::scalar("count", ScalarKind::Int).nullable(),
            FieldSpec::scalar("ratio", ScalarKind::Num).nullable(),
            FieldSpec::scalar("flag", ScalarKind::Bool).nullable(),
            FieldSpec::scalar("at", ScalarKind::Date).nullable(),
            FieldSpec::enumeration("state", &["ready", "done"]).nullable(),
            FieldSpec::list("labels", ScalarKind::Int).nullable(),
            typed(FieldSpec::array("items"), "WireItem", item).nullable(),
            typed(FieldSpec::object("tree"), "WireNode", node).nullable(),
            typed(FieldSpec::object("meta"), "WireMeta", vec![owner, FieldSpec::enumeration("kind", &["a"]).nullable()]).nullable(),
            FieldSpec::raw("data").nullable(),
            FieldSpec::object("free").nullable(),
            FieldSpec::file("photo").nullable(),
        ],
    );
    spec.ephemeral = vec![EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).nullable())];
    resolve_types(std::slice::from_mut(&mut spec));
    spec
}

fn record(data: Value) -> Record {
    Record { id: RowId::new("w1").unwrap(), revision: 1, data: data.as_object().unwrap().clone(), sync: SyncInfo::default() }
}

fn text(value: &str) -> Cell {
    Cell::Text { value: value.to_string() }
}

fn int(value: i64) -> Cell {
    Cell::Int { value }
}

fn seq(items: Vec<Cell>) -> Cell {
    Cell::Seq { items }
}

fn keyed(entries: Vec<(&str, Cell)>) -> Cell {
    Cell::Keyed { entries: entries.into_iter().map(|(key, value)| CellEntry { key: key.to_string(), value }).collect() }
}

fn cells(data: Value) -> Vec<Cell> {
    WireModel::of(&spec()).row(&Arc::new(record(data))).cells
}

fn names() -> Vec<String> {
    wire_fields(&spec()).iter().map(|field| field.name.clone()).collect()
}

#[test]
fn the_cells_follow_the_declared_order_and_end_with_the_ephemeral_fields() {
    assert_eq!(names(), ["title", "count", "ratio", "flag", "at", "state", "labels", "items", "tree", "meta", "data", "free", "photo", "typing"]);
}

#[test]
fn every_field_kind_becomes_the_cell_the_decoder_reads() {
    let row = cells(json!({
        "title": "t", "count": 3, "ratio": 1.5, "flag": true, "at": "2026-10-14T00:00:00.000Z", "state": "done", "labels": [1, 2],
        "items": [{"title": "a", "price": 2.0, "tags": ["x"], "size": {"w": 1, "h": 2}}, {"title": "b", "tags": [], "size": {"w": 3}}],
        "tree": {"name": "root", "children": [{"name": "a", "children": [{"name": "a1", "children": []}]}, {"name": "b", "children": []}]},
        "meta": {"owner_id": "u9"},
        "data": {"k": [1, "x", null, {"deep": true}]},
        "free": {"n": 1.25},
        "photo": {"id": "f1", "size": 10},
        "typing": true,
    }));
    let expected = vec![
        text("t"),
        int(3),
        Cell::Real { value: 1.5 },
        Cell::Bool { value: true },
        text("2026-10-14T00:00:00.000Z"),
        text("done"),
        seq(vec![int(1), int(2)]),
        seq(vec![
            seq(vec![text("a"), Cell::Real { value: 2.0 }, seq(vec![text("x")]), seq(vec![int(1), int(2)])]),
            seq(vec![text("b"), Cell::Null, seq(vec![]), seq(vec![int(3), Cell::Null])]),
        ]),
        seq(vec![text("root"), seq(vec![seq(vec![text("a"), seq(vec![seq(vec![text("a1"), seq(vec![])])])]), seq(vec![text("b"), seq(vec![])])])]),
        seq(vec![text("u9"), Cell::Null]),
        keyed(vec![("k", seq(vec![int(1), text("x"), Cell::Null, keyed(vec![("deep", Cell::Bool { value: true })])]))]),
        keyed(vec![("n", Cell::Real { value: 1.25 })]),
        keyed(vec![("id", text("f1")), ("size", int(10))]),
        Cell::Bool { value: true },
    ];
    assert_eq!(row, expected);
}

#[test]
fn null_and_absent_are_the_same_cell() {
    let absent = cells(json!({"title": "t"}));
    let nulled = cells(json!({"title": "t", "count": null, "labels": null, "items": null, "tree": null, "data": null, "typing": null}));
    assert_eq!(absent, nulled);
    assert_eq!(absent[0], text("t"));
    assert!(absent[1..].iter().all(|cell| *cell == Cell::Null));
}

#[test]
fn the_row_carries_its_id_sync_state_and_upload() {
    let mut data = record(json!({"title": "t"}));
    data.sync.state = crate::value::sync::SyncState::Failed;
    data.sync.actions = vec!["update".to_string()];
    data.sync.dirty_fields = vec!["title".to_string()];
    data.sync.error = Some(crate::value::sync::OperationFailure::new(crate::value::sync::FailureKind::Server, "http 503"));
    data.sync.attempts = 2;
    data.sync.since = 7;
    data.sync.uploads.insert("upload".to_string(), crate::upload::UploadInfo {
        status: crate::upload::UploadStatus::Uploading,
        stage: crate::upload::UploadStage::Part,
        sent_bytes: 40,
        total_bytes: 90,
        wait_reason: Some(crate::upload::WaitReason::CreateUnconfirmed),
        attempts: 1,
        error: None,
        local_url: Some("file:///copy".to_string()),
        processing: Some(crate::upload::UploadProcessing { progress: Some(0.5) }),
        copy_file: None,
    });
    let wire = WireModel::of(&spec()).row(&Arc::new(data));
    assert_eq!(wire.id, "w1");
    assert_eq!(
        wire.sync,
        WireSync {
            state: WireSyncState::Failed,
            actions: vec!["update".into()],
            dirty_fields: vec!["title".into()],
            error: Some(WireFailure { kind: "server".into(), session_code: None, request_size: None, request_limit: None, message: "http 503".into(), extensions: None }),
            attempts: 2,
            since: 7,
        }
    );
    assert_eq!(
        wire.uploads,
        vec![WireUploadEntry {
            action: "upload".into(),
            upload: WireUpload {
                status: WireUploadStatus::Uploading,
                stage: "part".into(),
                sent_bytes: 40,
                total_bytes: 90,
                wait_reason: Some("create_unconfirmed".into()),
                attempts: 1,
                error: None,
                local_url: Some("file:///copy".into()),
                processing: Some(WireProcessing { progress: Some(0.5) }),
            },
        }]
    );
}

#[test]
fn the_layout_hash_follows_the_order_and_the_shape_of_the_fields() {
    let base = wire_layout(&spec());
    assert_eq!(base.len(), 64);
    assert_eq!(base, wire_layout(&spec()));
    let mut swapped = spec();
    swapped.fields.swap(1, 2);
    assert_ne!(base, wire_layout(&swapped));
    let mut reshaped = spec();
    reshaped.fields[1] = FieldSpec::scalar("title", ScalarKind::Int);
    assert_ne!(base, wire_layout(&reshaped));
    let mut nullable_only = spec();
    nullable_only.fields[1].nullable = true;
    assert_eq!(base, wire_layout(&nullable_only));
    assert_eq!(WireModel::of(&spec()).row(&Arc::new(record(json!({})))).layout, base);
}
