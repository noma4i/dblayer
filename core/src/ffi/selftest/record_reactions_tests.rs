use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::cell;
use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Fault, Gate, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::{CommandRequest, WireOperationOutcome, WireOperationReceipt, WireSyncState};
use crate::test_wait::yield_until;

const OPERATIONS: &str = r#"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"RecordReact","document":"mutation RecordReact($id: ID!, $reactionBody: String!, $userId: ID!) { recordReact(id: $id, reactionBody: $reactionBody, userId: $userId) { id containerId authorId text createdAt reactions { body userId } } }"}]"#;

const PATCH: &str = r#"{"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"reactionBody"},"userId":{"$arg":"userId"}}}}}"#;

fn record(reactions: Value) -> Value {
    json!({"__typename":"Record","id":"x1","containerId":"c1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":reactions})
}

fn created(reactions: Value) -> Value { json!({"data":{"createRecord":record(reactions)}}) }

fn reacted(reactions: Value) -> Fault { Fault::Answer(json!({"data":{"recordReact":record(reactions)}})) }

struct Session {
    rig: Rig,
    epoch: u64,
    list: Arc<Rows>,
}

impl Session {
    async fn settled(created: Value) -> Session {
        let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
        rig.database.register_operations(OPERATIONS.into()).await.unwrap();
        rig.world.answer_with("CreateRecord", created);
        rig.world.answer_with("RecordReact", json!({"data":{"recordReact":record(json!([]))}}));
        let epoch = rig.database.session_epoch();
        let list = Arc::new(Rows::default());
        drop(rig.database.watch_query(r#"{"model":"Record"}"#, list.clone()));
        let values = json!({"containerId":"c1","authorId":"u1","text":"hello"}).to_string();
        let sent = bound(rig.database.send(CommandRequest { epoch, model: "Record".into(), values_json: values, args_json: "{}".into(), upload: None })).await.unwrap();
        assert!(matches!(bound(rig.database.outcome(sent)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
        yield_until("the confirmed record is listed", || list.0.lock().unwrap().iter().any(|row| row.id == "x1")).await;
        Session { rig, epoch, list }
    }

    fn shown(&self) -> WireRow { self.list.0.lock().unwrap().iter().find(|row| row.id == "x1").unwrap().clone() }

    async fn react(&self, body: &str, user: &str) -> WireOperationReceipt {
        let args = json!({"reactionBody": body, "userId": user}).to_string();
        bound(self.rig.database.act(self.epoch, "Record".into(), "x1".into(), "react".into(), PATCH.into(), args)).await.unwrap()
    }

    fn reactions(&self, row: &WireRow) -> Vec<(String, String)> {
        let Cell::Seq { items } = cell(&self.rig, "Record", row, "reactions") else { return Vec::new() };
        items.iter().map(|item| (member(item, 1), member(item, 0))).collect()
    }
}

fn member(item: &Cell, at: usize) -> String {
    let Cell::Seq { items } = item else { return String::new() };
    match items.get(at) {
        Some(Cell::Text { value }) => value.clone(),
        _ => String::new(),
    }
}

fn pair(user: &str, body: &str) -> (String, String) { (user.to_string(), body.to_string()) }

#[tokio::test(flavor = "multi_thread")]
async fn react_then_undo_keeps_the_other_users_reaction_of_a_record() {
    let session = Session::settled(created(json!([{"body":"B","user":{"id":"u2"}}]))).await;
    assert_eq!(session.reactions(&session.shown()), vec![pair("u2", "B")], "the reaction of the answer is not stored by its declared key");
    let first = session.rig.world.hold_then("RecordReact", reacted(json!([{"body":"B","user":{"id":"u2"}},{"body":"A","user":{"id":"u1"}}])));
    let second = session.rig.world.hold_then("RecordReact", reacted(json!([{"body":"B","user":{"id":"u2"}}])));
    let one = session.react("A", "u1").await;
    first.entered().await;
    let undo = session.react("", "u1").await;
    assert_eq!(session.reactions(&undo.optimistic_value.clone().unwrap()), vec![pair("u2", "B")]);
    first.release();
    assert!(matches!(bound(session.rig.database.outcome(one)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    second.entered().await;
    second.release();
    assert!(matches!(bound(session.rig.database.outcome(undo)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the other reaction stays", || session.reactions(&session.shown()) == vec![pair("u2", "B")]).await;
    session.rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reaction_uses_its_declared_key_after_an_update_of_the_record() {
    let session = Session::settled(created(json!([]))).await;
    let patch = json!({"reactions":[{"body":"A","userId":"u1"}]}).to_string();
    bound(session.rig.database.update(session.epoch, "Record".into(), "x1".into(), patch, "{}".into())).await.unwrap();
    let hold = session.rig.world.hold_then("RecordReact", reacted(json!([{"body":"C","userId":"u1"}])));
    let changed = session.react("C", "u1").await;
    assert_eq!(session.reactions(&changed.optimistic_value.clone().unwrap()), vec![pair("u1", "C")]);
    hold.release();
    session.rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_optimistic_reaction_carries_the_nested_user_of_the_local_row() {
    let session = Session::settled(created(json!([]))).await;
    let ann = bound(session.rig.database.create(session.epoch, "User".into(), json!({"name":"Ann"}).to_string(), "{}".into())).await.unwrap().optimistic_value.unwrap().id;
    let hold_a = session.rig.world.hold_then("RecordReact", reacted(json!([])));
    let hold_b = session.rig.world.hold_then("RecordReact", reacted(json!([])));
    let with_user = session.react("A", &ann).await.optimistic_value.unwrap();
    let nested = |row: &WireRow, user: &str| {
        let Cell::Seq { items } = cell(&session.rig, "Record", row, "reactions") else { return Cell::Null };
        let Some(Cell::Seq { items }) = items.iter().find(|item| member(item, 1) == user).cloned() else { return Cell::Null };
        items.get(2).cloned().unwrap_or(Cell::Null)
    };
    assert_eq!(member(&nested(&with_user, &ann), 1), "Ann", "the nested user of the local row is missing");
    let without_user = session.react("B", "nobody").await.optimistic_value.unwrap();
    assert!(matches!(nested(&without_user, "nobody"), Cell::Null), "a user that is no row got a nested value");
    for gate in [hold_a, hold_b] { gate.release(); }
    session.rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reaction_with_a_delayed_answer_shows_as_unsent_before_the_answer_and_synced_after() {
    let session = Session::settled(created(json!([]))).await;
    let hold: Arc<Gate> = session.rig.world.hold_then("RecordReact", reacted(json!([{"body":"C","userId":"u1"}])));
    let receipt = session.react("C", "u1").await;
    hold.entered().await;
    assert_eq!(session.reactions(&receipt.optimistic_value.clone().unwrap()), vec![pair("u1", "C")], "the reaction was not shown before the answer");
    yield_until("the row shows the reaction as unsent", || {
        let row = session.shown();
        row.sync.state != WireSyncState::Synced && session.reactions(&row) == vec![pair("u1", "C")]
    }).await;
    hold.release();
    assert!(matches!(bound(session.rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the row is synced", || session.shown().sync.state == WireSyncState::Synced).await;
    session.rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_reaction_on_the_row_of_the_previous_call_keeps_other_users_reactions() {
    let session = Session::settled(created(json!([]))).await;
    let gates: Vec<Arc<Gate>> = (0..4).map(|_| session.rig.world.hold_then("RecordReact", reacted(json!([])))).collect();
    let mut seen = Vec::new();
    for (body, user) in [("A", "u1"), ("B", "u2"), ("C", "u1"), ("", "u1")] {
        let receipt = session.react(body, user).await;
        seen.push(session.reactions(&receipt.optimistic_value.unwrap()));
    }
    assert_eq!(seen[2], vec![pair("u1", "C"), pair("u2", "B")]);
    assert_eq!(seen[3], vec![pair("u2", "B")]);
    for gate in gates { gate.release(); }
    session.rig.database.close_now();
}
