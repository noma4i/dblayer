use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::pacing::ManualClock;
use crate::schema::{ActionDeclaration, ActionLanding, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RootShape};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::upload::{UploadBatchInput, UploadItem, UploadOperations};
use crate::value::ScalarKind;

use super::{EventEffect, OperationDef, SubscriptionDef, Sync, SyncConfig};

fn fields(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn rid(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn spec() -> ModelSpec {
    let mut spec = ModelSpec::new("Record", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("body", ScalarKind::Str).optional(),
        FieldSpec::scalar("clientKey", ScalarKind::Str).optional(),
        FieldSpec::scalar("position", ScalarKind::Int).optional(),
        FieldSpec::scalar("size", ScalarKind::Int).optional(),
    ]);
    let mut recent = RelationSpec::global("recent");
    recent.joins_events = true;
    recent.remote = Some(super::rig::remote("Records", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    spec.relations.push(recent);
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("UpdateRecord".into());
    update.lands = vec![ActionLanding { model: "Record".into(), root: "updateRecord.related".into() }];
    spec.actions.push(update);
    let mut single = ActionDeclaration::new("create_file", RootShape::Upsert);
    single.upload = true;
    single.mutation = Some("CreateRecord".into());
    single.correlate = vec!["clientKey".into()];
    let mut batch = single.clone();
    batch.name = "create_files".into();
    batch.mutation = Some("CreateRecords".into());
    batch.batch = Some(serde_json::from_value(json!({
        "min":2,"max":10,"items":"items","single":"create_file",
        "input":{"clientKey":"clientKey","position":"position","size":"size"},
        "renumber":{"position":"position","size":"size","position_from":0}
    })).unwrap());
    spec.actions.extend([single, batch]);
    spec
}

fn rig(spec: ModelSpec) -> (Db, Sync, Arc<MockTransport>) {
    let db = Db::open_in_memory(vec![spec], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(Duration::ZERO)), SyncConfig::default());
    for (name, root) in [("Records", "records"), ("Changed", "changed"), ("UpdateRecord", "updateRecord.record")] {
        sync.register(OperationDef::new(name, "query Probe { id }", root));
    }
    (db, sync, transport)
}

fn save(db: &Db, values: Value) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Record"), id: rid(values["id"].as_str().unwrap()), row: fields(values) });
    db.land(envelope).unwrap();
}

#[tokio::test(start_paused = true)]
async fn stale_generation_preserves_the_undelivered_attempt_budget() {
    let mut failures = Vec::new();
    let action = "create";
    for (attempts, expected) in [(1, crate::operation::OperationStatus::Pending), (40, crate::operation::OperationStatus::Failed)] {
        let mut model = spec();
        let mut declaration = ActionDeclaration::new(action, RootShape::Upsert);
        declaration.mutation = Some("UpdateRecord".into());
        declaration.durable = true;
        declaration.has_optimistic = true;
        declaration.has_temp_row_retention = true;
        model.actions.push(declaration);
        let db = Db::open_in_memory(vec![model, ModelSpec::new("Folder", vec![FieldSpec::scalar("id", ScalarKind::Id)])], None).unwrap();
        let transport = Arc::new(MockTransport::new());
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(Duration::ZERO)), SyncConfig::default());
        let id = db.create("Record", fields(json!({"body":"pending"}))).unwrap().id.clone();
        let operation = OperationDef::new("UpdateRecord", "mutation Probe { id }", "updateRecord.record");
        for attempt in 1..=attempts {
            let delay = if attempt == attempts { Duration::from_secs(5) } else { Duration::ZERO };
            transport.enqueue("UpdateRecord", Reply::fail(crate::transport::TransportError::Network("unreachable".into())).after(delay));
        }
        for attempt in 1..=attempts {
            let entry = db.outbox().unwrap().remove(0);
            let sending = super::send::Sending { entry: &entry, model: "Record".into(), action: action.into(), generation: db.generation(), base_revision: db.revision_seq().unwrap() };
            let reset = async {
                if attempt == attempts {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    db.reset(&["Folder"]).unwrap();
                    assert_ne!(db.generation(), sending.generation);
                    assert_eq!(db.outbox().unwrap().len(), 1, "operation survives unrelated reset");
                }
            };
            let (result, ()) = tokio::join!(sync.inner.send_owned(&sending, &operation, None, false), reset);
            result.unwrap();
        }
        let mut entries = db.outbox().unwrap();
        assert_eq!(entries.len(), 1, "after {attempts} undelivered attempts");
        let entry = entries.remove(0);
        if (entry.operation.status, entry.operation.attempts) != (expected, attempts) {
            failures.push(format!("{action}/{attempts}: {:?}, expected {expected:?}", entry.operation.status));
        }
        assert_eq!(transport.call_count("UpdateRecord"), attempts as usize);
        assert!(!(entry.operation.status == crate::operation::OperationStatus::Pending && entry.operation.transport_started));
        let body = db.find("Record", &id).unwrap().map(|row| row.data["body"].clone());
        let expected_body = Some(json!("pending"));
        if body != expected_body { failures.push(format!("{action}/{attempts}: body {body:?}, expected {expected_body:?}")); }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn load_empty(sync: &Sync, transport: &MockTransport) {
    transport.enqueue("Records", Reply::data(json!({"records":[]})));
    sync.load("Record", "recent", &Map::new(), Default::default()).await.unwrap();
}

fn members(db: &Db) -> Vec<String> {
    let scope = spec().relations[0].scope_key("Record", &Default::default()).unwrap();
    db.shared.store.scope_members("Record", scope.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

fn batch(db: &Db, count: usize) -> Vec<RowId> {
    let items = (0..count).map(|position| UploadItem {
        values: fields(json!({"clientKey":format!("key-{position}"),"position":position,"size":count})),
        path: "/missing/probe.bin".into(), name: None, mime: None, variables: Map::new(),
    }).collect();
    db.create_upload_batch(db.generation(), "Record", items, UploadBatchInput { operation: "create_files".into(), variables: Map::new() }, &UploadOperations::empty().create_blob("CreateBlob")).unwrap()
}

#[test]
fn local_batch_allocates_distinct_order_keys() {
    let mut model = spec();
    model.relations[0].joins_created = true;
    let db = Db::open_in_memory(vec![model], None).unwrap();
    let ids = batch(&db, 3);
    let found = members(&db);
    assert_eq!(found.len(), ids.len());
    assert!(ids.iter().all(|id| found.contains(&id.to_string())));
}

#[tokio::test(start_paused = true)]
async fn event_burst_allocates_distinct_order_keys() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let clock = Arc::new(ManualClock::new(Duration::ZERO));
    let sync = Sync::new(db.clone(), transport.clone(), clock.clone(), SyncConfig::default());
    for (name, root) in [("Records", "records"), ("Changed", "changed")] {
        sync.register(OperationDef::new(name, "query Probe { id }", root));
    }
    load_empty(&sync, &transport).await;
    let mut event = EventSpec::new("changed").merged_by_row();
    event.debounce_interval = Some(Duration::from_millis(10));
    let subscription = sync.subscribe(SubscriptionDef::new(event, "Changed", "Record", EventEffect::Upsert)).await.unwrap();
    for id in ["r1", "r2"] {
        transport.emit("Changed", SubscriptionEvent::Next(Response::data(json!({"changed":{"id":id,"body":id}}))));
    }
    transport.await_consumed("Changed").await;
    let waits = transport.waits("Changed");
    clock.advance(Duration::from_millis(20));
    transport.await_waits("Changed", waits + 1).await;
    assert_eq!(subscription.status().rejected, 0);
    assert_eq!(members(&db), ["r1", "r2"]);
}

#[tokio::test(start_paused = true)]
async fn network_writes_preserve_pending_fields() {
    let mut failures = Vec::new();
    for source in ["event", "event-related", "load", "load-related", "response", "lands"] {
        let mut model = spec();
        model.fields.push(FieldSpec::scalar("linkedId", ScalarKind::Id).optional().indexed());
        model.associations.push(crate::schema::AssociationSpec::new("linked", crate::schema::AssociationKind::BelongsTo, "Record", "linkedId"));
        let (db, sync, transport) = rig(model);
        save(&db, json!({"id":"r1","body":"before"}));
        let closing = match source {
            "response" => {
                db.update("Record", &rid("r1"), fields(json!({"size":0}))).unwrap();
                Some(db.outbox().unwrap().remove(0))
            }
            "lands" => {
                save(&db, json!({"id":"r0","body":"before"}));
                db.update("Record", &rid("r0"), fields(json!({"body":"pending"}))).unwrap();
                Some(db.outbox().unwrap().remove(0))
            }
            _ => None,
        };
        db.update("Record", &rid("r1"), fields(json!({"body":"pending"}))).unwrap();
        match source {
            "event" | "event-related" => {
                let _subscription = sync.subscribe(SubscriptionDef::new(EventSpec::new("changed"), "Changed", "Record", EventEffect::Upsert)).await.unwrap();
                let node = json!({"id":"r1","body":"server","size":7});
                let root = if source == "event-related" { json!({"id":"r0","linked":node}) } else { node };
                transport.emit("Changed", SubscriptionEvent::Next(Response::data(json!({"changed":root}))));
                transport.await_consumed("Changed").await;
            }
            "load" | "load-related" => {
                let node = json!({"id":"r1","body":"server","size":7});
                let root = if source == "load-related" { json!({"id":"r0","linked":node}) } else { node };
                transport.enqueue("Records", Reply::data(json!({"records":[root]})));
                sync.load("Record", "recent", &Map::new(), Default::default()).await.unwrap();
            }
            "response" | "lands" => {
                let entry = closing.as_ref().unwrap();
                let sending = super::send::Sending { entry, model: "Record".into(), action: "update".into(), generation: db.generation(), base_revision: db.revision_seq().unwrap() };
                let node = json!({"id":"r1","body":"server","size":7});
                let payload = if source == "response" { json!({"updateRecord":{"record":node}}) } else { json!({"updateRecord":{"record":{"id":"r0","body":"confirmed"},"related":node}}) };
                transport.enqueue("UpdateRecord", Reply::data(payload));
                sync.inner.send_owned(&sending, &OperationDef::new("UpdateRecord", "mutation UpdateRecord { updateRecord { record { id } } }", "updateRecord.record"), None, true).await.unwrap();
            }
            _ => unreachable!(),
        }
        let row = db.shared.store.point("Record", "r1").unwrap().unwrap();
        let seq = db.outbox().unwrap()[0].operation.id.0;
        db.roll_back(seq).unwrap();
        let restored = db.shared.store.point("Record", "r1").unwrap().unwrap();
        if row["body"] != "pending" || row["size"] != 7 || restored["body"] != "before" {
            failures.push(format!("{source}: row={row:?}, restored={restored:?}"));
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test(start_paused = true)]
async fn event_joins_an_existing_row_and_resolves_nested_paths() {
    event_join(false).await;
}

#[tokio::test(start_paused = true)]
async fn event_resolves_nested_id() {
    event_join(true).await;
}

async fn event_join(nested: bool) {
        let (db, sync, transport) = rig(spec());
        load_empty(&sync, &transport).await;
        save(&db, json!({"id":"r1","body":"before"}));
        let mut def = SubscriptionDef::new(EventSpec::new("changed"), "Changed", "Record", EventEffect::Upsert);
        if nested {
            def.id = vec!["actor.id".into()];
            def.deleted_by = Some("meta.deleted".into());
        }
        let _subscription = sync.subscribe(def).await.unwrap();
        let node = if nested { json!({"actor":{"id":"r1"},"body":"after"}) } else { json!({"id":"r1","body":"after"}) };
        transport.emit("Changed", SubscriptionEvent::Next(Response::data(json!({"changed":node}))));
        transport.await_consumed("Changed").await;
        assert_eq!(db.shared.store.point("Record", "r1").unwrap().unwrap()["body"], "after", "nested={nested}");
        assert_eq!(members(&db), ["r1"], "nested={nested}");
        if nested {
            transport.emit("Changed", SubscriptionEvent::Next(Response::data(json!({"changed":{"actor":{"id":"r1"},"meta":{"deleted":true}}}))));
            transport.await_consumed("Changed").await;
            assert!(db.shared.store.point("Record", "r1").unwrap().is_none());
            assert!(members(&db).is_empty());
        }
}

#[tokio::test(start_paused = true)]
async fn lands_accepts_lists() {
        let (db, sync, transport) = rig(spec());
        load_empty(&sync, &transport).await;
        save(&db, json!({"id":"r1","body":"before"}));
        save(&db, json!({"id":"r2","body":"known"}));
        db.update("Record", &rid("r1"), fields(json!({"body":"pending"}))).unwrap();
        let related = json!([{"id":"r2","body":"landed"},{"id":"r3","body":"landed"}]);
        transport.enqueue("UpdateRecord", Reply::data(json!({"updateRecord":{"record":{"id":"r1","body":"confirmed"},"related":related}})));
        sync.flush().await.unwrap();
        assert_eq!(members(&db), ["r2", "r3"]);
        assert_eq!(db.shared.store.point("Record", "r2").unwrap().unwrap()["body"], "landed");
}

#[tokio::test]
async fn removing_upload_item_rewrites_mapped_values() {
    let (db, sync, transport) = rig(spec());
    let ids = batch(&db, 3);
    let seq = db.outbox().unwrap()[0].operation.id.0;
    let mut transferred = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    for position in 0..3 {
        let mut state = crate::upload::UploadState::queued();
        state.transferred = true;
        state.signed_id = Some(format!("signed-{position}"));
        transferred.operation_transitions.push(crate::commit::OperationTransition::Progress { seq, item_position: position, state: Box::new(state) });
    }
    db.land(transferred).unwrap();
    db.remove_upload_item("Record", &ids[1], seq, true).unwrap();
    let entry = db.outbox().unwrap().remove(0);
    let sending = super::send::Sending { entry: &entry, model: "Record".into(), action: "create_files".into(), generation: db.generation(), base_revision: db.revision_seq().unwrap() };
    transport.enqueue("CreateRecords", Reply::data(json!({"records":[{"id":"r0","clientKey":"key-0"},{"id":"r2","clientKey":"key-2"}]})));
    sync.inner.send_owned(&sending, &OperationDef::new("CreateRecords", "mutation CreateRecords { records { id } }", "records"), None, true).await.unwrap();
    let call = transport.calls().into_iter().find(|call| call.operation_name == "CreateRecords").unwrap();
    assert_eq!(call.variables["items"][0]["position"], 0);
    assert_eq!(call.variables["items"][1]["position"], 1);
    assert_eq!(call.variables["items"][0]["size"], 2);
    assert_eq!(call.variables["items"][1]["size"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiters_woken_by_the_commit_of_a_batch_get_the_real_ids() {
    let (db, sync, transport) = rig(spec());
    let ids = batch(&db, 3);
    let seq = db.outbox().unwrap()[0].operation.id.0;
    let mut transferred = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    for position in 0..3 {
        let mut state = crate::upload::UploadState::queued();
        state.transferred = true;
        state.signed_id = Some(format!("signed-{position}"));
        transferred.operation_transitions.push(crate::commit::OperationTransition::Progress { seq, item_position: position, state: Box::new(state) });
    }
    db.land(transferred).unwrap();
    let entry = db.outbox().unwrap().remove(0);
    let sending = super::send::Sending { entry: &entry, model: "Record".into(), action: "create_files".into(), generation: db.generation(), base_revision: db.revision_seq().unwrap() };
    let received = super::send::confirmed_tests::confirm_while_landing(&sync, ids.iter().map(|id| ("Record".to_string(), id.clone())).collect());
    transport.enqueue("CreateRecords", Reply::data(json!({"records":[{"id":"r0","clientKey":"key-0"},{"id":"r1","clientKey":"key-1"},{"id":"r2","clientKey":"key-2"}]})));
    sync.inner.send_owned(&sending, &OperationDef::new("CreateRecords", "mutation CreateRecords { records { id } }", "records"), None, true).await.unwrap();
    let confirmed = crate::test_wait::recv_within(&received);
    *super::locked(&sync.inner.landed_probe) = None;
    assert_eq!(confirmed.into_iter().map(|result| result.unwrap().to_string()).collect::<Vec<_>>(), ["r0", "r1", "r2"]);
}

#[test]
fn removing_upload_item_erases_children() {
        let (db, _, _) = rig(spec());
        let ids = batch(&db, 3);
        let seq = db.outbox().unwrap()[0].operation.id.0;
        db.shared.read(&[], |reader| {
            for position in 0..3 {
                reader.connection().execute("INSERT INTO op_item_header (seq,position,name,value) VALUES (?1,?2,'X-Probe','yes')", rusqlite::params![seq, position])?;
                reader.connection().execute("INSERT INTO op_item_retry (seq,position,stage,part_number,attempts,retry_at) VALUES (?1,?2,'put',0,1,42)", rusqlite::params![seq, position])?;
            }
            Ok(())
        }).unwrap();
        db.remove_upload_item("Record", &ids[1], seq, true).unwrap();
        db.shared.read(&[], |reader| {
                for table in ["op_item_header", "op_item_retry"] {
                    let count: i64 = reader.connection().query_row(&format!("SELECT COUNT(*) FROM {table} WHERE seq = ?1 AND position = 1"), [seq], |row| row.get(0))?;
                    assert_eq!(count, 0, "{table}");
                }
            Ok(())
        }).unwrap();
}

thread_local! {
    static QUEUE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FULL_QUEUE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static CHILD_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FULL_UPLOAD_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn count_queue_reads(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.contains("FROM op WHERE status IN") {
            QUEUE_READS.with(|reads| reads.set(reads.get() + 1));
            if sql.starts_with("SELECT seq, action_key") && sql.contains("client_mutation_id") { FULL_QUEUE_READS.with(|reads| reads.set(reads.get() + 1)); }
        }
        if sql.contains("FROM row_Record WHERE id IN") { CHILD_READS.with(|reads| reads.set(reads.get() + 1)); }
        if sql.starts_with("SELECT seq, position, phase") && !sql.contains("WHERE seq =") { FULL_UPLOAD_READS.with(|reads| reads.set(reads.get() + 1)); }
    }
}

#[test]
fn landing_delete_follows_parent_once() {
    use crate::schema::{AssociationKind, AssociationSpec, Newer};
    use rusqlite::trace::TraceEventCodes;
    let parent = ModelSpec::new("Folder", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("lastId", ScalarKind::Id).nullable(), FieldSpec::scalar("lastAt", ScalarKind::Date).nullable()]);
    let mut child = spec();
    child.fields.extend([FieldSpec::scalar("folderId", ScalarKind::Id).indexed(), FieldSpec::scalar("createdAt", ScalarKind::Date)]);
    let mut association = AssociationSpec::new("folder", AssociationKind::BelongsTo, "Folder", "folderId");
    association.sets = [("lastId".into(), "id".into()), ("lastAt".into(), "createdAt".into())].into();
    association.newer = vec![Newer { child: "createdAt".into(), parent: "lastAt".into() }];
    child.associations.push(association);
    let db = Db::open_in_memory(vec![parent, child], None).unwrap();
    let mut seed = CommitEnvelope::new(WriteOrigin::Snapshot, db.generation());
    seed.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Folder"), id: rid("f1"), row: fields(json!({"id":"f1","lastId":null,"lastAt":null})) });
    db.land(seed).unwrap();
    save(&db, json!({"id":"r1","folderId":"f1","createdAt":"2026-01-01T00:00:00Z"}));
    CHILD_READS.with(|reads| reads.set(0));
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_queue_reads));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("Record"), id: rid("r1"), reason: crate::commit::DestroyReason::Server });
    db.land(envelope).unwrap();
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert_eq!(CHILD_READS.with(|reads| reads.get()), 1);
    assert!(db.shared.store.point("Folder", "f1").unwrap().unwrap().get("lastId").is_none_or(Value::is_null));
}

#[test]
fn related_admission_reads_pending_ownership_once() {
    use rusqlite::trace::TraceEventCodes;
    let (db, _, _) = rig(spec());
    save(&db, json!({"id":"r1","body":"before"}));
    db.update("Record", &rid("r1"), fields(json!({"body":"pending"}))).unwrap();
    QUEUE_READS.with(|reads| reads.set(0));
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_queue_reads));
    db.shared.read(&[], |reader| {
        for _ in 0..4 {
            let admitted = super::land::admit(&db, reader, reader.revision_seq()?, None, "Record", &[fields(json!({"id":"r1","body":"server"}))], &mut 0).unwrap();
            assert_eq!(admitted.rows[0]["body"], "pending");
        }
        Ok(())
    }).unwrap();
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert!(QUEUE_READS.with(|reads| reads.get()) <= 1, "queue scans={}", QUEUE_READS.with(|reads| reads.get()));
}

#[test]
fn inv_identity_03_correlation_replaces_the_same_temp_id_in_each_model() {
    let specs = ["First", "Second"].map(|name| {
        let mut model = ModelSpec::new(name, vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)]);
        let mut create = ActionDeclaration::new("create", RootShape::Upsert);
        create.mutation = Some("CreateRecord".into());
        create.has_optimistic = true;
        create.has_temp_row_retention = true;
        create.correlate = vec!["body".into()];
        model.actions.push(create);
        model
    });
    let db = Db::open_in_memory(specs.to_vec(), None).unwrap();
    for name in ["First", "Second"] {
        db.create(name, fields(json!({"id": "temp-shared", "body": "match"}))).unwrap();
    }
    let mut started = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    started.operation_transitions = db.outbox().unwrap().iter().map(|entry| crate::commit::OperationTransition::Start { seq: entry.operation.id.0 }).collect();
    db.land(started).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for name in ["First", "Second"] {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(name), id: rid("server"), row: fields(json!({"id": "server", "body": "match"})) });
    }
    db.land(envelope).unwrap();
    for name in ["First", "Second"] {
        assert!(db.find(name, &rid("temp-shared")).unwrap().is_none(), "{name} keeps its temp row");
        assert_eq!(db.find(name, &rid("server")).unwrap().unwrap().text("body"), Some("match"));
    }
}

#[test]
fn correlation_reads_candidates_once_for_all_incoming_rows() {
    use rusqlite::trace::TraceEventCodes;
    for matching in [false, true] {
    let mut model = spec();
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateRecord".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.correlate = vec!["body".into()];
    model.actions.push(create);
    let (db, _, _) = rig(model);
    for i in 0..4 { db.create("Record", fields(json!({"body":format!("local-{i}")}))).unwrap(); }
    let mut started = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    started.operation_transitions = db.outbox().unwrap().iter().map(|entry| crate::commit::OperationTransition::Start { seq: entry.operation.id.0 }).collect();
    db.land(started).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for i in 0..4 {
        let prefix = if matching { "local" } else { "remote" };
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Record"), id: rid(&format!("r{i}")), row: fields(json!({"id":format!("r{i}"),"body":format!("{prefix}-{i}")})) });
    }
    CHILD_READS.with(|reads| reads.set(0));
    FULL_QUEUE_READS.with(|reads| reads.set(0));
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_queue_reads));
    db.land(envelope).unwrap();
    db.shared.store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    if !matching { assert!(CHILD_READS.with(|reads| reads.get()) <= 9, "row reads={}", CHILD_READS.with(|reads| reads.get())); }
    assert_eq!(FULL_QUEUE_READS.with(|reads| reads.get()), usize::from(matching), "matching={matching}");
    }
}

#[test]
fn committing_and_discarding_read_only_affected_operations() {
    use rusqlite::trace::TraceEventCodes;
    let mut counts = Vec::new();
    for action in ["update", "rollback", "discard", "retry"] {
        let db = Db::open_in_memory(vec![spec()], None).unwrap();
        save(&db, json!({"id":"r1","body":"before"}));
        db.update("Record", &rid("r1"), fields(json!({"body":"pending"}))).unwrap();
        let seq = db.outbox().unwrap()[0].operation.id.0;
        FULL_QUEUE_READS.with(|reads| reads.set(0));
        FULL_UPLOAD_READS.with(|reads| reads.set(0));
        db.shared.store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_queue_reads));
        match action {
            "update" => { db.update("Record", &rid("r1"), fields(json!({"body":"next"}))).unwrap(); }
            "rollback" => { db.roll_back(seq).unwrap(); }
            "retry" => { db.retry("Record", &rid("r1"), None).unwrap(); }
            _ => { db.discard("Record", &rid("r1"), None).unwrap(); }
        }
        db.shared.store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
        counts.push((action, FULL_QUEUE_READS.with(|reads| reads.get()), FULL_UPLOAD_READS.with(|reads| reads.get())));
    }
    assert!(counts.iter().all(|(_, queue, uploads)| *queue == 0 && *uploads == 0), "{counts:?}");
}

#[test]
fn prior_row_lookup_uses_an_index() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let plan = db.shared.read(&[], |reader| {
        let mut statement = reader.connection().prepare("EXPLAIN QUERY PLAN SELECT ids.seq FROM op_value AS ids JOIN op_value AS models ON models.seq = ids.seq AND models.item_position = -1 AND models.part = 'prior' AND models.parent = ids.parent AND models.name = 'model' AND models.text = 'Record' WHERE ids.item_position = -1 AND ids.part = 'prior' AND ids.name = 'id' AND ids.text = 'r1'")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }).unwrap();
    assert!(plan.iter().all(|step| !step.contains("SCAN")), "{plan:?}");
}

#[test]
fn batch_parent_edits_survive_item_removal_and_rollback() {
    use crate::schema::{AssociationKind, AssociationSpec};
    for external in [false, true] {
    let parent = ModelSpec::new("Folder", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("lastId", ScalarKind::Id).nullable(), FieldSpec::scalar("lastPosition", ScalarKind::Int).nullable()]);
    let mut child = spec();
    child.fields.push(FieldSpec::scalar("folderId", ScalarKind::Id).indexed());
    let mut association = AssociationSpec::new("folder", AssociationKind::BelongsTo, "Folder", "folderId");
    association.sets = [("lastId".into(), "id".into()), ("lastPosition".into(), "position".into())].into();
    child.associations.push(association);
    let db = Db::open_in_memory(vec![parent, child], None).unwrap();
    let mut seed = CommitEnvelope::new(WriteOrigin::Snapshot, db.generation());
    seed.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Folder"), id: rid("f1"), row: fields(json!({"id":"f1","lastId":"old","lastPosition":9})) });
    db.land(seed).unwrap();
    let items = (0..3).map(|position| UploadItem {
        values: fields(json!({"folderId":"f1","clientKey":format!("key-{position}"),"position":position,"size":3})),
        path: "/missing/probe.bin".into(), name: None, mime: None, variables: Map::new(),
    }).collect();
    let ids = db.create_upload_batch(db.generation(), "Record", items, UploadBatchInput { operation: "create_files".into(), variables: Map::new() }, &UploadOperations::empty().create_blob("CreateBlob")).unwrap();
    let seq = db.outbox().unwrap()[0].operation.id.0;
    assert_ne!(db.shared.store.point("Folder", "f1").unwrap().unwrap()["lastId"], "old");
    if external {
        let mut changed = CommitEnvelope::new(WriteOrigin::Response, db.generation());
        changed.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Folder"), id: rid("f1"), row: fields(json!({"id":"f1","lastId":"outside","lastPosition":11})) });
        db.land(changed).unwrap();
    }
    db.remove_upload_item("Record", &ids[1], seq, true).unwrap();
    assert_eq!(db.shared.store.point("Folder", "f1").unwrap().unwrap()["lastPosition"], 1);
    db.roll_back(seq).unwrap();
    let restored = db.shared.store.point("Folder", "f1").unwrap().unwrap();
    let expected = if external { (json!("outside"), json!(11)) } else { (json!("old"), json!(9)) };
    assert_eq!((restored["lastId"].clone(), restored["lastPosition"].clone()), expected);
    }
}

#[test]
fn older_cache_format_reopens_without_removing_application_data() {
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("cache.sqlite");
    let application = directory.path().join("application.sqlite");
    let connection = rusqlite::Connection::open(&application).unwrap();
    connection.execute_batch("CREATE TABLE preferences (value TEXT); INSERT INTO preferences VALUES ('kept')").unwrap();
    drop(connection);
    {
        let db = Db::open(&cache, vec![spec()], None).unwrap();
        save(&db, json!({"id":"r1","body":"cached"}));
        db.shared.store.lock().connection.execute_batch("DROP INDEX IF EXISTS op_prior_row; UPDATE manifest SET value = 'b6645369333c0eb3da346a620c827dd0354187ca75485ae57a0e0f6ee0bd46e9' WHERE key = 'format_version'").unwrap();
    }
    let db = Db::open(&cache, vec![spec()], None).unwrap();
    assert_eq!(db.open_report().recreated, Some(crate::ddl::RecreateReason::CoreFormat));
    assert!(db.shared.store.point("Record", "r1").unwrap().is_none());
    let connection = rusqlite::Connection::open(application).unwrap();
    assert_eq!(connection.query_row("SELECT value FROM preferences", [], |row| row.get::<_, String>(0)).unwrap(), "kept");
}
