use rusqlite::Connection;
use crate::owners::cloud::CloudScope;

use crate::store::cloud::{self, PendingRecord};
use crate::owners::{contract::BatchAuthor, store::{RowValue, StoreBatch, StoreOp}};
use crate::schema::{FieldSpec, ModelSpec, RowSource, Storage};
use crate::store::{BatchPlan, DestroyReason, EntityOp, Store};
use crate::value::ScalarKind;

#[test]
fn delayed_cloud_acknowledgement_keeps_the_newer_change_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite");
    drop(Store::open_models(Some(&path), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let mut connection = Connection::open(&path).unwrap();
    let original = PendingRecord { model: "notes".into(), row_id: "n1".into(), version: 1, deleted: false };
    let deletion = PendingRecord { version: 2, deleted: true, ..original.clone() };

    let transaction = connection.transaction().unwrap();
    cloud::enable(&transaction).unwrap();
    cloud::stage(&transaction, "account-a", &original).unwrap();
    cloud::stage(&transaction, "account-b", &original).unwrap();
    cloud::write_progress(&transaction, "account-b", b"other-cursor").unwrap();
    transaction.commit().unwrap();
    assert_eq!(cloud::pending(&connection, "account-a", 1).unwrap(), vec![original.clone()]);

    let transaction = connection.transaction().unwrap();
    cloud::stage(&transaction, "account-a", &deletion).unwrap();
    cloud::acknowledge(&transaction, "account-a", &original, None).unwrap();
    cloud::write_progress(&transaction, "account-a", b"committed-cursor").unwrap();
    transaction.commit().unwrap();

    let transaction = connection.transaction().unwrap();
    cloud::write_progress(&transaction, "account-a", b"uncommitted-cursor").unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion, None).unwrap();
    transaction.rollback().unwrap();
    drop(connection);

    let mut reopened = Connection::open(path).unwrap();
    assert_eq!(cloud::pending(&reopened, "account-a", 1).unwrap(), vec![deletion.clone()]);
    assert_eq!(cloud::progress(&reopened, "account-a").unwrap().as_deref(), Some(b"committed-cursor".as_slice()));

    let transaction = reopened.transaction().unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion, None).unwrap();
    cloud::stage(&transaction, "account-a", &deletion).unwrap();
    transaction.commit().unwrap();
    assert!(cloud::pending(&reopened, "account-a", 1).unwrap().is_empty());

    let transaction = reopened.transaction().unwrap();
    cloud::reset(&transaction, "account-a").unwrap();
    transaction.commit().unwrap();
    assert_eq!(cloud::progress(&reopened, "account-a").unwrap(), None);
    assert_eq!(cloud::pending(&reopened, "account-b", 1).unwrap(), vec![original]);
    assert_eq!(cloud::progress(&reopened, "account-b").unwrap().as_deref(), Some(b"other-cursor".as_slice()));
}

#[test]
fn opt_in_captures_local_commits_at_their_row_revision() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("local.sqlite");
    let mut notes = ModelSpec::new("notes", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("text", ScalarKind::Str).optional(),
        FieldSpec {
            app_type: Some("NoteMetadata".into()),
            fields: vec![FieldSpec::scalar("first", ScalarKind::Str), FieldSpec::scalar("second", ScalarKind::Str)],
            ..FieldSpec::object("metadata").optional()
        },
    ]);
    notes.storage = Storage::Local;
    let store = Store::open_models(Some(&path), vec![notes], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let write = |id: &str, text: &str, first: &str| {
        let batch = StoreBatch {
            author: BatchAuthor::LocalWrites, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1),
            rows: Vec::new(), intents: 0, credential: None,
            ops: vec![StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
                row_id: id.into(), values: serde_json::json!({
                    "id": id, "text": text, "metadata": {"first": first, "second": "unchanged"}
                }).to_string(),
            }] }],
        };
        store.write_batch(&batch, store.files(), None).unwrap().revision as i64
    };
    write("offline", "ordinary write", "initial");
    assert!(!store.table_exists("cloud_record").unwrap());
    assert!(!store.table_exists("cloud_progress").unwrap());

    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    store.activate_cloud_scope(scope.clone()).unwrap();
    let version = write("n1", "save me", "initial");
    let connection = Connection::open(&path).unwrap();
    let created = PendingRecord { model: "notes".into(), row_id: "n1".into(), version, deleted: false };
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);
    write("n1", "save me", "initial");
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);

    let materialized = |expected_version| {
        let pending = store.pending_cloud_records(&scope, 2).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].version, expected_version);
        crate::cloud::decode(&pending[0].key, pending[0].payload.as_ref().unwrap()).unwrap()
    };
    let before = materialized(version);
    let earlier = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!((earlier.model.as_str(), earlier.row_id.as_str()), ("notes", "n1"));
    store.confirm_cloud_send(&scope, &earlier, Some(b"system")).unwrap();
    let updated_version = write("n1", "save me", "edited");
    store.confirm_cloud_send(&scope, &earlier, Some(b"stale")).unwrap();
    let after = materialized(updated_version);
    assert_eq!(store.pending_cloud_records(&scope, 1).unwrap()[0].system_fields.as_deref(), Some(b"system".as_slice()));
    assert_eq!(before.fields.len(), 3);
    assert_eq!(after.fields.len(), 3);
    assert_eq!(after.fields[0].path, ["metadata", "first"]);
    assert_eq!(after.fields[0].data.value, serde_json::json!("edited"));
    assert!(after.fields[0].data.stamp > before.fields[0].data.stamp);
    assert_eq!(after.fields[1..], before.fields[1..]);

    write("n1", &"a".repeat(900_001), "edited");
    let oversized = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(oversized.payload, Err(crate::cloud::RecordError::TooLarge));
    store.block_cloud_send(&scope, &oversized, crate::ffi::CloudIssueCause::TooLarge).unwrap();
    write("other", "still sends", "initial");
    let other = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(other.row_id, "other");
    store.confirm_cloud_send(&scope, &other, None).unwrap();
    assert!(store.pending_cloud_records(&scope, 1).unwrap().is_empty());
    let repaired_version = write("n1", "repaired", "edited");
    assert!(!materialized(repaired_version).deleted);

    let mut deletion = BatchPlan::from_source(RowSource::Local);
    deletion.entity_ops.push(EntityOp::Destroy {
        model: crate::identity::ModelKey::new("notes"),
        id: crate::identity::RowId::new("n1").unwrap(),
        reason: DestroyReason::Local,
    });
    let version = store.commit(deletion).unwrap().revision.unwrap();
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![PendingRecord {
        version, deleted: true, ..created
    }]);
    let tombstone = materialized(version);
    assert!(tombstone.deleted);
    assert!(tombstone.fields.is_empty());
}

#[test]
fn durable_cloud_field_stamps_reopen_with_the_original_replica() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stamps.sqlite");
    let field = ["body".to_string(), "title".to_string()];
    let mut connection = Connection::open(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    cloud::enable(&transaction).unwrap();
    let original = cloud::tick_stamp(&transaction, "account", 1000).unwrap();
    let remote = crate::cloud::CloudStamp { millis: u64::MAX - 1, counter: u32::MAX, replica: [9; 16] };
    cloud::observe_stamp(&transaction, "account", remote).unwrap();
    cloud::write_field_stamp(&transaction, "account", "notes", "one", &field, remote).unwrap();
    transaction.commit().unwrap();
    drop(connection);

    let mut connection = Connection::open(path).unwrap();
    assert_eq!(cloud::field_stamp(&connection, "account", "notes", "one", &field).unwrap(), Some(remote));
    let transaction = connection.transaction().unwrap();
    let next = cloud::tick_stamp(&transaction, "account", 1).unwrap();
    assert_eq!((next.millis, next.counter, next.replica), (u64::MAX, 0, original.replica));
    cloud::reset(&transaction, "account").unwrap();
    transaction.commit().unwrap();
    assert_eq!(cloud::field_stamp(&connection, "account", "notes", "one", &field).unwrap(), None);
}

#[test]
fn cloud_landing_moves_rows_and_progress_together_without_an_echo() {
    let directory = tempfile::tempdir().unwrap();
    let notes = ModelSpec {
        storage: Storage::Local,
        ..ModelSpec::new("notes", vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("text", ScalarKind::Str).nullable().optional(),
        ])
    };
    let open = |name| Store::open_models(Some(&directory.path().join(name)), vec![notes.clone()], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let source = open("source.sqlite");
    let destination = open("destination.sqlite");
    let write_local = |store: &Store, id: &str, text: &str| store.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
        row_id: id.into(), values: serde_json::json!({"id": id, "text": text}).to_string(),
    }] }]).unwrap();
    write_local(&destination, "existing", "before attaching");
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    for store in [&source, &destination] {
        store.activate_cloud_scope(scope.clone()).unwrap();
    }
    let identity_only = crate::cloud::CloudRecord {
        model: "notes".into(), row_id: "existing".into(), server_revision: None, deleted: false, fields: Vec::new(),
    };
    destination.land_local_cloud(&scope, vec![(identity_only, Some(b"existing-tag".to_vec()))], None).unwrap();
    assert!(destination.pending_cloud_records(&scope, 1).unwrap().is_empty());
    write_local(&destination, "existing", "after fetch");
    let first = destination.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(first.row_id, "existing");
    assert_eq!(first.system_fields.as_deref(), Some(b"existing-tag".as_slice()));
    destination.confirm_cloud_send(&scope, &first, None).unwrap();
    source.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
        row_id: "n1".into(), values: r#"{"id":"n1","text":"saved on the first replica"}"#.into(),
    }] }]).unwrap();
    let sent = source.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    let incoming = crate::cloud::decode(&sent.key, &sent.payload.unwrap()).unwrap();

    destination.land_local_cloud(&scope, vec![(incoming.clone(), Some(b"first-tag".to_vec()))], Some(b"first")).unwrap();
    assert_eq!(destination.read(&["notes"], |reader| reader.point("notes", "n1")).unwrap().unwrap()["text"], "saved on the first replica");
    assert!(destination.pending_cloud_records(&scope, 1).unwrap().is_empty());

    source.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
        row_id: "n1".into(), values: r#"{"id":"n1","text":null}"#.into(),
    }] }]).unwrap();
    let work = source.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    let cleared = crate::cloud::decode(&work.key, work.payload.as_deref().unwrap()).unwrap();
    assert!(cleared.fields[0].data.value.is_null());
    assert!(cleared.fields[0].data.stamp > incoming.fields[0].data.stamp);
    destination.land_local_cloud(&scope, vec![(cleared, None)], Some(b"cleared")).unwrap();
    destination.land_local_cloud(&scope, vec![(incoming.clone(), None)], None).unwrap();
    let row = destination.read(&["notes"], |reader| reader.point("notes", "n1")).unwrap().unwrap();
    assert!(row.get("text").is_none_or(serde_json::Value::is_null));
    assert_eq!(destination.cloud_resume(&scope).unwrap().as_deref(), Some(b"cleared".as_slice()));

    source.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
        row_id: "identity".into(), values: r#"{"id":"identity"}"#.into(),
    }] }]).unwrap();
    let identity_key = crate::cloud::record_key("notes", "identity");
    let identity = source.pending_cloud_records(&scope, 2).unwrap().into_iter().find(|record| record.key == identity_key).unwrap();
    let identity = crate::cloud::decode(&identity.key, &identity.payload.unwrap()).unwrap();
    assert!(identity.fields.is_empty());
    destination.land_local_cloud(&scope, vec![(identity, None)], Some(b"identity")).unwrap();
    assert!(destination.read(&["notes"], |reader| reader.point("notes", "identity")).unwrap().is_some());
    assert!(destination.pending_cloud_records(&scope, 2).unwrap().is_empty());
    assert_eq!(destination.cloud_resume(&scope).unwrap().as_deref(), Some(b"identity".as_slice()));

    write_local(&destination, "n1", "local edit");
    let pending = destination.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(pending.system_fields.as_deref(), Some(b"first-tag".as_slice()));
    destination.land_local_cloud(&scope, vec![(incoming.clone(), Some(b"conflict-tag".to_vec()))], None).unwrap();
    let retained = destination.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(retained.version, pending.version);
    assert_eq!(retained.payload, pending.payload);
    assert_eq!(retained.system_fields.as_deref(), Some(b"conflict-tag".as_slice()));

    destination.retire_cloud_scope(&scope);
    let current = CloudScope { generation: 2, ..scope.clone() };
    destination.activate_cloud_scope(current.clone()).unwrap();
    let stale = crate::cloud::CloudRecord { row_id: "stale".into(), ..incoming.clone() };
    assert!(destination.land_local_cloud(&scope, vec![(stale, None)], Some(b"stale")).is_err());
    assert!(destination.pending_cloud_records(&scope, 1).is_err());
    assert_eq!(destination.read(&["notes"], |reader| reader.point("notes", "stale")).unwrap(), None);

    let second = crate::cloud::CloudRecord { row_id: "n2".into(), ..incoming.clone() };
    let mut malformed = crate::cloud::CloudRecord { row_id: "n3".into(), ..incoming };
    malformed.fields[0].path = vec!["unknown".into()];
    assert!(destination.land_local_cloud(&current, vec![(second, Some(b"invalid-tag".to_vec())), (malformed, None)], Some(b"invalid")).is_err());
    assert_eq!(destination.read(&["notes"], |reader| reader.point("notes", "n2")).unwrap(), None);
    write_local(&destination, "n2", "local edit");
    let record = destination.pending_cloud_records(&current, 2).unwrap().into_iter().find(|record| record.row_id == "n2").unwrap();
    assert_eq!(record.system_fields, None);
    drop(destination);

    let reopened = open("destination.sqlite");
    let connection = Connection::open(directory.path().join("destination.sqlite")).unwrap();
    assert_eq!(cloud::progress(&connection, "example").unwrap().as_deref(), Some(b"identity".as_slice()));
    assert!(reopened.read(&["notes"], |reader| reader.point("notes", "n1")).unwrap().is_some());
}

#[test]
fn cloud_account_erase_fences_reopen_and_only_evicts_tracked_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite");
    let notes = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id)]) };
    let open = |models| Store::open_models(Some(&path), models, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let has = |store: &Store, id: &str| store.read(&["notes"], |reader| reader.point("notes", id)).unwrap().is_some();
    let removed = ModelSpec { storage: Storage::Local, ..ModelSpec::new("removed", vec![FieldSpec::scalar("id", ScalarKind::Id)]) };
    let store = open(vec![notes.clone(), removed]);
    store.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
        row_id: "untracked".into(), values: r#"{"id":"untracked"}"#.into(),
    }] }]).unwrap();
    let first = CloudScope { epoch: 1, generation: 1, domain: "first-owner".into(), provider_fingerprint: "first".into() };
    store.activate_cloud_scope(first.clone()).unwrap();
    let row = crate::cloud::CloudRecord { model: "notes".into(), row_id: "tracked".into(), server_revision: None, deleted: false, fields: Vec::new() };
    let removed_row = crate::cloud::CloudRecord { model: "removed".into(), ..row.clone() };
    store.land_local_cloud(&first, vec![(row.clone(), None), (removed_row, None)], Some(b"first-cursor")).unwrap();
    drop(store);

    let reopened = open(vec![notes]);
    let second = CloudScope { epoch: 1, generation: 2, domain: "next-owner".into(), provider_fingerprint: "second".into() };
    assert_eq!(reopened.cloud_domain().unwrap().as_deref(), Some("first-owner"));
    assert!(reopened.activate_cloud_scope(second.clone()).is_err());
    assert!(has(&reopened, "tracked"));
    reopened.activate_cloud_scope(first.clone()).unwrap();
    assert!(reopened.erase_cloud_domain(&first.domain).is_err());
    assert_eq!(reopened.cloud_resume(&first).unwrap().as_deref(), Some(b"first-cursor".as_slice()));
    reopened.retire_cloud_scope(&first);
    reopened.erase_cloud_domain(&first.domain).unwrap();
    assert!(!has(&reopened, "tracked"));
    assert!(has(&reopened, "untracked"));
    assert_eq!(reopened.cloud_domain().unwrap(), None);
    assert!(reopened.activate_cloud_scope(first).is_err());

    reopened.activate_cloud_scope(second.clone()).unwrap();
    assert_eq!(reopened.cloud_resume(&second).unwrap(), None);
    reopened.land_local_cloud(&second, vec![(row, None)], None).unwrap();
    reopened.erase_cloud_domain("first-owner").unwrap();
    assert!(has(&reopened, "tracked"));
}

#[test]
fn cloud_bootstrap_resumes_its_checkpoint_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite");
    let model = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id)]) };
    let open = || Store::open_models(Some(&path), vec![model.clone()], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let first = open();
    let rows = (0..129).map(|index| {
        let row_id = format!("note-{index:03}");
        RowValue { values: serde_json::json!({"id": row_id}).to_string(), row_id }
    }).collect();
    first.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows }]).unwrap();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    first.activate_cloud_scope(scope.clone()).unwrap();
    assert!(!first.bootstrap_cloud_chunk(&scope).unwrap());
    assert_eq!(first.pending_cloud_records(&scope, 130).unwrap().len(), 128);
    drop(first);

    let second = open();
    second.activate_cloud_scope(scope.clone()).unwrap();
    assert!(second.bootstrap_cloud_chunk(&scope).unwrap());
    let records = second.pending_cloud_records(&scope, 130).unwrap();
    assert_eq!(records.len(), 129);
    assert_eq!(records.last().unwrap().row_id, "note-128");
    second.confirm_cloud_send(&scope, &records[0], None).unwrap();
    assert!(second.bootstrap_cloud_chunk(&scope).unwrap());
    assert_eq!(second.pending_cloud_records(&scope, 130).unwrap().len(), 128);
}

#[test]
fn disk_cloud_landing_keeps_newer_server_values_and_applies_local_fields() {
    use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
    use crate::schema::MergeRule;
    use serde_json::json;

    let mut score = FieldSpec::scalar("score", ScalarKind::Int);
    score.merge = Some(MergeRule::Monotonic);
    let mut memo = FieldSpec::scalar("memo", ScalarKind::Str).optional();
    memo.merge = Some(MergeRule::Local);
    let notes = ModelSpec {
        server_revision: Some("rev".into()),
        ..ModelSpec::new("notes", vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("rev", ScalarKind::Int),
            FieldSpec::scalar("title", ScalarKind::Str),
            score, memo,
        ])
    };
    let store = Store::open_models(None, vec![notes], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    let row = || store.read(&["notes"], |reader| reader.point("notes", "n1")).unwrap().unwrap();
    let mut plan = BatchPlan::from_source(RowSource::Local);
    plan.entity_ops.push(EntityOp::Upsert {
        model: crate::identity::ModelKey::new("notes"),
        id: crate::identity::RowId::new("n1").unwrap(),
        row: json!({"id":"n1", "rev":4, "title":"server", "score":10, "memo":"device"}).as_object().unwrap().clone(),
    });
    store.commit(plan).unwrap();
    store.activate_cloud_scope(scope.clone()).unwrap();
    let record = |revision, millis, values: serde_json::Value| CloudRecord {
        model: "notes".into(), row_id: "n1".into(), server_revision: Some(revision), deleted: false,
        fields: values.as_object().unwrap().iter().map(|(name, value)| CloudField {
            path: vec![name.clone()],
            data: StampedValue { value: value.clone(), stamp: CloudStamp { millis, counter: 0, replica: [1; 16] } },
        }).collect(),
    };
    let older = record(3, 200, json!({"rev":3, "title":"old", "score":20, "memo":"cloud"}));
    store.land_local_cloud(&scope, vec![(older, None)], Some(b"old")).unwrap();
    assert_eq!((row()["rev"].clone(), row()["title"].clone(), row()["score"].clone(), row()["memo"].clone()), (json!(4), json!("server"), json!(10), json!("cloud")));

    let newer = record(5, 100, json!({"rev":5, "title":"fresh", "score":7}));
    store.land_local_cloud(&scope, vec![(newer, None)], Some(b"new")).unwrap();
    let current = row();
    assert_eq!((&current["rev"], &current["title"], &current["score"], &current["memo"]), (&json!(5), &json!("fresh"), &json!(10), &json!("cloud")));

    let malformed = record(6, 300, json!({"rev":7}));
    assert!(store.land_local_cloud(&scope, vec![(malformed, None)], Some(b"malformed")).is_err());
    assert_eq!(row(), current);
    assert_eq!(store.cloud_resume(&scope).unwrap().as_deref(), Some(b"new".as_slice()));
    assert!(store.pending_cloud_records(&scope, 1).unwrap().is_empty());
}

#[test]
fn disk_cloud_sends_only_settled_changes_and_confirmed_removals() {
    use crate::owners::write::operation_status::OperationStatus;
    use crate::store::{OperationRecord, OperationTransition};
    use serde_json::json;

    let model = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let store = Store::open_models(None, vec![model], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    let id = |value: &str| crate::identity::RowId::new(value).unwrap();
    let upsert = |row: &str, text: &str| EntityOp::Upsert {
        model: crate::identity::ModelKey::new("notes"),
        id: id(row),
        row: json!({"id":row, "text":text}).as_object().unwrap().clone(),
    };
    let write = |source, ops| {
        let mut plan = BatchPlan::from_source(source);
        plan.entity_ops = ops;
        store.commit(plan).unwrap()
    };
    write(RowSource::Page, vec![upsert("held", "first confirmed")]);
    store.activate_cloud_scope(scope.clone()).unwrap();
    assert!(store.bootstrap_cloud_chunk(&scope).unwrap());

    let mut optimistic = BatchPlan::from_source(RowSource::Local);
    optimistic.entity_ops.push(upsert("held", "unconfirmed"));
    optimistic.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
        seq: None, action_key: "notes:update".into(), status: OperationStatus::Pending, row_ids: vec![id("held")], temp_id: None,
        input: json!({"id":"held","text":"unconfirmed"}), base_revision: None, created_at: 0, dedupe_key: None,
        rollback: Some(json!({"rows":[{"kind":"edited","model":"notes","id":"held","before":{"text":"first confirmed"},"owned":{"text":1}}]})),
    })));
    let operation = *store.commit(optimistic).unwrap().operations.iter().next().unwrap();

    write(RowSource::Page, vec![upsert("ready", "confirmed")]);
    let pending = store.pending_cloud_records(&scope, 1).unwrap();
    let [ready] = pending.as_slice() else { panic!("expected the settled row") };
    assert_eq!(ready.row_id, "ready");
    store.confirm_cloud_send(&scope, ready, None).unwrap();
    write(RowSource::Page, vec![upsert("ready", "confirmed")]);
    assert!(store.pending_cloud_records(&scope, 1).unwrap().is_empty());

    let removed = |row: &str| EntityOp::Destroy { model: crate::identity::ModelKey::new("notes"), id: id(row), reason: DestroyReason::Server };
    write(RowSource::Event, vec![removed("ready"), removed("missing")]);
    let records = store.pending_cloud_records(&scope, 3).unwrap();
    let saved = records.iter().map(|record| {
        let body = crate::cloud::decode(&record.key, record.payload.as_deref().unwrap()).unwrap();
        (body.row_id, body.deleted)
    }).collect::<Vec<_>>();
    assert_eq!(saved, vec![("missing".into(), true), ("ready".into(), true)]);
    for record in &records { store.confirm_cloud_send(&scope, record, None).unwrap(); }

    let mut confirmed = BatchPlan::from_source(RowSource::Response);
    confirmed.operation_transitions.push(OperationTransition::Close { seq: operation, status: OperationStatus::Committed });
    let confirmation = store.commit(confirmed).unwrap().revision.unwrap();
    let sent = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    assert_eq!(sent.version, confirmation);
    let confirmed_body = crate::cloud::decode(&sent.key, sent.payload.as_deref().unwrap()).unwrap();
    assert_eq!(confirmed_body.row_id, "held");
    assert_eq!(confirmed_body.fields[0].data.value, "unconfirmed");
    assert!(confirmed_body.fields[0].data.stamp.millis > 0);
    store.confirm_cloud_send(&scope, &sent, None).unwrap();

    let mut optimistic_delete = BatchPlan::from_source(RowSource::Local);
    optimistic_delete.entity_ops.push(EntityOp::Destroy { model: crate::identity::ModelKey::new("notes"), id: id("held"), reason: DestroyReason::Local });
    optimistic_delete.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
        seq: None, action_key: "notes:delete".into(), status: OperationStatus::Pending, row_ids: vec![id("held")], temp_id: None,
        input: json!({"id":"held"}), base_revision: None, created_at: 0, dedupe_key: None,
        rollback: Some(json!({"rows":[{"kind":"destroyed","model":"notes","id":"held","before":{"id":"held","text":"unconfirmed"},"members":[],"files":[]}]})),
    })));
    let deleting = *store.commit(optimistic_delete).unwrap().operations.iter().next().unwrap();
    assert!(store.pending_cloud_records(&scope, 1).unwrap().is_empty());

    let mut confirmed_delete = BatchPlan::from_source(RowSource::Response);
    confirmed_delete.operation_transitions.push(OperationTransition::Close { seq: deleting, status: OperationStatus::Committed });
    store.commit(confirmed_delete).unwrap();
    let deletion = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
    let body = crate::cloud::decode(&deletion.key, deletion.payload.as_deref().unwrap()).unwrap();
    assert_eq!(body.row_id, "held");
    assert!(body.deleted);
    assert!(body.fields.is_empty());
}

#[test]
fn cloud_cursor_keeps_a_held_disk_record_durable_across_reopen() {
    use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
    use crate::owners::write::operation_status::OperationStatus;
    use crate::store::{cloud_waiting, OperationRecord, OperationTransition};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("waiting.sqlite");
    let spec = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let open = || Store::open_models(Some(&path), vec![spec.clone()], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let store = open();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    store.activate_cloud_scope(scope.clone()).unwrap();
    let mut plan = BatchPlan::from_source(RowSource::Local);
    plan.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
        seq: None, action_key: "notes:update".into(), status: OperationStatus::Pending,
        row_ids: vec![crate::identity::RowId::new("held").unwrap()], temp_id: None, input: json!({"id":"held"}),
        base_revision: None, created_at: 0, dedupe_key: None, rollback: None,
    })));
    let operation = *store.commit(plan).unwrap().operations.iter().next().unwrap();
    let incoming = CloudRecord { model: "notes".into(), row_id: "held".into(), server_revision: None, deleted: false, fields: vec![
        CloudField { path: vec!["text".into()], data: StampedValue { value: json!("remote"), stamp: CloudStamp { millis: u64::MAX - 1, counter: 4, replica: [4; 16] } } },
    ] };
    let provider_fields = b"system".to_vec();
    store.land_local_cloud(&scope, vec![(incoming.clone(), Some(provider_fields.clone()))], Some(b"first")).unwrap();
    store.land_local_cloud(&scope, vec![(incoming.clone(), None)], Some(b"latest")).unwrap();
    assert!(store.read(&["notes"], |reader| reader.point("notes", "held")).unwrap().is_none());
    drop(store);

    let store = open();
    store.activate_cloud_scope(scope.clone()).unwrap();
    assert_eq!(store.cloud_resume(&scope).unwrap().as_deref(), Some(b"latest".as_slice()));
    let read = || {
        let connection = Connection::open(&path).unwrap();
        cloud_waiting::entries(&connection, &scope.domain, "notes", "held").unwrap()
    };
    let entries = read();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].record, incoming);
    assert_eq!(entries[0].system_fields.as_ref(), Some(&provider_fields));

    let close = StoreBatch {
        author: BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1),
        rows: Vec::new(), intents: 0, credential: None,
        ops: vec![StoreOp::RemoveIntent { op_id: operation as u64 }],
    };
    store.write_batch(&close, store.files(), None).unwrap();
    assert_eq!(store.replay_cloud_waiting(&scope, 16).unwrap(), 1);
    assert_eq!(store.read(&["notes"], |reader| reader.point("notes", "held")).unwrap().unwrap()["text"], "remote");
    assert!(read().is_empty());
    assert_eq!(store.cloud_resume(&scope).unwrap().as_deref(), Some(b"latest".as_slice()));
    assert!(store.pending_cloud_records(&scope, 2).unwrap().is_empty());

    store.retire_cloud_scope(&scope);
    store.erase_cloud_domain(&scope.domain).unwrap();
    assert!(read().is_empty());
}

#[test]
fn cloud_child_and_cursor_wait_for_the_declared_parent_across_reopen() {
    use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
    use crate::schema::{AssociationKind, AssociationSpec};
    use crate::store::cloud_waiting;
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("relationships.sqlite");
    let parent = ModelSpec { storage: Storage::Local, ..ModelSpec::new("parents", vec![FieldSpec::scalar("id", ScalarKind::Id)]) };
    let child = ModelSpec {
        storage: Storage::Local,
        associations: vec![AssociationSpec::new("parent", AssociationKind::BelongsTo, "parents", "parent_id")],
        ..ModelSpec::new("children", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("parent_id", ScalarKind::Id).indexed()])
    };
    let open = || Store::open_models(Some(&path), vec![parent.clone(), child.clone()], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    let store = open();
    store.activate_cloud_scope(scope.clone()).unwrap();
    let stamp = CloudStamp { millis: 120, counter: 3, replica: [6; 16] };
    let incoming = CloudRecord { model: "children".into(), row_id: "child".into(), server_revision: None, deleted: false,
        fields: vec![CloudField { path: vec!["parent_id".into()], data: StampedValue { value: json!("parent"), stamp } }],
    };
    store.land_local_cloud(&scope, vec![(incoming.clone(), Some(b"provider-fields".to_vec()))], Some(b"child-page")).unwrap();
    assert!(store.read(&["children"], |reader| reader.point("children", "child")).unwrap().is_none());
    drop(store);

    let reopened = open();
    reopened.activate_cloud_scope(scope.clone()).unwrap();
    assert_eq!(reopened.cloud_resume(&scope).unwrap().as_deref(), Some(b"child-page".as_slice()));
    let connection = Connection::open(&path).unwrap();
    assert!(cloud::field_stamp(&connection, &scope.domain, "children", "child", &["parent_id".into()]).unwrap().is_none());
    assert!(cloud_waiting::entries(&connection, &scope.domain, "parents", "unrelated").unwrap().is_empty());
    let queued = cloud_waiting::entries(&connection, &scope.domain, "parents", "parent").unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].record, incoming);
    assert_eq!(queued[0].system_fields.as_deref(), Some(b"provider-fields".as_slice()));

    reopened.replay_cloud_waiting(&scope, 16).unwrap();
    assert!(reopened.read(&["children"], |reader| reader.point("children", "child")).unwrap().is_none());

    let mut parent_write = BatchPlan::from_source(RowSource::Local);
    parent_write.entity_ops.push(EntityOp::Upsert {
        model: crate::identity::ModelKey::new("parents"), id: crate::identity::RowId::new("parent").unwrap(),
        row: json!({"id":"parent"}).as_object().unwrap().clone(),
    });
    reopened.commit(parent_write).unwrap();
    assert_eq!(reopened.replay_cloud_waiting(&scope, 16).unwrap(), 1);
    assert_eq!(reopened.read(&["children"], |reader| reader.point("children", "child")).unwrap().unwrap()["parent_id"], "parent");
    assert_eq!(cloud::field_stamp(&connection, &scope.domain, "children", "child", &["parent_id".into()]).unwrap(), Some(stamp));
    assert!(cloud_waiting::entries(&connection, &scope.domain, "parents", "parent").unwrap().is_empty());
    let pending = reopened.pending_cloud_records(&scope, 2).unwrap();
    assert_eq!(pending.iter().map(|record| record.model.as_str()).collect::<Vec<_>>(), ["parents"]);

    for (suffix, parent_first) in [("a", true), ("b", false)] {
        let parent_id = format!("parent-{suffix}");
        let child_id = format!("child-{suffix}");
        let parent_record = CloudRecord { model: "parents".into(), row_id: parent_id.clone(), server_revision: None, deleted: false, fields: Vec::new() };
        let mut child_record = incoming.clone();
        child_record.row_id = child_id.clone();
        child_record.fields[0].data.value = json!(parent_id);
        let mut records = vec![(parent_record, None), (child_record, None)];
        if !parent_first { records.reverse(); }
        reopened.land_local_cloud(&scope, records, Some(suffix.as_bytes())).unwrap();
        reopened.replay_cloud_waiting(&scope, 16).unwrap();
        let row = reopened.read(&["children"], |reader| reader.point("children", &child_id)).unwrap().unwrap();
        assert_eq!(row["parent_id"], parent_id);
        assert_eq!(reopened.cloud_resume(&scope).unwrap().as_deref(), Some(suffix.as_bytes()));
    }

}

#[test]
fn cloud_capture_clears_a_typed_object_as_one_path() {
    use serde_json::json;

    let spec = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec { app_type: Some("NoteDetail".into()), fields: vec![FieldSpec::scalar("title", ScalarKind::Str)], ..FieldSpec::object("detail").nullable().optional() },
    ]) };
    let store = Store::open_models(None, vec![spec], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let scope = CloudScope { epoch: 1, generation: 1, domain: "example".into(), provider_fingerprint: "provider".into() };
    store.activate_cloud_scope(scope.clone()).unwrap();
    let write = |detail| {
        store.write_queue_ops(&[StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
            row_id: "note".into(), values: json!({"id":"note","detail":detail}).to_string(),
        }] }]).unwrap();
        let work = store.pending_cloud_records(&scope, 1).unwrap().pop().unwrap();
        crate::cloud::decode(&work.key, work.payload.as_deref().unwrap()).unwrap()
    };

    let original = write(json!({"title":"before"}));
    assert_eq!(original.fields[0].path, ["detail", "title"]);
    let cleared = write(serde_json::Value::Null);
    assert_eq!(cleared.fields.len(), 1);
    assert_eq!(cleared.fields[0].path, ["detail"]);
    assert!(cleared.fields[0].data.value.is_null());
    assert!(cleared.fields[0].data.stamp > original.fields[0].data.stamp);

    let restored = write(json!({"title":"new"}));
    assert_eq!(restored.fields.len(), 1);
    assert_eq!(restored.fields[0].path, ["detail", "title"]);
    assert_eq!(restored.fields[0].data.value, "new");
}
