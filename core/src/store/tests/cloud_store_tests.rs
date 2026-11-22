use rusqlite::Connection;

use crate::store::cloud::{self, PendingRecord};
use crate::owners::{contract::BatchAuthor, store::{RowValue, StoreBatch, StoreOp}};
use crate::schema::{FieldSpec, ModelSpec, RowSource, Storage};
use crate::store::{BatchPlan, DestroyReason, EntityOp, Store};
use crate::value::ScalarKind;

#[test]
fn delayed_cloud_acknowledgement_keeps_the_newer_change_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite");
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
    cloud::acknowledge(&transaction, "account-a", &original).unwrap();
    cloud::write_progress(&transaction, "account-a", b"committed-cursor").unwrap();
    transaction.commit().unwrap();

    let transaction = connection.transaction().unwrap();
    cloud::write_progress(&transaction, "account-a", b"uncommitted-cursor").unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion).unwrap();
    transaction.rollback().unwrap();
    drop(connection);

    let mut reopened = Connection::open(path).unwrap();
    assert_eq!(cloud::pending(&reopened, "account-a", 1).unwrap(), vec![deletion.clone()]);
    assert_eq!(cloud::progress(&reopened, "account-a").unwrap().as_deref(), Some(b"committed-cursor".as_slice()));

    let transaction = reopened.transaction().unwrap();
    cloud::acknowledge(&transaction, "account-a", &deletion).unwrap();
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

    store.activate_cloud_domain("example".into()).unwrap();
    let version = write("n1", "save me", "initial");
    let connection = Connection::open(&path).unwrap();
    let created = PendingRecord { model: "notes".into(), row_id: "n1".into(), version, deleted: false };
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);
    write("n1", "save me", "initial");
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);

    let materialized = |expected_version| {
        let pending = store.pending_cloud_records("example", 2).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].version, expected_version);
        crate::cloud::decode(&pending[0].key, pending[0].payload.as_ref().unwrap()).unwrap()
    };
    let before = materialized(version);
    let updated_version = write("n1", "save me", "edited");
    let after = materialized(updated_version);
    assert_eq!(before.fields.len(), 3);
    assert_eq!(after.fields.len(), 3);
    assert_eq!(after.fields[0].path, ["metadata", "first"]);
    assert_eq!(after.fields[0].data.value, serde_json::json!("edited"));
    assert!(after.fields[0].data.stamp > before.fields[0].data.stamp);
    assert_eq!(after.fields[1..], before.fields[1..]);

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
    let remote = crate::cloud::Stamp { millis: u64::MAX - 1, counter: u32::MAX, replica: [9; 16] };
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
