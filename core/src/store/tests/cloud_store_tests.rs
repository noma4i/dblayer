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
    ]);
    notes.storage = Storage::Local;
    let store = Store::open_models(Some(&path), vec![notes], std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let write = |id: &str, text: &str| {
        let batch = StoreBatch {
            author: BatchAuthor::LocalWrites, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1),
            rows: Vec::new(), intents: 0, credential: None,
            ops: vec![StoreOp::LocalRows { model: "notes".into(), rows: vec![RowValue {
                row_id: id.into(), values: serde_json::json!({"id": id, "text": text}).to_string(),
            }] }],
        };
        store.write_batch(&batch, store.files(), None).unwrap().revision as i64
    };
    write("offline", "ordinary write");
    assert!(!store.table_exists("cloud_record").unwrap());
    assert!(!store.table_exists("cloud_progress").unwrap());

    store.activate_cloud_domain("example".into()).unwrap();
    let version = write("n1", "save me");
    let connection = Connection::open(&path).unwrap();
    let created = PendingRecord { model: "notes".into(), row_id: "n1".into(), version, deleted: false };
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);
    write("n1", "save me");
    assert_eq!(cloud::pending(&connection, "example", 2).unwrap(), vec![created.clone()]);

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
}
