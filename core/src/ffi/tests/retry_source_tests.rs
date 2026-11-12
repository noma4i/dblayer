use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::encrypted_tests::{SubscriptionsOnly, ThroughServer};
use super::sealed_errors_tests::moment_spec;
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest, WireUploadSource};
use crate::ffi::{Config, Database, DbError, EncryptedConfig, OpenPorts, ReadRequest, ReadValue, WireUploadStatus};
use crate::test_wait::within;
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};

const BLOB: &str = "mutation CreateBlob { createBlob { url headers signedId blobId } }";
const MUTATION: &str = "mutation MakeMoment { makeMoment { id text } }";

async fn open(path: &Path, rig: &Rig) -> Arc<Database> {
    let config = EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    let ports = OpenPorts { transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let database = within(Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), ports)).await.unwrap();
    let operations = serde_json::json!([{"name":"CreateBlob","document":BLOB,"rootPath":"createBlob"},{"name":"MakeMoment","document":MUTATION,"rootPath":"makeMoment"}]);
    database.register_operations(operations.to_string()).await.unwrap();
    database
}

async fn failed_uploads(database: &Database) -> Vec<crate::ffi::OperationInfo> {
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Operations);
    loop {
        match within(stream.pull_next()).await {
            Some(ReadValue::Operations { items, .. }) if items.iter().any(|item| item.upload.as_ref().is_some_and(|upload| upload.status == WireUploadStatus::Failed)) => return items,
            Some(ReadValue::Operations { .. }) => {}
            other => panic!("operations: {other:?}"),
        }
    }
}

fn names(path: &Path) -> Vec<PathBuf> {
    let mut names: Vec<_> = std::fs::read_dir(path).unwrap().map(|entry| entry.unwrap().path()).collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_retry_source_swap_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let original = directory.path().join("old.jpg");
    std::fs::write(&original, b"original image").unwrap();
    let rig = Rig::new();
    rig.server.register(BLOB);
    rig.server.register(MUTATION);
    let database = open(&path, &rig).await;
    database.set_online(true).unwrap();
    within(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    crate::test_wait::settled(&database, "session setup completes", || true).await;
    let epoch = database.session_epoch();
    rig.server.reply_with(serde_json::json!({"makeMoment":{"id":"r1","text":"original"}}));
    let created = within(database.create(epoch, "Moment".into(), r#"{"text":"original"}"#.into(), "{}".into())).await.unwrap();
    assert_eq!(within(database.confirmed(created, false)).await.unwrap().value.unwrap().id, "r1");
    rig.server.refuse_next("FORBIDDEN", true);
    let source = WireSource::File { path: original.to_string_lossy().into_owned(), name: None, mime: None, prepare: None };
    let receipt = within(database.upload(epoch, "Moment".into(), "r1".into(), "upload".into(), WireUploadRequest { source, variables_json: "{}".into() })).await.unwrap();
    assert!(matches!(within(database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));
    let failed = failed_uploads(&database).await;
    assert_eq!(failed.len(), 1);
    let upload = failed[0].upload.as_ref().unwrap();
    assert_eq!(upload.status, WireUploadStatus::Failed);
    let copy = PathBuf::from(upload.local_url.as_ref().unwrap().strip_prefix("file://").unwrap());
    assert_eq!(std::fs::read(&copy).unwrap(), b"original image");
    let before = names(copy.parent().unwrap());
    database.set_online(false).unwrap();
    let bad = WireUploadSource { path: directory.path().to_string_lossy().into_owned(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None };
    let retry = within(database.retry(epoch, "Moment".into(), "r1".into(), Some("upload".into()), Some(bad))).await;
    assert!(matches!(retry, Err(DbError::Copy { .. })), "retry: {retry:?}");
    assert_eq!(std::fs::read(&copy).unwrap(), b"original image");
    assert_eq!(names(copy.parent().unwrap()), before);
    within(database.close_database()).await.unwrap();
    drop(database);
    let reopened = open(&path, &rig).await;
    let restored = failed_uploads(&reopened).await;
    assert_eq!(restored.len(), 1);
    let upload = restored[0].upload.as_ref().unwrap();
    assert_eq!(upload.status, WireUploadStatus::Failed);
    assert_eq!(upload.local_url.as_deref(), Some(format!("file://{}", copy.display()).as_str()));
    assert_eq!(std::fs::read(&copy).unwrap(), b"original image");
    assert_eq!(names(copy.parent().unwrap()), before);
    within(reopened.close_database()).await.unwrap();
}
