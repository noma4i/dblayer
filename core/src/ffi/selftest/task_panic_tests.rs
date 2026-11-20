use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::bound;
use super::upload_prepare_tests::{send, SOURCE};
use super::upload_stand::{Stand, CREATE_MOMENT};
use crate::ffi::{ByteProgress, Database, DbError, FakeTransport, PrepareJob, PrepareOutcome, PrepareProgress, Preparer, ProbeOutcome, PutReply, TransportRequest};
use crate::test_wait::settled;

struct PanickingQuery;

#[async_trait]
impl FakeTransport for PanickingQuery {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { panic!("the transport failed") }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

/// Task panic of Calls (D-472, P-RT-12): the call ends with `Runtime` from its owner and the database closes, the caller does not wait for the end of the epoch.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_whose_transport_panics_ends_with_runtime_and_closes_the_database() {
    let database = bound(Database::open(crate::ffi::Config { path: None }, crate::ffi::specs(crate::ffi::tests::lifecycle_tests::models()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(PanickingQuery)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() })).await.unwrap();
    database.register_operations(json!([{"name":"Users","document":"query Users { users { id name } }"}]).to_string()).await.unwrap();
    database.set_online(true).await.unwrap();
    let called = bound(database.run(database.session_epoch(), "Users".into(), "{}".into())).await;
    assert!(matches!(called, Err(DbError::Runtime { .. })), "{called:?}");
    settled(&database.life, "the database did not close after the panic of the call task", || database.life.published().closed).await;
}

#[derive(Default)]
struct PanickingClear {
    held: crate::ffi::tests::session_guards_tests::WatchedHeld,
    armed: std::sync::atomic::AtomicBool,
}

impl crate::ffi::CredentialStore for PanickingClear {
    fn get(&self) -> Result<Option<String>, DbError> { self.held.get() }
    fn set(&self, credential: String) -> Result<(), DbError> { self.held.set(credential) }
    fn clear(&self) -> Result<(), DbError> {
        assert!(!self.armed.load(std::sync::atomic::Ordering::SeqCst), "the credential store failed");
        self.held.clear()
    }
}

/// Task panic of Store (D-472, P-RT-12): the erase of the secret panics, the reset of the session ends by the row of a failed erase and the database closes.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_reset_whose_credential_clear_panics_ends_and_closes_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(PanickingClear::default());
    let mut stand = Stand::new();
    stand.store = Some(store.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    store.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = bound(database.session_reset()).await;
    settled(&database.life, "the database did not close after the panic of the erase task", || database.life.published().closed).await;
}

struct PanickingPrepare;

#[async_trait]
impl Preparer for PanickingPrepare {
    async fn probe(&self, _: String, _: Option<Vec<u8>>, _: Option<String>) -> ProbeOutcome { ProbeOutcome::Failed { message: "unused".into() } }
    async fn run(&self, _: PrepareJob, _: Arc<PrepareProgress>) -> PrepareOutcome { panic!("the platform failed") }
    fn cancel(&self, _: u64) {}
    fn import_descriptor(&self, _: crate::ffi::SavedPreparation) -> crate::ffi::PrepareImportOutcome { crate::ffi::PrepareImportOutcome::Failed { message: "unused".into() } }
}

/// Task panic of Transfer ends its receipt and closes the database (D-472, P-RT-12).
#[tokio::test(flavor = "multi_thread")]
async fn a_transfer_whose_preparation_panics_ends_its_receipt_and_closes_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, SOURCE).unwrap();
    let stand = Stand::new().keep_moment().with_preparer(Arc::new(PanickingPrepare));
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    let receipt = bound(database.send(send(database.session_epoch(), &source, super::upload_prepare_tests::descriptor()))).await.unwrap();
    let confirmed = bound(database.confirmed(receipt, false)).await;
    assert!(confirmed.is_err(), "{confirmed:?}");
    settled(&database.life, "the database did not close after the panic of the prepare task", || database.life.published().closed).await;
}
