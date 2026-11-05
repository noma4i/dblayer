use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into(), uploads: None, power_saving: false, low_ram: false } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), None, None, None, None, None).await
}

#[tokio::test]
async fn a_second_open_is_refused_even_with_equal_arguments_until_the_database_is_closed() {
    let slot = OpenDatabase::new();
    let first = open(&slot).await.unwrap();
    for _ in 0..2 {
        match open(&slot).await {
            Err(DbError::Invalid { detail }) => assert_eq!(detail, "database is already open; open it once at app start"),
            other => panic!("second open gave {:?}", other.map(|_| ())),
        }
    }
    first.close_now();
    let second = open(&slot).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(open(&slot).await.is_err());
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), None, None, None, None, None).await.is_err());
    assert!(open(&slot).await.is_ok());
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use super::super::database_tests::Plain;
    use super::super::upload_wire::{WireUploadOperation, WireUploadOperations};
    let blob = |name: &str| WireUploadOperations { create_blob: Some(WireUploadOperation { name: name.into(), root_path: None }), multipart: None };
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }"}]"#.into(), uploads: Some(blob(name)), power_saving: false, low_ram: false };
        let opened = slot.open(config(), "[]".into(), options, Some(Arc::new(Plain)), None, None, None, None).await;
        match opened {
            Ok(_) => assert!(accepted, "{name} was opened"),
            Err(error) => assert!(!accepted && error.to_string().ends_with("invalid: upload operation Missing is not registered"), "{name}: {error}"),
        }
    }
}

struct CountingFeed(Arc<std::sync::atomic::AtomicUsize>);

#[tokio::test]
async fn inv_upload_22_uploads_with_empty_operations_explain_the_cause_and_the_fix() {
    use super::super::database_tests::Plain;
    use super::super::upload_wire::{WireUploadOperation, WireUploadOperations};
    let slot = OpenDatabase::new();
    let options = OpenOptions {
        uploads: Some(WireUploadOperations { create_blob: Some(WireUploadOperation { name: "CreateBlob".into(), root_path: None }), multipart: None }),
        ..options()
    };
    let opened = slot.open(config(), "[]".into(), options, Some(Arc::new(Plain)), None, None, None, None).await;
    let error = match opened {
        Err(error) => error.to_string(),
        Ok(_) => panic!("uploads with empty operations were accepted"),
    };
    assert!(error.ends_with("invalid: uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open"), "{error}");
    assert!(open(&slot).await.is_ok(), "the refused open must release its slot");
}

#[async_trait::async_trait]
impl Transport for CountingFeed {
    async fn open_cable(&self, _: Arc<super::super::CableLease>, _url: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) { unreachable!() }
    async fn query(&self, _: super::super::TransportRequest) -> Result<String, DbError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(r#"{"data":{"feed":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#.into())
    }
    async fn mutate(&self, _: super::super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::super::TransportRequest, _: Arc<super::super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::super::ByteProgress>) -> Result<super::super::PutReply, DbError> { unreachable!() }
}
