use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into(), uploads: None } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), None, None, None, None).await
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
    first.shutdown();
    let second = open(&slot).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(open(&slot).await.is_err());
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), None, None, None, None).await.is_err());
    assert!(open(&slot).await.is_ok());
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use super::super::database_tests::Plain;
    use super::super::upload_wire::{WireUploadOperation, WireUploadOperations};
    let blob = |name: &str| WireUploadOperations { create_blob: Some(WireUploadOperation { name: name.into(), root_path: None }), multipart: None };
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }"}]"#.into(), uploads: Some(blob(name)) };
        let opened = slot.open(config(), "[]".into(), options, Some(Arc::new(Plain)), None, None, None).await;
        match opened {
            Ok(_) => assert!(accepted, "{name} was opened"),
            Err(error) => assert!(!accepted && error.to_string().ends_with("invalid upload: upload operation Missing is not registered"), "{name}: {error}"),
        }
    }
}
