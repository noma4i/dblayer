use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into(), power_saving: false, low_ram: false } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), None, None, None, None, None).await
}

#[tokio::test]
async fn life_open_twice() {
    let slot = OpenDatabase::new();
    let first = open(&slot).await.unwrap();
    match open(&slot).await {
        Err(DbError::Invalid { detail }) => assert_eq!(detail, "database is already open; open it once at app start"),
        other => panic!("second open gave {:?}", other.map(|_| ())),
    }
    first.close_database().await.unwrap();
}

#[tokio::test]
async fn life_open_after_close() {
    let slot = OpenDatabase::new();
    let first = open(&slot).await.unwrap();
    first.close_database().await.unwrap();
    let second = open(&slot).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    second.close_database().await.unwrap();
}

#[tokio::test]
async fn life_open_concurrent() {
    let slot = OpenDatabase::new();
    let (first, second) = tokio::join!(open(&slot), open(&slot));
    let database = match (first, second) {
        (Ok(database), Err(DbError::Invalid { .. })) | (Err(DbError::Invalid { .. }), Ok(database)) => database,
        _ => panic!("exactly one open must succeed"),
    };
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), None, None, None, None, None).await.is_err());
    assert!(open(&slot).await.is_ok());
}

fn clip_with_blob(name: &str) -> String {
    let mut clip = crate::schema::ModelSpec::new("Clip", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    clip.upload_steps = Some(crate::upload::UploadOperations { create_blob: Some(crate::upload::UploadOperation::new(name)), multipart: None });
    serde_json::to_string(&vec![clip]).unwrap()
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }"}]"#.into(), power_saving: false, low_ram: false };
        let opened = slot.open(config(), clip_with_blob(name), options, Some(Arc::new(Plain)), None, None, None, None).await;
        match opened {
            Ok(_) => assert!(accepted, "{name} was opened"),
            Err(error) => assert!(!accepted && error.to_string().ends_with("invalid: upload operation Missing is not registered"), "{name}: {error}"),
        }
    }
}


#[tokio::test]
async fn inv_upload_22_uploads_with_empty_operations_explain_the_cause_and_the_fix() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let opened = slot.open(config(), clip_with_blob("CreateBlob"), options(), Some(Arc::new(Plain)), None, None, None, None).await;
    let error = match opened {
        Err(error) => error.to_string(),
        Ok(_) => panic!("uploads with empty operations were accepted"),
    };
    assert!(error.ends_with("invalid: uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open"), "{error}");
    assert!(open(&slot).await.is_ok(), "the refused open must release its slot");
}

