use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into(), power_saving: false, low_ram: false } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts::default()).await
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
async fn life_register_no_transport() {
    let slot = OpenDatabase::new();
    let database = open(&slot).await.unwrap();
    let result = database.register_operations(r#"[{"name":"Query","document":"query Query { items }"}]"#.into());
    assert!(matches!(result, Err(DbError::NoSession { .. })));
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn life_register_closed() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await.unwrap();
    database.close_database().await.unwrap();
    let result = database.register_operations(r#"[{"name":"Query","document":"query Query { items }"}]"#.into());
    assert!(matches!(result, Err(DbError::Closed { .. })));
}

#[tokio::test]
async fn life_register_invalid() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await.unwrap();
    for definitions in [
        r#"[{"document":"query Query { items }"}]"#,
        r#"[{"name":"Query"}]"#,
        r#"[{"name":"Query","document":"query Query"}]"#,
        r#"[{"name":"Query","document":"query Query { items }","pageSize":4294967296}]"#,
        r#"[{"name":"Query","document":"query Query { items }","pageSize":-1}]"#,
    ] {
        assert!(matches!(database.register_operations(definitions.into()), Err(DbError::Invalid { .. })), "{definitions}");
    }
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn life_register_bad_preload() {
    use super::super::database_tests::Plain;
    use crate::schema::{FieldSpec, ModelSpec, PreloadChoice, PreloadSource, RelationSpec};
    use crate::value::ScalarKind;
    for bad_source in [false, true] {
        let mut model = ModelSpec::new("Item", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
        let mut selection = RelationSpec::new("all", &[]);
        selection.text_params.push("category".into());
        let choice = if bad_source { PreloadChoice::RowsOf { from: PreloadSource { model: "Item".into(), relation: "missing".into() } } } else { PreloadChoice::Listed(vec![serde_json::json!("x")]) };
        selection.preload = Some([(if bad_source { "category" } else { "unknown" }.to_string(), choice)].into());
        selection.preload_limit = Some(1);
        model.relations.push(selection);
        let database = Database::open(config(), serde_json::to_string(&vec![model]).unwrap(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await.unwrap();
        let definitions = r#"[{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}]"#;
        assert!(matches!(database.register_operations(definitions.into()), Err(DbError::Invalid { .. })));
        database.set_online(true).unwrap();
        let answer = crate::test_wait::within(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into())).await;
        assert!(matches!(answer, Err(DbError::Invalid { .. })));
        database.close_database().await.unwrap();
    }
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), crate::ffi::OpenPorts::default()).await.is_err());
    assert!(open(&slot).await.is_ok());
}

fn clip_with_blob(name: &str) -> String {
    let mut clip = crate::schema::ModelSpec::new("Clip", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    clip.upload_steps = Some(crate::owners::write::upload::UploadOperations { create_blob: Some(crate::owners::write::upload::UploadOperation::new(name)), multipart: None });
    serde_json::to_string(&vec![clip]).unwrap()
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }"}]"#.into(), power_saving: false, low_ram: false };
        let opened = slot.open(config(), clip_with_blob(name), options, crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await;
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
    let opened = slot.open(config(), clip_with_blob("CreateBlob"), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), ..Default::default() }).await;
    let error = match opened {
        Err(error) => error.to_string(),
        Ok(_) => panic!("uploads with empty operations were accepted"),
    };
    assert!(error.ends_with("invalid: uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open"), "{error}");
    assert!(open(&slot).await.is_ok(), "the refused open must release its slot");
}

