use super::*;
use crate::ffi::ConfigCode;

fn config() -> Config { Config { path: None, data_version: None } }
fn options() -> OpenOptions { OpenOptions::default() }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts::default(), None).await
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

#[tokio::test(flavor = "multi_thread")]
async fn life_close_concurrent() {
    let slot = OpenDatabase::new();
    let database = open(&slot).await.unwrap();
    let first = tokio::spawn({ let database = database.clone(); async move { database.close_database().await } });
    crate::test_wait::yield_until("the first close started", || database.life.is_shut()).await;
    let second = tokio::spawn({ let database = database.clone(); async move { database.close_database().await } });
    assert!(crate::test_wait::within(first).await.unwrap().is_ok());
    assert!(crate::test_wait::within(second).await.unwrap().is_ok());
    assert_eq!(database.life.owners_alive(), 0);
}

#[tokio::test]
async fn life_register_no_transport() {
    let slot = OpenDatabase::new();
    let database = open(&slot).await.unwrap();
    let result = database.register_operations(r#"[{"name":"Query","document":"query Query { items }"}]"#.into()).await;
    assert!(matches!(result, Err(DbError::NoSession { .. })));
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn life_register_closed() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
    database.close_database().await.unwrap();
    let result = database.register_operations(r#"[{"name":"Query","document":"query Query { items }"}]"#.into()).await;
    assert!(matches!(result, Err(DbError::Closed { .. })));
}

#[tokio::test]
async fn life_register_invalid() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
    for definitions in [
        r#"[{"document":"query Query { items }"}]"#,
        r#"[{"name":"Query"}]"#,
        r#"[{"name":"Query","document":"query Query"}]"#,
        r#"[{"name":"Query","document":"query Query { items }","pageSize":4294967296}]"#,
        r#"[{"name":"Query","document":"query Query { items }","pageSize":-1}]"#,
    ] {
        assert!(matches!(database.register_operations(definitions.into()).await, Err(DbError::Invalid { .. })), "{definitions}");
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
        let database = Database::open(config(), serde_json::to_string(&vec![model]).unwrap(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
        let definitions = r#"[{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}]"#;
        assert!(matches!(database.register_operations(definitions.into()).await, Err(DbError::Invalid { .. })));
        database.set_online(true).unwrap();
        let answer = crate::test_wait::within(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into())).await;
        assert!(matches!(answer, Err(DbError::Invalid { .. })));
        database.close_database().await.unwrap();
    }
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), crate::ffi::OpenPorts::default(), None).await.is_err());
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
        let options = OpenOptions { operations: vec![OperationDefinition { name: "CreateBlob".into(), document: "mutation CreateBlob { createBlob { url } }".into() }], ..Default::default() };
        let opened = slot.open(config(), clip_with_blob(name), options, crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await;
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
    let opened = slot.open(config(), clip_with_blob("CreateBlob"), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await;
    let error = match opened {
        Err(error) => error.to_string(),
        Ok(_) => panic!("uploads with empty operations were accepted"),
    };
    assert!(error.ends_with("invalid: uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open"), "{error}");
    assert!(open(&slot).await.is_ok(), "the refused open must release its slot");
}


#[derive(Default)]
struct Observed(std::sync::Mutex<Vec<&'static str>>);

impl PlatformObserver for Observed {
    fn start(&self, _sink: Arc<crate::ffi::OnlineSink>) { self.0.lock().unwrap().push("start"); }
    fn stop(&self) { self.0.lock().unwrap().push("stop"); }
}

#[tokio::test]
async fn life_closing_observers_stopped_last_reaches_the_platform_observer() {
    let slot = OpenDatabase::new();
    let observed = Arc::new(Observed::default());
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts::default(), Some(observed.clone())).await.unwrap();
    assert_eq!(*observed.0.lock().unwrap(), ["start"]);
    database.close_database().await.unwrap();
    assert_eq!(*observed.0.lock().unwrap(), ["start", "stop"]);
    database.close_database().await.unwrap();
    assert_eq!(*observed.0.lock().unwrap(), ["start", "stop"]);
}

#[tokio::test]
async fn life_prop_not_open_typed_for_census_report_and_admission() {
    let slot = OpenDatabase::new();
    let database = open(&slot).await.unwrap();
    assert_eq!(database.admit(None).unwrap(), database.session_epoch());
    assert_eq!(database.admit(Some(7)).unwrap(), 7);
    assert!(database.open_report().is_ok() && database.subscription_census().await.is_ok());
    database.close_database().await.unwrap();
    assert!(matches!(database.admit(None), Err(DbError::Closed { .. })));
    assert!(matches!(database.admit(Some(7)), Err(DbError::Closed { .. })));
    assert!(matches!(database.open_report(), Err(DbError::Closed { .. })));
    assert!(matches!(database.subscription_census().await, Err(DbError::Closed { .. })));
}

#[tokio::test]
async fn a_load_call_without_a_relation_is_invalid() {
    use super::super::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), "[]".into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
    let epoch = database.session_epoch();
    assert!(matches!(database.begin_load(epoch, "Item".into(), None, "{}".into()), Err(DbError::Invalid { detail }) if detail.contains("no relation")));
    assert!(matches!(database.refresh(epoch, "Item".into(), None, "{}".into()).await, Err(DbError::Invalid { .. })));
    assert!(matches!(database.load_more(epoch, "Item".into(), None, "{}".into()).await, Err(DbError::Invalid { .. })));
    assert!(matches!(database.edge_values(epoch, "Item".into(), None, "{}".into(), vec![]).await, Err(DbError::Invalid { .. })));
    database.close_database().await.unwrap();
}

#[test]
fn the_round_job_carries_the_round_id_the_core_numbered() {
    use super::super::upload_rounds::{RoundJob, RoundJobsSink, RoundJobsSlot};
    use crate::owners::write::rounds_executor::RoundJobs;
    struct Asked(std::sync::Mutex<Vec<RoundJob>>);
    impl RoundJobsSink for Asked {
        fn schedule_job(&self, job: RoundJob) { self.0.lock().unwrap().push(job); }
        fn end_job(&self, _round_id: u32) {}
    }
    let asked = Arc::new(Asked(Default::default()));
    let slot = RoundJobsSlot::default();
    assert!(slot.set(asked.clone()));
    slot.schedule_job((1u64 << 32) + 5, 9);
    assert_eq!(*asked.0.lock().unwrap(), [RoundJob { request_id: (1 << 32) + 5, round_id: 5, epoch: 9 }]);
}

#[tokio::test]
async fn xfer_android_permission_checked_at_open() {
    let slot = OpenDatabase::new();
    for (declared, refused) in [(Some(false), true), (Some(true), false), (None, false)] {
        let options = OpenOptions { data_sync_declared: declared, ..options() };
        let opened = slot.open(config(), "[]".into(), options, crate::ffi::OpenPorts::default(), None).await;
        match (opened, refused) {
            (Err(DbError::Config { code: ConfigCode::InvalidRequest, detail }), true) => assert!(detail.contains("FOREGROUND_SERVICE_DATA_SYNC"), "{detail}"),
            (Ok(database), false) => database.close_database().await.unwrap(),
            (other, _) => panic!("declared {declared:?} gave {:?}", other.map(|_| ())),
        }
    }
}
