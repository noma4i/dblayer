use super::*;
use crate::ffi::ConfigCode;

fn config() -> Config { Config { path: None, data_version: None } }
fn options() -> OpenOptions { OpenOptions::default() }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts::default(), None).await
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
    let first = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), { let database = database.clone(); async move { database.close_database().await } }));
    crate::test_wait::settled(&database, "the first close started", || database.life.is_shut()).await;
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
    use crate::ffi::tests::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
    database.close_database().await.unwrap();
    let result = database.register_operations(r#"[{"name":"Query","document":"query Query { items }"}]"#.into()).await;
    assert!(matches!(result, Err(DbError::Closed { .. })));
}

#[tokio::test]
async fn life_register_invalid() {
    use crate::ffi::tests::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
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
    use crate::ffi::tests::database_tests::Plain;
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
        let database = Database::open(config(), crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
        let definitions = r#"[{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}]"#;
        assert!(matches!(database.register_operations(definitions.into()).await, Err(DbError::Invalid { .. })));
        database.set_online(true).unwrap();
        let answer = crate::test_wait::within(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into())).await;
        assert!(matches!(answer, Err(DbError::Invalid { .. })));
        database.close_database().await.unwrap();
    }
}

/// Requirement life_register_missing_own_operation: `RegisterOperations` refuses definitions that lack an operation the library sends by itself (D-379). Input: a model with `changed_by` reads, a `node` relation, a relation of a second read and `listens`, registered with some operations left out. Observable: the answer and the next call. Assert: `Invalid` names every missing operation in alphabetical order in 1 error, no definition of the refused set is accepted, and the complete set is accepted.
#[tokio::test]
async fn life_register_missing_own_operation() {
    use crate::ffi::tests::database_tests::Plain;
    use crate::schema::{ChangeFeed, FieldSpec, ModelSpec, RelationSpec, RemoteShape, UpdatesSpec};
    use crate::value::ScalarKind;
    let documents = [
        ("Changes", r#"{"name":"Changes","document":"subscription Changes { changes { topic action recordId } }","rootPath":"changes"}"#),
        ("ItemNode", r#"{"name":"ItemNode","document":"query ItemNode($id: ID!) { node(id: $id) { id } }"}"#),
        ("OwnerItems", r#"{"name":"OwnerItems","document":"query OwnerItems { ownerItems { id } }"}"#),
        ("ItemListen", r#"{"name":"ItemListen","document":"subscription ItemListen { itemListen { id } }"}"#),
    ];
    let greeting = r#"{"name":"Greeting","document":"query Greeting($name: String!) { greeting(name: $name) }"}"#;
    let mut model = ModelSpec::new("Item", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut node = RelationSpec::new("node", &[crate::schema::NODE_PARAM]);
    node.remote = Some(crate::schema_fixtures::remote("ItemNode", RemoteShape::Single, "1m"));
    let mut owned = RelationSpec::new("byOwner", &[]);
    owned.remote = Some(crate::schema_fixtures::remote("OwnerItems", RemoteShape::Single, "1m"));
    model.relations.extend([node, owned]);
    model.change_feed = Some(ChangeFeed { subscription: "Changes".into(), root: "changes".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    model.changes = vec![
        serde_json::from_value(serde_json::json!({"area":"ITEM", "relation":"node", "parameter":"id"})).unwrap(),
        serde_json::from_value(serde_json::json!({"area":"OWNER", "relation":"byOwner"})).unwrap(),
    ];
    model.listens.push(UpdatesSpec { subscription: "ItemListen".into(), deleted_by: None, deleted_unless: None, set: Default::default(), removes: false, debounce: None, root: None, id: Vec::new(), apply: None });
    let database = Database::open(config(), crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.set_online(true).unwrap();
    for (left_out, expected) in [(vec!["Changes"], "Changes"), (vec!["ItemNode"], "ItemNode"), (vec!["OwnerItems"], "OwnerItems"), (vec!["ItemListen"], "ItemListen"), (vec!["ItemNode", "Changes"], "Changes, ItemNode")] {
        let given: Vec<&str> = documents.iter().filter(|(name, _)| !left_out.contains(name)).map(|(_, text)| *text).chain([greeting]).collect();
        match database.register_operations(format!("[{}]", given.join(","))).await {
            Err(DbError::Invalid { detail }) => assert!(detail.ends_with(expected), "{left_out:?}: {detail}"),
            other => panic!("{left_out:?}: {other:?}"),
        }
        let answer = database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into()).await;
        assert!(matches!(answer, Err(DbError::Invalid { .. })), "{left_out:?}: no definition of a refused set is accepted");
    }
    let all: Vec<&str> = documents.iter().map(|(_, text)| *text).chain([greeting]).collect();
    database.register_operations(format!("[{}]", all.join(","))).await.unwrap();
    assert_eq!(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into()).await.unwrap(), r#""Hello Ada""#);
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    let refused = slot.open(config(), "invalid".into(), options(), crate::ffi::OpenPorts::default(), None).await;
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "{:?}", refused.as_ref().err());
    assert!(open(&slot).await.is_ok());
}

fn clip_with_blob(name: &str) -> String {
    let mut clip = crate::schema::ModelSpec::new("Clip", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    clip.upload_steps = Some(crate::owners::write::upload::UploadOperations { create_blob: Some(crate::owners::write::upload::UploadOperation::new(name)), multipart: None });
    serde_json::to_string(&vec![clip]).unwrap()
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use crate::ffi::tests::database_tests::Plain;
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: vec![OperationDefinition { name: "CreateBlob".into(), document: "mutation CreateBlob { createBlob { url } }".into() }] };
        let opened = slot.open(config(), crate::ffi::specs(clip_with_blob(name)), options, crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await;
        match opened {
            Ok(_) => assert!(accepted, "{name} was opened"),
            Err(error) => assert!(!accepted && error.to_string().ends_with("invalid: upload operation Missing is not registered"), "{name}: {error}"),
        }
    }
}


#[tokio::test]
async fn inv_upload_22_uploads_with_empty_operations_explain_the_cause_and_the_fix() {
    use crate::ffi::tests::database_tests::Plain;
    let slot = OpenDatabase::new();
    let opened = slot.open(config(), crate::ffi::specs(clip_with_blob("CreateBlob")), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await;
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
    let database = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts::default(), Some(observed.clone())).await.unwrap();
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
    use crate::ffi::tests::database_tests::Plain;
    let slot = OpenDatabase::new();
    let database = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts { transport: Some(Arc::new(Plain)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, None).await.unwrap();
    let epoch = database.session_epoch();
    assert!(matches!(database.begin_load(epoch, "Item".into(), None, "{}".into()), Err(DbError::Invalid { detail }) if detail.contains("no relation")));
    assert!(matches!(database.refresh(epoch, "Item".into(), None, "{}".into()).await, Err(DbError::Invalid { .. })));
    assert!(matches!(database.load_more(epoch, "Item".into(), None, "{}".into()).await, Err(DbError::Invalid { .. })));
    assert!(matches!(database.edge_values(epoch, "Item".into(), None, "{}".into(), vec![]).await, Err(DbError::Invalid { .. })));
    database.close_database().await.unwrap();
}

#[test]
fn every_round_job_carries_a_round_id_no_other_job_of_the_process_has() {
    use crate::ffi::upload_rounds::{RoundJob, RoundJobsSink, RoundJobsSlot};
    use crate::owners::write::rounds_executor::RoundJobs;
    struct Asked(std::sync::Mutex<Vec<RoundJob>>);
    impl RoundJobsSink for Asked {
        fn schedule_job(&self, job: RoundJob) { self.0.lock().unwrap().push(job); }
        fn end_job(&self, _round_id: u32) {}
    }
    let asked = Arc::new(Asked(Default::default()));
    let slot = RoundJobsSlot::default();
    assert!(slot.set(asked.clone()));
    let other = RoundJobsSlot::default();
    assert!(other.set(asked.clone()));
    slot.schedule_job(1, 9);
    other.schedule_job(1, 9);
    slot.schedule_job(2, 9);
    let jobs = asked.0.lock().unwrap();
    assert_eq!(jobs.iter().map(|job| job.request_id).collect::<Vec<_>>(), [1, 1, 2]);
    let ids: std::collections::BTreeSet<u32> = jobs.iter().map(|job| job.round_id).collect();
    assert_eq!(ids.len(), 3, "a request id repeats between databases of one process, a round id does not");
}

#[tokio::test]
async fn xfer_android_permission_checked_at_open() {
    let slot = OpenDatabase::new();
    for (declared, refused) in [(Some(false), true), (Some(true), false), (None, false)] {
        let opened = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), options(), crate::ffi::OpenPorts { data_sync_declared: declared, ..Default::default() }, None).await;
        match (opened, refused) {
            (Err(DbError::Config { code: ConfigCode::InvalidRequest, detail }), true) => assert!(detail.contains("FOREGROUND_SERVICE_DATA_SYNC"), "{detail}"),
            (Ok(database), false) => database.close_database().await.unwrap(),
            (other, _) => panic!("declared {declared:?} gave {:?}", other.map(|_| ())),
        }
    }
}

struct Recording(Arc<std::sync::Mutex<Vec<String>>>);

#[async_trait::async_trait]
impl Transport for Recording {
    fn app_headers(&self) -> HashMap<String, String> { HashMap::new() }
    async fn send(&self, request: crate::transport::HttpRequest) -> crate::transport::RawOutcome {
        self.0.lock().unwrap().push(request.url);
        crate::transport::RawOutcome::response(200, "{}")
    }
    async fn open_cable(&self, _: String, _: HashMap<String, String>, _: u64, _: Arc<dyn crate::ffi::CableEvents>) -> crate::ffi::CableOutcome { unreachable!() }
    async fn put(&self, _: crate::ffi::PutTarget, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> crate::transport::RawOutcome { unreachable!() }
}

fn request(url: &str) -> crate::transport::HttpRequest {
    crate::transport::HttpRequest { author: crate::transport::Author::Window, request_id: 1, attempt: 1, url: url.into(), method: "POST".into(), headers: HashMap::new(), body: String::new(), connect_ms: 1, request_ms: 1, purpose: crate::transport::NetworkPurpose::Standard, response_limit: crate::transport::MAX_HTTP_RESPONSE_BYTES }
}

fn encrypted() -> crate::ffi::EncryptedConfig {
    use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
    crate::ffi::EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }
}

#[tokio::test]
async fn life_open_ports_executor() {
    let log: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let executor = || Some(Arc::new(Recording(log.clone())) as Arc<dyn Transport>);
    let credentials = || Some(Arc::new(crate::ffi::tests::database_tests::Unused) as Arc<dyn CredentialStore>);

    let plain = OpenPorts { endpoint: Some("https://graphql.test/graphql".into()), executor: executor(), ..Default::default() }.resolved();
    plain.transport.unwrap().send(request("plain")).await;
    assert!(plain.http.is_none());

    let sealed = OpenPorts { encrypted: Some(encrypted()), credentials: credentials(), executor: executor(), ..Default::default() }.resolved();
    sealed.transport.unwrap().send(request("sealed transport")).await;
    sealed.http.unwrap().send(request("sealed http")).await;

    let local = OpenPorts { executor: executor(), ..Default::default() }.resolved();
    assert!(local.transport.is_none() && local.http.is_none());

    let own = Arc::new(Recording(Arc::default()));
    let tested = OpenPorts { endpoint: Some("https://graphql.test/graphql".into()), transport: Some(own.clone()), executor: executor(), ..Default::default() }.resolved();
    tested.transport.unwrap().send(request("own")).await;

    assert_eq!(*log.lock().unwrap(), ["plain", "sealed transport", "sealed http"]);
    assert_eq!(*own.0.lock().unwrap(), ["own"]);
}

#[tokio::test]
async fn life_open_executor_runs_the_operation() {
    let slot = OpenDatabase::new();
    let operations = vec![OperationDefinition { name: "Greeting".into(), document: "query Greeting($name: String!) { greeting(name: $name) }".into() }];
    let ports = OpenPorts { endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), executor: Some(Arc::new(crate::ffi::tests::database_tests::Plain)), ..Default::default() };
    let database = slot.open(config(), crate::ffi::EMPTY_SPECS.into(), OpenOptions { operations }, ports, None).await.unwrap();
    database.set_online(true).unwrap();
    assert_eq!(database.run(database.session_epoch(), "Greeting".into(), r#"{"name":"Ada"}"#.into()).await.unwrap(), r#""Hello Ada""#);
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn life_opened_database() {
    let slot = OpenDatabase::new();
    assert!(matches!(slot.opened(), Err(DbError::NotOpen { .. })));
    let first = open(&slot).await.unwrap();
    assert!(Arc::ptr_eq(&slot.opened().unwrap(), &first));
    first.close_database().await.unwrap();
    assert!(Arc::ptr_eq(&slot.opened().unwrap(), &first), "a closed database answers its calls with Closed");
    let second = open(&slot).await.unwrap();
    assert!(Arc::ptr_eq(&slot.opened().unwrap(), &second));
    second.close_database().await.unwrap();
}
