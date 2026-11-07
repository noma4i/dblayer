use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{Config, Database, DbError, TestClock, FakeTransport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::test_wait::within;
use crate::value::ScalarKind;

struct Accounts {
    asked: Mutex<HashMap<String, usize>>,
    held: AtomicUsize,
    hold: Option<Arc<tokio::sync::Semaphore>>,
    entered: tokio::sync::Notify,
    ended: Arc<tokio::sync::Notify>,
}

struct AnswerEnd(Arc<tokio::sync::Notify>);

impl Drop for AnswerEnd {
    fn drop(&mut self) { self.0.notify_one(); }
}

impl Accounts {
    fn new(hold: Option<Arc<tokio::sync::Semaphore>>) -> Arc<Self> {
        Arc::new(Self { asked: Mutex::new(HashMap::new()), held: AtomicUsize::new(0), hold, entered: tokio::sync::Notify::new(), ended: Arc::new(tokio::sync::Notify::new()) })
    }

    fn asked(&self, operation: &str) -> usize { self.asked.lock().unwrap().get(operation).copied().unwrap_or(0) }
}

#[async_trait::async_trait]
impl FakeTransport for Accounts {
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        let nth = {
            let mut asked = self.asked.lock().unwrap();
            let count = asked.entry(request.operation_name.clone()).or_insert(0);
            *count += 1;
            *count
        };
        if nth == 1 && request.operation_name == "Profile" {
            if let Some(hold) = &self.hold {
                let _ended = AnswerEnd(self.ended.clone());
                self.held.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                hold.acquire().await.unwrap().forget();
            }
        }
        let (root, model) = if request.operation_name == "Profile" { ("profiles", "profile") } else { ("vibes", "vibe") };
        let account = if nth == 1 { "a" } else { "b" };
        Ok(format!(r#"{{"data":{{"{root}":[{{"id":"{model}-{account}","name":"{model} of {account}"}}]}}}}"#))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open(transport: Arc<Accounts>) -> Arc<Database> {
    let mut profile = ModelSpec::new("Profile", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut vibe = ModelSpec::new("Vibe", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut remote = super::load_tests::relation("profile", "10.minutes");
    remote.remote.as_mut().unwrap().operation = "Profile".into();
    profile.relations.push(remote);
    let mut list = super::load_tests::relation("list", "10.minutes");
    list.remote.as_mut().unwrap().operation = "Vibe".into();
    vibe.relations.push(list);
    let models = serde_json::to_string(&vec![profile, vibe]).unwrap();
    let database = Database::open_with_clock(Config { path: None, data_version: None }, crate::ffi::specs(models), crate::ffi::OpenPorts { transport: Some(transport), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000)).await.unwrap();
    database.register_operations(r#"[{"name":"Profile","document":"query Profile { profiles { id name } }","rootPath":"profiles"},{"name":"Vibe","document":"query Vibe { vibes { id name } }","rootPath":"vibes"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    database
}

async fn load(database: &Database, model: &str, relation: &str) -> Result<(), DbError> {
    database.load(database.session_epoch(), model.into(), relation.into(), "{}".into()).await
}

async fn ids(database: &Database, model: &str, relation: &str) -> Vec<String> {
    let query = format!(r#"{{"model":"{model}","relation":"{relation}","params":{{}}}}"#);
    database.read_query(database.session_epoch(), crate::ffi::QuerySpec::json(query)).await.unwrap().into_iter().map(|row| row.id).collect()
}

#[tokio::test]
async fn a_load_after_a_session_reset_needs_a_new_session_and_asks_nothing() {
    let transport = Accounts::new(None);
    let database = open(transport.clone()).await;
    within(load(&database, "Profile", "profile")).await.unwrap();
    within(load(&database, "Vibe", "list")).await.unwrap();
    assert_eq!((ids(&database, "Profile", "profile").await, ids(&database, "Vibe", "list").await), (vec!["profile-a".to_string()], vec!["vibe-a".to_string()]));

    database.session_reset().await.unwrap();
    assert!(matches!(within(load(&database, "Profile", "profile")).await, Err(DbError::NoSession { .. })));
    assert!(matches!(within(load(&database, "Vibe", "list")).await, Err(DbError::NoSession { .. })));
    assert_eq!((transport.asked("Profile"), transport.asked("Vibe")), (1, 1), "a load without a session asked the server");
    database.close_database().await.unwrap();
}

#[tokio::test]
async fn a_reset_during_a_load_interrupts_it_and_its_late_answer_never_reaches_the_next_session() {
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let transport = Accounts::new(Some(hold.clone()));
    let database = open(transport.clone()).await;
    let old = tokio::spawn({ let database = database.clone(); async move { load(&database, "Profile", "profile").await } });
    within(transport.entered.notified()).await;
    database.session_reset().await.unwrap();
    assert!(matches!(within(old).await.unwrap(), Err(DbError::Interrupted { .. })), "a call of the old epoch survived the reset");
    assert!(matches!(within(load(&database, "Profile", "profile")).await, Err(DbError::NoSession { .. })), "a load without a session waits instead of failing");
    hold.add_permits(1);
    within(transport.ended.notified()).await;
    assert_eq!(transport.asked("Profile"), 1, "no request left for the epoch without a session");
    assert!(ids(&database, "Profile", "profile").await.is_empty(), "the late answer of the old epoch landed");
    database.close_database().await.unwrap();
}
