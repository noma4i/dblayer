
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::LifeReply;
use crate::owners::session::AuthorOutcome;
use crate::runtime::ManualClock;
use crate::store::Store;
use crate::transport::encrypted::graphql_body;
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::tests::exchange_tests::Client;
use crate::transport::GraphQLRequest;

const GRAPHQL: &str = "/api/graphql";

struct World {
    server: Arc<FakeServer>,
    credentials: Arc<MemoryCredentials>,
    clock: Arc<ManualClock>,
}

impl World {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        Self { server: FakeServer::new(clock.clone()), credentials: Arc::new(MemoryCredentials::default()), clock }
    }

    async fn open(&self, db: &Arc<Store>) -> (Arc<Client>, Arc<crate::owners::life::Life>) {
        let config = crate::transport::EncryptedConfig { origin: crate::transport::encrypted::tests::vectors_tests::origin().to_string(), server_key: crate::transport::encrypted::tests::vectors_tests::public_key_der_b64().to_string(), pin: crate::transport::encrypted::tests::vectors_tests::pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() };
        let client = Client::new(&config, self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone(), Arc::new(crate::transport::MockTransport::new())).unwrap();
        let life = client.start_owners();
        crate::test_wait::within(life.set_online(true, crate::ffi::next_stamp()).wait()).await;
        (client, life)
    }
}

fn mutation(name: &str) -> String {
    graphql_body(&GraphQLRequest::new(format!("mutation {name} {{ {name} {{ id }} }}"), name.to_string(), json!({"input": {"n": 1}})), true).to_string()
}

fn pending_of(db: &Store) -> Option<crate::store::session_state::Pending> {
    let inner = db.lock();
    crate::store::session_state::pending(&inner.connection).unwrap()
}

fn sequenced(world: &World) -> Vec<(i64, String, Value)> {
    world.server.seen().into_iter().filter_map(|request| Some((request["sequence"].as_i64()?, request["requestId"].as_str()?.to_string(), request["body"].clone()))).collect()
}

fn document_of(name: &str) -> String {
    format!("mutation {name} {{ {name} {{ id }} }}")
}

struct LifeOutcome(String);

fn answer_text(outcome: &crate::runtime::Outcome<LifeReply>) -> LifeOutcome {
    match outcome {
        crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response { body: text, .. })) => LifeOutcome(text.clone()),
        other => panic!("no answer: {other:?}"),
    }
}

fn stored_sequence(db: &Store) -> i64 {
    let inner = db.lock();
    crate::store::session_state::session(&inner.connection).unwrap().unwrap().sequence
}

fn has_document(body: &Value) -> bool {
    body.get("query").is_some_and(Value::is_string)
}

fn at_number(sent: &[(i64, String, Value)], number: i64) -> Vec<&(i64, String, Value)> {
    sent.iter().filter(|(sequence, ..)| *sequence == number).collect()
}

async fn signed_in() -> (World, Arc<Store>, Arc<crate::owners::life::Life>, Arc<Client>) {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    (world, db, life, client)
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_one_per_pair() {
    let (world, _db, life, _client) = signed_in().await;
    let first = crate::test_wait::within(life.mutate_answered(None, 1 << 42, GRAPHQL.into(), mutation("First"), 0).wait()).await;
    assert!(matches!(first, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response { body: ref text, .. })) if text.contains("done")), "{first:?}");
    let sent = sequenced(&world);
    let pairs = at_number(&sent, 2);
    assert_eq!(pairs.len(), 2, "an empty registry: the hash-only pair, then the full pair with the same number: {sent:?}");
    assert!(!has_document(&pairs[0].2), "the first pair of a call is hash-only: {}", pairs[0].2);
    assert!(has_document(&pairs[1].2), "the second pair carries the document: {}", pairs[1].2);
    assert_ne!(pairs[0].1, pairs[1].1, "the full pair is a new request id");
    assert_eq!(pairs[1].2["query"], json!(document_of("First")));
    world.server.register(&document_of("Second"));
    let second = crate::test_wait::within(life.mutate_answered(None, (1 << 42) + 1, GRAPHQL.into(), mutation("Second"), 0).wait()).await;
    assert!(matches!(second, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response { .. }))), "{second:?}");
    let sent = sequenced(&world);
    let pairs = at_number(&sent, 3);
    assert_eq!(pairs.len(), 1, "a registered document: one hash-only pair: {sent:?}");
    assert!(!has_document(&pairs[0].2));
    let LifeOutcome(first_text) = answer_text(&first);
    let LifeOutcome(second_text) = answer_text(&second);
    assert!(!first_text.contains("SEQUENCE_CONFLICT") && !second_text.contains("SEQUENCE_CONFLICT"), "{first_text} {second_text}");
    assert_eq!(stored_sequence(&_db), 3, "login spent 1, each landed call spent exactly 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_one_per_pair_retry() {
    let (world, _db, life, _client) = signed_in().await;
    world.server.register(&document_of("Retry"));
    world.server.lose_next_answer();
    let req_id = (1 << 42) + 10;
    let lost = crate::test_wait::within(life.mutate_answered(None, req_id, GRAPHQL.into(), mutation("Retry"), 0).wait()).await;
    assert!(matches!(lost, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Dropped | AuthorOutcome::Error { .. }))), "{lost:?}");
    let again = crate::test_wait::within(life.mutate_answered(None, req_id, GRAPHQL.into(), mutation("Retry"), 0).wait()).await;
    assert!(matches!(again, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response { .. }))), "{again:?}");
    let sent = sequenced(&world);
    let pairs = at_number(&sent, 2);
    assert_eq!(pairs.len(), 2, "the pair went twice: {sent:?}");
    assert_eq!(pairs[0].1, pairs[1].1, "a retry keeps the request id");
    assert_eq!(pairs[0].2.to_string(), pairs[1].2.to_string(), "a retry sends the same bytes");
    assert!(!has_document(&pairs[0].2));
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_one_per_pair_restart() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    {
        let db = Store::open_models(Some(&path), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = world.server.clone();
        world.server.on_request(move || {
            if requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                server.drop_next_request();
            }
        });
        let waiter = life.mutate_for_call(None, None, GRAPHQL.into(), mutation("Restart"), 0);
        crate::test_wait::settled(&life, "the full pair was never stored", || pending_of(&db).is_some_and(|pending| pending.body.contains("\"query\""))).await;
        drop(waiter);
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    let before = sequenced(&world);
    assert_eq!(at_number(&before, 2).len(), 1, "only the hash-only pair reached the server: {before:?}");
    world.server.on_request(|| {});
    let db = Store::open_models(Some(&path), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    let (_client, life) = world.open(&db).await;
    crate::test_wait::settled(&life, "the stored full pair was not sent again", || pending_of(&db).is_none()).await;
    let sent = sequenced(&world);
    let pairs = at_number(&sent, 2);
    assert_eq!(pairs.len(), 2, "the restart sends the recorded full pair with the same number: {sent:?}");
    assert!(!has_document(&pairs[0].2) && has_document(&pairs[1].2));
}
