
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
        crate::test_wait::within(life.set_online(true).wait()).await;
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

fn assert_full_and_identical(sent: &[(i64, String, Value)], from: i64) {
    let ours: Vec<_> = sent.iter().filter(|(sequence, ..)| *sequence >= from).collect();
    assert!(!ours.is_empty(), "the server saw no sequenced request");
    for (sequence, _, body) in &ours {
        assert!(body.get("query").is_some_and(|query| query.is_string()), "sequence {sequence} went without its document: {body}");
    }
    for sequence in ours.iter().map(|(sequence, ..)| *sequence).collect::<std::collections::BTreeSet<_>>() {
        let bodies: Vec<String> = ours.iter().filter(|(candidate, ..)| *candidate == sequence).map(|(_, _, body)| body.to_string()).collect();
        assert!(bodies.windows(2).all(|pair| pair[0] == pair[1]), "the pair with sequence {sequence} went with two bodies: {bodies:?}");
    }
}

async fn signed_in() -> (World, Arc<Store>, Arc<crate::owners::life::Life>, Arc<Client>) {
    let world = World::new();
    let db = Store::open_models(None, Vec::new(), None).map(Arc::new).unwrap();
    let (client, life) = world.open(&db).await;
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    world.server.spend_sequence_on_miss();
    (world, db, life, client)
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_full_once() {
    let (world, _db, life, _client) = signed_in().await;
    let first = crate::test_wait::within(life.mutate_answered(None, 1 << 42, GRAPHQL.into(), mutation("First"), 0).wait()).await;
    assert!(matches!(first, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response(ref text))) if text.contains("done")), "{first:?}");
    let second = crate::test_wait::within(life.mutate_answered(None, (1 << 42) + 1, GRAPHQL.into(), mutation("Second"), 0).wait()).await;
    assert!(matches!(second, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response(_)))), "{second:?}");
    let sent = sequenced(&world);
    let graphql: Vec<_> = sent.iter().filter(|(sequence, ..)| *sequence >= 2).collect();
    assert_eq!(graphql.iter().map(|(sequence, ..)| *sequence).collect::<Vec<_>>(), [2, 3], "each pair went once and the numbers grow by 1");
    assert_full_and_identical(&sent, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_full_once_retry() {
    let (world, _db, life, _client) = signed_in().await;
    world.server.lose_next_answer();
    let req_id = (1 << 42) + 10;
    let lost = crate::test_wait::within(life.mutate_answered(None, req_id, GRAPHQL.into(), mutation("Retry"), 0).wait()).await;
    assert!(matches!(lost, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Dropped | AuthorOutcome::Error { .. }))), "{lost:?}");
    let again = crate::test_wait::within(life.mutate_answered(None, req_id, GRAPHQL.into(), mutation("Retry"), 0).wait()).await;
    assert!(matches!(again, crate::runtime::Outcome::Value(LifeReply::Mutated(AuthorOutcome::Response(_)))), "{again:?}");
    let sent = sequenced(&world);
    assert_eq!(sent.iter().filter(|(sequence, ..)| *sequence == 2).count(), 2, "the pair went twice");
    assert_full_and_identical(&sent, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_full_once_restart() {
    let world = World::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    {
        let db = Store::open_models(Some(&path), Vec::new(), None).map(Arc::new).unwrap();
        let (client, life) = world.open(&db).await;
        client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
        world.server.spend_sequence_on_miss();
        world.server.drop_next_request();
        let waiter = life.mutate_for_call(None, GRAPHQL.into(), mutation("Restart"), 0);
        crate::test_wait::settled(&life, "the pending request was never stored", || pending_of(&db).is_some()).await;
        drop(waiter);
        let _ = crate::test_wait::within(life.close().wait()).await;
    }
    let db = Store::open_models(Some(&path), Vec::new(), None).map(Arc::new).unwrap();
    let (_client, life) = world.open(&db).await;
    crate::test_wait::settled(&life, "the stored request was not sent again", || pending_of(&db).is_none()).await;
    let sent = sequenced(&world);
    assert_eq!(sent.iter().filter(|(sequence, ..)| *sequence == 2).count(), 1, "the dropped request never reached the server, the restart sent the pair once");
    assert_full_and_identical(&sent, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_full_once_session_call() {
    let (world, _db, _life, client) = signed_in().await;
    client.session_call("social", serde_json::from_str(&mutation("Visit")).unwrap()).await.unwrap();
    assert_full_and_identical(&sequenced(&world), 2);
}
