use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, Transport};

const READ: &str = "query Me { me { id } }";
pub(super) const WRITE: &str = "mutation Rename { rename { id } }";

pub(super) struct Rig {
    pub(super) server: Arc<FakeServer>,
    pub(super) credentials: Arc<MemoryCredentials>,
    pub(super) clock: Arc<ManualClock>,
    pub(super) db: Db,
}

impl Rig {
    pub(super) async fn signed_in() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        let rig = Self { server: FakeServer::new(clock.clone()), credentials: Arc::new(MemoryCredentials::default()), clock, db: Db::open_in_memory(Vec::new(), None).unwrap() };
        rig.client().mutate(&rig.client().admit("test call").unwrap(), "/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
        rig
    }

    pub(super) fn client(&self) -> Arc<EncryptedClient> {
        let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
        Arc::new(EncryptedClient::new(&config, self.server.clone(), self.credentials.clone(), self.db.clone(), self.clock.clone()).unwrap())
    }

    pub(super) fn transport(&self) -> EncryptedTransport {
        EncryptedTransport::new(self.client(), Arc::new(MockTransport::new()))
    }

    pub(super) fn graphql_requests(&self) -> Vec<Value> {
        self.server.seen().into_iter().filter(|request| request["path"] == "/api/graphql").collect()
    }

    pub(super) fn pending_sequence(&self) -> Option<i64> {
        let inner = self.db.shared.store.lock();
        state::pending(&inner.connection).unwrap().map(|pending| pending.sequence)
    }
}

pub(super) fn request(document: &str) -> GraphQLRequest {
    GraphQLRequest::new(document, "", json!({}))
}

pub(super) fn documents(requests: &[Value]) -> Vec<bool> {
    requests.iter().map(|request| request["body"].get("query").is_some()).collect()
}

fn ids(requests: &[Value]) -> Vec<Value> {
    requests.iter().map(|request| request["requestId"].clone()).collect()
}

#[tokio::test]
async fn local_413_after_apq_miss_leaves_the_next_sequence_available() {
    let rig = Rig::signed_in().await;
    let transport = rig.transport();
    assert!(transport.mutate(&rig.db.admit().unwrap(), request(&format!("mutation Huge {{ {} }}", "x".repeat(260_000)))).await.is_err());
    assert_eq!(documents(&rig.graphql_requests()), [false]);
    assert_eq!(rig.pending_sequence(), None);
    transport.mutate(&rig.db.admit().unwrap(), request(WRITE)).await.unwrap();
    assert_eq!(rig.server.executed().len(), 2);
}

#[tokio::test]
async fn a_read_missing_from_the_registry_is_sent_again_once_with_the_document_and_a_new_request_id() {
    let rig = Rig::signed_in().await;

    let response = rig.transport().query(&rig.db.admit().unwrap(), request(READ)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!(response.data, Some(json!({ "read": READ })));
    assert_eq!(documents(&sent), [false, true]);
    assert_ne!(sent[0]["requestId"], sent[1]["requestId"]);
    assert_ne!(sent[0]["responseKey"], sent[1]["responseKey"]);
    assert_eq!(sent[1]["body"]["query"], READ);
}

#[tokio::test]
async fn a_registered_read_is_answered_from_its_hash_alone() {
    let rig = Rig::signed_in().await;
    rig.server.register(READ);

    let response = rig.transport().query(&rig.db.admit().unwrap(), request(READ)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!(response.data, Some(json!({ "read": READ })));
    assert_eq!(documents(&sent), [false]);
}

#[tokio::test]
async fn a_read_that_misses_twice_gives_the_error_to_the_app_and_sends_no_third_request() {
    let rig = Rig::signed_in().await;
    rig.server.forget_registrations();

    let response = rig.transport().query(&rig.db.admit().unwrap(), request(READ)).await.unwrap();

    assert_eq!(response.errors[0].code(), Some("PERSISTED_QUERY_NOT_FOUND"));
    assert_eq!(documents(&rig.graphql_requests()), [false, true]);
}

#[tokio::test]
async fn an_invalid_hash_is_not_sent_again() {
    let rig = Rig::signed_in().await;
    rig.server.refuse_hashes();

    let response = rig.transport().query(&rig.db.admit().unwrap(), request(READ)).await.unwrap();

    assert_eq!(response.errors[0].code(), Some("INVALID_INPUT"));
    assert_eq!(documents(&rig.graphql_requests()), [false]);
}

#[tokio::test]
async fn a_mutation_missing_from_the_registry_is_sent_again_as_the_next_sequence_with_the_document() {
    let rig = Rig::signed_in().await;

    let response = rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!(response.data, Some(json!({ "done": WRITE })));
    assert_eq!(documents(&sent), [false, true]);
    assert_eq!(sent.iter().map(|request| request["sequence"].clone()).collect::<Vec<_>>(), [json!(2), json!(3)]);
    assert_eq!(ids(&sent).iter().collect::<std::collections::HashSet<_>>().len(), 2);
    assert_eq!((rig.server.executed().len(), rig.pending_sequence()), (2, None));
}

#[tokio::test]
async fn a_registered_mutation_is_sent_once_as_the_next_sequence() {
    let rig = Rig::signed_in().await;
    rig.server.register(WRITE);

    rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!((documents(&sent), sent[0]["sequence"].clone()), (vec![false], json!(2)));
}

#[tokio::test]
async fn a_restart_between_the_miss_and_the_answer_to_the_second_send_resumes_the_stored_document() {
    let rig = Rig::signed_in().await;
    let server = rig.server.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    rig.server.on_request(move || {
        if seen.fetch_add(1, Ordering::SeqCst) == 1 {
            server.lose_next_answer();
        }
    });

    super::client_tests::cancel_pending(rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)));
    assert_eq!(rig.pending_sequence(), Some(3));
    let answer = rig.client().resume(&rig.client().admit("test call").unwrap()).await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "data": { "done": WRITE } }));
    let sent = rig.graphql_requests();
    assert_eq!(documents(&sent), [false, true, true]);
    assert_eq!(sent[2]["requestId"], sent[1]["requestId"]);
    assert_eq!((rig.server.executed().len(), rig.pending_sequence()), (2, None));
}

#[tokio::test]
async fn a_restart_between_the_miss_and_the_second_send_leaves_no_effect_and_the_resent_mutation_runs_once() {
    let rig = Rig::signed_in().await;
    let miss = rig.client().mutate(&rig.client().admit("test call").unwrap(), "/api/graphql", super::graphql::graphql_body(&request(WRITE), false)).await.unwrap();
    assert_eq!(miss.body["errors"][0]["extensions"]["code"], "PERSISTED_QUERY_NOT_FOUND");
    assert_eq!((rig.server.executed().len(), rig.pending_sequence()), (1, None));

    assert_eq!(rig.client().resume(&rig.client().admit("test call").unwrap()).await, Ok(None));
    rig.transport().mutate(&rig.db.admit().unwrap(), request(WRITE)).await.unwrap();

    assert_eq!(rig.server.executed().len(), 2);
    assert_eq!(rig.graphql_requests().iter().map(|request| request["sequence"].clone()).collect::<Vec<_>>(), [json!(2), json!(3), json!(4)]);
}
