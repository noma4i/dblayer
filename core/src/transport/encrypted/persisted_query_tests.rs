use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::exchange_tests::Client;
use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::vectors_tests::*;
use super::*;
use crate::store::Store;
use crate::runtime::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, Upstream};

const READ: &str = "query Me { me { id } }";
pub(super) const WRITE: &str = "mutation Rename { rename { id } }";

pub(super) struct Rig {
    pub(super) server: Arc<FakeServer>,
    pub(super) credentials: Arc<MemoryCredentials>,
    pub(super) clock: Arc<ManualClock>,
    pub(super) db: Arc<Store>,
    owned: std::sync::OnceLock<Arc<Client>>,
}

impl Rig {
    pub(super) async fn signed_in() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        let rig = Self { server: FakeServer::new(clock.clone()), credentials: Arc::new(MemoryCredentials::default()), clock, db: Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap(), owned: std::sync::OnceLock::new() };
        rig.client().session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
        rig
    }

    pub(super) fn client(&self) -> Arc<Client> {
        self.owned.get_or_init(|| {
            let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
            let client = Client::new(&config, self.server.clone(), self.credentials.clone(), self.db.clone(), self.clock.clone(), Arc::new(MockTransport::new())).unwrap();
            client.start_owners();
            client
        }).clone()
    }

    pub(super) fn transport(&self) -> Arc<EncryptedTransport> {
        self.client().transport()
    }

    pub(super) fn graphql_requests(&self) -> Vec<Value> {
        self.server.seen().into_iter().filter(|request| request["path"] == "/api/graphql").collect()
    }

    pub(super) fn pending_sequence(&self) -> Option<i64> {
        let inner = self.db.lock();
        crate::store::session_state::pending(&inner.connection).unwrap().map(|pending| pending.sequence)
    }
}

pub(super) fn request(document: &str) -> GraphQLRequest {
    GraphQLRequest::new(document, "", json!({})).by(crate::transport::WireIds::new(crate::transport::Author::Window, 1, 1))
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
    assert!(transport.mutate(request(&format!("mutation Huge {{ {} }}", "x".repeat(260_000)))).await.is_err());
    assert_eq!(documents(&rig.graphql_requests()), [false]);
    assert_eq!(rig.pending_sequence(), None);
    transport.mutate(request(WRITE)).await.unwrap();
    assert_eq!(rig.server.executed().len(), 2);
}

#[tokio::test]
async fn a_read_missing_from_the_registry_is_sent_again_once_with_the_document_and_a_new_request_id() {
    let rig = Rig::signed_in().await;

    let response = rig.transport().query(request(READ)).await.unwrap();

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

    let response = rig.transport().query(request(READ)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!(response.data, Some(json!({ "read": READ })));
    assert_eq!(documents(&sent), [false]);
}

#[tokio::test]
async fn a_read_that_misses_twice_gives_the_error_to_the_app_and_sends_no_third_request() {
    let rig = Rig::signed_in().await;
    rig.server.forget_registrations();

    let response = rig.transport().query(request(READ)).await.unwrap();

    assert_eq!(response.errors[0].code(), Some("PERSISTED_QUERY_NOT_FOUND"));
    assert_eq!(documents(&rig.graphql_requests()), [false, true]);
}

#[tokio::test]
async fn an_invalid_hash_is_not_sent_again() {
    let rig = Rig::signed_in().await;
    rig.server.refuse_hashes();

    let response = rig.transport().query(request(READ)).await.unwrap();

    assert_eq!(response.errors[0].code(), Some("INVALID_INPUT"));
    assert_eq!(documents(&rig.graphql_requests()), [false]);
}

#[tokio::test]
async fn a_mutation_missing_from_the_registry_is_sent_again_with_the_document_under_the_same_request_id_and_sequence() {
    let rig = Rig::signed_in().await;

    let response = rig.transport().mutate(request(WRITE)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!(response.data, Some(json!({ "done": WRITE })));
    assert_eq!(documents(&sent), [false, true]);
    assert_eq!(sent.iter().map(|request| request["sequence"].clone()).collect::<Vec<_>>(), [json!(2), json!(2)]);
    assert_eq!(ids(&sent).iter().collect::<std::collections::HashSet<_>>().len(), 1);
    assert_eq!((rig.server.executed().len(), rig.pending_sequence()), (2, None));
}

#[tokio::test]
async fn a_registered_mutation_is_sent_once_as_the_next_sequence() {
    let rig = Rig::signed_in().await;
    rig.server.register(WRITE);

    rig.transport().mutate(request(WRITE)).await.unwrap();

    let sent = rig.graphql_requests();
    assert_eq!((documents(&sent), sent[0]["sequence"].clone()), (vec![false], json!(2)));
}

