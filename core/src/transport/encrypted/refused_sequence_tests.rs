use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::exchange_tests::Client;
use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::vectors_tests::*;
use super::*;
use crate::store::Store;
use crate::runtime::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, SessionCode, Upstream, TransportError};

const WRITE: &str = "mutation Rename { rename { id } }";

async fn signed_in() -> (Arc<FakeServer>, Arc<EncryptedTransport>, Arc<Client>) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let client = Client::new(&config, server.clone(), Arc::new(MemoryCredentials::default()), db, clock, Arc::new(MockTransport::new())).unwrap();
    client.start_owners();
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    server.register(WRITE);
    let transport = client.transport();
    (server, transport, client)
}

#[tokio::test]
async fn a_confirmed_refusal_spends_its_sequence_and_the_next_mutation_is_accepted() {
    let (server, transport, _client) = signed_in().await;
    let request = GraphQLRequest::new(WRITE, "", json!({}));
    server.refuse_next("RATE_LIMITED", true);

    let refused = transport.mutate(request.clone()).await.unwrap_err();
    let executed_before = server.executed().len();
    let accepted = transport.mutate(request).await;

    assert!(matches!(refused, TransportError::Session { code: SessionCode::RateLimited, .. }), "{refused:?}");
    assert!(accepted.is_ok(), "{accepted:?}");
    assert_eq!(server.executed().len() - executed_before, 1);
}
