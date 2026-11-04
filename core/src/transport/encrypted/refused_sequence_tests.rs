use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, SessionCode, Transport, TransportError};

const WRITE: &str = "mutation Rename { rename { id } }";

async fn signed_in() -> (Arc<FakeServer>, EncryptedTransport) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = EncryptedClient::new(&config, server.clone(), Arc::new(MemoryCredentials::default()), db, clock).unwrap();
    client.mutate(&client.admit("test call").unwrap(), "/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    server.register(WRITE);
    (server, EncryptedTransport::new(Arc::new(client), Arc::new(MockTransport::new())))
}

#[tokio::test]
async fn a_confirmed_refusal_spends_its_sequence_and_the_next_mutation_is_accepted() {
    let (server, transport) = signed_in().await;
    let request = GraphQLRequest::new(WRITE, "", json!({}));
    server.refuse_next("RATE_LIMITED", true);

    let refused = transport.mutate(&transport.test_admission().unwrap(), request.clone()).await.unwrap_err();
    let executed_before = server.executed().len();
    let accepted = transport.mutate(&transport.test_admission().unwrap(), request).await;

    assert!(matches!(refused, TransportError::Session { code: SessionCode::RateLimited, .. }), "{refused:?}");
    assert!(accepted.is_ok(), "{accepted:?}");
    assert_eq!(server.executed().len() - executed_before, 1);
}
