use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::exchange_tests::Client;
use super::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::store::session_state;
use super::vectors_tests::*;
use super::*;
use crate::store::Store;
use crate::runtime::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, SessionCode, Upstream, TransportError};

const TABLE: [(&str, SessionCode); 11] = [
    ("SEQUENCE_CONFLICT", SessionCode::SequenceConflict),
    ("REPLAYED_REQUEST", SessionCode::ReplayedRequest),
    ("RATE_LIMITED", SessionCode::RateLimited),
    ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable),
    ("UNAUTHENTICATED", SessionCode::Unauthenticated),
    ("SESSION_ENDED", SessionCode::SessionEnded),
    ("INVALID_CODE", SessionCode::InvalidCode),
    ("INVALID_PHONE", SessionCode::InvalidPhone),
    ("INVALID_PROVIDER", SessionCode::InvalidProvider),
    ("PROVIDER_REJECTED", SessionCode::ProviderRejected),
    ("ALREADY_AUTHENTICATED", SessionCode::AlreadyAuthenticated),
];

struct Rig {
    transport: Arc<EncryptedTransport>,
    server: Arc<FakeServer>,
    credentials: Arc<MemoryCredentials>,
    db: Arc<Store>,
    _client: Arc<Client>,
}

async fn rig() -> Rig {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let credentials = Arc::new(MemoryCredentials::default());
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    let client = Client::new(&config, server.clone(), credentials.clone(), db.clone(), clock, Arc::new(MockTransport::new())).unwrap();
    client.start_owners();
    client.session_call("social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    server.register("query Q { q }");
    server.register("mutation M { m }");
    Rig { transport: client.transport(), server, credentials, db, _client: client }
}

fn pending(db: &Store) -> bool {
    session_state::pending(&db.lock().connection).unwrap().is_some()
}

#[tokio::test]
async fn query_replayed_single_retry_owner() {
    let rig = rig().await;
    let before = rig.server.seen().len();
    rig.server.refuse_next("REPLAYED_REQUEST", false);
    let _ = rig.transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({})).by(crate::transport::WireIds::new(crate::transport::Author::Window, 1, 1))).await;
    assert_eq!(rig.server.seen().len() - before, 1, "the transport sends the query once; REPLAYED_REQUEST is the owner's retry by RetryPolicy (D-220, calls.md call_response_transient)");
}

#[tokio::test]
async fn a_session_code_in_a_sealed_answer_is_a_typed_error_and_keeps_the_credential_and_clears_the_pending_request() {
    for (wire, code) in TABLE {
        for mutation in [false, true] {
            for confirmed in [false, true] {
                if wire == "REPLAYED_REQUEST" && !mutation {
                    continue;
                }
                let rig = rig().await;
                let credential = rig.credentials.value();
                rig.server.refuse_next(wire, confirmed);

                let outcome = match mutation {
                    true => rig.transport.mutate(GraphQLRequest::new("mutation M { m }", "M", json!({}))).await,
                    false => rig.transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({})).by(crate::transport::WireIds::new(crate::transport::Author::Window, 1, 1))).await,
                };

                let case = format!("{wire} mutation={mutation} confirmed={confirmed}");
                let error = outcome.unwrap_err();
                if code == SessionCode::Unauthenticated {
                    assert!(matches!(error, TransportError::Http { status: 401, .. }), "{case}: {error:?}");
                } else {
                    assert_eq!(error, TransportError::Session { code, detail: wire.to_string() }, "{case}");
                }
                assert_eq!((rig.credentials.value(), pending(&rig.db)), (credential, false), "{case}");
            }
        }
    }
}

#[tokio::test]
async fn a_code_outside_the_session_table_stays_a_graphql_error_in_the_response() {
    for wire in ["BLOCKED", "SERVER_KEY_MISMATCH", "STATE_LOST"] {
        let rig = rig().await;
        rig.server.refuse_next(wire, true);

        let response = rig.transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({})).by(crate::transport::WireIds::new(crate::transport::Author::Window, 1, 1))).await.unwrap();

        assert_eq!((response.data, response.errors[0].code()), (None, Some(wire)));
    }
}
