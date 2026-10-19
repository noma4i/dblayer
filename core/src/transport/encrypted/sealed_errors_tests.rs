use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::{GraphQLRequest, MockTransport, SessionCode, Transport, TransportError};

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
    transport: EncryptedTransport,
    server: Arc<FakeServer>,
    credentials: Arc<MemoryCredentials>,
    db: Db,
}

async fn rig() -> Rig {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let server = FakeServer::new(clock.clone());
    let credentials = Arc::new(MemoryCredentials::default());
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let client = EncryptedClient::new(&config, server.clone(), credentials.clone(), db.clone(), clock).unwrap();
    client.mutate("/api/auth/social", json!({ "provider": "google", "idToken": "t" })).await.unwrap();
    server.register("query Q { q }");
    server.register("mutation M { m }");
    Rig { transport: EncryptedTransport::new(Arc::new(client), Arc::new(MockTransport::new())), server, credentials, db }
}

fn pending(db: &Db) -> bool {
    state::pending(&db.shared.store.lock().connection).unwrap().is_some()
}

#[tokio::test]
async fn a_session_code_in_a_sealed_answer_is_a_typed_error_and_keeps_the_credential_and_the_pending_request() {
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
                    false => rig.transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({}))).await,
                };

                let case = format!("{wire} mutation={mutation} confirmed={confirmed}");
                assert_eq!(outcome.unwrap_err(), TransportError::Session { code, detail: wire.to_string() }, "{case}");
                assert_eq!((rig.credentials.value(), pending(&rig.db)), (credential, mutation && !confirmed), "{case}");
            }
        }
    }
}

#[tokio::test]
async fn a_code_outside_the_session_table_stays_a_graphql_error_in_the_response() {
    for wire in ["INTERNAL_SERVER_ERROR", "BLOCKED"] {
        let rig = rig().await;
        rig.server.refuse_next(wire, true);

        let response = rig.transport.query(GraphQLRequest::new("query Q { q }", "Q", json!({}))).await.unwrap();

        assert_eq!((response.data, response.errors[0].code()), (None, Some(wire)));
    }
}
