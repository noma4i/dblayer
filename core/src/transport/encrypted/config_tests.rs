use std::sync::Arc;
use std::time::Duration;

use super::fake_server_tests::{config_body, FakeServer, MemoryCredentials};
use super::vectors::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::TransportError;

fn client(server: &Arc<FakeServer>) -> EncryptedClient {
    let config = EncryptedConfig { origin: ORIGIN.to_string(), server_key: PUBLIC_KEY_DER_B64.to_string(), pin: PIN.to_string() };
    EncryptedClient::new(&config, server.clone(), Arc::new(MemoryCredentials::default()), Db::open_in_memory(Vec::new(), None).unwrap(), Arc::new(ManualClock::new(Duration::from_secs(1)))).unwrap()
}

fn server(body: serde_json::Value) -> Arc<FakeServer> {
    let server = FakeServer::new(Arc::new(ManualClock::new(Duration::from_secs(1))));
    server.serve_config(HttpReply { status: 200, body: body.to_string() });
    server
}

#[tokio::test]
async fn the_config_of_the_server_is_returned_typed_when_its_key_is_the_pinned_key() {
    let config = client(&server(config_body(PIN, PUBLIC_KEY_DER_B64, 3072))).server_config().await.unwrap();

    assert_eq!((config.protocol_version, config.min_app_version.as_str()), (1, "3.8.0"));
    assert_eq!((config.graphql_url.as_str(), config.cable_url.as_str()), ("https://api.example.com/api/v3/graphql", "wss://api.example.com/api/v3/cable"));
    assert_eq!(config.server_key, ServerKeyInfo { kid: PIN.to_string(), public_key_der: PUBLIC_KEY_DER_B64.to_string(), alg: "RSA-OAEP".to_string(), enc: "A256GCM".to_string(), bits: 3072 });
}

#[tokio::test]
async fn a_config_that_announces_another_key_is_refused_and_a_failed_read_is_a_transport_error() {
    let mismatches = [
        config_body(SHORT_KEY_PIN, PUBLIC_KEY_DER_B64, 3072),
        config_body(PIN, SHORT_KEY_DER_B64, 3072),
        config_body(PIN, PUBLIC_KEY_DER_B64, 2048),
        {
            let mut body = config_body(PIN, PUBLIC_KEY_DER_B64, 3072);
            body["server_key"]["alg"] = "RSA-OAEP-256".into();
            body
        },
    ];
    for body in mismatches {
        assert!(matches!(client(&server(body.clone())).server_config().await, Err(ConfigError::KeyMismatch(_))), "{body}");
    }

    let unavailable = FakeServer::new(Arc::new(ManualClock::new(Duration::from_secs(1))));
    unavailable.serve_config(HttpReply { status: 503, body: String::new() });
    assert_eq!(client(&unavailable).server_config().await, Err(ConfigError::Transport(TransportError::Http { status: 503, body: String::new() })));
    unavailable.serve_config(HttpReply { status: 200, body: "not json".to_string() });
    assert!(matches!(client(&unavailable).server_config().await, Err(ConfigError::Transport(TransportError::Decode(_)))));
}
