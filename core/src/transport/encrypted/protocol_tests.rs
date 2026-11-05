use super::*;

pub(crate) fn config() -> EncryptedConfig {
    let value: serde_json::Value = serde_json::from_str(include_str!("../../../../fixtures/protocol-config.json")).unwrap();
    let endpoints = &value["endpoints"];
    let text = |key: &str| value[key].as_str().unwrap().to_string();
    let path = |key: &str| endpoints[key].as_str().unwrap().to_string();
    EncryptedConfig {
        origin: vectors_tests::origin().into(), server_key: vectors_tests::public_key_der_b64().into(), pin: vectors_tests::pin().into(),
        endpoints: EncryptedEndpoints { graphql: path("graphql"), cable: path("cable"), logout: path("logout"), bootstrap: path("bootstrap"), session_operations: endpoints["session_operations"].as_object().unwrap().iter().map(|(name, path)| (name.clone(), EncryptedSessionOperation { path: path.as_str().unwrap().into(), class: crate::transport::EncryptedOperationClass::Authenticated })).collect() },
        protocol_label: text("protocol_label"), cable_purpose: text("cable_purpose"), cable_channel: text("cable_channel"),
    }
}

#[test]
fn rejects_paths_that_socket_clients_would_rewrite() {
    for path in ["/a/../cable", "/a/./cable", "/a/%2e%2e/cable", "/a/%", "/a/é"] {
        let mut config = config();
        config.endpoints.cable = path.into();
        assert!(config.validate().is_err(), "accepted {path}");
    }
}
