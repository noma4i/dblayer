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
    let config = client(&server(config_body(PIN, PUBLIC_KEY_DER_B64, 3072))).server_config(None, None).await.unwrap();

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
        assert!(matches!(client(&server(body.clone())).server_config(None, None).await, Err(ConfigError::KeyMismatch(_))), "{body}");
    }

    let unavailable = FakeServer::new(Arc::new(ManualClock::new(Duration::from_secs(1))));
    unavailable.serve_config(HttpReply { status: 503, body: String::new() });
    assert_eq!(client(&unavailable).server_config(None, None).await, Err(ConfigError::Transport(TransportError::Http { status: 503, body: String::new() })));
    unavailable.serve_config(HttpReply { status: 200, body: "not json".to_string() });
    assert!(matches!(client(&unavailable).server_config(None, None).await, Err(ConfigError::Transport(TransportError::Decode(_)))));
}

fn full_body() -> serde_json::Value {
    let mut body = config_body(PIN, PUBLIC_KEY_DER_B64, 3072);
    let extra = serde_json::json!({
        "update_required": true,
        "country": "AU",
        "country_disabled": true,
        "countries": [
            { "name": "Australia", "code": "AU", "flag": "🇦🇺", "phone_code": "+61", "phone_mask": "XXX XXX XXX", "phone_formatter": { "default": "[0-9]{9}", "mobile": "04XX" }, "enabled": true, "phone_auth_enabled": true, "unknown": 1 },
            { "name": "Nowhere", "code": "NW", "flag": null, "phone_code": null, "phone_mask": null, "phone_formatter": null, "enabled": true, "phone_auth_enabled": false }
        ],
        "tracking_prompt_enabled": true,
        "ios_store_url": "https://apps.apple.com/au/app/yupi/id1594187122",
        "android_store_url": "https://play.google.com/store/apps/details?id=com.yupi",
        "future_field": { "a": 1 }
    });
    body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    body
}

#[tokio::test]
async fn the_full_controller_response_is_read_into_typed_fields() {
    let config = client(&server(full_body())).server_config(None, None).await.unwrap();

    assert_eq!((config.update_required, config.country.as_deref(), config.country_disabled, config.tracking_prompt_enabled), (true, Some("AU"), true, true));
    assert_eq!((config.ios_store_url.as_deref(), config.android_store_url.as_deref()), (Some("https://apps.apple.com/au/app/yupi/id1594187122"), Some("https://play.google.com/store/apps/details?id=com.yupi")));
    assert_eq!(config.countries.len(), 2);
    assert_eq!(
        config.countries[0],
        Country {
            name: "Australia".into(),
            code: "AU".into(),
            flag: Some("🇦🇺".into()),
            phone_code: Some("+61".into()),
            phone_mask: Some("XXX XXX XXX".into()),
            phone_formatter: [("default".to_string(), "[0-9]{9}".to_string()), ("mobile".to_string(), "04XX".to_string())].into(),
            enabled: true,
            phone_auth_enabled: true,
        }
    );
    assert_eq!((config.countries[1].flag.clone(), config.countries[1].phone_formatter.is_empty(), config.countries[1].phone_auth_enabled), (None, true, false));
}

#[tokio::test]
async fn missing_and_null_fields_read_as_defaults() {
    let mut nulls = config_body(PIN, PUBLIC_KEY_DER_B64, 3072);
    for name in ["update_required", "country", "country_disabled", "countries", "tracking_prompt_enabled", "ios_store_url", "android_store_url"] {
        nulls[name] = serde_json::Value::Null;
    }
    for body in [config_body(PIN, PUBLIC_KEY_DER_B64, 3072), nulls] {
        let config = client(&server(body.clone())).server_config(None, None).await.unwrap();

        assert_eq!((config.update_required, config.country, config.country_disabled, config.tracking_prompt_enabled), (false, None, false, false), "{body}");
        assert_eq!((config.countries, config.ios_store_url, config.android_store_url), (Vec::new(), None, None), "{body}");
    }
}

#[tokio::test]
async fn platform_and_version_go_out_as_an_encoded_query_and_no_arguments_keep_the_plain_path() {
    let server = server(config_body(PIN, PUBLIC_KEY_DER_B64, 3072));
    let client = client(&server);
    client.server_config(None, None).await.unwrap();
    client.server_config(Some("ios"), Some("1.2.3")).await.unwrap();
    client.server_config(Some("ios"), None).await.unwrap();
    client.server_config(Some("a b&c=d"), Some("1.0+é")).await.unwrap();

    assert_eq!(
        server.config_paths(),
        ["/api/v3/config", "/api/v3/config?platform=ios&version=1.2.3", "/api/v3/config?platform=ios", "/api/v3/config?platform=a%20b%26c%3Dd&version=1.0%2B%C3%A9"]
    );
}

#[tokio::test]
async fn a_phone_formatter_value_that_is_not_a_string_is_a_parse_error_naming_the_country() {
    for formatter in [serde_json::json!({ "default": 5 }), serde_json::json!({ "default": null }), serde_json::json!("[0-9]{9}"), serde_json::json!(["a"])] {
        let mut body = full_body();
        body["countries"][1]["phone_formatter"] = formatter.clone();

        let outcome = client(&server(body)).server_config(None, None).await;

        assert!(matches!(&outcome, Err(ConfigError::Transport(TransportError::Decode(message))) if message.contains("Nowhere") && message.contains("phone_formatter")), "{formatter}: {outcome:?}");
    }
}
