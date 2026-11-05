use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde_json::{json, Value};
use super::*;
use super::fake_server_tests::{decrypt_request, seal_response, FakeServer, MemoryCredentials};
use crate::db::Db;

struct Fault {
    error: Mutex<Option<Result<HttpReply, TransportError>>>,
    code: Option<(&'static str, bool)>,
    seen: Mutex<Vec<Value>>,
    server: Arc<FakeServer>,
}

#[async_trait::async_trait]
impl HttpExchange for Fault {
    async fn post(&self, path: &str, token: String, _: crate::transport::NetworkPurpose) -> Result<HttpReply, TransportError> {
        let request = decrypt_request(&token).unwrap();
        let first = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(request.clone());
            seen.len() == 1
        };
        if first {
            if let Some((code, confirmed)) = self.code {
                if confirmed {
                    let credential = request["credential"].as_str().unwrap();
                    self.server.clone().with_sequence(credential, request["sequence"].as_i64().unwrap());
                }
                return Ok(HttpReply { status: 200, body: seal_response(&request, if confirmed { request["sequence"].as_i64() } else { None }, json!({"error": code})) });
            }
            if let Some(error) = self.error.lock().unwrap().take() { return error; }
        }
        if request.get("credential").is_none() { return Ok(HttpReply { status: 200, body: seal_response(&request, None, json!({"ok": true})) }); }
        self.server.post(path, token, crate::transport::NetworkPurpose::Standard).await
    }
}

fn rig(error: Option<Result<HttpReply, TransportError>>, code: Option<(&'static str, bool)>) -> (EncryptedClient, Arc<Fault>, Db) {
    rig_at(error, code, Arc::new(crate::test_wait::clock(Duration::from_secs(1_800_000_000))))
}

fn rig_at(error: Option<Result<HttpReply, TransportError>>, code: Option<(&'static str, bool)>, clock: Arc<dyn crate::pacing::Clock>) -> (EncryptedClient, Arc<Fault>, Db) {
    let http = Arc::new(Fault { error: Mutex::new(error), code, seen: Mutex::new(Vec::new()), server: FakeServer::new(clock.clone()) });
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let mut config = EncryptedConfig { origin: super::vectors_tests::origin().into(), server_key: super::vectors_tests::public_key_der_b64().into(), pin: super::vectors_tests::pin().into(), ..super::protocol_tests::config() };
    config.endpoints.session_operations.insert("ticket".into(), EncryptedSessionOperation { path: "/api/ticket".into(), class: crate::transport::EncryptedOperationClass::Sessionless });
    let client = EncryptedClient::new(&config, http.clone(), Arc::new(MemoryCredentials::default()), db.clone(), clock).unwrap();
    (client, http, db)
}

