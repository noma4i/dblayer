//! The server side of the cable socket for tests: the handshake of the protocol and the sealed frames, shared by the selftest rigs and the corpus mock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::ffi::{CableLease, DbError, EncryptedSocketEvent};
use crate::transport::encrypted::fake_server_tests::decrypt_request;

struct Connection {
    input: mpsc::UnboundedSender<EncryptedSocketEvent>,
    output: tokio::sync::Mutex<mpsc::UnboundedReceiver<EncryptedSocketEvent>>,
    secret: StaticSecret,
    salt: [u8; 32],
    keys: Mutex<Option<([u8; 32], [u8; 32])>>,
    sequence: AtomicUsize,
    subscription: Mutex<Option<String>>,
}

pub(crate) struct CablePeer {
    config: crate::transport::EncryptedConfig,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
    next: AtomicUsize,
}

impl CablePeer {
    pub(crate) fn new(config: crate::transport::EncryptedConfig) -> Self {
        Self { config, connections: Mutex::new(HashMap::new()), next: AtomicUsize::new(0) }
    }

    fn connection(&self, token: &str) -> Option<Arc<Connection>> { self.connections.lock().unwrap().get(token).cloned() }

    /// A socket opens: the token of the lease and the challenge of the server.
    pub(crate) fn open(&self, lease: &Arc<CableLease>) -> Result<String, DbError> {
        let token = self.next.fetch_add(1, Ordering::SeqCst).to_string();
        let (input, output) = mpsc::unbounded_channel();
        let connection = Arc::new(Connection { input, output: tokio::sync::Mutex::new(output), secret: StaticSecret::from([9; 32]), salt: [5; 32], keys: Mutex::new(None), sequence: AtomicUsize::new(0), subscription: Mutex::new(None) });
        self.connections.lock().unwrap().insert(token.clone(), connection.clone());
        lease.opened(token.clone())?;
        let challenge = json!({"type":"challenge", "v":1, "kid":self.config.pin, "challenge":URL_SAFE_NO_PAD.encode(connection.salt), "x25519":URL_SAFE_NO_PAD.encode(PublicKey::from(&connection.secret).as_bytes())});
        let _ = connection.input.send(EncryptedSocketEvent::Text { text: challenge.to_string() });
        Ok(token)
    }

    /// The client sends a text: the hello answers `ready`, a subscribe answers `subscribed`. The result is the id of the subscription a subscribe opened.
    pub(crate) fn receive(&self, token: &str, text: &str) -> Result<Option<String>, DbError> {
        let Some(connection) = self.connection(token) else { return Err(DbError::Network { detail: "no such socket".into() }) };
        let command: Value = serde_json::from_str(text)?;
        if command["command"] == "subscribe" {
            let _ = connection.input.send(EncryptedSocketEvent::Text { text: json!({"type":"confirm_subscription"}).to_string() });
            return Ok(None);
        }
        let data: Value = serde_json::from_str(command["data"].as_str().unwrap())?;
        if let Some(hello) = data["hello"].as_str() {
            let plain = decrypt_request(hello).unwrap();
            let client: [u8; 32] = URL_SAFE_NO_PAD.decode(plain["clientKey"].as_str().unwrap()).unwrap().try_into().unwrap();
            let response = URL_SAFE_NO_PAD.decode(plain["responseKey"].as_str().unwrap()).unwrap();
            let mut material = connection.secret.diffie_hellman(&PublicKey::from(client)).as_bytes().to_vec();
            material.extend(response);
            let hkdf = hkdf::Hkdf::<Sha256>::new(Some(&connection.salt), &material);
            let derive = |direction: &str| {
                let info = [&self.config.protocol_label, direction, &self.config.pin, &self.config.origin, &self.config.endpoints.cable].join("\0");
                let mut key = [0; 32];
                hkdf.expand(info.as_bytes(), &mut key).unwrap();
                key
            };
            *connection.keys.lock().unwrap() = Some((derive("s2c"), derive("c2s")));
            let digest: String = Sha256::digest(connection.salt).iter().map(|byte| format!("{byte:02x}")).collect();
            self.emit(token, json!({"t":"ready", "connectionId":"00000000-0000-4000-8000-000000000001", "challengeDigest":digest}));
            return Ok(None);
        }
        let frame = data["frame"].as_str().unwrap();
        let parts: Vec<_> = frame.split('.').collect();
        let iv = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let mut sealed = URL_SAFE_NO_PAD.decode(parts[3]).unwrap();
        sealed.extend(URL_SAFE_NO_PAD.decode(parts[4]).unwrap());
        let key = connection.keys.lock().unwrap().as_ref().unwrap().1;
        let plain = Aes256Gcm::new_from_slice(&key).unwrap().decrypt(Nonce::from_slice(&iv), Payload { msg: &sealed, aad: parts[0].as_bytes() }).unwrap();
        let body: Value = serde_json::from_slice(&plain)?;
        if body["t"] == "subscribe" {
            let id = body["id"].as_str().unwrap().to_string();
            *connection.subscription.lock().unwrap() = Some(id.clone());
            self.emit(token, json!({"t":"subscribed", "id":id}));
            return Ok(Some(id));
        }
        Ok(None)
    }

    /// The next event the client reads from the socket.
    pub(crate) async fn next(&self, token: &str) -> Option<EncryptedSocketEvent> {
        let connection = self.connection(token)?;
        let event = crate::test_wait::within(async { connection.output.lock().await.recv().await }).await;
        event
    }

    /// A sealed server frame.
    pub(crate) fn emit(&self, token: &str, mut body: Value) {
        let Some(connection) = self.connection(token) else { return };
        let n = connection.sequence.fetch_add(1, Ordering::SeqCst);
        body["n"] = json!(n);
        let key = connection.keys.lock().unwrap().as_ref().unwrap().0;
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"dir","enc":"A256GCM"}"#);
        let iv = [(n + 1) as u8; 12];
        let sealed = Aes256Gcm::new_from_slice(&key).unwrap().encrypt(Nonce::from_slice(&iv), Payload { msg: body.to_string().as_bytes(), aad: header.as_bytes() }).unwrap();
        let (text, tag) = sealed.split_at(sealed.len() - 16);
        let frame = format!("{header}..{}.{}.{}", URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(text), URL_SAFE_NO_PAD.encode(tag));
        let _ = connection.input.send(EncryptedSocketEvent::Text { text: json!({"message":{"frame":frame}}).to_string() });
    }

    /// A data event of the open subscription.
    pub(crate) fn event(&self, token: &str, result: Value) -> bool {
        let Some(id) = self.connection(token).and_then(|connection| connection.subscription.lock().unwrap().clone()) else { return false };
        self.emit(token, json!({"t":"next", "id":id, "result":result}));
        true
    }

    /// The socket breaks.
    pub(crate) fn close(&self, token: &str) -> bool {
        let Some(connection) = self.connection(token) else { return false };
        let _ = connection.input.send(EncryptedSocketEvent::Closed { code: 1006, reason: "dropped".into() });
        true
    }
}
