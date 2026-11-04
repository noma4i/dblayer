use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use x25519_dalek::{PublicKey, StaticSecret};

use super::rig_tests::{bound, plain_models};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, PutReply, StreamDelivery, StreamRequest, SubscriptionSink, Transport, TransportRequest, ValueSink};
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

struct Peer {
    config: crate::transport::EncryptedConfig,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
    subscribed: mpsc::UnboundedSender<String>,
    closed: mpsc::UnboundedSender<String>,
    next: AtomicUsize,
}

impl Peer {
    fn connection(&self, token: &str) -> Arc<Connection> { self.connections.lock().unwrap().get(token).unwrap().clone() }

    fn emit(&self, token: &str, mut body: Value) {
        let connection = self.connection(token);
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

    fn emit_old(&self, token: &str) {
        let id = self.connection(token).subscription.lock().unwrap().clone().unwrap();
        self.emit(token, json!({"t":"next", "id":id, "result":{"data":{"notices":{"id":"old"}}}}));
    }
}

#[async_trait]
impl Transport for Peer {
    async fn open_cable(&self, lease: Arc<CableLease>) -> Result<(), DbError> {
        let token = self.next.fetch_add(1, Ordering::SeqCst).to_string();
        let (input, output) = mpsc::unbounded_channel();
        let connection = Arc::new(Connection { input, output: tokio::sync::Mutex::new(output), secret: StaticSecret::from([9; 32]), salt: [5; 32], keys: Mutex::new(None), sequence: AtomicUsize::new(0), subscription: Mutex::new(None) });
        self.connections.lock().unwrap().insert(token.clone(), connection.clone());
        lease.opened(token)?;
        let challenge = json!({"type":"challenge", "v":1, "kid":self.config.pin, "challenge":URL_SAFE_NO_PAD.encode(connection.salt), "x25519":URL_SAFE_NO_PAD.encode(PublicKey::from(&connection.secret).as_bytes())});
        let _ = connection.input.send(EncryptedSocketEvent::Text { text: challenge.to_string() });
        Ok(())
    }

    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError> {
        let connection = self.connection(&token);
        let command: Value = serde_json::from_str(&text)?;
        if command["command"] == "subscribe" {
            let _ = connection.input.send(EncryptedSocketEvent::Text { text: json!({"type":"confirm_subscription"}).to_string() });
            return Ok(());
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
            self.emit(&token, json!({"t":"ready", "connectionId":"00000000-0000-4000-8000-000000000001", "challengeDigest":digest}));
            return Ok(());
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
            self.emit(&token, json!({"t":"subscribed", "id":id}));
            let _ = self.subscribed.send(token);
        }
        Ok(())
    }

    async fn cable_next(&self, token: String) -> Option<EncryptedSocketEvent> {
        let connection = self.connection(&token);
        let event = connection.output.lock().await.recv().await;
        event
    }
    async fn cable_close(&self, token: String) { let _ = self.closed.send(token); }
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

struct Rows;
impl ValueSink for Rows { fn delivered(&self, _: StreamDelivery) {} }

#[tokio::test(flavor = "multi_thread")]
async fn an_old_encrypted_cable_cannot_deliver_into_the_next_account() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, mut closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer { config: config.clone(), connections: Mutex::new(HashMap::new()), subscribed, closed, next: AtomicUsize::new(0) });
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, plain_models(), Some(peer.clone()), Some(config), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None, None).await.unwrap();
    database.register_operations(r#"[{"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}]"#.into()).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let query = r#"{"model":"Notice","relation":"recent","params":{}}"#;
    let first = database.subscribe(database.session_epoch(), StreamRequest::Query { query_json: query.into() }, Arc::new(Rows));
    let old = bound(subscriptions.recv()).await.unwrap();
    bound(database.session_reset()).await.unwrap();
    assert_eq!(bound(closes.recv()).await.as_deref(), Some(old.as_str()));
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let second = database.subscribe(database.session_epoch(), StreamRequest::Query { query_json: query.into() }, Arc::new(Rows));
    let current = bound(subscriptions.recv()).await.unwrap();
    assert_ne!(current, old);
    peer.emit_old(&old);
    assert!(bound(database.read_query(database.session_epoch(), query.into())).await.unwrap().is_empty());
    database.cancel_stream(first);
    database.cancel_stream(second);
    database.close_now();
}
