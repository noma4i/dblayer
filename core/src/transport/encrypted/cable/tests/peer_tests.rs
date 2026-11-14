//! The server side of the cable socket for tests: the handshake of the protocol and the sealed frames, shared by the selftest rigs and the corpus mock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

use crate::ffi::{PlatformSocket, CableEvents, CableOutcome, DbError};
use crate::transport::encrypted::tests::fake_server_tests::decrypt_request;

pub(crate) enum PeerEvent {
    Subscribed(String),
    Closed(String),
}

pub(crate) type Observer = Arc<dyn Fn(PeerEvent) + Send + Sync>;

struct Connection {
    events: Arc<dyn CableEvents>,
    secret: StaticSecret,
    salt: [u8; 32],
    keys: Mutex<Option<([u8; 32], [u8; 32])>>,
    sequence: AtomicUsize,
    subscription: Mutex<Option<String>>,
    subscribes: Mutex<Vec<Value>>,
}

#[derive(Clone)]
pub(crate) struct CablePeer {
    config: crate::transport::EncryptedConfig,
    connections: Arc<Mutex<HashMap<String, Arc<Connection>>>>,
    next: Arc<AtomicUsize>,
    received: Arc<AtomicUsize>,
    observer: Option<Observer>,
    contract: Option<Arc<Mutex<crate::server_contract::CableContract>>>,
}

struct PeerLink {
    peer: CablePeer,
    token: String,
}

impl PlatformSocket for PeerLink {
    fn send(&self, text: String) -> bool {
        match self.peer.receive(&self.token, &text) {
            Ok(subscribed) => {
                if subscribed.is_some() { self.peer.observe(PeerEvent::Subscribed(self.token.clone())); }
                true
            }
            Err(_) => false,
        }
    }

    fn close(&self, _: u16, _: String) {
        if let Some(connection) = self.peer.connection(&self.token) { *connection.subscription.lock().unwrap() = None; }
        self.peer.observe(PeerEvent::Closed(self.token.clone()));
    }

    fn cancel(&self) {}
}

impl CablePeer {
    pub(crate) fn new(config: crate::transport::EncryptedConfig) -> Self {
        Self { config, connections: Arc::default(), next: Arc::default(), received: Arc::default(), observer: None, contract: None }
    }

    pub(crate) fn with_contract(mut self, contract: Arc<Mutex<crate::server_contract::CableContract>>) -> Self {
        self.contract = Some(contract);
        self
    }

    pub(crate) fn observed(config: crate::transport::EncryptedConfig, observer: impl Fn(PeerEvent) + Send + Sync + 'static) -> Self {
        Self { observer: Some(Arc::new(observer)), ..Self::new(config) }
    }

    /// The sockets opened so far.
    pub(crate) fn connects(&self) -> usize { self.next.load(Ordering::SeqCst) }

    /// The texts the client sent so far.
    pub(crate) fn received(&self) -> usize { self.received.load(Ordering::SeqCst) }

    /// The sockets, the texts received and the subscribe frames per socket, for the message of a red wait.
    pub(crate) fn counters(&self) -> String {
        let subscribes: Vec<String> = (0..self.connects()).map(|token| format!("{token}:{}", self.subscribes(&token.to_string()).len())).collect();
        format!("cable peer: connects={}, received={}, subscribes per socket [{}]", self.connects(), self.received(), subscribes.join(", "))
    }

    fn observe(&self, event: PeerEvent) {
        if let Some(observer) = &self.observer { observer(event); }
    }

    fn connection(&self, token: &str) -> Option<Arc<Connection>> { self.connections.lock().unwrap().get(token).cloned() }

    /// A socket opens: the token of the connection and the challenge of the server.
    pub(crate) fn connect(&self, events: Arc<dyn CableEvents>) -> (String, CableOutcome) {
        let token = self.next.fetch_add(1, Ordering::SeqCst).to_string();
        let connection = Arc::new(Connection { events: events.clone(), secret: StaticSecret::from([9; 32]), salt: [5; 32], keys: Mutex::new(None), sequence: AtomicUsize::new(0), subscription: Mutex::new(None), subscribes: Mutex::new(Vec::new()) });
        self.connections.lock().unwrap().insert(token.clone(), connection.clone());
        let challenge = json!({"type":"challenge", "v":1, "kid":self.config.pin, "challenge":URL_SAFE_NO_PAD.encode(connection.salt), "x25519":URL_SAFE_NO_PAD.encode(PublicKey::from(&connection.secret).as_bytes())});
        events.frame(challenge.to_string());
        (token.clone(), CableOutcome::Open { connection: Arc::new(PeerLink { peer: self.clone(), token }) })
    }

    /// The client sends a text: the hello answers `ready`, a subscribe answers `subscribed`. The result is the id of the subscription a subscribe opened.
    pub(crate) fn receive(&self, token: &str, text: &str) -> Result<Option<String>, DbError> {
        self.received.fetch_add(1, Ordering::SeqCst);
        let Some(connection) = self.connection(token) else { return Err(DbError::Network { detail: "no such socket".into() }) };
        let command: Value = serde_json::from_str(text)?;
        if command["command"] == "subscribe" {
            connection.events.frame(json!({"type":"confirm_subscription"}).to_string());
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
        if let Some(contract) = &self.contract {
            let response = contract.lock().unwrap().receive(&body);
            if let Some(response) = response { self.emit(token, response); }
        }
        if body["t"] == "subscribe" {
            let id = body["id"].as_str().unwrap().to_string();
            connection.subscribes.lock().unwrap().push(body.clone());
            *connection.subscription.lock().unwrap() = Some(id.clone());
            self.emit(token, json!({"t":"subscribed", "id":id}));
            return Ok(Some(id));
        }
        Ok(None)
    }

    /// A sealed server frame.
    pub(crate) fn emit(&self, token: &str, body: Value) {
        let Some(connection) = self.connection(token) else { return };
        connection.events.frame(Self::seal(&connection, body));
    }

    fn seal(connection: &Connection, mut body: Value) -> String {
        let n = connection.sequence.fetch_add(1, Ordering::SeqCst);
        body["n"] = json!(n);
        let key = connection.keys.lock().unwrap().as_ref().unwrap().0;
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"dir","enc":"A256GCM"}"#);
        let iv = [(n + 1) as u8; 12];
        let sealed = Aes256Gcm::new_from_slice(&key).unwrap().encrypt(Nonce::from_slice(&iv), Payload { msg: body.to_string().as_bytes(), aad: header.as_bytes() }).unwrap();
        let (text, tag) = sealed.split_at(sealed.len() - 16);
        let frame = format!("{header}..{}.{}.{}", URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(text), URL_SAFE_NO_PAD.encode(tag));
        json!({"message":{"frame":frame}}).to_string()
    }

    pub(crate) fn subscribes(&self, token: &str) -> Vec<Value> { self.connection(token).map(|connection| connection.subscribes.lock().unwrap().clone()).unwrap_or_default() }

    /// A data event of the open subscription.
    pub(crate) fn event(&self, token: &str, result: Value) -> bool {
        let Some(id) = self.connection(token).and_then(|connection| connection.subscription.lock().unwrap().clone()) else { return false };
        self.emit(token, json!({"t":"next", "id":id, "result":result}));
        true
    }

    /// The socket breaks.
    pub(crate) fn close(&self, token: &str) -> bool {
        let Some(connection) = self.connection(token) else { return false };
        connection.events.ended(Some(1006), Some("dropped".into()), None);
        true
    }
}
