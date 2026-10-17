use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::transport::{Connectivity, TransportError};
use crate::transport::{GraphQLRequest, SubscriptionEvent};
use serde_json::json;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rsa::{pkcs1::DecodeRsaPrivateKey, Oaep, RsaPrivateKey};
use sha2::{Digest, Sha256};

struct Peer {
    incoming: mpsc::UnboundedSender<SocketEvent>,
    outgoing: mpsc::UnboundedReceiver<String>,
    send_key: [u8; 32],
    receive_key: [u8; 32],
    sequence: u64,
}

impl Peer {
    async fn handshake(&mut self, kat: &Value, config: &EncryptedConfig, challenge: Value) {
        self.incoming.send(SocketEvent::Text(challenge.to_string())).unwrap();
        let subscribe: Value = serde_json::from_str(&self.outgoing.recv().await.unwrap()).unwrap();
        assert_eq!(subscribe["command"], "subscribe");
        self.incoming.send(SocketEvent::Text(json!({"type":"confirm_subscription"}).to_string())).unwrap();
        let hello: Value = serde_json::from_str(&self.outgoing.recv().await.unwrap()).unwrap();
        let data: Value = serde_json::from_str(hello["data"].as_str().unwrap()).unwrap();
        let token = data["hello"].as_str().unwrap();
        let private = RsaPrivateKey::from_pkcs1_pem(kat["hello"]["rsa_private_pem"].as_str().unwrap()).unwrap();
        let key = private.decrypt(Oaep::new::<sha1::Sha1>(), &URL_SAFE_NO_PAD.decode(token.split('.').nth(1).unwrap()).unwrap()).unwrap();
        let plain: Value = serde_json::from_slice(&super::link_tests::decrypt(token, &key)).unwrap();
        let server_private: Vec<u8> = kat["x25519"]["server_private"].as_str().unwrap().as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect();
        let secret = x25519_dalek::StaticSecret::from(<[u8; 32]>::try_from(server_private).unwrap());
        let client_key = x25519_dalek::PublicKey::from(keys::decode(&plain["clientKey"]).unwrap());
        let mut ikm = secret.diffie_hellman(&client_key).as_bytes().to_vec();
        ikm.extend(keys::decode(&plain["responseKey"]).unwrap());
        let salt = keys::decode(&challenge["challenge"]).unwrap();
        let hkdf = hkdf::Hkdf::<Sha256>::new(Some(&salt), &ikm);
        for (direction, key) in [("s2c", &mut self.send_key), ("c2s", &mut self.receive_key)] {
            let info = ["yupi-v3-ws-v1", direction, &config.pin, &config.origin, CABLE_PATH].join("\0");
            hkdf.expand(info.as_bytes(), key).unwrap();
        }
        let digest: String = Sha256::digest(salt).iter().map(|byte| format!("{byte:02x}")).collect();
        self.emit(json!({"t":"ready", "connectionId":"00000000-0000-4000-8000-000000000001", "challengeDigest":digest}));
    }

    fn emit(&mut self, mut body: Value) {
        body["n"] = json!(self.sequence);
        self.sequence += 1;
        let token = frame::encrypt(&self.send_key, &serde_json::to_vec(&body).unwrap(), &[self.sequence as u8; 12]).unwrap();
        self.incoming.send(SocketEvent::Text(json!({"message":{"frame":token}}).to_string())).unwrap();
    }

    async fn next(&mut self) -> Value {
        let text = tokio::time::timeout(std::time::Duration::from_secs(1), self.outgoing.recv()).await.unwrap().unwrap();
        let command: Value = serde_json::from_str(&text).unwrap();
        let data: Value = serde_json::from_str(command["data"].as_str().unwrap()).unwrap();
        frame::decrypt(&self.receive_key, data["frame"].as_str().unwrap()).unwrap()
    }
}

struct FakeSocket {
    incoming: tokio::sync::Mutex<mpsc::UnboundedReceiver<SocketEvent>>,
    outgoing: mpsc::UnboundedSender<String>,
    closes: AtomicUsize,
}

#[async_trait]
impl CableSocket for FakeSocket {
    async fn send(&self, text: String) -> Result<(), TransportError> {
        self.outgoing.send(text).map_err(|_| TransportError::Network("closed fake".into()))
    }
    async fn next(&self) -> Option<SocketEvent> { self.incoming.lock().await.recv().await }
    async fn close(&self) { self.closes.fetch_add(1, Ordering::SeqCst); }
}

struct FakeConnect {
    socket: Arc<FakeSocket>,
    opens: AtomicUsize,
}

#[async_trait]
impl CableConnect for FakeConnect {
    async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(self.socket.clone())
    }
}

#[async_trait]
impl crate::transport::Transport for FakeConnect {
    async fn open_cable(&self) -> Result<Arc<dyn CableSocket>, TransportError> { CableConnect::open(self).await }
    async fn query(&self, _: GraphQLRequest) -> Result<crate::transport::Response, TransportError> { panic!("unencrypted query") }
    async fn mutate(&self, _: GraphQLRequest) -> Result<crate::transport::Response, TransportError> { panic!("unencrypted mutation") }
    async fn subscribe(&self, _: GraphQLRequest) -> Result<crate::transport::Subscription, TransportError> { panic!("V2 subscription") }
    async fn put(&self, _: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, TransportError> { unreachable!() }
}

struct Http {
    key: RsaPrivateKey,
    reads: AtomicUsize,
}

struct Credentials {
    value: std::sync::Mutex<Option<String>>,
    writes: AtomicUsize,
}

impl crate::transport::CredentialSlot for Credentials {
    fn get(&self) -> Result<Option<String>, TransportError> { Ok(self.value.lock().unwrap().clone()) }
    fn set(&self, value: &str) -> Result<(), TransportError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        *self.value.lock().unwrap() = Some(value.into());
        Ok(())
    }
    fn clear(&self) -> Result<(), TransportError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn every_close_keeps_credentials_and_revoke_stops_while_redis_outage_retries() {
    use crate::transport::CredentialSlot;
    for (code, reason, terminal) in [(4400,"",false), (4401,"",true), (4403,"",true), (4408,"",false), (4409,"",false), (4429,"",false), (4430,"SUPERSEDED",false), (4500,"",false), (4503,"",false), (1000,"LOGGED_OUT",true)] {
        let (incoming, receiver) = mpsc::unbounded_channel();
        let (outgoing, sent) = mpsc::unbounded_channel();
        let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
        let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
        let (kat, config, challenge, credential) = super::link_tests::inputs();
        let credentials = Arc::new(Credentials { value:std::sync::Mutex::new(Some(credential.clone())), writes:AtomicUsize::new(0) });
        let slot = credentials.clone();
        let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(slot.get()?.unwrap())), connect.clone(), Connectivity::new());
        let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({}))).await.unwrap();
        let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
        peer.handshake(&kat, &config, challenge).await;
        let subscribe = peer.next().await;
        peer.emit(json!({"t":"subscribed", "id":subscribe["id"]}));
        assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Ready)));
        if reason == "LOGGED_OUT" { peer.emit(json!({"t":"closing", "code":code, "reason":reason})); }
        else { peer.incoming.send(SocketEvent::Closed { code, reason:reason.into() }).unwrap(); }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        assert_eq!(credentials.get().unwrap(), Some(credential), "code {code}");
        assert_eq!(credentials.writes.load(Ordering::SeqCst), 0, "code {code}");
        assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
        assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
        if terminal {
            let Some(SubscriptionEvent::Failed(failure)) = tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next()).await.unwrap() else { panic!("terminal {code}") };
            assert_eq!(failure.close.unwrap().code, code);
            assert!(matches!(failure.error, TransportError::Session { .. }));
            assert!(subscription.next().await.is_none());
            tokio::time::advance(std::time::Duration::from_secs(2)).await;
            cable.settled().await.unwrap();
            assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
        } else {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(connect.opens.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn authentication_refusal_before_ready_stops_without_reopening() {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, _sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let (_, config, _, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config, Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new());
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({}))).await.unwrap();
    incoming.send(SocketEvent::Closed { code:4401, reason:"UNAUTHENTICATED".into() }).unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next()).await.expect("terminal refusal");
    assert!(matches!(event, Some(SubscriptionEvent::Failed(crate::transport::SubscriptionFailure { error:TransportError::Session { .. }, .. }))));
    tokio::time::advance(std::time::Duration::from_secs(100)).await;
    cable.settled().await.unwrap();
    assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
}

#[async_trait]
impl crate::transport::HttpExchange for Http {
    async fn get(&self, _: &str) -> Result<crate::transport::HttpReply, TransportError> { unreachable!() }
    async fn post(&self, path: &str, token: String) -> Result<crate::transport::HttpReply, TransportError> {
        assert_eq!(path, "/api/v3/graphql");
        let key = self.key.decrypt(Oaep::new::<sha1::Sha1>(), &URL_SAFE_NO_PAD.decode(token.split('.').nth(1).unwrap()).unwrap()).unwrap();
        let request: Value = serde_json::from_slice(&super::link_tests::decrypt(&token, &key)).unwrap();
        assert_eq!(request["body"]["operationName"], "ChatNode");
        self.reads.fetch_add(1, Ordering::SeqCst);
        let body = json!({"data":{"node":{"id":request["body"]["variables"]["id"], "title":"from HTTP"}}});
        Ok(crate::transport::HttpReply { status: 200, body: super::super::fake_server_tests::seal_response(&request, None, body) })
    }
}

#[tokio::test(start_paused = true)]
async fn encrypted_change_rereads_over_jwe_and_commits_the_http_row() {
    use crate::schema::{ChangeRead, FieldSpec, ModelSpec, NodeRead, RelationSpec, VariableSource, NODE_PARAM};
    use crate::value::ScalarKind;
    let (kat, config, challenge, _) = super::link_tests::inputs();
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let mut model = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str).optional()]);
    let mut relation = RelationSpec::new("node", &[]);
    relation.node = Some(NodeRead::One);
    relation.text_params.push(NODE_PARAM.into());
    relation.remote = Some(serde_json::from_value(json!({"operation":"ChatNode", "shape":{"type":"single"}, "stale_time":"5.minutes"})).unwrap());
    relation.remote.as_mut().unwrap().variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    model.relations.push(relation);
    model.changes.push(ChangeRead { area:"CHAT".into(), relation:"node".into(), parameter:None });
    let db = crate::db::Db::open_in_memory(vec![model], None).unwrap();
    let http = Arc::new(Http { key:RsaPrivateKey::from_pkcs1_pem(kat["hello"]["rsa_private_pem"].as_str().unwrap()).unwrap(), reads:AtomicUsize::new(0) });
    let clock = Arc::new(crate::pacing::ManualClock::default());
    let client = Arc::new(crate::transport::EncryptedClient::new(&config, http.clone(), Arc::new(super::super::fake_server_tests::MemoryCredentials::default()), db.clone(), clock.clone()).unwrap());
    let transport = Arc::new(crate::transport::EncryptedTransport::new(client, connect));
    let sync = crate::sync::Sync::new(db.clone(), transport, clock, Default::default());
    sync.register(crate::sync::OperationDef::new("ChatNode", "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title } } }", "node"));
    let _watch = sync.watch("Chat", "node", &serde_json::Map::from_iter([(NODE_PARAM.into(), json!("c1"))])).unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscription = peer.next().await;
    peer.emit(json!({"t":"subscribed", "id":subscription["id"]}));
    peer.emit(json!({"t":"next", "id":subscription["id"], "result":{"data":{"viewerChanges":{"changes":[{"area":"CHAT", "entityId":"c1", "title":"forged"}]}}}}));
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let row = db.find("Chat", &crate::identity::RowId::new("c1").unwrap()).unwrap().expect("HTTP row");
    assert_eq!(row.text("title"), Some("from HTTP"));
    assert_eq!(http.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn no_socket_or_timer_work_without_subscribers() {
    let (_incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, mut sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let config = EncryptedConfig { origin: "https://example.test".into(), server_key: String::new(), pin: String::new() };
    let cable = CableSubscriptions::new(config, Arc::new(|| panic!("idle cable read credential")), connect.clone(), Connectivity::new());
    cable.settled().await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(100)).await;
    tokio::task::yield_now().await;
    assert_eq!(connect.opens.load(Ordering::SeqCst), 0);
    assert!(sent.try_recv().is_err());
    assert_eq!(cable.wake_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn closing_the_last_subscription_or_transport_releases_the_socket() {
    for (drop_transport, ready) in [(false, true), (true, true), (false, false), (true, false)] {
        let (incoming, receiver) = mpsc::unbounded_channel();
        let (outgoing, sent) = mpsc::unbounded_channel();
        let socket = Arc::new(FakeSocket { incoming:tokio::sync::Mutex::new(receiver), outgoing, closes:AtomicUsize::new(0) });
        let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
        let (kat, config, challenge, credential) = super::link_tests::inputs();
        let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new());
        let subscription = cable.subscribe(GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({}))).await.unwrap();
        let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
        let subscribe = if ready { peer.handshake(&kat, &config, challenge).await; Some(peer.next().await) } else { None };
        if drop_transport { drop(cable); }
        else {
            subscription.cancel();
            if let Some(subscribe) = subscribe {
                let frame = peer.next().await;
                assert_eq!(frame["t"], "unsubscribe");
                assert_eq!(frame["id"], subscribe["id"]);
            }
            cable.settled().await.unwrap();
            tokio::time::advance(std::time::Duration::from_secs(100)).await;
            cable.settled().await.unwrap();
            assert_eq!(cable.wake_count(), 0);
        }
        tokio::task::yield_now().await;
        assert_eq!(socket.closes.load(Ordering::SeqCst), 1, "drop_transport={drop_transport}, ready={ready}");
        assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_closes_the_connection_before_backoff() {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming:tokio::sync::Mutex::new(receiver), outgoing, closes:AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new());
    let request = GraphQLRequest::new("subscription { viewerChanges { __typename } }", "Changes", json!({}));
    let _first = cable.subscribe(request.clone()).await.unwrap();
    let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
    peer.handshake(&kat, &config, challenge).await;
    peer.next().await;
    drop(peer.outgoing);
    let _second = cable.subscribe(request).await.unwrap();
    cable.settled().await.unwrap();
    assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
    assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn encrypted_subscription_sends_one_ping_then_closes_on_missing_pong() {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket: socket.clone(), opens: AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect, Connectivity::new());
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription Changes { viewerChanges { changes { entityId } } }", "Changes", json!({}))).await.unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key: [0;32], receive_key:[0;32], sequence:0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscribe = peer.next().await;
    assert_eq!(subscribe["t"], "subscribe");
    let id = subscribe["id"].clone();
    peer.emit(json!({"t":"subscribed", "id":id}));
    assert!(matches!(subscription.next().await, Some(SubscriptionEvent::Ready)));
    tokio::time::advance(std::time::Duration::from_secs(25)).await;
    assert_eq!(peer.next().await["t"], "ping");
    assert_eq!(cable.wake_count(), 1);
    tokio::time::advance(std::time::Duration::from_secs(20)).await;
    tokio::task::yield_now().await;
    assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
}
