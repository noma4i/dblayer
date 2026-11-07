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
use crate::test_wait::{serving, within};

struct ReceiveCancellation<'a>(&'a AtomicUsize, bool);

impl Drop for ReceiveCancellation<'_> {
    fn drop(&mut self) {
        if self.1 { self.0.fetch_add(1, Ordering::SeqCst); }
    }
}

struct LifecycleSocket {
    socket: Arc<FakeSocket>,
    cancellations: AtomicUsize,
}

#[async_trait]
impl CableSocket for LifecycleSocket {
    async fn send(&self, text: String) -> Result<(), TransportError> {
        if self.cancellations.load(Ordering::SeqCst) > 0 { return Err(TransportError::Network("cancelled receive closed socket".into())); }
        self.socket.send(text).await
    }
    async fn next(&self) -> Option<SocketEvent> {
        let mut cancellation = ReceiveCancellation(&self.cancellations, true);
        let event = serving(self.socket.next()).await;
        cancellation.1 = false;
        event
    }
    async fn close(&self) { self.socket.close().await; }
}

#[test]
fn server_pongs_keep_pending_receive_alive_for_120_seconds() { crate::test_wait::paused(async {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let lifecycle = Arc::new(LifecycleSocket { socket: socket.clone(), cancellations: AtomicUsize::new(0) });
    struct Connect(Arc<LifecycleSocket>, AtomicUsize);
    #[async_trait]
    impl CableConnect for Connect {
        async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(self.0.clone())
        }
    }
    let connect = Arc::new(Connect(lifecycle.clone(), AtomicUsize::new(0)));
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    let _subscription = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))).await.unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key: [0;32], receive_key: [0;32], sequence: 0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscribe = within(peer.next()).await;
    peer.emit(json!({"t":"subscribed", "id":subscribe["id"]}));
    let pongs = Arc::new(AtomicUsize::new(0));
    let count = pongs.clone();
    let server = tokio::spawn(async move {
        while let Some(text) = serving(peer.outgoing.recv()).await {
            let command: Value = serde_json::from_str(&text).unwrap();
            let data: Value = serde_json::from_str(command["data"].as_str().unwrap()).unwrap();
            let body = frame::decrypt(&peer.receive_key, data["frame"].as_str().unwrap()).unwrap();
            if body["t"] == "ping" {
                count.fetch_add(1, Ordering::SeqCst);
                peer.emit(json!({"t":"pong", "ts":body["ts"]}));
            }
        }
    });
    for _ in 0..120 {
        for _ in 0..20 { tokio::task::yield_now().await; }
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        for _ in 0..20 { tokio::task::yield_now().await; }
    }
    assert_eq!(socket.closes.load(Ordering::SeqCst), 0);
    assert_eq!(connect.1.load(Ordering::SeqCst), 1);
    assert_eq!(lifecycle.cancellations.load(Ordering::SeqCst), 0);
    assert_eq!(pongs.load(Ordering::SeqCst), 4);
    server.abort();
}) }

pub(super) struct Peer {
    pub(super) incoming: mpsc::UnboundedSender<SocketEvent>,
    pub(super) outgoing: mpsc::UnboundedReceiver<String>,
    pub(super) send_key: [u8; 32],
    pub(super) receive_key: [u8; 32],
    pub(super) sequence: u64,
}

impl Peer {
    pub(super) async fn handshake(&mut self, kat: &Value, config: &EncryptedConfig, challenge: Value) {
        self.incoming.send(SocketEvent::Text(challenge.to_string())).unwrap();
        let subscribe: Value = serde_json::from_str(&within(self.outgoing.recv()).await.unwrap()).unwrap();
        assert_eq!(subscribe["command"], "subscribe");
        assert_eq!(subscribe["identifier"], json!({"channel":config.cable_channel}).to_string());
        self.incoming.send(SocketEvent::Text(json!({"type":"confirm_subscription"}).to_string())).unwrap();
        let hello: Value = serde_json::from_str(&within(self.outgoing.recv()).await.unwrap()).unwrap();
        let data: Value = serde_json::from_str(hello["data"].as_str().unwrap()).unwrap();
        let token = data["hello"].as_str().unwrap();
        let private = RsaPrivateKey::from_pkcs1_pem(kat["hello"]["rsa_private_pem"].as_str().unwrap()).unwrap();
        let key = private.decrypt(Oaep::new::<sha1::Sha1>(), &URL_SAFE_NO_PAD.decode(token.split('.').nth(1).unwrap()).unwrap()).unwrap();
        let plain: Value = serde_json::from_slice(&super::link_tests::decrypt(token, &key)).unwrap();
        let server_private: Vec<u8> = kat["x25519"]["server_private"].as_str().unwrap().as_bytes().as_chunks::<2>().0.iter().map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect();
        let secret = x25519_dalek::StaticSecret::from(<[u8; 32]>::try_from(server_private).unwrap());
        let client_key = x25519_dalek::PublicKey::from(keys::decode(&plain["clientKey"]).unwrap());
        let mut ikm = secret.diffie_hellman(&client_key).as_bytes().to_vec();
        ikm.extend(keys::decode(&plain["responseKey"]).unwrap());
        let salt = keys::decode(&challenge["challenge"]).unwrap();
        let hkdf = hkdf::Hkdf::<Sha256>::new(Some(&salt), &ikm);
        for (direction, key) in [("s2c", &mut self.send_key), ("c2s", &mut self.receive_key)] {
            let info = [&config.protocol_label, direction, &config.pin, &config.origin, &config.endpoints.cable].join("\0");
            hkdf.expand(info.as_bytes(), key).unwrap();
        }
        let digest: String = Sha256::digest(salt).iter().map(|byte| format!("{byte:02x}")).collect();
        self.emit(json!({"t":"ready", "connectionId":"00000000-0000-4000-8000-000000000001", "challengeDigest":digest}));
    }

    pub(super) fn emit(&mut self, mut body: Value) {
        body["n"] = json!(self.sequence);
        self.sequence += 1;
        let token = frame::encrypt(&self.send_key, &serde_json::to_vec(&body).unwrap(), &[self.sequence as u8; 12]).unwrap();
        self.incoming.send(SocketEvent::Text(json!({"message":{"frame":token}}).to_string())).unwrap();
    }

    pub(super) async fn next(&mut self) -> Value {
        let text = tokio::time::timeout(std::time::Duration::from_secs(1), self.outgoing.recv()).await.unwrap().unwrap();
        let command: Value = serde_json::from_str(&text).unwrap();
        let data: Value = serde_json::from_str(command["data"].as_str().unwrap()).unwrap();
        frame::decrypt(&self.receive_key, data["frame"].as_str().unwrap()).unwrap()
    }
}

pub(super) struct FakeSocket {
    pub(super) incoming: tokio::sync::Mutex<mpsc::UnboundedReceiver<SocketEvent>>,
    pub(super) outgoing: mpsc::UnboundedSender<String>,
    pub(super) closes: AtomicUsize,
}

#[async_trait]
impl CableSocket for FakeSocket {
    async fn send(&self, text: String) -> Result<(), TransportError> {
        self.outgoing.send(text).map_err(|_| TransportError::Network("closed fake".into()))
    }
    async fn next(&self) -> Option<SocketEvent> { serving(self.incoming.lock().await.recv()).await }
    async fn close(&self) { self.closes.fetch_add(1, Ordering::SeqCst); }
}

pub(super) struct FakeConnect {
    pub(super) socket: Arc<FakeSocket>,
    pub(super) opens: AtomicUsize,
}

#[test]
fn stopping_subscriptions_drops_pending_open() { crate::test_wait::paused(async {
    struct PendingOpen {
        started: mpsc::UnboundedSender<()>,
        stopped: mpsc::UnboundedSender<()>,
    }
    struct Opening(mpsc::UnboundedSender<()>);
    impl Drop for Opening {
        fn drop(&mut self) { let _ = self.0.send(()); }
    }
    #[async_trait]
    impl CableConnect for PendingOpen {
        async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> {
            let _opening = Opening(self.stopped.clone());
            self.started.send(()).unwrap();
            std::future::pending().await
        }
    }
    let mut failures = Vec::new();
    for action in ["cancel", "offline", "drop"] {
        let (started, mut starts) = mpsc::unbounded_channel();
        let (stopped, mut stops) = mpsc::unbounded_channel();
        let connect = Arc::new(PendingOpen { started, stopped });
        let (_, config, _, _) = super::link_tests::inputs();
        let online = Connectivity::new(true);
        let cable = CableSubscriptions::new(config, Arc::new(|| Ok("credential".into())), connect, online.clone(), Arc::new(crate::runtime::TestClock::default()));
        let subscription = cable.subscribe(GraphQLRequest::new("subscription { x }", "X", json!({}))).await.unwrap();
        within(starts.recv()).await.unwrap();
        match action {
            "cancel" => subscription.cancel(),
            "offline" => online.set_online(false),
            _ => drop(cable),
        }
        if tokio::time::timeout(std::time::Duration::from_secs(1), stops.recv()).await.is_err() { failures.push(action); }
    }
    assert!(failures.is_empty(), "pending socket open survived: {failures:?}");
}) }

#[test]
fn terminal_open_refusals_are_not_retried() { crate::test_wait::paused(async {
    struct Refused(u16, AtomicUsize);
    #[async_trait]
    impl CableConnect for Refused {
        async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::Http { status: self.0, body: "Forbidden".into() })
        }
    }
    for status in [403, 401] {
        let connect = Arc::new(Refused(status, AtomicUsize::new(0)));
        let (_, config, _, _) = super::link_tests::inputs();
        let cable = CableSubscriptions::new(config, Arc::new(|| Ok("credential".into())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
        let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { x }", "X", json!({}))).await.unwrap();
        cable.settled().await.unwrap();
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        cable.settled().await.unwrap();
        assert_eq!(connect.1.load(Ordering::SeqCst), 1, "HTTP {status} opened the socket again");
        let Some(SubscriptionEvent::Failed(failure)) = within(subscription.next()).await else { panic!("terminal refusal") };
        assert!(matches!(failure.error, TransportError::Session { .. }));
        assert_eq!(cable.wake_count(), 0);
    }
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, _sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket: socket.clone(), opens: AtomicUsize::new(0) });
    let (_, config, _, _) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config, Arc::new(|| Ok("credential".into())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    incoming.send(SocketEvent::Text(json!({"type":"reject_subscription"}).to_string())).unwrap();
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { x }", "X", json!({}))).await.unwrap();
    cable.settled().await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    cable.settled().await.unwrap();
    assert_eq!(connect.opens.load(Ordering::SeqCst), 1, "rejected channel opened again");
    let Some(SubscriptionEvent::Failed(failure)) = within(subscription.next()).await else { panic!("terminal channel refusal") };
    assert!(matches!(failure.error, TransportError::Session { .. }));
    assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
    assert_eq!(cable.wake_count(), 0);
}) }

#[async_trait]
impl CableConnect for FakeConnect {
    async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(self.socket.clone())
    }
}

#[async_trait]
impl crate::transport::Transport for FakeConnect {
    async fn open_cable(&self, _url: &str) -> Result<Arc<dyn CableSocket>, TransportError> { CableConnect::open(self).await }
    async fn query(&self, _: GraphQLRequest) -> Result<crate::transport::Response, TransportError> { panic!("unencrypted query") }
    async fn mutate(&self, _: GraphQLRequest) -> Result<crate::transport::Response, TransportError> { panic!("unencrypted mutation") }
    async fn put(&self, _: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, TransportError> { unreachable!() }
}


struct Credentials {
    value: std::sync::Mutex<Option<String>>,
    writes: AtomicUsize,
}

#[test]
fn credential_failure_never_opens_a_socket() { crate::test_wait::paused(async {
    for credential in [Err(TransportError::Network("credential unavailable".into())), Ok(String::new())] {
        let (_incoming, receiver) = mpsc::unbounded_channel();
        let (outgoing, mut sent) = mpsc::unbounded_channel();
        let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
        let connect = Arc::new(FakeConnect { socket: socket.clone(), opens: AtomicUsize::new(0) });
        let (_, config, _, _) = super::link_tests::inputs();
        let cable = CableSubscriptions::new(config, Arc::new(move || credential.clone()), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
        let _subscription = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))).await.unwrap();
        for _ in 0..120 {
            cable.settled().await.unwrap();
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
        }
        assert_eq!(connect.opens.load(Ordering::SeqCst), 0);
        assert_eq!(socket.closes.load(Ordering::SeqCst), 0);
        assert!(sent.try_recv().is_err());
    }
}) }

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

#[test]
fn every_close_keeps_credentials_and_revoke_stops_while_redis_outage_retries() { crate::test_wait::paused(async {
    use crate::transport::CredentialSlot;
    for (code, reason, terminal) in [(4400,"",false), (4401,"",true), (4403,"",true), (4408,"",false), (4409,"",false), (4429,"",false), (4430,"SUPERSEDED",false), (4500,"",false), (4503,"",false), (1000,"LOGGED_OUT",true),
        (1003,"",false),
        (1006,"",false)] {
        let (incoming, receiver) = mpsc::unbounded_channel();
        let (outgoing, sent) = mpsc::unbounded_channel();
        let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
        let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
        let (kat, config, challenge, credential) = super::link_tests::inputs();
        let credentials = Arc::new(Credentials { value:std::sync::Mutex::new(Some(credential.clone())), writes:AtomicUsize::new(0) });
        let slot = credentials.clone();
        let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(slot.get()?.unwrap())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
        let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))).await.unwrap();
        let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
        peer.handshake(&kat, &config, challenge).await;
        let subscribe = within(peer.next()).await;
        peer.emit(json!({"t":"subscribed", "id":subscribe["id"]}));
        assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Ready)));
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
            assert!(within(subscription.next()).await.is_none());
            tokio::time::advance(std::time::Duration::from_secs(2)).await;
            cable.settled().await.unwrap();
            assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
        } else {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(connect.opens.load(Ordering::SeqCst), 2);
        }
    }
}) }

#[test]
fn authentication_refusal_before_ready_stops_without_reopening() { crate::test_wait::paused(async {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, _sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket: socket.clone(), opens: AtomicUsize::new(0) });
    let (_, config, _, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config, Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))).await.unwrap();
    incoming.send(SocketEvent::Closed { code:4401, reason:"UNAUTHENTICATED".into() }).unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), subscription.next()).await.expect("terminal refusal");
    assert!(matches!(event, Some(SubscriptionEvent::Failed(crate::transport::SubscriptionFailure { error:TransportError::Session { .. }, .. }))));
    tokio::time::advance(std::time::Duration::from_secs(120)).await;
    cable.settled().await.unwrap();
    let mut late = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "LateChanges", json!({}))).await.unwrap();
    cable.settled().await.unwrap();
    assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
    assert!(matches!(within(late.next()).await, Some(SubscriptionEvent::Failed(crate::transport::SubscriptionFailure { error: TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, .. }, .. }))));
    cable.reconnect();
    let _signed_in = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "SignedInChanges", json!({}))).await.unwrap();
    cable.settled().await.unwrap();
    assert_eq!(connect.opens.load(Ordering::SeqCst), 2);
}) }


#[test]
fn no_socket_or_timer_work_without_subscribers() { crate::test_wait::paused(async {
    let (_incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, mut sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket, opens: AtomicUsize::new(0) });
    let config = EncryptedConfig { origin: "https://example.test".into(), server_key: String::new(), pin: String::new(), ..crate::transport::encrypted::protocol_tests::config() };
    let cable = CableSubscriptions::new(config, Arc::new(|| panic!("idle cable read credential")), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    cable.settled().await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(100)).await;
    tokio::task::yield_now().await;
    assert_eq!(connect.opens.load(Ordering::SeqCst), 0);
    assert!(sent.try_recv().is_err());
    assert_eq!(cable.wake_count(), 0);
}) }

#[test]
fn closing_the_last_subscription_or_transport_releases_the_socket() { crate::test_wait::paused(async {
    for (drop_transport, ready) in [(false, true), (true, true), (false, false), (true, false)] {
        let (incoming, receiver) = mpsc::unbounded_channel();
        let (outgoing, sent) = mpsc::unbounded_channel();
        let socket = Arc::new(FakeSocket { incoming:tokio::sync::Mutex::new(receiver), outgoing, closes:AtomicUsize::new(0) });
        let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
        let (kat, config, challenge, credential) = super::link_tests::inputs();
        let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
        let subscription = cable.subscribe(GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}))).await.unwrap();
        let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
        let subscribe = if ready { peer.handshake(&kat, &config, challenge).await; Some(within(peer.next()).await) } else { None };
        if drop_transport { drop(cable); }
        else {
            subscription.cancel();
            if let Some(subscribe) = subscribe {
                let frame = within(peer.next()).await;
                assert_eq!(frame["t"], "unsubscribe");
                assert_eq!(frame["id"], subscribe["id"]);
            }
            tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            cable.settled().await.unwrap();
            assert_eq!(cable.wake_count(), 0, "ready={ready}");
        }
        tokio::task::yield_now().await;
        assert_eq!(socket.closes.load(Ordering::SeqCst), 1, "drop_transport={drop_transport}, ready={ready}");
        assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
    }
}) }

#[test]
fn a_failed_write_closes_the_connection_before_backoff() { crate::test_wait::paused(async {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming:tokio::sync::Mutex::new(receiver), outgoing, closes:AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket:socket.clone(), opens:AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect.clone(), Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    let request = GraphQLRequest::new("subscription { recordEvents { __typename } }", "Changes", json!({}));
    let _first = cable.subscribe(request.clone()).await.unwrap();
    let mut peer = Peer { incoming, outgoing:sent, send_key:[0;32], receive_key:[0;32], sequence:0 };
    peer.handshake(&kat, &config, challenge).await;
    within(peer.next()).await;
    drop(peer.outgoing);
    let _second = cable.subscribe(request).await.unwrap();
    cable.settled().await.unwrap();
    assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
    assert_eq!(connect.opens.load(Ordering::SeqCst), 1);
}) }

#[test]
fn encrypted_subscription_sends_one_ping_then_closes_on_missing_pong() { crate::test_wait::paused(async {
    let (incoming, receiver) = mpsc::unbounded_channel();
    let (outgoing, sent) = mpsc::unbounded_channel();
    let socket = Arc::new(FakeSocket { incoming: tokio::sync::Mutex::new(receiver), outgoing, closes: AtomicUsize::new(0) });
    let connect = Arc::new(FakeConnect { socket: socket.clone(), opens: AtomicUsize::new(0) });
    let (kat, config, challenge, credential) = super::link_tests::inputs();
    let cable = CableSubscriptions::new(config.clone(), Arc::new(move || Ok(credential.clone())), connect, Connectivity::new(true), Arc::new(crate::runtime::TestClock::default()));
    let mut subscription = cable.subscribe(GraphQLRequest::new("subscription Changes { recordEvents { changes { entityId } } }", "Changes", json!({}))).await.unwrap();
    let mut peer = Peer { incoming, outgoing: sent, send_key: [0;32], receive_key:[0;32], sequence:0 };
    peer.handshake(&kat, &config, challenge).await;
    let subscribe = within(peer.next()).await;
    assert_eq!(subscribe["t"], "subscribe");
    let id = subscribe["id"].clone();
    peer.emit(json!({"t":"subscribed", "id":id}));
    assert!(matches!(within(subscription.next()).await, Some(SubscriptionEvent::Ready)));
    tokio::time::advance(std::time::Duration::from_secs(25)).await;
    assert_eq!(within(peer.next()).await["t"], "ping");
    assert_eq!(cable.wake_count(), 1);
    tokio::time::advance(std::time::Duration::from_secs(20)).await;
    tokio::task::yield_now().await;
    assert_eq!(socket.closes.load(Ordering::SeqCst), 1);
}) }
