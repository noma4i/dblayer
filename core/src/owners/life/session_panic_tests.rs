use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use crate::store::Store;
use crate::owners::contract::ErrorClass;
use crate::owners::session::{AuthorOutcome, SessionError};
use crate::pacing::ManualClock;
use crate::test_wait::{within, yield_until};
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::{EncryptedClient, HttpExchange, HttpReply, NetworkPurpose, TransportError};

struct PanicHttp { server: Arc<FakeServer>, panic: AtomicBool }

#[async_trait]
impl HttpExchange for PanicHttp {
    async fn post(&self, path: &str, body: String, purpose: NetworkPurpose) -> Result<HttpReply, TransportError> {
        assert!(!self.panic.load(Ordering::SeqCst), "HTTP exchange failed");
        self.server.post(path, body, purpose).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_calling_http_panic() {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let http = Arc::new(PanicHttp { server: FakeServer::new(clock.clone()), panic: AtomicBool::new(false) });
    let db = Db::open_in_memory(vec![], None).unwrap();
    let client = Arc::new(EncryptedClient::new(&crate::transport::encrypted::protocol_tests::config(), http.clone(), Arc::new(MemoryCredentials::default()), db.clone(), clock).unwrap());
    let life = client.start_owners_in_tests(&db);
    within(client.session_call_in_tests("social", json!({}))).await.unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = Mutex::new(Some(sender));
    life.on_mutate_answer(7, move |_, outcome| { if let Some(sender) = sender.lock().unwrap().take() { let _ = sender.send(outcome); } });
    http.panic.store(true, Ordering::SeqCst);
    life.mutate(7, None, 11, "/api/auth/otp".into(), "{}".into(), 0);
    let outcome = within(receiver).await.unwrap();
    assert!(matches!(outcome, AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Http { status: 0 } }), "{outcome:?}");
    yield_until("Lifecycle did not close after HTTP panic", || life.published().closed).await;
}
