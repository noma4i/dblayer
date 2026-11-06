
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use crate::pacing::ManualClock;
use crate::store::Store;
use crate::test_wait::{within, yield_until};
use crate::transport::encrypted::exchange_tests::Client;
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::{CredentialSlot, TransportError};

const OTP: &str = "/api/auth/otp";

#[derive(Default)]
struct HoldSlot {
    memory: MemoryCredentials,
    reads: AtomicUsize,
    hold_at: AtomicUsize,
    released: AtomicBool,
}

impl CredentialSlot for HoldSlot {
    fn get(&self) -> Result<Option<String>, TransportError> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if self.hold_at.load(Ordering::SeqCst) == read {
            let limit = std::time::Instant::now() + Duration::from_secs(10);
            while !self.released.load(Ordering::SeqCst) {
                assert!(std::time::Instant::now() < limit, "the held read was never released");
                std::thread::yield_now();
            }
        }
        self.memory.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> { self.memory.set(credential) }

    fn clear(&self) -> Result<(), TransportError> { self.memory.clear() }
}

async fn mutate(life: &crate::owners::life::Life, req_id: u64) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = Mutex::new(Some(sender));
    life.on_mutate_answer(1 << 32, move |_, outcome| {
        if let Some(sender) = sender.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() { let _ = sender.send(outcome); }
    });
    life.mutate(1 << 32, None, req_id, OTP.into(), "{}".into(), 0);
    within(receiver).await.expect("the author has an outcome");
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_cut_before_transport() {
    for closing in [false, true] {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        let server = FakeServer::new(clock.clone());
        let slot = Arc::new(HoldSlot::default());
        let db = Store::open_models(None, Vec::new(), None).map(Arc::new).unwrap();
        let client = Client::new(&crate::transport::encrypted::protocol_tests::config(), server.clone(), slot.clone(), db, clock, Arc::new(crate::transport::MockTransport::new())).unwrap();
        let life = client.start_owners();
        within(life.set_online(true).wait()).await;
        client.session_call("social", json!({})).await.unwrap();
        let before = slot.reads.load(Ordering::SeqCst);
        mutate(&life, 1 << 41).await;
        let per_request = slot.reads.load(Ordering::SeqCst) - before;
        let sent = server.seen().len();
        slot.hold_at.store(slot.reads.load(Ordering::SeqCst) + per_request, Ordering::SeqCst);
        life.on_mutate_answer(1 << 32, |_, _| {});
        life.mutate(1 << 32, None, (1 << 41) + 1, OTP.into(), "{}".into(), 0);
        yield_until("the request task never read the secret", || slot.reads.load(Ordering::SeqCst) == slot.hold_at.load(Ordering::SeqCst)).await;
        if closing { drop(life.close()); } else { drop(life.session_reset()); }
        yield_until("the epoch never ended", || life.published().state != crate::owners::contract::PublishedState::Session).await;
        slot.released.store(true, Ordering::SeqCst);
        assert!(life.barrier(Duration::from_secs(10)), "the request task never ended");
        assert_eq!(server.seen().len(), sent, "closing {closing}: the transport was called after the cut grew");
    }
}
