
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::owners::life::LifeReply;
use crate::runtime::{ManualClock, Outcome};
use crate::store::Store;
use crate::test_wait::within;
use crate::transport::encrypted::tests::exchange_tests::Client;
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::{CredentialSlot, TransportError};

const OTP: &str = "/api/auth/otp";

#[derive(Default)]
struct HoldSlot {
    memory: MemoryCredentials,
    reads: AtomicUsize,
    hold_at: AtomicUsize,
    released: crate::test_wait::Flag,
    held: crate::test_wait::Flag,
}

impl CredentialSlot for HoldSlot {
    fn get(&self) -> Result<Option<String>, TransportError> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if self.hold_at.load(Ordering::SeqCst) == read {
            self.held.set();
            self.released.wait_blocking("the held read was never released");
        }
        self.memory.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> { self.memory.set(credential) }

    fn clear(&self) -> Result<(), TransportError> { self.memory.clear() }
}

async fn mutate(life: &crate::owners::life::Life, req_id: u64) {
    let Outcome::Value(LifeReply::Mutated(_)) = within(life.mutate_answered(None, req_id, OTP.into(), "{}".into(), 0).wait()).await else { panic!("the author has an outcome") };
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_cut_before_transport() {
    for closing in [false, true] {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        let server = FakeServer::new(clock.clone());
        let slot = Arc::new(HoldSlot::default());
        let db = Store::open_models(None, Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
        let client = Client::new(&crate::transport::encrypted::tests::protocol_tests::config(), server.clone(), slot.clone(), db, clock, Arc::new(crate::transport::MockTransport::new())).unwrap();
        let life = client.start_owners();
        within(life.set_online(true, crate::ffi::next_stamp()).wait()).await;
        client.session_call("social", json!({})).await.unwrap();
        let before = slot.reads.load(Ordering::SeqCst);
        mutate(&life, 1 << 41).await;
        let per_request = slot.reads.load(Ordering::SeqCst) - before;
        let sent = server.seen().len();
        slot.hold_at.store(slot.reads.load(Ordering::SeqCst) + per_request, Ordering::SeqCst);
        drop(life.mutate_answered(None, (1 << 41) + 1, OTP.into(), "{}".into(), 0));
        slot.held.wait("the request task never read the secret").await;
        if closing { within(life.close().wait()).await; } else { within(life.session_reset().wait()).await; }
        assert!(life.published().state != crate::owners::contract::PublishedState::Session, "the epoch never ended");
        slot.released.set();
        life.barrier();
        assert_eq!(server.seen().len(), sent, "closing {closing}: the transport was called after the cut grew");
    }
}
