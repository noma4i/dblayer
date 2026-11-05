use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use super::client::digest_of;
use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::protocol_tests;
use super::state;
use super::vectors_tests::*;
use super::{CredentialSlot, EncryptedClient, EncryptedConfig};
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::test_wait::{join_within, recv_within};
use crate::transport::TransportError;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    BlockInGet,
}

struct Slot {
    memory: MemoryCredentials,
    client: Mutex<Weak<EncryptedClient>>,
    mode: Mode,
    fired: AtomicBool,
    sets: AtomicUsize,
    entered: Mutex<Sender<()>>,
    release: Mutex<Receiver<()>>,
}

impl CredentialSlot for Slot {
    fn get(&self) -> Result<Option<String>, TransportError> {
        let value = self.memory.get()?;
        match self.mode {
            Mode::BlockInGet if !self.fired.swap(true, Ordering::SeqCst) => {
                self.entered.lock().unwrap().send(()).unwrap();
                recv_within(&self.release.lock().unwrap());
            }
            _ => {}
        }
        Ok(value)
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.memory.set(credential)?;
        Ok(())
    }

    fn clear(&self) -> Result<(), TransportError> {
        self.memory.clear()
    }
}

struct Rig {
    client: Arc<EncryptedClient>,
    slot: Arc<Slot>,
    db: Db,
    entered: Receiver<()>,
    release: Sender<()>,
}

fn rig(mode: Mode) -> Rig {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let (entered_to, entered) = channel();
    let (release, release_from) = channel();
    let slot = Arc::new(Slot { memory: MemoryCredentials::default(), client: Mutex::new(Weak::new()), mode, fired: AtomicBool::new(false), sets: AtomicUsize::new(0), entered: Mutex::new(entered_to), release: Mutex::new(release_from) });
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..protocol_tests::config() };
    let client = Arc::new(EncryptedClient::new(&config, FakeServer::new(clock.clone()), slot.clone(), db.clone(), clock).unwrap());
    *slot.client.lock().unwrap() = Arc::downgrade(&client);
    Rig { client, slot, db, entered, release }
}

fn session_digest(db: &Db) -> Option<String> {
    let inner = db.shared.store.lock();
    state::session(&inner.connection).unwrap().map(|session| session.credential_digest)
}

fn load_on_thread(client: &Arc<EncryptedClient>) -> std::thread::JoinHandle<Result<String, TransportError>> {
    let client = client.clone();
    std::thread::spawn(move || client.credential(&client.admit("test credential").unwrap()))
}

#[test]
fn concurrent_credential_loads_mint_one_credential() {
    let rig = rig(Mode::BlockInGet);
    let first = load_on_thread(&rig.client);
    recv_within(&rig.entered);
    let second = load_on_thread(&rig.client);
    rig.release.send(()).unwrap();

    let (first, second) = (join_within(first).unwrap(), join_within(second).unwrap());

    assert_eq!(first, second);
    assert_eq!(rig.slot.sets.load(Ordering::SeqCst), 1);
    assert_eq!(session_digest(&rig.db), Some(digest_of(&first)));
}

#[test]
fn credential_callback_asking_for_the_credential_is_refused() {
    struct Asks(Mutex<Weak<EncryptedClient>>, Mutex<Option<Result<String, TransportError>>>);
    impl CredentialSlot for Asks {
        fn get(&self) -> Result<Option<String>, TransportError> {
            let client = self.0.lock().unwrap().upgrade().unwrap();
            *self.1.lock().unwrap() = Some(client.credential(&client.admit("test credential").unwrap()));
            Ok(None)
        }
        fn set(&self, _: &str) -> Result<(), TransportError> { Ok(()) }
        fn clear(&self) -> Result<(), TransportError> { Ok(()) }
    }
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let slot = Arc::new(Asks(Mutex::new(Weak::new()), Mutex::new(None)));
    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..protocol_tests::config() };
    let client = Arc::new(EncryptedClient::new(&config, FakeServer::new(clock.clone()), slot.clone(), Db::open_in_memory(Vec::new(), None).unwrap(), clock).unwrap());
    *slot.0.lock().unwrap() = Arc::downgrade(&client);

    join_within(load_on_thread(&client)).unwrap();

    assert!(matches!(slot.1.lock().unwrap().take(), Some(Err(TransportError::Network(_)))));
}
