use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};

use super::credential::Secret;
use super::exchange::digest_of;
use super::fake_server_tests::MemoryCredentials;
use super::state;
use super::CredentialSlot;
use crate::store::Store;
use crate::test_wait::{join_within, recv_within};
use crate::transport::TransportError;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    BlockInGet,
}

struct Slot {
    memory: MemoryCredentials,
    client: Mutex<Weak<Held>>,
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

struct Held { secret: Arc<Secret>, db: Arc<Store> }

impl Held {
    fn new(slot: Arc<dyn CredentialSlot>, db: Arc<Store>) -> Arc<Self> { Arc::new(Self { secret: Arc::new(Secret::new(slot)), db }) }

    fn credential(&self) -> Result<String, TransportError> { self.secret.read(&self.db) }

    fn erase_credential(&self) -> Result<(), TransportError> { self.secret.erase() }
}

struct Rig {
    client: Arc<Held>,
    slot: Arc<Slot>,
    db: Arc<Store>,
    entered: Receiver<()>,
    release: Sender<()>,
}

fn rig(mode: Mode) -> Rig {
    let (entered_to, entered) = channel();
    let (release, release_from) = channel();
    let slot = Arc::new(Slot { memory: MemoryCredentials::default(), client: Mutex::new(Weak::new()), mode, fired: AtomicBool::new(false), sets: AtomicUsize::new(0), entered: Mutex::new(entered_to), release: Mutex::new(release_from) });
    let db = Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap();
    let client = Held::new(slot.clone(), db.clone());
    *slot.client.lock().unwrap() = Arc::downgrade(&client);
    Rig { client, slot, db, entered, release }
}

fn session_digest(db: &Store) -> Option<String> {
    let inner = db.lock();
    state::session(&inner.connection).unwrap().map(|session| session.credential_digest)
}

fn load_on_thread(client: &Arc<Held>) -> std::thread::JoinHandle<Result<String, TransportError>> {
    let client = client.clone();
    std::thread::spawn(move || client.credential())
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
    struct Asks(Mutex<Weak<Held>>, Mutex<Option<Result<String, TransportError>>>);
    impl CredentialSlot for Asks {
        fn get(&self) -> Result<Option<String>, TransportError> {
            let client = self.0.lock().unwrap().upgrade().unwrap();
            *self.1.lock().unwrap() = Some(client.credential());
            Ok(None)
        }
        fn set(&self, _: &str) -> Result<(), TransportError> { Ok(()) }
        fn clear(&self) -> Result<(), TransportError> { Ok(()) }
    }
    let slot = Arc::new(Asks(Mutex::new(Weak::new()), Mutex::new(None)));
    let client = Held::new(slot.clone(), Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap());
    *slot.0.lock().unwrap() = Arc::downgrade(&client);

    join_within(load_on_thread(&client)).unwrap();

    assert!(matches!(slot.1.lock().unwrap().take(), Some(Err(TransportError::Network(_)))));
}

#[test]
fn sess_prop_credential_in_store() {
    struct Counting { memory: MemoryCredentials, gets: AtomicUsize, clears: AtomicUsize }
    impl CredentialSlot for Counting {
        fn get(&self) -> Result<Option<String>, TransportError> { self.gets.fetch_add(1, Ordering::SeqCst); self.memory.get() }
        fn set(&self, credential: &str) -> Result<(), TransportError> { self.memory.set(credential) }
        fn clear(&self) -> Result<(), TransportError> { self.clears.fetch_add(1, Ordering::SeqCst); self.memory.clear() }
    }
    let slot = Arc::new(Counting { memory: MemoryCredentials::default(), gets: AtomicUsize::new(0), clears: AtomicUsize::new(0) });
    let client = Held::new(slot.clone(), Store::open_models(None, Vec::new(), None).map(std::sync::Arc::new).unwrap());

    let first = client.credential().unwrap();
    let second = client.credential().unwrap();
    assert_eq!((first.as_str(), slot.gets.load(Ordering::SeqCst)), (second.as_str(), 2), "every request reads the secret from the store");

    client.erase_credential().unwrap();
    assert_eq!(slot.clears.load(Ordering::SeqCst), 1, "the erase reaches the store once");
    assert_eq!(slot.memory.get().unwrap(), None);
}
