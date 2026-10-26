use super::*;

fn config() -> Config { Config { path: None, data_version: None, cache_rows: 8 } }
fn options() -> OpenOptions { OpenOptions { operations: "[]".into(), uploads: None, power_saving: false, low_ram: false, preload: true } }
async fn open(slot: &OpenDatabase) -> Result<Arc<Database>, DbError> {
    slot.open(config(), "[]".into(), options(), None, None, None, None, None).await
}

#[tokio::test]
async fn a_second_open_is_refused_even_with_equal_arguments_until_the_database_is_closed() {
    let slot = OpenDatabase::new();
    let first = open(&slot).await.unwrap();
    for _ in 0..2 {
        match open(&slot).await {
            Err(DbError::Invalid { detail }) => assert_eq!(detail, "database is already open; open it once at app start"),
            other => panic!("second open gave {:?}", other.map(|_| ())),
        }
    }
    first.shutdown();
    let second = open(&slot).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(open(&slot).await.is_err());
}

#[tokio::test]
async fn an_open_that_fails_leaves_the_slot_free() {
    let slot = OpenDatabase::new();
    assert!(slot.open(config(), "invalid".into(), options(), None, None, None, None, None).await.is_err());
    assert!(open(&slot).await.is_ok());
}

#[tokio::test]
async fn open_carries_the_upload_declaration_and_frees_the_slot_when_it_is_refused() {
    use super::super::database_tests::Plain;
    use super::super::upload_wire::{WireUploadOperation, WireUploadOperations};
    let blob = |name: &str| WireUploadOperations { create_blob: Some(WireUploadOperation { name: name.into(), root_path: None }), multipart: None };
    let slot = OpenDatabase::new();
    for (name, accepted) in [("Missing", false), ("CreateBlob", true)] {
        let options = OpenOptions { operations: r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }"}]"#.into(), uploads: Some(blob(name)), power_saving: false, low_ram: false, preload: true };
        let opened = slot.open(config(), "[]".into(), options, Some(Arc::new(Plain)), None, None, None, None).await;
        match opened {
            Ok(_) => assert!(accepted, "{name} was opened"),
            Err(error) => assert!(!accepted && error.to_string().ends_with("invalid upload: upload operation Missing is not registered"), "{name}: {error}"),
        }
    }
}

struct CountingFeed(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl Transport for CountingFeed {
    async fn open_cable(&self, _: Arc<super::super::CableLease>) -> Result<(), DbError> { unreachable!() }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) { unreachable!() }
    async fn query(&self, _: super::super::TransportRequest) -> Result<String, DbError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(r#"{"data":{"feed":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#.into())
    }
    async fn mutate(&self, _: super::super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::super::TransportRequest, _: Arc<super::super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::super::ByteProgress>) -> Result<super::super::PutReply, DbError> { unreachable!() }
}

#[tokio::test]
async fn the_power_and_memory_facts_given_to_open_hold_the_first_preload() {
    use crate::sync::rig::{moments_spec, users_spec};
    use crate::test_wait::yield_until;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut moments = moments_spec();
    moments.relations.iter_mut().find(|relation| relation.name == "feed").unwrap().preload = Some(Default::default());
    let models = serde_json::to_string(&vec![users_spec(), moments]).unwrap();
    let operations = r#"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id } pageInfo { hasNextPage endCursor } } }"}]"#;
    for (power_saving, low_ram, loads_at_open) in [(true, false, false), (false, true, false), (false, false, true)] {
        let requests = Arc::new(AtomicUsize::new(0));
        let slot = OpenDatabase::new();
        let options = OpenOptions { operations: operations.into(), uploads: None, power_saving, low_ram, preload: true };
        let database = slot.open(config(), models.clone(), options, Some(Arc::new(CountingFeed(requests.clone()))), None, None, None, None).await.unwrap();
        let sync = database.sync().unwrap();
        if loads_at_open {
            yield_until("the preload at open", || requests.load(Ordering::SeqCst) == 1).await;
            database.shutdown();
            continue;
        }
        assert!(requests.load(Ordering::SeqCst) == 0 && !sync.preload_running(), "power_saving={power_saving} low_ram={low_ram} started a preload at open");
        let _entered = super::super::database::executor().unwrap().enter();
        sync.set_power_saving(true);
        sync.set_power_saving(false);
        if low_ram {
            assert!(requests.load(Ordering::SeqCst) == 0 && !sync.preload_running(), "a low-RAM device loaded after power saving ended");
        } else {
            yield_until("the preload after power saving ends", || requests.load(Ordering::SeqCst) == 1).await;
        }
        database.shutdown();
    }
}

#[tokio::test]
async fn the_preload_waits_until_the_listener_has_taken_its_first_snapshot() {
    use crate::sync::rig::{moments_spec, users_spec};
    use crate::test_wait::{recv_or_closed, yield_until};
    use super::super::{LiveChange, QueryListener};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Taking { entered: std::sync::mpsc::Sender<()>, release: std::sync::Mutex<std::sync::mpsc::Receiver<()>> }
    impl QueryListener for Taking {
        fn changed(&self, _: LiveChange) {
            let _ = self.entered.send(());
            let _ = recv_or_closed(&self.release.lock().unwrap());
        }
    }
    let mut moments = moments_spec();
    moments.relations.iter_mut().find(|relation| relation.name == "feed").unwrap().preload = Some(Default::default());
    let models = serde_json::to_string(&vec![users_spec(), moments]).unwrap();
    let operations = r#"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id } pageInfo { hasNextPage endCursor } } }"}]"#;
    let requests = Arc::new(AtomicUsize::new(0));
    let options = OpenOptions { operations: operations.into(), uploads: None, power_saving: true, low_ram: false, preload: true };
    let database = OpenDatabase::new().open(config(), models, options, Some(Arc::new(CountingFeed(requests.clone()))), None, None, None, None).await.unwrap();
    let (entered, inside) = std::sync::mpsc::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    let live = database.live_query(r#"{"model":"users"}"#.into(), Arc::new(Taking { entered, release: std::sync::Mutex::new(waiting) }));
    inside.recv_timeout(std::time::Duration::from_secs(5)).expect("the listener takes its first snapshot");
    let sync = database.sync().unwrap();
    let _entered = super::super::database::executor().unwrap().enter();
    sync.set_power_saving(false);
    for _ in 0..200 { tokio::task::yield_now().await; }
    assert!(requests.load(Ordering::SeqCst) == 0 && !sync.preload_running(), "the preload started while the first snapshot was still being taken");
    drop(release);
    yield_until("the preload after the first snapshot", || requests.load(Ordering::SeqCst) == 1).await;
    live.cancel();
    database.shutdown();
}

#[tokio::test]
async fn preload_off_sends_no_request_on_any_event_and_on_loads() {
    use crate::sync::rig::{moments_spec, users_spec};
    use crate::test_wait::yield_until;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut moments = moments_spec();
    moments.relations.iter_mut().find(|relation| relation.name == "feed").unwrap().preload = Some(Default::default());
    let models = serde_json::to_string(&vec![users_spec(), moments]).unwrap();
    let operations = r#"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id } pageInfo { hasNextPage endCursor } } }"}]"#;
    for (clocked, preload) in [(false, false), (true, false), (false, true), (true, true)] {
        let requests = Arc::new(AtomicUsize::new(0));
        let slot = OpenDatabase::new();
        let transport: Arc<dyn Transport> = Arc::new(CountingFeed(requests.clone()));
        let database = if clocked {
            let database = slot.open_with_clock(config(), models.clone(), Some(transport), None, super::super::TestClock::new(1_000_000), preload).await.unwrap();
            database.register_operations(operations.into()).unwrap();
            database
        } else {
            let options = OpenOptions { operations: operations.into(), uploads: None, power_saving: false, low_ram: false, preload };
            slot.open(config(), models.clone(), options, Some(transport), None, None, None, None).await.unwrap()
        };
        let sync = database.sync().unwrap();
        if preload {
            yield_until("the preload", || requests.load(Ordering::SeqCst) == 1).await;
        } else {
            let _entered = super::super::database::executor().unwrap().enter();
            sync.set_online(false);
            sync.set_online(true);
            sync.set_power_saving(true);
            sync.set_power_saving(false);
            sync.check_session();
            sync.inner.preload_on_event();
            assert!(requests.load(Ordering::SeqCst) == 0 && !sync.preload_running(), "clocked={clocked} preload off started a request");
        }
        database.shutdown();
    }
}
