use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use crate::db::locked;
use crate::db::Db;
use crate::pacing::{Clock, ManualClock};
use crate::schema::{FieldSpec, MergeRule, ModelSpec, Storage};
use crate::transport::Connectivity;
use crate::value::ScalarKind;

use super::{Cloud, CloudAccount, CloudBlob, CloudChange, CloudError, CloudIo, CloudPage};

#[derive(Default)]
struct Inner {
    log: Vec<CloudBlob>,
    accounts: u32,
    pulls: u32,
    pushes: u32,
    push_failures: VecDeque<(CloudError, bool)>,
    account: Option<CloudAccount>,
    hook: Option<Arc<dyn Fn() + Send + Sync>>,
    page: Option<usize>,
}

#[derive(Default)]
pub struct FakeCloud {
    inner: Mutex<Inner>,
}

impl FakeCloud {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn calls(&self) -> u32 {
        let inner = locked(&self.inner);
        inner.accounts + inner.pulls + inner.pushes
    }

    pub fn pushes(&self) -> u32 {
        locked(&self.inner).pushes
    }

    pub fn blobs(&self) -> Vec<CloudBlob> {
        locked(&self.inner).log.clone()
    }

    pub fn log(&self) -> Vec<CloudChange> {
        self.blobs().iter().map(|blob| CloudChange::from_blob(&blob.bytes).unwrap()).collect()
    }

    pub fn fail_pushes(&self, errors: Vec<(CloudError, bool)>) {
        locked(&self.inner).push_failures.extend(errors);
    }

    pub fn set_account(&self, account: CloudAccount) {
        locked(&self.inner).account = Some(account);
    }

    pub fn before_push(&self, hook: impl Fn() + Send + Sync + 'static) {
        locked(&self.inner).hook = Some(Arc::new(hook));
    }

    pub fn set_page(&self, size: usize) {
        locked(&self.inner).page = Some(size);
    }
}

#[async_trait]
impl CloudIo for FakeCloud {
    async fn account(&self) -> Result<CloudAccount, CloudError> {
        let mut inner = locked(&self.inner);
        inner.accounts += 1;
        Ok(inner.account.unwrap_or(CloudAccount::Available))
    }

    async fn pull(&self, cursor: Option<Vec<u8>>, limit: u32) -> Result<CloudPage, CloudError> {
        let mut inner = locked(&self.inner);
        inner.pulls += 1;
        let from = cursor.and_then(|bytes| <[u8; 8]>::try_from(bytes).ok()).map_or(0, |bytes| u64::from_le_bytes(bytes) as usize);
        let size = inner.page.unwrap_or(limit as usize).min(limit as usize);
        let to = (from + size).min(inner.log.len());
        Ok(CloudPage { blobs: inner.log[from..to].to_vec(), cursor: (to as u64).to_le_bytes().to_vec(), more: to < inner.log.len() })
    }

    async fn push(&self, blobs: Vec<CloudBlob>) -> Result<(), CloudError> {
        let hook = {
            let mut inner = locked(&self.inner);
            inner.pushes += 1;
            inner.hook.clone()
        };
        if let Some(hook) = hook {
            hook();
        }
        let mut inner = locked(&self.inner);
        match inner.push_failures.pop_front() {
            Some((error, applied)) => {
                if applied {
                    inner.log.extend(blobs);
                }
                Err(error)
            }
            None => {
                inner.log.extend(blobs);
                Ok(())
            }
        }
    }
}

pub async fn until(condition: impl Fn() -> bool) {
    for _ in 0..2_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the condition was not reached");
}

pub fn obj(value: Value) -> Map<String, Value> {
    value.as_object().expect("object literal").clone()
}

pub fn notes_spec(cloud: &[&str]) -> ModelSpec {
    let mut rank = FieldSpec::scalar("rank", ScalarKind::Int).optional();
    rank.merge = Some(MergeRule::Monotonic);
    let mut spec = ModelSpec::new(
        "notes",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            FieldSpec::scalar("body", ScalarKind::Str).optional(),
            FieldSpec::scalar("secret", ScalarKind::Str).optional(),
            rank,
            FieldSpec::array("tags").optional(),
        ],
    );
    spec.storage = Storage::Local;
    spec.cloud = cloud.iter().map(|name| name.to_string()).collect();
    spec
}

pub fn profiles_spec() -> ModelSpec {
    let mut draft = FieldSpec::scalar("draft", ScalarKind::Str).optional();
    draft.merge = Some(MergeRule::Local);
    let mut spec = ModelSpec::new("profiles", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional(), draft]);
    spec.cloud = vec!["draft".to_string()];
    spec
}

pub fn specs() -> Vec<ModelSpec> {
    vec![notes_spec(&["title", "body", "rank", "tags"]), profiles_spec()]
}

#[derive(Default)]
struct Sleeps {
    started: usize,
    polls: usize,
    active: usize,
}

pub struct WatchedClock {
    inner: ManualClock,
    sleeps: Arc<Mutex<Sleeps>>,
    changed: Arc<tokio::sync::Notify>,
}

struct Active(Arc<Mutex<Sleeps>>);

impl Drop for Active {
    fn drop(&mut self) {
        locked(&self.0).active -= 1;
    }
}

impl WatchedClock {
    pub fn new(start: Duration) -> Self {
        Self { inner: ManualClock::new(start), sleeps: Arc::default(), changed: Arc::default() }
    }

    pub fn advance(&self, by: Duration) {
        self.inner.advance(by);
    }

    pub fn started(&self) -> usize {
        locked(&self.sleeps).started
    }

    pub fn polls(&self) -> usize {
        locked(&self.sleeps).polls
    }

    pub fn active(&self) -> usize {
        locked(&self.sleeps).active
    }

    pub async fn await_started(&self, count: usize) {
        self.until(|sleeps| sleeps.started >= count).await;
    }

    pub async fn await_polls(&self, count: usize) {
        self.until(|sleeps| sleeps.polls >= count).await;
    }

    async fn until(&self, ready: impl Fn(&Sleeps) -> bool) {
        let wait = async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if ready(&locked(&self.sleeps)) {
                    return;
                }
                notified.await;
            }
        };
        tokio::time::timeout(Duration::from_secs(60), wait).await.expect("the clock never reached the awaited sleeps");
    }
}

impl Clock for WatchedClock {
    fn now(&self) -> Duration {
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        {
            let mut sleeps = locked(&self.sleeps);
            sleeps.started += 1;
            sleeps.active += 1;
        }
        self.changed.notify_waiters();
        let guard = Active(self.sleeps.clone());
        let mut inner = self.inner.sleep(duration);
        let changed = self.changed.clone();
        Box::pin(std::future::poll_fn(move |context| {
            locked(&guard.0).polls += 1;
            changed.notify_waiters();
            inner.as_mut().poll(context)
        }))
    }
}

pub struct Device {
    pub db: Db,
    pub cloud: Cloud,
    pub clock: Arc<WatchedClock>,
    pub online: Arc<Connectivity>,
}

pub fn device_with(db: Db, fake: &Arc<FakeCloud>, start_ms: u64) -> Device {
    let clock = Arc::new(WatchedClock::new(Duration::from_millis(start_ms)));
    db.set_clock(clock.clone());
    let online = db.connectivity();
    let cloud = Cloud::attach(&db, fake.clone(), clock.clone());
    Device { db, cloud, clock, online }
}

pub fn device(fake: &Arc<FakeCloud>, start_ms: u64) -> Device {
    device_with(Db::open_in_memory(specs(), None).unwrap(), fake, start_ms)
}

pub fn title_of(device: &Device, id: &str) -> Option<String> {
    let id = crate::identity::RowId::new(id).unwrap();
    device.db.find("notes", &id).unwrap().and_then(|row| row.text("title").map(str::to_string))
}

pub fn note(device: &Device, values: Value) -> String {
    device.db.create("notes", obj(values)).unwrap().id.to_string()
}

pub fn edit(device: &Device, id: &str, values: Value) {
    device.db.update("notes", &crate::identity::RowId::new(id).unwrap(), obj(values)).unwrap();
}

pub fn field_of(device: &Device, id: &str, field: &str) -> Value {
    let id = crate::identity::RowId::new(id).unwrap();
    device.db.find("notes", &id).unwrap().and_then(|row| row.get(field).cloned()).unwrap_or(json!(null))
}
