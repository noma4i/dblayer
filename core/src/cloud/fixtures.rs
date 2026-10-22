use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use crate::db::locked;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::schema::{FieldSpec, MergeRule, ModelSpec, Storage};
use crate::transport::Connectivity;
use crate::value::ScalarKind;

use super::{Cloud, CloudAccount, CloudChange, CloudError, CloudIo, CloudPage};

#[derive(Default)]
struct Inner {
    log: Vec<CloudChange>,
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

    pub fn log(&self) -> Vec<CloudChange> {
        locked(&self.inner).log.clone()
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
        Ok(CloudPage { changes: inner.log[from..to].to_vec(), cursor: (to as u64).to_le_bytes().to_vec(), more: to < inner.log.len() })
    }

    async fn push(&self, changes: Vec<CloudChange>) -> Result<(), CloudError> {
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
                    inner.log.extend(changes);
                }
                Err(error)
            }
            None => {
                inner.log.extend(changes);
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

pub struct Device {
    pub db: Db,
    pub cloud: Cloud,
    pub clock: ManualClock,
    pub online: Arc<Connectivity>,
}

pub fn device_with(db: Db, fake: &Arc<FakeCloud>, start_ms: u64) -> Device {
    let clock = ManualClock::new(Duration::from_millis(start_ms));
    db.set_clock(Arc::new(clock.clone()));
    let online = Connectivity::new(true);
    let cloud = Cloud::attach(&db, fake.clone(), Arc::new(clock.clone()), online.clone());
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
