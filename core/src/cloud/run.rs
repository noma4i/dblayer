use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};

use crate::db::dispatch::OrderedEffects;
use crate::db::{locked, Db, DbError};
use crate::pacing::{Backoff, Clock};
use crate::transport::Connectivity;

use super::machine::{Phase, PhaseEvent};
use super::{CloudAccount, CloudChange, CloudError, CloudIo, CloudPage, CloudState, MergeSource, Hlc};

const BATCH: u32 = 100;
const RETRY: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(300)).limited(40);

#[derive(Clone)]
pub struct Cloud {
    inner: Arc<Inner>,
    task: Arc<Runner>,
}

struct Runner {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Runner {
    fn stop(&self) {
        let task = locked(&self.task).take();
        if let Some(task) = task {
            crate::db::lock::effect(|| task.abort());
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Inner {
    db: Db,
    io: Arc<dyn CloudIo>,
    clock: Arc<dyn Clock>,
    online: Arc<Connectivity>,
    wake: Notify,
    state: Arc<watch::Sender<CloudState>>,
    phase: Mutex<Phase>,
    effects: OrderedEffects,
    pass: tokio::sync::Mutex<()>,
}

impl Drop for Inner {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("cloud::Inner::drop"); }
}

struct Failure {
    error: CloudError,
    retry: Option<(Duration, bool)>,
}

fn decode(page: &CloudPage) -> Result<Vec<CloudChange>, CloudError> {
    page.blobs.iter().map(|blob| CloudChange::from_blob(&blob.bytes)).collect()
}

impl From<DbError> for CloudError {
    fn from(error: DbError) -> Self {
        CloudError::Rejected { detail: error.to_string() }
    }
}

impl Cloud {
    #[cfg(test)]
    pub(crate) fn connectivity(&self) -> Arc<Connectivity> {
        self.inner.online.clone()
    }

    pub fn attach(db: &Db, io: Arc<dyn CloudIo>, clock: Arc<dyn Clock>) -> Cloud {
        let online = db.connectivity();
        let state = Arc::new(watch::channel(CloudState::Idle).0);
        Cloud { inner: Arc::new(Inner { db: db.clone(), io, clock, online, wake: Notify::new(), state, phase: Mutex::new(Phase::new()), effects: OrderedEffects::default(), pass: tokio::sync::Mutex::new(()) }), task: Arc::new(Runner { task: Mutex::new(None) }) }
    }

    #[cfg(test)]
    pub(crate) fn is_running(&self) -> bool {
        locked(&self.task.task).as_ref().is_some_and(|task| !task.is_finished())
    }

    pub fn state(&self) -> CloudState {
        self.inner.state.borrow().clone()
    }

    pub fn state_changes(&self) -> watch::Receiver<CloudState> {
        self.inner.state.subscribe()
    }

    pub fn remote_changed(&self) {
        crate::db::lock::effect(|| self.inner.wake.notify_one());
    }

    pub async fn resolve_initial_merge(&self, source: MergeSource) -> Result<(), CloudError> {
        let _turn = self.inner.pass.lock().await;
        let generation = self.inner.db.generation();
        let rows = self.inner.db.cloud_unstamped(generation)?;
        if source == MergeSource::UseLocal {
            let mut cursor = None;
            loop {
                crate::db::lock::assert_no_library_lock("CloudIo::pull");
                let page = self.inner.io.pull(cursor, BATCH).await?;
                for change in &decode(&page)? {
                    self.inner.db.cloud_observe(generation, &Hlc::new(change.clock_ms, change.clock_count, &change.replica))?;
                }
                if !page.more {
                    break;
                }
                cursor = Some(page.cursor);
            }
        }
        self.inner.db.cloud_stamp_rows(generation, &rows, source)?;
        crate::db::lock::effect(|| self.inner.wake.notify_one());
        Ok(())
    }

    pub async fn sync_now(&self) -> Result<(), CloudError> {
        self.inner.pass(true, None).await.map_err(|failure| failure.error)
    }

    pub fn start(&self) {
        let mut task = locked(&self.task.task);
        if task.is_some() {
            return;
        }
        let inner = self.inner.clone();
        *task = Some(tokio::spawn(async move { inner.run().await }));
    }

    pub fn stop(&self) {
        self.task.stop();
    }
}

impl Inner {
    async fn run(&self) {
        let mut local = self.db.cloud_changes();
        local.mark_unchanged();
        let mut pull = true;
        let mut first = true;
        loop {
            if !first {
                tokio::select! {
                    changed = local.changed() => { if changed.is_err() { return; } pull = false; }
                    () = self.wake.notified() => pull = true,
                }
            }
            first = false;
            self.finish_cycle(pull).await;
            pull = false;
        }
    }

    async fn finish_cycle(&self, pull: bool) {
        let mut attempt = 0u32;
        loop {
            self.online.online().await;
            let Err(failure) = self.pass(pull, Some(attempt)).await else { return };
            let Some((delay, spends)) = failure.retry else { return };
            attempt += u32::from(spends);
            self.online.wait(self.clock.as_ref(), delay).await;
        }
    }

    fn retry_of(error: &CloudError, attempt: u32) -> Option<(Duration, bool)> {
        let delay = RETRY.delay(attempt);
        match error {
            CloudError::Offline { .. } => Some((delay, false)),
            CloudError::Unavailable { .. } if RETRY.allows(attempt + 1) => Some((delay, true)),
            CloudError::Throttled { retry_after_ms } if RETRY.allows(attempt + 1) => Some((delay.max(Duration::from_millis(*retry_after_ms)), true)),
            _ => None,
        }
    }

    fn publish(&self, event: PhaseEvent) {
        let state = self.state.clone();
        self.effects.under(&self.phase, |phase, effects| {
            let next = phase.handle(&event);
            effects.push(move || { crate::db::lock::effect(|| state.send_replace(next)); });
        });
    }

    async fn pass(&self, pull: bool, attempt: Option<u32>) -> Result<(), Failure> {
        let _turn = self.pass.lock().await;
        let Err(error) = self.cycle(pull).await else { return Ok(()) };
        let retry = attempt.and_then(|attempt| Self::retry_of(&error, attempt));
        self.publish(match (&retry, attempt, &error) {
            (Some(_), _, _) => PhaseEvent::Wait,
            (None, Some(_), CloudError::Unavailable { .. } | CloudError::Throttled { .. }) => PhaseEvent::GaveUp(error.clone()),
            _ => PhaseEvent::Stop(error.clone()),
        });
        Err(Failure { error, retry })
    }

    async fn cycle(&self, pull: bool) -> Result<(), CloudError> {
        if !self.db.shared.store.lifecycle.snapshot().admits(crate::lifecycle::Origin::Cloud) {
            return Err(CloudError::Rejected { detail: "the database is closed".into() });
        }
        let generation = self.db.generation();
        let unstamped = crate::blocking::blocking_unit(|| self.db.cloud_unstamped(generation))?;
        if !unstamped.is_empty() {
            self.publish(PhaseEvent::Merge { rows: unstamped.len() as u64 });
            return Ok(());
        }
        crate::db::lock::assert_no_library_lock("CloudIo::account");
        match self.io.account().await? {
            CloudAccount::Available => {}
            CloudAccount::SignedOut | CloudAccount::NeedsConsent => return Err(CloudError::NeedsAuthorization { detail: "no cloud account is available".into() }),
            CloudAccount::Restricted => return Err(CloudError::Rejected { detail: "the cloud account is restricted".into() }),
        }
        self.publish(PhaseEvent::Start);
        let own = crate::blocking::blocking_unit(|| self.db.cloud_replica(generation))?;
        loop {
            loop {
                let batch = crate::blocking::blocking_unit(|| self.db.cloud_batch(generation, BATCH))?;
                if batch.is_empty() {
                    break;
                }
                let (seqs, changes): (Vec<i64>, Vec<_>) = batch.into_iter().unzip();
                crate::db::lock::assert_no_library_lock("CloudIo::push");
                self.io.push(changes.iter().map(CloudChange::blob).collect()).await?;
                crate::blocking::blocking_unit(|| self.db.cloud_ack(generation, &seqs))?;
            }
            if pull || crate::blocking::blocking_unit(|| self.db.cloud_cursor(generation))?.is_none() {
                loop {
                    crate::db::lock::assert_no_library_lock("CloudIo::pull");
                    let page = self.io.pull(crate::blocking::blocking_unit(|| self.db.cloud_cursor(generation))?, BATCH).await?;
                    let changes = decode(&page)?;
                    crate::blocking::blocking_unit(|| self.db.apply_cloud_page(generation, &own, &changes, &page.cursor))?;
                    if !page.more {
                        break;
                    }
                }
            }
            if crate::blocking::blocking_unit(|| self.db.cloud_pending())? == 0 {
                break;
            }
        }
        self.publish(PhaseEvent::Done);
        Ok(())
    }
}

#[cfg(test)]
mod wake_tests;
