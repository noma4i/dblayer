use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};

use crate::db::{locked, Db, DbError};
use crate::pacing::{Backoff, Clock};
use crate::transport::Connectivity;

use super::{CloudAccount, CloudChange, CloudError, CloudIo, CloudPage, CloudState, MergeSource, Hlc};

const BATCH: u32 = 100;
const RETRY: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(300)).limited(40);

#[derive(Clone)]
pub struct Cloud {
    inner: Arc<Inner>,
}

struct Inner {
    db: Db,
    io: Arc<dyn CloudIo>,
    clock: Arc<dyn Clock>,
    online: Arc<Connectivity>,
    wake: Notify,
    state: watch::Sender<CloudState>,
    pass: tokio::sync::Mutex<()>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

enum Next {
    Retry(Duration, bool),
    Stop(CloudState),
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
        let state = watch::channel(CloudState::Idle).0;
        Cloud { inner: Arc::new(Inner { db: db.clone(), io, clock, online, wake: Notify::new(), state, pass: tokio::sync::Mutex::new(()), task: Mutex::new(None) }) }
    }

    pub fn state(&self) -> CloudState {
        self.inner.state.borrow().clone()
    }

    pub fn state_changes(&self) -> watch::Receiver<CloudState> {
        self.inner.state.subscribe()
    }

    pub fn remote_changed(&self) {
        self.inner.wake.notify_one();
    }

    pub async fn resolve_initial_merge(&self, source: MergeSource) -> Result<(), CloudError> {
        if source == MergeSource::UseLocal {
            let mut cursor = None;
            loop {
                let page = self.inner.io.pull(cursor, BATCH).await?;
                for change in &decode(&page)? {
                    self.inner.db.cloud_observe(&Hlc::new(change.clock_ms, change.clock_count, &change.replica))?;
                }
                if !page.more {
                    break;
                }
                cursor = Some(page.cursor);
            }
        }
        self.inner.db.cloud_stamp_unstamped(source)?;
        self.inner.wake.notify_one();
        Ok(())
    }

    pub async fn sync_now(&self) -> Result<(), CloudError> {
        let outcome = self.inner.pass(true).await;
        if let Err(error) = &outcome {
            self.inner.state.send_replace(Self::stopped(error));
        }
        outcome
    }

    pub fn start(&self) {
        let mut task = locked(&self.inner.task);
        if task.is_some() {
            return;
        }
        let inner = self.inner.clone();
        *task = Some(tokio::spawn(async move { inner.run().await }));
    }

    pub fn stop(&self) {
        if let Some(task) = locked(&self.inner.task).take() {
            task.abort();
        }
    }

    fn stopped(error: &CloudError) -> CloudState {
        match error {
            CloudError::Offline { .. } | CloudError::Unavailable { .. } | CloudError::Throttled { .. } => CloudState::Offline,
            CloudError::NeedsAuthorization { .. } => CloudState::NeedsAuthorization,
            CloudError::QuotaExceeded { .. } => CloudState::QuotaExceeded,
            CloudError::Rejected { detail } => CloudState::Failed { detail: detail.clone() },
        }
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
            self.settle(pull).await;
            pull = false;
        }
    }

    async fn settle(&self, pull: bool) {
        let mut attempt = 0u32;
        loop {
            self.online.online().await;
            let error = match self.pass(pull).await {
                Ok(()) => return,
                Err(error) => error,
            };
            match self.classify(&error, attempt) {
                Next::Stop(state) => {
                    self.state.send_replace(state);
                    return;
                }
                Next::Retry(delay, spends) => {
                    self.state.send_replace(CloudState::Offline);
                    attempt += u32::from(spends);
                    self.online.wait(self.clock.as_ref(), delay).await;
                }
            }
        }
    }

    fn classify(&self, error: &CloudError, attempt: u32) -> Next {
        let delay = RETRY.delay(attempt);
        match error {
            CloudError::Offline { .. } => Next::Retry(delay, false),
            CloudError::Unavailable { .. } if RETRY.allows(attempt + 1) => Next::Retry(delay, true),
            CloudError::Throttled { retry_after_ms } if RETRY.allows(attempt + 1) => Next::Retry(delay.max(Duration::from_millis(*retry_after_ms)), true),
            CloudError::Unavailable { detail } => Next::Stop(CloudState::Failed { detail: detail.clone() }),
            CloudError::Throttled { .. } => Next::Stop(CloudState::Failed { detail: error.to_string() }),
            other => Next::Stop(Cloud::stopped(other)),
        }
    }

    async fn pass(&self, pull: bool) -> Result<(), CloudError> {
        let _turn = self.pass.lock().await;
        let unstamped = self.db.cloud_unstamped()?;
        if !unstamped.is_empty() {
            self.state.send_replace(CloudState::NeedsInitialMerge { rows: unstamped.len() as u64 });
            return Ok(());
        }
        match self.io.account().await? {
            CloudAccount::Available => {}
            CloudAccount::SignedOut | CloudAccount::NeedsConsent => return Err(CloudError::NeedsAuthorization { detail: "no cloud account is available".into() }),
            CloudAccount::Restricted => return Err(CloudError::Rejected { detail: "the cloud account is restricted".into() }),
        }
        self.state.send_replace(CloudState::Syncing);
        let own = self.db.cloud_replica()?;
        loop {
            loop {
                let batch = self.db.cloud_batch(BATCH)?;
                if batch.is_empty() {
                    break;
                }
                let (seqs, changes): (Vec<i64>, Vec<_>) = batch.into_iter().unzip();
                self.io.push(changes.iter().map(CloudChange::blob).collect()).await?;
                self.db.cloud_ack(&seqs)?;
            }
            if pull || self.db.cloud_cursor()?.is_none() {
                loop {
                    let page = self.io.pull(self.db.cloud_cursor()?, BATCH).await?;
                    self.db.apply_cloud_page(&own, &decode(&page)?, &page.cursor)?;
                    if !page.more {
                        break;
                    }
                }
            }
            if self.db.cloud_pending()? == 0 {
                break;
            }
        }
        self.state.send_replace(CloudState::Idle);
        Ok(())
    }
}
