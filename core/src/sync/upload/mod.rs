mod multipart;
mod run;
mod steps;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

use crate::commit::{CommitEnvelope, OperationRecord, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::OutboxOperation;
use crate::identity::RowId;
use crate::operation::Operation;
use crate::transport::CancelToken;
use crate::upload::{UploadInput, UploadState};

use super::error::SyncError;
use super::{locked, Inner, Sync};

use run::Mode;

struct ActiveRun {
    run: u64,
    token: CancelToken,
    done: watch::Sender<bool>,
}

pub(crate) struct UploadRuns {
    permits: Arc<Semaphore>,
    active: HashMap<i64, ActiveRun>,
    next_run: u64,
    tasks: JoinSet<()>,
}

impl UploadRuns {
    pub(crate) fn new(max_files: usize) -> Self {
        Self { permits: Arc::new(Semaphore::new(max_files.max(1))), active: HashMap::new(), next_run: 0, tasks: JoinSet::new() }
    }
}

fn upload_entry(entries: Vec<OutboxOperation>, model: &str, id: &RowId) -> Option<OutboxOperation> {
    entries.into_iter().find(|entry| {
        entry.operation.upload.is_some()
            && Operation::split_action_key(&entry.operation.action_key).is_some_and(|(owner, _)| owner == model)
            && entry.operation.row_ids.contains(id)
    })
}

impl Inner {
    pub(crate) fn begin_upload(&self, model: &str, id: &RowId, input: &UploadInput, state: UploadState) -> Result<i64, SyncError> {
        self.db.spec(model)?;
        if upload_entry(self.db.outbox()?, model, id).is_some() {
            return Err(SyncError::Params(format!("{model} {id} already has an upload")));
        }
        let record = OperationRecord {
            seq: None,
            action_key: Operation::action_key(model, "upload"),
            status: OperationStatus::Pending,
            row_ids: vec![id.clone()],
            temp_id: None,
            input: serde_json::to_value(input).map_err(|error| SyncError::Params(error.to_string()))?,
            transport_started: false,
            base_revision: Some(self.db.revision_seq()?),
            created_at: self.now_ms(),
            dedupe_key: None,
            rollback: None,
            upload: Some(state),
        };
        let generation = self.db.generation();
        let committed = self.commit(generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            envelope.operation_transitions.push(OperationTransition::Begin(record));
            Ok(Some(envelope))
        })?;
        committed.and_then(|result| result.operations.into_iter().next()).ok_or(SyncError::Cancelled)
    }

    pub(crate) fn is_current(&self, seq: i64, run: u64) -> bool {
        locked(&self.uploads).active.get(&seq).is_some_and(|active| active.run == run)
    }

    fn finish_run(&self, seq: i64, run: u64) {
        let mut runs = locked(&self.uploads);
        if runs.active.get(&seq).is_some_and(|active| active.run == run) {
            if let Some(active) = runs.active.remove(&seq) {
                active.done.send_replace(true);
            }
        }
    }

    fn register(&self, seq: i64) -> Option<(u64, CancelToken, Arc<Semaphore>)> {
        let mut runs = locked(&self.uploads);
        if runs.active.contains_key(&seq) {
            return None;
        }
        runs.next_run += 1;
        let run = runs.next_run;
        let token = CancelToken::new();
        runs.active.insert(seq, ActiveRun { run, token: token.clone(), done: watch::channel(false).0 });
        Some((run, token, runs.permits.clone()))
    }
}

impl Sync {
    pub async fn upload(&self, model: &str, id: &RowId, input: UploadInput) -> Result<i64, SyncError> {
        let seq = self.inner.begin_upload(model, id, &input, UploadState::queued())?;
        self.spawn_run(seq, Mode::Transfer);
        Ok(seq)
    }

    pub async fn cancel_upload(&self, model: &str, id: &RowId) -> Result<bool, SyncError> {
        let Some(entry) = upload_entry(self.inner.db.outbox()?, model, id) else { return Ok(false) };
        let seq = entry.operation.id.0;
        let running = locked(&self.inner.uploads).active.get(&seq).map(|active| (active.token.clone(), active.done.subscribe()));
        match running {
            Some((token, mut done)) => {
                token.cancel();
                let _ = done.wait_for(|finished| *finished).await;
            }
            None => self.run_inline(seq, Mode::Cancel).await,
        }
        Ok(true)
    }

    pub fn set_online(&self, online: bool) {
        self.inner.connectivity.set(online);
    }

    pub async fn wait_uploads(&self) {
        loop {
            let mut tasks = std::mem::take(&mut locked(&self.inner.uploads).tasks);
            while tasks.join_next().await.is_some() {}
            let runs = locked(&self.inner.uploads);
            if runs.tasks.is_empty() && runs.active.is_empty() {
                return;
            }
        }
    }

    pub(crate) fn start_uploads(&self) -> Result<(), SyncError> {
        for entry in self.inner.db.outbox()? {
            let Some(state) = &entry.operation.upload else { continue };
            let seq = entry.operation.id.0;
            match entry.operation.status {
                OperationStatus::Pending if !entry.operation.transport_started => self.spawn_run(seq, Mode::Transfer),
                OperationStatus::Failed | OperationStatus::DeliveryUnknown if state.upload_id.is_some() => self.spawn_run(seq, Mode::Orphan),
                _ => {}
            }
        }
        Ok(())
    }

    fn spawn_run(&self, seq: i64, mode: Mode) {
        let Some((run, token, permits)) = self.inner.register(seq) else { return };
        let inner = self.inner.clone();
        locked(&self.inner.uploads).tasks.spawn(async move {
            run::drive(inner.clone(), seq, run, token, permits, mode).await;
            inner.finish_run(seq, run);
        });
    }

    async fn run_inline(&self, seq: i64, mode: Mode) {
        let Some((run, token, permits)) = self.inner.register(seq) else { return };
        run::drive(self.inner.clone(), seq, run, token, permits, mode).await;
        self.inner.finish_run(seq, run);
    }
}

#[cfg(test)]
mod upload_tests;
