mod multipart;
mod run;
#[cfg(test)]
mod run_tests;
mod steps;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

use crate::commit::{CommitEnvelope, OperationRecord, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::{read_outbox, OutboxOperation};
use crate::identity::RowId;
use crate::operation::Operation;
use crate::transport::CancelToken;
use crate::upload::{UploadInput, UploadState, UploadStatus};

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
    active: HashMap<(i64, i64), ActiveRun>,
    next_run: u64,
    tasks: JoinSet<()>,
}

pub(crate) struct UploadResume {
    inner: Arc<Inner>,
    seq: i64,
    position: i64,
    run: u64,
    token: CancelToken,
    permits: Arc<Semaphore>,
}

impl Drop for UploadResume {
    fn drop(&mut self) {
        self.inner.finish_run(self.seq, self.position, self.run);
    }
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
    pub(crate) fn start_uploads(self: &Arc<Self>) -> Result<(), SyncError> {
        let items = self.upload_items()?;
        for entry in self.db.outbox()? {
            let seq = entry.operation.id.0;
            for (&(owner, position), state) in &items {
                if owner != seq { continue; }
                match entry.operation.status {
                    OperationStatus::Pending if !entry.operation.transport_started && !self.is_waiting(&entry.operation) => self.spawn_run(seq, position, Mode::Transfer),
                    OperationStatus::Failed | OperationStatus::DeliveryUnknown if state.upload_id.is_some() && !crate::transport::encrypted::pending_matches(&self.db, seq, position, &crate::operation::enum_text(&state.stage).unwrap_or_default()) => {
                        let mode = if state.phase == UploadStatus::Cancelled { Mode::Cancel } else { Mode::Orphan };
                        self.spawn_run(seq, position, mode);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub(crate) fn upload_items(&self) -> Result<HashMap<(i64, i64), UploadState>, SyncError> {
        Ok(self.db.shared.read(&[], |reader| Ok(crate::operation::read_upload_items(reader.connection())?))?)
    }

    pub(crate) fn begin_upload(&self, model: &str, id: &RowId, input: &UploadInput, state: UploadState) -> Result<i64, SyncError> {
        self.db.spec(model)?;
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
            dedupe_key: Some(format!("{model}:{id}:upload")),
            rollback: None,
            upload: Some(state),
        };
        let generation = self.db.generation();
        let committed = self.commit(generation, |reader| {
            if upload_entry(read_outbox(reader)?, model, id).is_some() {
                return Err(SyncError::Params(format!("{model} {id} already has an upload")));
            }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            envelope.operation_transitions.push(OperationTransition::Begin(record.clone()));
            Ok(Some(envelope))
        })?;
        committed.and_then(|result| result.operations.into_iter().next()).ok_or(SyncError::Cancelled)
    }

    pub(crate) fn is_current(&self, seq: i64, position: i64, run: u64) -> bool {
        locked(&self.uploads).active.get(&(seq, position)).is_some_and(|active| active.run == run)
    }

    fn finish_run(self: &Arc<Self>, seq: i64, position: i64, run: u64) {
        {
            let mut runs = locked(&self.uploads);
            if runs.active.get(&(seq, position)).is_some_and(|active| active.run == run) {
                if let Some(active) = runs.active.remove(&(seq, position)) {
                    active.done.send_replace(true);
                }
            }
        }
        let retried = self.db.outbox().ok().into_iter().flatten().any(|entry| {
            entry.operation.id.0 == seq && entry.operation.upload.is_some() && entry.operation.status == OperationStatus::Pending && !entry.operation.transport_started && !self.is_waiting(&entry.operation)
        });
        let unfinished = self.upload_items().ok().and_then(|mut items| items.remove(&(seq, position))).is_some_and(|state| !state.is_transferred());
        if retried && unfinished {
            self.spawn_run(seq, position, Mode::Transfer);
        }
    }

    fn spawn_run(self: &Arc<Self>, seq: i64, position: i64, mode: Mode) {
        let Some((run, token, permits)) = self.register(seq, position) else { return };
        let inner = self.clone();
        locked(&self.uploads).tasks.spawn(self.until_closed(async move {
            run::drive(inner.clone(), seq, position, run, token, permits, mode, None).await;
            inner.finish_run(seq, position, run);
        }));
    }

    fn register(&self, seq: i64, position: i64) -> Option<(u64, CancelToken, Arc<Semaphore>)> {
        let mut runs = locked(&self.uploads);
        if runs.active.contains_key(&(seq, position)) {
            return None;
        }
        runs.next_run += 1;
        let run = runs.next_run;
        let token = CancelToken::new();
        runs.active.insert((seq, position), ActiveRun { run, token: token.clone(), done: watch::channel(false).0 });
        Some((run, token, runs.permits.clone()))
    }

    pub(crate) async fn claim_upload_resume(self: &Arc<Self>, owner: &crate::transport::RequestOwner) -> Result<Option<UploadResume>, SyncError> {
        loop {
            let mut items: Vec<_> = self.upload_items()?.into_iter().filter(|(key, state)| key.0 == owner.seq && (key.1 == owner.item_position || owner.item_position == -1 && state.stage == crate::upload::UploadStage::Mutation)).collect();
            items.sort_by_key(|(key, _)| *key);
            let Some(((seq, position), state)) = items.into_iter().next() else { return Ok(None) };
            if crate::operation::enum_text(&state.stage).as_deref() != Some(&owner.stage) { return Ok(None); }
            if let Some((run, token, permits)) = self.register(seq, position) {
                return Ok(Some(UploadResume { inner: self.clone(), seq, position, run, token, permits }));
            }
            let waiting = locked(&self.uploads).active.get(&(seq, position)).map(|active| active.done.subscribe());
            if let Some(mut done) = waiting {
                let _ = done.wait_for(|finished| *finished).await;
            }
        }
    }

    pub(crate) async fn resume_upload(self: &Arc<Self>, owner: UploadResume, result: Result<crate::transport::Response, crate::transport::TransportError>) {
        let seq = owner.seq;
        let mode = self.upload_items().ok().and_then(|mut items| items.remove(&(seq, owner.position))).map_or(Mode::Transfer, |state| match (state.stage, state.phase) {
                (crate::upload::UploadStage::Abort, crate::upload::UploadStatus::Cancelled) => Mode::Cancel,
                (crate::upload::UploadStage::Abort, _) => Mode::Orphan,
                _ => Mode::Transfer,
            });
        run::drive(self.clone(), seq, owner.position, owner.run, owner.token.clone(), owner.permits.clone(), mode, Some(result)).await;
    }
}

impl Sync {
    pub async fn upload(&self, model: &str, id: &RowId, input: UploadInput) -> Result<i64, SyncError> {
        let seq = self.inner.begin_upload(model, id, &input, UploadState::queued())?;
        self.inner.spawn_run(seq, 0, Mode::Transfer);
        Ok(seq)
    }

    pub async fn upload_create(&self, model: &str, values: serde_json::Map<String, serde_json::Value>, input: UploadInput) -> Result<RowId, SyncError> {
        let queued = serde_json::to_value(&input).map_err(|error| SyncError::Params(error.to_string()))?;
        let id = self.inner.db.create_held(model, values, queued, UploadState::queued())?;
        let entry = upload_entry(self.inner.db.outbox()?, model, &id).ok_or(SyncError::Cancelled)?;
        self.inner.spawn_run(entry.operation.id.0, 0, Mode::Transfer);
        Ok(id)
    }

    pub async fn cancel_upload(&self, model: &str, id: &RowId) -> Result<bool, SyncError> {
        let Some(entry) = upload_entry(self.inner.db.outbox()?, model, id) else { return Ok(false) };
        let seq = entry.operation.id.0;
        let running: Vec<_> = locked(&self.inner.uploads).active.iter().filter(|(key, _)| key.0 == seq).map(|(_, active)| (active.token.clone(), active.done.subscribe())).collect();
        for (token, _) in &running { token.cancel(); }
        if running.is_empty() {
            for ((owner, position), _) in self.inner.upload_items()? {
                if owner == seq { self.run_inline(seq, position, Mode::Cancel).await; }
            }
        } else {
            for (_, mut done) in running { let _ = done.wait_for(|finished| *finished).await; }
        }
        Ok(true)
    }

    pub async fn wait_uploads(&self) {
        loop {
            let mut tasks = std::mem::take(&mut locked(&self.inner.uploads).tasks);
            while tasks.join_next().await.is_some() {}
            let waiting = {
                let runs = locked(&self.inner.uploads);
                if runs.tasks.is_empty() && runs.active.is_empty() {
                    return;
                }
                runs.active.values().next().map(|active| active.done.subscribe())
            };
            if let Some(mut done) = waiting {
                let _ = done.wait_for(|finished| *finished).await;
            }
        }
    }

    async fn run_inline(&self, seq: i64, position: i64, mode: Mode) {
        let Some((run, token, permits)) = self.inner.register(seq, position) else { return };
        run::drive(self.inner.clone(), seq, position, run, token, permits, mode, None).await;
        self.inner.finish_run(seq, position, run);
    }
}

#[cfg(test)]
mod upload_tests;
#[cfg(test)]
mod generation_tests;
#[cfg(test)]
mod resume_tests;
