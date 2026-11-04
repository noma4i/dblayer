mod multipart;
mod orphan;
mod run;
#[cfg(test)]
mod run_tests;
mod steps;
mod blob;
#[cfg(test)]
mod blob_tests;

#[cfg(test)]
mod batch_tests;

#[cfg(test)]
mod settle_tests;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod source_tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::watch;
use crate::task::Children;

use crate::commit::{CommitEnvelope, OperationRecord, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::db::dispatch::Effects;
use crate::db::OutboxOperation;
use crate::identity::RowId;
use crate::operation::Operation;
use crate::transport::CancelToken;
use crate::upload::{RunMode, UploadFile, UploadInput, UploadOperations, UploadState};

use super::error::SyncError;
use super::{locked, Inner, Sync};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ItemId(pub String);

#[derive(Debug, Clone)]
pub struct ItemReceipt {
    pub item: ItemId,
    pub optimistic_value: crate::db::Row,
}

#[derive(Debug, Clone)]
pub struct OperationReceipt {
    pub epoch: u64,
    pub operation: i64,
    pub revision: i64,
    pub items: Vec<ItemReceipt>,
    pub(crate) ticket: Arc<crate::sync::OutcomeTicket>,
    pub(crate) life: Arc<crate::lifecycle::Lifecycle>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ItemOutcome {
    Confirmed { item: ItemId, value: crate::db::Row },
    Discarded { item: ItemId },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationOutcome {
    pub result: crate::sync::SendResult,
    pub items: Vec<ItemOutcome>,
}

impl OperationReceipt {
    pub async fn outcome(&self) -> Result<OperationOutcome, SyncError> {
        let admitted = self.life.admit_passive(crate::lifecycle::Origin::Explicit, "upload batch outcome", Some(self.epoch))?;
        let result = admitted.run(self.ticket.result()).await?;
        let mut items = Vec::new();
        for receipt in &self.items {
            let item = receipt.item.clone();
            if result.discarded.contains(&item.0) {
                items.push(ItemOutcome::Discarded { item });
            } else if let Some((_, value)) = result.items.iter().find(|(key, _)| *key == item.0) {
                items.push(ItemOutcome::Confirmed { item, value: value.clone() });
            } else if result.result == crate::sync::SendResult::Committed {
                return Err(SyncError::Store(format!("upload item {} has no confirmed value", item.0)));
            }
        }
        Ok(OperationOutcome { result: result.result, items })
    }
}

struct ActiveRun {
    run: u64,
    token: CancelToken,
    done: watch::Sender<bool>,
    file_slot: bool,
    ended: bool,
}

struct ReadyRun {
    resumed: Option<Result<crate::transport::Response, crate::transport::TransportError>>,
}

pub(crate) struct UploadRuns {
    max_files: usize,
    active_files: usize,
    ready: BTreeMap<(i64, i64), ReadyRun>,
    active: BTreeMap<(i64, i64), ActiveRun>,
    next_run: u64,
    #[cfg(test)]
    walked: usize,
    tasks: Children<()>,
}

async fn survives_panic(work: impl std::future::Future<Output = ()>) -> bool {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
        Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
        Ok(std::task::Poll::Ready(())) => std::task::Poll::Ready(true),
        Err(_) => std::task::Poll::Ready(false),
    }).await
}

struct FileTask {
    pending: Arc<watch::Sender<usize>>,
    _admitted: crate::lifecycle::Admitted,
}

impl Drop for FileTask {
    fn drop(&mut self) {
        crate::db::lock::effect(|| self.pending.send_modify(|pending| *pending -= 1));
    }
}

pub(crate) struct UploadResume {
    inner: Arc<Inner>,
    admitted: crate::lifecycle::Admitted,
    seq: i64,
    position: i64,
    run: u64,
}

impl Drop for UploadResume {
    fn drop(&mut self) {
        self.inner.finish_run(&self.admitted, self.seq, self.position, self.run);
    }
}

impl UploadRuns {
    pub(crate) fn new(max_files: usize) -> Self {
        Self { max_files: max_files.max(1), active_files: 0, ready: BTreeMap::new(), active: BTreeMap::new(), next_run: 0, #[cfg(test)] walked: 0, tasks: Children::new() }
    }
}

fn upload_entry(entries: Vec<OutboxOperation>, model: &str, id: &RowId, family: &[String]) -> Option<OutboxOperation> {
    entries.into_iter().find(|entry| {
        entry.operation.upload.is_some()
            && Operation::upload_action(&entry.operation.action_key).is_some_and(|(owner, action)| owner == model && family.contains(&action))
            && entry.operation.row_ids.contains(id)
    })
}

impl Inner {
    pub(crate) fn start_uploads(self: &Arc<Self>) -> Result<(), SyncError> {
        if !self.admitted(crate::lifecycle::Origin::Session) { return Ok(()); }
        let sequences = {
            let mut queue = locked(self.operation_queue()?);
            let mut changed = std::mem::take(&mut queue.dirty_uploads);
            changed.extend(queue.retrying.iter().filter(|seq| queue.entries.get(seq).is_some_and(|entry| entry.uploading && !self.is_waiting(&entry.operation))));
            changed
        };
        let mut failure = None;
        for seq in sequences {
            if let Err(error) = self.refresh_upload(seq) {
                if let Ok(queue) = self.operation_queue() { locked(queue).dirty_uploads.insert(seq); }
                failure.get_or_insert(error);
            }
        }
        self.drain_uploads();
        failure.map_or(Ok(()), Err)
    }

    fn refresh_upload(self: &Arc<Self>, seq: i64) -> Result<(), SyncError> {
        let operation = locked(self.operation_queue()?).entries.get(&seq).map(|entry| entry.operation.clone());
        let items = self.operation_uploads(seq)?;
        let mut ready = Vec::new();
        if let Some(operation) = operation {
            for ((_, position), state) in items {
                let waiting = self.is_waiting(&operation);
                let pending = crate::transport::encrypted::pending_matches(&self.db, seq, position, &crate::operation::enum_text(&state.stage).unwrap_or_default())?;
                let mode = state.run_mode(operation.status, operation.transport_started, waiting, || pending);
                if let Some(mode) = mode { ready.push((position, mode)); }
            }
        }
        let mut runs = locked(&self.uploads);
        if !self.admitted(crate::lifecycle::Origin::Session) {
            drop(runs);
            self.restart_uploads([seq]);
            return Ok(());
        }
        let keys: Vec<_> = runs.ready.range((seq, i64::MIN)..=(seq, i64::MAX)).map(|(key, _)| *key).collect();
        let mut previous: BTreeMap<_, _> = keys.into_iter().filter_map(|key| runs.ready.remove(&key).map(|run| (key, run))).collect();
        for (position, mode) in ready {
            let key = (seq, position);
            if runs.active.contains_key(&key) { continue; }
            let pending = previous.remove(&key).unwrap_or(ReadyRun { resumed: None });
            if matches!(mode, RunMode::Transfer) {
                runs.ready.insert(key, pending);
            } else {
                self.launch_run(&mut runs, seq, position, mode, pending.resumed);
            }
        }
        Ok(())
    }

    pub(crate) fn operation_uploads(&self, seq: i64) -> Result<HashMap<(i64, i64), UploadState>, SyncError> {
        Ok(self.db.shared.read(&[], |reader| Ok(crate::operation::read_operation_uploads(reader.connection(), seq)?))?)
    }

    pub(crate) fn upload_operations(&self) -> Result<&UploadOperations, crate::db::DbError> {
        let uploads = self.config.uploads.as_ref().ok_or_else(|| crate::db::DbError::InvalidUpload("upload operations are not declared: pass UploadOperations to open".into()))?;
        if locked(&self.operations).is_empty() {
            return Err(crate::db::DbError::InvalidUpload("uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open or openTesting".into()));
        }
        uploads.check(|name| locked(&self.operations).contains_key(name)).map_err(crate::db::DbError::InvalidUpload)?;
        Ok(uploads)
    }

    fn upload_input(&self, model: &str, action: &str, file: UploadFile) -> Result<UploadInput, crate::db::DbError> {
        let uploads = self.upload_operations()?;
        let mutation = self.db.declared_upload(model, action)?;
        Ok(file.with_steps(uploads.steps(&mutation)))
    }

    pub(crate) fn begin_upload(&self, admitted: &crate::lifecycle::Admitted, generation: crate::identity::Generation, model: &str, id: &RowId, action: &str, file: &UploadFile, state: UploadState) -> Result<(i64, i64, crate::db::Row, Arc<crate::sync::OutcomeTicket>), SyncError> {
        let input = self.upload_input(model, action, file.clone())?;
        let family = self.db.spec(model)?.upload_family(action);
        let record = OperationRecord {
            seq: None,
            action_key: Operation::upload_action_key(model, action),
            status: OperationStatus::Pending,
            row_ids: vec![id.clone()],
            temp_id: None,
            input: serde_json::Value::Object(input.variables.clone()),
            transport_started: false,
            base_revision: Some(self.db.revision_seq()?),
            created_at: self.now_ms(),
            dedupe_key: Some(format!("{model}:{id}:upload")),
            rollback: None,
            upload: Some(state.clone()),
        };
        self.db.preparing_uploads(admitted, model, generation, |files, generation, pending| {
            let item = files.prepare(model, id.clone(), uuid::Uuid::new_v4().to_string(), input.clone(), state)?;
            let committed = self.commit_deferred(admitted, generation, |reader| {
                if upload_entry(OutboxOperation::for_rows(reader, &[(model.to_string(), id.to_string())].into())?, model, id, &family).is_some() {
                    return Err(SyncError::Params(format!("{model} {id} already has the upload {action}")));
                }
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.snapshot = Some(crate::identity::RowRef::new(crate::identity::ModelKey::new(model), id.clone()));
                envelope.operation_transitions.push(OperationTransition::BeginUpload { record: Box::new(record.clone()), items: vec![item.clone()] });
                Ok(Some(envelope))
            })?;
            let result = committed.as_ref().map(|result| result.result()).ok_or(SyncError::Cancelled)?;
            let seq = result.operations.iter().next().copied().ok_or(SyncError::Cancelled)?;
            let revision = result.revision.ok_or(SyncError::Cancelled)?;
            let row = result.snapshot.clone().ok_or(SyncError::Cancelled)?;
            let ticket = locked(&self.outcomes).claim(seq);
            *pending = committed;
            Ok((seq, revision, row, ticket))
        })
    }

    pub(crate) fn is_current(&self, seq: i64, position: i64, run: u64) -> bool {
        locked(&self.uploads).active.get(&(seq, position)).is_some_and(|active| active.run == run)
    }

    /// Every transition of the upload map goes through here: wakes, sender drops and the destruction of an active run run after the map is released, in transition order.
    fn with_uploads<R>(&self, apply: impl FnOnce(&mut UploadRuns, &mut Effects) -> R) -> R {
        self.upload_effects.under(&self.uploads, apply)
    }

    /// Ends every running upload, including the platform PUT, and drops the queued ones. Their operations stay pending and return to the restart set, so the next session starts them.
    pub(super) fn cancel_uploads(&self) {
        let (tokens, restart) = locked(&self.uploads).end_all();
        self.restart_uploads(restart);
        for token in tokens { token.cancel(); }
    }

    pub(super) fn end_uploads_of_reset(&self) {
        let (tokens, _) = locked(&self.uploads).end_all();
        for token in tokens { token.cancel(); }
    }

    fn restart_uploads(&self, sequences: impl IntoIterator<Item = i64>) {
        if let Ok(queue) = self.operation_queue() { locked(queue).dirty_uploads.extend(sequences); }
    }

    pub(super) fn run_ended(&self, seq: i64, position: i64, run: u64) -> bool {
        locked(&self.uploads).active.get(&(seq, position)).is_some_and(|active| active.run == run && active.ended)
    }

    pub(super) fn running_uploads(&self, seq: i64, position: Option<i64>) -> Vec<(CancelToken, watch::Receiver<bool>)> {
        locked(&self.uploads).active.range((seq, i64::MIN)..=(seq, i64::MAX)).filter(|(key, _)| position.is_none_or(|position| key.1 == position)).map(|(_, active)| (active.token.clone(), active.done.subscribe())).collect()
    }

    fn finish_run(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, seq: i64, position: i64, run: u64) {
        if !self.with_uploads(|runs, effects| runs.finish(seq, position, run, effects)) { return; }
        let retry = (|| -> Result<(), SyncError> {
            let retried = locked(self.operation_queue()?).entries.get(&seq).is_some_and(|entry| entry.uploading && entry.operation.status == OperationStatus::Pending && !entry.operation.transport_started && !self.is_waiting(&entry.operation));
            if !retried { return Ok(()); }
            let unfinished = self.operation_uploads(seq)?.remove(&(seq, position)).is_some_and(|state| state.awaits_transfer());
            if unfinished { locked(&self.uploads).ready.entry((seq, position)).or_insert(ReadyRun { resumed: None }); }
            Ok(())
        })();
        if let Err(error) = retry {
            self.fail_upload(admitted, seq, error);
        }
        self.drain_uploads();
    }

    #[cfg(test)]
    fn spawn_run(self: &Arc<Self>, seq: i64, position: i64, mode: RunMode) -> Result<(), SyncError> {
        let transferred = self.operation_uploads(seq)?.remove(&(seq, position)).is_some_and(|state| state.is_transferred());
        let mode = if matches!(mode, RunMode::Transfer) && transferred { RunMode::Mutation } else { mode };
        {
            let mut runs = locked(&self.uploads);
            if runs.active.contains_key(&(seq, position)) { return Ok(()); }
            if matches!(mode, RunMode::Transfer) {
                runs.ready.insert((seq, position), ReadyRun { resumed: None });
            } else {
                self.launch_run(&mut runs, seq, position, mode, None);
            }
        }
        self.drain_uploads();
        Ok(())
    }

    fn drain_uploads(self: &Arc<Self>) {
        let mut runs = locked(&self.uploads);
        if !self.admitted(crate::lifecycle::Origin::Session) { return; }
        while runs.active_files < runs.max_files {
            let Some(((seq, position), pending)) = runs.ready.pop_first() else { break };
            if runs.active.contains_key(&(seq, position)) { continue; }
            self.launch_run(&mut runs, seq, position, RunMode::Transfer, pending.resumed);
        }
    }

    fn launch_run(self: &Arc<Self>, runs: &mut UploadRuns, seq: i64, position: i64, mode: RunMode, resumed: Option<Result<crate::transport::Response, crate::transport::TransportError>>) {
        while runs.tasks.try_join_next().is_some() {}
        let (run, token) = runs.register(seq, position, matches!(mode, RunMode::Transfer));
        let inner = self.clone();
        let epoch = self.db.shared.store.lifecycle.snapshot().session_epoch;
        runs.tasks.spawn(crate::task::Carry::Detached, self.until_closed(async move {
            let Ok(admitted) = inner.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Session, "upload", Some(epoch)) else {
                inner.with_uploads(|runs, effects| runs.finish(seq, position, run, effects));
                return;
            };
            if let Ok(false) = admitted.run(survives_panic(run::drive(inner.clone(), admitted.clone(), seq, position, run, token, mode, resumed))).await {
                inner.fail_upload(&admitted, seq, SyncError::Store("the upload task panicked".into()));
            }
            inner.finish_run(&admitted, seq, position, run);
        }));
    }

    fn spawn_file_task(&self, admitted: &crate::lifecycle::Admitted, runtime: &tokio::runtime::Handle, task: impl FnOnce() + Send + 'static) {
        crate::db::lock::effect(|| self.file_tasks.send_modify(|pending| *pending += 1));
        let finished = FileTask { pending: self.file_tasks.clone(), _admitted: admitted.clone() };
        let admitted = admitted.clone();
        drop(crate::task::spawn_blocking_on(crate::task::Carry::from(admitted.clone()), runtime, move || { if admitted.is_current() { task(); } drop(finished); }));
    }

    fn spawn_file_cleanup(&self, admitted: &crate::lifecycle::Admitted, runtime: &tokio::runtime::Handle, task: impl FnOnce() + Send + 'static) {
        crate::db::lock::effect(|| self.file_tasks.send_modify(|pending| *pending += 1));
        let finished = FileTask { pending: self.file_tasks.clone(), _admitted: admitted.clone() };
        drop(crate::task::spawn_blocking_on(crate::task::Carry::Detached, runtime, move || { task(); drop(finished); }));
    }

    pub(crate) async fn discard_files(&self, admitted: &crate::lifecycle::Admitted, ids: Vec<String>) -> Result<(), SyncError> {
        if ids.is_empty() { return Ok(()); }
        let (sent, received) = tokio::sync::oneshot::channel();
        let shared = self.db.shared.clone();
        self.spawn_file_task(admitted, &tokio::runtime::Handle::current(), move || { crate::db::lock::effect(|| { let _ = sent.send(shared.files.discard(&ids)); }); });
        received.await.map_err(|_| SyncError::Cancelled)?.map_err(|error| crate::store::StoreError::from(error).into())
    }

    fn fail_upload(&self, admitted: &crate::lifecycle::Admitted, seq: i64, error: SyncError) {
        let generation = self.db.generation();
        if let Err(failed) = self.commit(admitted, generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Unrecognized, error.to_string()) });
            Ok(Some(envelope))
        }) {
            self.fail_commit(admitted, SyncError::Store(format!("upload {seq}: {error}, recording failure: {failed}")));
        }
    }

    pub(super) fn release_file_slot(self: &Arc<Self>, seq: i64, position: i64, run: u64) {
        {
            let mut runs = locked(&self.uploads);
            if let Some(active) = runs.active.get_mut(&(seq, position)).filter(|active| active.run == run && active.file_slot) {
                active.file_slot = false;
                runs.active_files -= 1;
            }
        }
        self.drain_uploads();
    }

    fn register(&self, seq: i64, position: i64) -> Option<(u64, CancelToken)> {
        let mut runs = locked(&self.uploads);
        if runs.active.contains_key(&(seq, position)) {
            return None;
        }
        runs.ready.remove(&(seq, position));
        Some(runs.register(seq, position, false))
    }

    pub(crate) async fn claim_upload_resume(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, owner: &crate::transport::RequestOwner) -> Result<Option<UploadResume>, SyncError> {
        loop {
            let mut items: Vec<_> = self.operation_uploads(owner.seq)?.into_iter().filter(|(key, state)| key.1 == owner.item_position || owner.item_position == -1 && state.stage == crate::upload::UploadStage::Mutation).collect();
            items.sort_by_key(|(key, _)| *key);
            let Some(((seq, position), state)) = items.into_iter().next() else { return Ok(None) };
            if crate::operation::enum_text(&state.stage).as_deref() != Some(&owner.stage) { return Ok(None); }
            if let Some((run, _)) = self.register(seq, position) {
                return Ok(Some(UploadResume { inner: self.clone(), admitted: admitted.clone(), seq, position, run }));
            }
            let waiting = locked(&self.uploads).active.get(&(seq, position)).map(|active| active.done.subscribe());
            if let Some(mut done) = waiting {
                let _ = done.wait_for(|finished| *finished).await;
            }
        }
    }

    pub(crate) async fn resume_upload(self: &Arc<Self>, owner: UploadResume, result: Result<crate::transport::Response, crate::transport::TransportError>) {
        let seq = owner.seq;
        let state = match self.operation_uploads(seq) {
            Ok(mut items) => items.remove(&(seq, owner.position)),
            Err(error) => { self.fail_upload(&owner.admitted, seq, error); return; }
        };
        let Some(state) = state else { return };
        let mode = state.resume_mode();
        let mut parked = false;
        let resumed = self.with_uploads(|runs, effects| {
            if runs.active.get(&(seq, owner.position)).is_none_or(|active| active.run != owner.run) { return false; }
            if let Some(active) = runs.active.remove(&(seq, owner.position)) { effects.push(move || { crate::db::lock::effect(|| active.done.send_replace(true)); }); }
            if !self.admitted(crate::lifecycle::Origin::Session) {
                parked = true;
                return false;
            }
            if matches!(mode, RunMode::Transfer) {
                runs.ready.insert((seq, owner.position), ReadyRun { resumed: Some(result) });
            } else {
                self.launch_run(runs, seq, owner.position, mode, Some(result));
            }
            true
        });
        if parked { self.restart_uploads([seq]); }
        if resumed { self.drain_uploads(); }
    }
}

impl Sync {
    pub async fn resume_uploads(&self, admitted: &crate::lifecycle::Admitted) -> Result<(), SyncError> {
        self.inner.require_session(admitted)?;
        admitted.run(async {
            self.inner.start_uploads()?;
            self.wait_uploads().await;
            Ok(())
        }).await?
    }

    pub(crate) async fn upload_create_claiming(&self, admitted: &crate::lifecycle::Admitted, model: &str, action: &str, values: serde_json::Map<String, serde_json::Value>, file: UploadFile) -> Result<(crate::db::Row, i64, i64, Arc<crate::sync::OutcomeTicket>), SyncError> {
        self.inner.require_session(admitted)?;
        let inner = self.inner.clone();
        let admitted = admitted.clone();
        let generation = inner.db.generation();
        let (model, action) = (model.to_string(), action.to_string());
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let input = inner.upload_input(&model, &action, file)?;
            let (row, seq, revision, ticket) = inner.db.create_held_claiming(&admitted, generation, &model, &action, values, input, UploadState::queued(), |seq| locked(&inner.outcomes).claim(seq))?;
            inner.start_uploads()?;
            inner.db.report_upload_accepted(&admitted, &model, inner.db.accepted_items(std::slice::from_ref(&row.id))?);
            Ok((row, seq, revision, ticket))
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub fn check_uploads(&self) -> Result<(), SyncError> {
        if self.inner.config.uploads.is_some() { self.inner.upload_operations()?; }
        Ok(())
    }

    pub async fn upload_create_batch(&self, admitted: &crate::lifecycle::Admitted, model: &str, items: Vec<crate::upload::UploadItem>, input: crate::upload::UploadBatchInput) -> Result<OperationReceipt, SyncError> {
        self.inner.require_session(admitted)?;
        let inner = self.inner.clone();
        let admitted = admitted.clone();
        let generation = inner.db.generation();
        let model = model.to_string();
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let (rows, operation, revision, ticket) = inner.db.create_upload_batch(&admitted, generation, &model, items, input, inner.upload_operations()?, |seq| locked(&inner.outcomes).claim(seq))?;
            let ids: Vec<_> = rows.iter().map(|(_, row)| row.id.clone()).collect();
            inner.start_uploads()?;
            inner.db.report_upload_accepted(&admitted, &model, inner.db.accepted_items(&ids)?);
            Ok(OperationReceipt { epoch: admitted.value(), operation, revision, items: rows.into_iter().map(|(item, optimistic_value)| ItemReceipt { item: ItemId(item), optimistic_value }).collect(), ticket, life: admitted.life().clone() })
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub(crate) async fn upload(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, action: &str, file: UploadFile) -> Result<(i64, i64, crate::db::Row, Arc<crate::sync::OutcomeTicket>), SyncError> {
        self.inner.require_session(admitted)?;
        let inner = self.inner.clone();
        let admitted = admitted.clone();
        let generation = inner.db.generation();
        let (model, id, action) = (model.to_string(), id.clone(), action.to_string());
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let (seq, revision, row, ticket) = inner.begin_upload(&admitted, generation, &model, &id, &action, &file, UploadState::queued())?;
            let row = inner.db.shared.overlay_snapshot(&model, row)?;
            inner.start_uploads()?;
            inner.db.report_upload_accepted(&admitted, &model, vec![crate::db::AcceptedUpload { seq, position: 0 }]);
            Ok((seq, revision, row, ticket))
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub async fn upload_create(&self, admitted: &crate::lifecycle::Admitted, model: &str, action: &str, values: serde_json::Map<String, serde_json::Value>, file: UploadFile) -> Result<RowId, SyncError> {
        self.inner.require_session(admitted)?;
        let inner = self.inner.clone();
        let admitted = admitted.clone();
        let generation = inner.db.generation();
        let (model, action) = (model.to_string(), action.to_string());
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let input = inner.upload_input(&model, &action, file)?;
            let id = inner.db.create_held(&admitted, generation, &model, &action, values, input, UploadState::queued())?;
            inner.start_uploads()?;
            inner.db.report_upload_accepted(&admitted, &model, inner.db.accepted_items(std::slice::from_ref(&id))?);
            Ok(id)
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub async fn cancel_upload(&self, admitted: &crate::lifecycle::Admitted, model: &str, id: &RowId, action: &str) -> Result<bool, SyncError> {
        self.inner.require_session(admitted)?;
        let spec = self.inner.db.spec(model)?;
        if spec.upload_action(action).is_none() { return Err(crate::db::DbError::InvalidUpload(format!("{model} declares no upload action {action}")).into()); }
        let family = spec.upload_family(action);
        let entries = self.inner.db.shared.read(&[], |reader| Ok(OutboxOperation::for_rows(reader, &[(model.to_string(), id.to_string())].into())?))?;
        let Some(entry) = upload_entry(entries, model, id, &family) else { return Ok(false) };
        let seq = entry.operation.id.0;
        let batched = spec.is_batch_upload(&entry.operation.action_key);
        if entry.operation.transport_started {
            if batched { return Err(crate::db::DbError::InFlight { model: model.into(), id: id.to_string() }.into()); }
            return Ok(false);
        }
        let position = if batched {
            Some(self.inner.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT position FROM op_item WHERE seq = ?1 AND row_id = ?2", rusqlite::params![seq, id.as_str()], |row| row.get::<_, i64>(0))?))?)
        } else { None };
        let running = self.inner.running_uploads(seq, position);
        for (token, _) in &running { token.cancel(); }
        if running.is_empty() {
            for ((_, item), _) in self.inner.operation_uploads(seq)? {
                if position.is_none_or(|position| item == position) { self.run_inline(admitted, seq, item, RunMode::Cancel).await; }
            }
        } else {
            for (_, mut done) in running { let _ = done.wait_for(|finished| *finished).await; }
        }
        self.inner.db.shared.read(&[], |_| Ok(()))?;
        Ok(true)
    }

    pub(crate) async fn wait_uploads(&self) {
        loop {
            let mut tasks = std::mem::take(&mut locked(&self.inner.uploads).tasks);
            while tasks.join_next().await.is_some() {}
            let _ = self.inner.file_tasks.subscribe().wait_for(|pending| *pending == 0).await;
            let waiting = {
                let runs = locked(&self.inner.uploads);
                if runs.tasks.is_empty() && runs.active.is_empty() && *self.inner.file_tasks.borrow() == 0 {
                    return;
                }
                runs.active.values().next().map(|active| active.done.subscribe())
            };
            if let Some(mut done) = waiting {
                let _ = done.wait_for(|finished| *finished).await;
            }
        }
    }

    async fn run_inline(&self, admitted: &crate::lifecycle::Admitted, seq: i64, position: i64, mode: RunMode) {
        let Some((run, token)) = self.inner.register(seq, position) else { return };
        run::drive(self.inner.clone(), admitted.clone(), seq, position, run, token, mode, None).await;
        self.inner.finish_run(admitted, seq, position, run);
    }
}

impl UploadRuns {
    /// Marks every running upload ended and empties the queue. Returns the tokens to cancel and the operations to start again. The walk costs the number of running uploads.
    fn end_all(&mut self) -> (Vec<CancelToken>, BTreeSet<i64>) {
        let mut restart: BTreeSet<i64> = self.ready.keys().map(|(seq, _)| *seq).collect();
        self.ready.clear();
        let mut tokens = Vec::new();
        for ((seq, _), active) in self.active.range_mut::<(i64, i64), _>(..) {
            active.ended = true;
            tokens.push(active.token.clone());
            restart.insert(*seq);
            #[cfg(test)]
            { self.walked += 1; }
        }
        (tokens, restart)
    }

    #[cfg(test)]
    pub(super) fn walked(&self) -> usize { self.walked }

    fn finish(&mut self, seq: i64, position: i64, run: u64, effects: &mut Effects) -> bool {
        if self.active.get(&(seq, position)).is_none_or(|active| active.run != run) { return false; }
        let Some(active) = self.active.remove(&(seq, position)) else { return false };
        self.active_files -= usize::from(active.file_slot);
        effects.push(move || { crate::db::lock::effect(|| active.done.send_replace(true)); });
        true
    }

    fn register(&mut self, seq: i64, position: i64, file_slot: bool) -> (u64, CancelToken) {
        self.next_run += 1;
        let run = self.next_run;
        let token = CancelToken::new();
        self.active_files += usize::from(file_slot);
        self.active.insert((seq, position), ActiveRun { run, token: token.clone(), done: watch::channel(false).0, file_slot, ended: false });
        (run, token)
    }
}

#[cfg(test)]
mod upload_tests;
#[cfg(test)]
mod generation_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod steps_tests;
#[cfg(test)]
mod observe_tests;
#[cfg(test)]
mod outcome_tests;
#[cfg(test)]
mod file_tests;
