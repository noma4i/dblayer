use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use rusqlite::Connection;
use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, EnvelopeFault, FileOp, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::db::{DbError, OutboxOperation, Prior, TempReplacement};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::operation::{quarantine_add, Operation, QuarantineReason};
use crate::schema::{OnFailure, RemoteShape};
use crate::store::{PlanCommitError, Reader, StoreError};
use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphQLRequest, Response, ResponseOutcome, RetryClass, TransportError};
use crate::value::read_id;

use super::super::error::SyncError;
use super::super::extract::{extract, root_value};
use super::super::normalize::{normalize_nodes, Normalized};
use super::super::registry::OperationDef;
use super::super::{locked, Inner};
use super::report::{FlushReport, SendResult, SentOperation, SkipReason};
use super::settle::response_ops;
use super::variables::{idempotent_variables, mutation_variables};

const SETTLE_ATTEMPTS: usize = 3;
const MAX_IN_FLIGHT: usize = 16;

type SendFuture<'a> = Pin<Box<dyn Future<Output = (OutboxOperation, Result<SendResult, SyncError>)> + Send + 'a>>;

async fn next_finished(sends: &mut Vec<SendFuture<'_>>) -> (OutboxOperation, Result<SendResult, SyncError>) {
    std::future::poll_fn(|cx| {
        for index in 0..sends.len() {
            if let Poll::Ready(result) = sends[index].as_mut().poll(cx) {
                drop(sends.swap_remove(index));
                return Poll::Ready(result);
            }
        }
        Poll::Pending
    }).await
}

/// An operation that names the temporary id of another record waits for the sends in flight: the answer of that record's create swaps the id first.
fn names_unconfirmed_record(entry: &OutboxOperation) -> bool {
    let own = entry.operation.temp_id.as_ref();
    entry.operation.row_ids.iter().any(|id| id.is_temp() && Some(id) != own) || mentions_temp(&entry.input, own)
}

fn mentions_temp(value: &Value, own: Option<&RowId>) -> bool {
    match value {
        Value::String(text) => crate::identity::is_temp_id(text) && own.is_none_or(|own| own.as_str() != text),
        Value::Array(items) => items.iter().any(|item| mentions_temp(item, own)),
        Value::Object(fields) => fields.values().any(|item| mentions_temp(item, own)),
        _ => false,
    }
}

enum Delivery {
    Response(Response),
    Rejected(OperationFailure),
    Unreached(OperationFailure),
    Unknown(OperationFailure),
}

pub(crate) struct Sending<'a> {
    pub admitted: &'a crate::lifecycle::Admitted,
    pub effects: std::sync::Arc<DeliveryEffects>,
    pub entry: &'a OutboxOperation,
    pub model: String,
    pub action: String,
    pub generation: Generation,
    pub base_revision: i64,
    pub checked: Option<RefusalState>,
}

pub(crate) type RefusalState = (i64, i64, bool);

#[derive(Default)]
pub(crate) struct FinalAttempts {
    next: u64,
    active: HashMap<(Generation, i64), u64>,
    next_start: Option<tokio::sync::oneshot::Receiver<()>>,
}

struct BatchStep {
    position: i64,
    key: String,
    row: RowId,
    file: Option<String>,
    last: bool,
    confirmed: Vec<(String, String)>,
}

struct Bound {
    model: String,
    action: String,
    mutation: String,
    variables: Value,
}

struct InFlight<'a> {
    admitted: &'a crate::lifecycle::Admitted,
    inner: &'a Inner,
    effects: std::sync::Arc<DeliveryEffects>,
    seq: i64,
    generation: Generation,
    settled: bool,
    attempt: u64,
    before: Option<tokio::sync::oneshot::Receiver<()>>,
    started: Option<tokio::sync::oneshot::Sender<()>>,
}

pub(crate) struct DeliveryEffects {
    pub(crate) pending: std::sync::Mutex<crate::db::dispatch::Effects>,
    pub(crate) inner: std::sync::Weak<Inner>,
}

impl DeliveryEffects {
    pub(crate) fn new(inner: Option<&std::sync::Arc<Inner>>) -> Self {
        Self { pending: std::sync::Mutex::new(crate::db::dispatch::Effects::default()), inner: inner.map(std::sync::Arc::downgrade).unwrap_or_default() }
    }

    pub(crate) fn run(&self) {
        let effects = std::mem::take(&mut *locked(&self.pending));
        effects.run();
    }
}

impl Drop for DeliveryEffects {
    fn drop(&mut self) {
        std::mem::take(self.pending.get_mut().unwrap_or_else(|error| error.into_inner())).run();
    }
}

struct PassEffects<'a> {
    serial: Option<tokio::sync::MutexGuard<'a, ()>>,
    inner: &'a std::sync::Arc<Inner>,
    effects: std::sync::Arc<DeliveryEffects>,
}

impl PassEffects<'_> {
    fn release(&mut self) {
        drop(self.serial.take());
        self.effects.run();
    }

    async fn acquire(&mut self) {
        self.serial = Some(self.inner.outbox_gate.lock().await);
    }
}

impl Drop for PassEffects<'_> {
    fn drop(&mut self) { self.release(); }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut attempts = locked(&self.inner.final_attempts);
        if attempts.active.get(&(self.generation, self.seq)) != Some(&self.attempt) {
            return;
        }
        attempts.active.remove(&(self.generation, self.seq));
        drop(attempts);
        if !self.settled { self.give_up(); }
        locked(&self.inner.outcomes).end_send(self.seq);
    }
}

impl InFlight<'_> {
    fn give_up(&mut self) {
        let failure = OperationFailure::new(FailureKind::Network, "the send was cancelled before an answer was settled");
        let seq = self.seq;
        let generation = self.generation;
        let committed = self.inner.commit(self.admitted, generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure: failure.clone() });
            Ok(Some(envelope))
        });
        match committed {
            Ok(Some(_)) => self.inner.finish_ticket(&self.effects, seq, SendResult::DeliveryUnknown, None, None, Vec::new()),
            Ok(None) => {},
            Err(error) => self.inner.fail_commit(self.admitted, error),
        }
    }
}

fn answer_kind_matches(kind: &str, value: &Value) -> bool {
    match kind {
        "int" => value.is_i64() || value.is_u64(),
        "num" => value.is_number(),
        "bool" => value.is_boolean(),
        _ => value.is_string(),
    }
}

fn delivery_of(result: Result<Response, TransportError>, replayable: bool) -> Delivery {
    let retryable = result.as_ref().err().is_some_and(|error| RetryClass::of_error(error) != RetryClass::Fatal);
    match result {
        Ok(response) if response.shape().outcome() == ResponseOutcome::Commit => Delivery::Response(response),
        Ok(response) => {
            let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
            let auth = codes.iter().filter_map(|code| crate::transport::SessionCode::from_wire(code)).find(|code| code.keeps_queue());
            if auth.is_none() && RetryClass::classify(false, None, &codes) == RetryClass::Retriable {
                return Delivery::Unreached(OperationFailure { kind: FailureKind::Server, message: message_of(&response), extensions: response.extensions() });
            }
            let kind = auth.map_or(FailureKind::Rejected, FailureKind::Session);
            Delivery::Rejected(OperationFailure { kind, message: message_of(&response), extensions: response.extensions() })
        }
        Err(error @ TransportError::Network(_)) => Delivery::Unreached(OperationFailure::new(FailureKind::Network, error.to_string())),
        Err(error @ TransportError::Interrupted(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, error.to_string())),
        Err(error @ TransportError::Http { status, .. }) if status == 408 || status == 429 || replayable && (500..600).contains(&status) => Delivery::Unreached(OperationFailure::new(FailureKind::Server, error.to_string())),
        Err(error @ TransportError::Http { status, .. }) if status >= 500 => Delivery::Unknown(OperationFailure::new(FailureKind::Server, error.to_string())),
        Err(TransportError::Session { code, detail }) => Delivery::Rejected(OperationFailure::new(FailureKind::Session(code), detail)),
        Err(TransportError::Graphql { messages, extensions }) if retryable => {
            Delivery::Unreached(OperationFailure { kind: FailureKind::Server, message: messages.join("\n"), extensions })
        }
        Err(TransportError::Graphql { messages, extensions }) => {
            let auth = extensions.as_ref().and_then(|fields| fields.code.as_deref()).and_then(crate::transport::SessionCode::from_wire).filter(|code| code.keeps_queue());
            Delivery::Rejected(OperationFailure { kind: auth.map_or(FailureKind::Rejected, FailureKind::Session), message: messages.join("\n"), extensions })
        }
        Err(error @ TransportError::Decode(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
        Err(error) => Delivery::Rejected(OperationFailure::new(FailureKind::Rejected, error.to_string())),
    }
}

fn message_of(response: &Response) -> String {
    response.errors.first().map_or("empty response".to_string(), |error| error.message.clone())
}

impl Inner {
    fn finish_ticket(&self, effects: &DeliveryEffects, sequence: i64, result: SendResult, answer: Option<Value>, value: Option<crate::db::Row>, items: Vec<(String, crate::db::Row)>) {
        let ticket = locked(&self.outcomes).take_ticket(sequence);
        if let Some(ticket) = ticket {
            locked(&effects.pending).push(move || ticket.finish(super::FinishedOperation { result, answer, value, items, discarded: Vec::new() }));
        }
    }
    pub(crate) fn fail_commit(&self, admitted: &crate::lifecycle::Admitted, error: SyncError) {
        let stored = self.db.shared.store.lock();
        let life = &self.db.shared.store.lifecycle;
        let line = life.commit_line();
        if !std::sync::Arc::ptr_eq(life, admitted.life()) || !admitted.is_current() { return; }
        *locked(&stored.poison) = Some(error.to_string());
        drop(line);
        drop(stored);
        crate::db::lock::effect(|| self.db.shared.outbox_changes.send_modify(|version| *version += 1));
    }

    /// Boxed here once: the future of a pass is large in a debug build and must not sit inside the future of every caller.
    pub(crate) fn flush<'a>(self: &'a std::sync::Arc<Self>, admitted: &'a crate::lifecycle::Admitted) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<FlushReport, SyncError>> + Send + 'a>> {
        Box::pin(async move {
            admitted.run(self.flush_pass(admitted)).await.map_err(SyncError::from)?
        })
    }

    async fn flush_pass(self: &std::sync::Arc<Self>, admitted: &crate::lifecycle::Admitted) -> Result<FlushReport, SyncError> {
        let mut publish = PassEffects { serial: Some(self.outbox_gate.lock().await), inner: self, effects: std::sync::Arc::new(DeliveryEffects::new(Some(self))) };
        let mut report = FlushReport::default();
        if !self.admitted(crate::lifecycle::Origin::Session) { return Ok(report); }
        publish.release();
        let mut checked = self.refuse_non_durable(admitted, &publish.effects)?;
        publish.release();
        publish.acquire().await;
        locked(self.operation_queue()?).begin_pass();
        let mut cursor = i64::MIN;
        let mut sends: Vec<SendFuture<'_>> = Vec::new();
        let mut flying: std::collections::BTreeMap<i64, Vec<RowRef>> = std::collections::BTreeMap::new();
        let mut changes = self.db.outbox_changes();
        loop {
            if self.refusal_state() != checked {
                publish.release();
                checked = self.refuse_non_durable(admitted, &publish.effects)?;
                publish.release();
                publish.acquire().await;
            }
            changes.borrow_and_update();
            let Some(entry) = crate::blocking::blocking_unit(|| self.next_ready(&mut cursor, &mut report))? else {
                if sends.is_empty() { break; }
                publish.release();
                let completed = tokio::select! {
                    result = next_finished(&mut sends) => Some(result),
                    changed = changes.changed() => { if changed.is_err() { break; } None }
                };
                publish.release();
                publish.acquire().await;
                if let Some((entry, result)) = completed {
                    flying.remove(&entry.operation.id.0);
                    self.record_send(&publish.effects, &mut report, &mut cursor, checked, entry, result?)?;
                }
                continue;
            };
            if !self.admitted(crate::lifecycle::Origin::Session) { break; }
            let rows = crate::operation::row_keys(&entry.operation);
            let row_in_flight = flying.values().flatten().any(|row| rows.contains(row));
            if !sends.is_empty() && (row_in_flight || names_unconfirmed_record(&entry)) {
                while !sends.is_empty() {
                    publish.release();
                    let (finished, result) = next_finished(&mut sends).await;
                    publish.release();
                    publish.acquire().await;
                    flying.remove(&finished.operation.id.0);
                    self.record_send(&publish.effects, &mut report, &mut cursor, checked, finished, result?)?;
                }
                cursor = cursor.min(entry.operation.id.0 - 1);
                continue;
            }
            if self.refusal_state() != checked {
                publish.release();
                checked = self.refuse_non_durable(admitted, &publish.effects)?;
                publish.release();
                publish.acquire().await;
                if !locked(self.operation_queue()?).pending.contains(&entry.operation.id.0) { continue; }
            }
            locked(&self.backoff_released).remove(&entry.operation.id.0);
            let effects = publish.effects.clone();
            let Some(reserved) = self.reserve_send(admitted, entry.operation.id.0, self.db.generation(), effects.clone(), true) else { continue };
            let sequence = entry.operation.id.0;
            let mut send: SendFuture<'_> = Box::pin(async move {
                let result = self.send(admitted, &entry, None, Some(checked), effects, Some(reserved)).await;
                (entry, result)
            });
            publish.release();
            let first = std::future::poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await;
            publish.release();
            publish.acquire().await;
            match first {
                Poll::Ready((entry, result)) => self.record_send(&publish.effects, &mut report, &mut cursor, checked, entry, result?)?,
                Poll::Pending => {
                    sends.push(send);
                    flying.insert(sequence, rows);
                }
            }
            if sends.len() >= MAX_IN_FLIGHT {
                publish.release();
                let (entry, result) = next_finished(&mut sends).await;
                publish.release();
                publish.acquire().await;
                flying.remove(&entry.operation.id.0);
                self.record_send(&publish.effects, &mut report, &mut cursor, checked, entry, result?)?;
            }
            publish.release();
            publish.acquire().await;
        }
        locked(self.operation_queue()?).begin_pass();
        Ok(report)
    }

    fn record_send(&self, effects: &DeliveryEffects, report: &mut FlushReport, cursor: &mut i64, checked: RefusalState, entry: OutboxOperation, result: SendResult) -> Result<(), SyncError> {
        if result == SendResult::Discarded && self.refusal_state() != checked {
            *cursor = (*cursor).min(entry.operation.id.0 - 1);
        }
        if !matches!(result, SendResult::Committed) { locked(self.operation_queue()?).attempted(entry.operation.id.0); }
        if result == SendResult::Discarded && self.outbox_operation(entry.operation.id.0)?.is_none() {
            self.finish_ticket(effects, entry.operation.id.0, SendResult::Discarded, None, None, Vec::new());
        }
        report.operations.push(SentOperation { sequence: entry.operation.id.0, action_key: entry.operation.action_key, result });
        Ok(())
    }

    pub(crate) fn newest_operation(&self) -> Option<i64> {
        let published = self.operation_queue().ok().and_then(|queue| locked(queue).pending.iter().next_back().copied());
        let committed = self.db.shared.store.newest_committed_operation();
        published.into_iter().chain((committed > 0).then_some(committed)).max()
    }

    /// Marks every pending operation as belonging to the account that left when another identity takes the session. They are all committed before the session start: the commit line orders them.
    pub(crate) fn account_changed(&self, session: Option<&str>) {
        let _line = self.network_line.enter();
        let newest = self.newest_operation();
        let mut account = locked(&self.account);
        let leaving = account.0.is_some() && session.is_some() && account.0.as_deref() != session;
        if session.is_some() { account.0 = session.map(str::to_string); }
        if leaving { if let Some(newest) = newest { account.1 = account.1.max(newest); } }
    }

    /// What decides whether a pending operation may still go out: the drop of the network, the account that left and the connection itself. A change of it during a flush means that an operation picked earlier may no longer be sent.
    fn refusal_state(&self) -> RefusalState {
        (*locked(&self.network_lost), locked(&self.account).1, self.connectivity.is_online())
    }

    /// Only sending a message and a media upload wait for the network (`specs/architecture.md` D). Any other action that needs the server is refused and rolled back when the network is away or when it was left over from before this open.
    fn refuse_non_durable(&self, admitted: &crate::lifecycle::Admitted, effects: &std::sync::Arc<DeliveryEffects>) -> Result<RefusalState, SyncError> {
        let state = self.refusal_state();
        let (lost, left, online) = state;
        let pending: Vec<i64> = locked(self.operation_queue()?).pending.iter().copied().collect();
        for seq in pending {
            let of_left_account = seq <= left;
            if !of_left_account && seq > lost && online && seq > self.reopened_through { continue; }
            let Some(entry) = self.outbox_operation(seq)? else { continue };
            let Some((model, action)) = Operation::split_action_key(&entry.operation.action_key) else { continue };
            if self.survives_offline(&model, &action) && !of_left_account { continue; }
            let generation = self.db.generation();
            let Some(_reservation) = self.reserve_send(admitted, seq, generation, effects.clone(), false) else { continue };
            let failure = OperationFailure::new(if of_left_account { FailureKind::Rejected } else { FailureKind::Network }, if of_left_account { format!("{model}.{action} belongs to the account that signed out") } else { format!("{model}.{action} needs the network") });
            let sending = Sending { admitted, effects: effects.clone(), entry: &entry, model, action, generation, base_revision: 0, checked: None };
            if self.keeps_on_refusal(&sending.model, &sending.action)? {
                self.fail(&sending, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure))?;
            } else {
                self.roll_back_refused(&sending, generation, &failure)?;
            }
        }
        #[cfg(test)]
        if let Some(probe) = locked(&self.refusal_probe).as_ref() { probe(); }
        Ok(state)
    }

    /// Only a durable command, an upload and an action that sends nothing wait for the network. Any other action ends with the network error and rolls back.
    pub(crate) fn survives_offline(&self, model: &str, action: &str) -> bool {
        let Ok(spec) = self.db.spec(model) else { return true };
        spec.actions.iter().find(|declared| declared.name == action).is_none_or(|declared| declared.mutation.is_none() || declared.durable || declared.upload)
    }

    fn keeps_on_refusal(&self, model: &str, action: &str) -> Result<bool, SyncError> {
        Ok(self.db.spec(model)?.actions.iter().find(|declared| declared.name == action).is_some_and(|declared| declared.on_failure == OnFailure::Keep))
    }

    fn roll_back_refused(&self, sending: &Sending<'_>, generation: Generation, failure: &OperationFailure) -> Result<(), DbError> {
        crate::blocking::blocking_unit(|| self.db.roll_back_if(sending.admitted, sending.entry.operation.id.0, generation, None))?;
        self.finish_ticket(&sending.effects, sending.entry.operation.id.0, SendResult::Rejected(failure.clone()), None, None, Vec::new());
        Ok(())
    }

    pub(crate) fn operation_queue(&self) -> Result<&std::sync::Arc<std::sync::Mutex<crate::operation::OperationQueue>>, SyncError> {
        self.queue.as_ref().map_err(|error| SyncError::Store(error.clone()))
    }

    pub(crate) fn outbox_operation(&self, seq: i64) -> Result<Option<OutboxOperation>, SyncError> {
        let entry = crate::blocking::blocking_unit(|| self.db.shared.read(&[], |reader| Ok(OutboxOperation::read(reader, seq)?)))?;
        locked(&self.db.shared.counters).outbox_rows_loaded += usize::from(entry.is_some());
        Ok(entry)
    }

    fn next_ready(&self, cursor: &mut i64, report: &mut FlushReport) -> Result<Option<OutboxOperation>, SyncError> {
        loop {
            let next = {
                let queue = locked(self.operation_queue()?);
                queue.pending.range((Excluded(*cursor), Unbounded)).next().and_then(|seq| queue.entries.get(seq)).map(|entry| {
                    let (held, blocked) = queue.blocked_before(&entry.operation);
                    (entry.operation.clone(), held, blocked)
                })
            };
            let Some((operation, held_back, blocked)) = next else { return Ok(None) };
            let sequence = operation.id.0;
            *cursor = sequence;
            locked(&self.counters).outbox_candidates += 1;
            let waiting = if held_back { None } else { self.waiting_reason(&operation)? };
            if waiting.is_none() && !held_back && !blocked {
                if let Some(entry) = self.outbox_operation(sequence)? { return Ok(Some(entry)); }
                continue;
            }
            locked(self.operation_queue()?).attempted(sequence);
            report.operations.push(SentOperation {
                sequence,
                action_key: operation.action_key,
                result: SendResult::Skipped(waiting.unwrap_or(SkipReason::BlockedByHead)),
            });
        }
    }

    fn waiting_reason(&self, operation: &Operation) -> Result<Option<SkipReason>, SyncError> {
        if !self.connectivity.is_online() {
            return Ok(Some(SkipReason::Offline));
        }
        if self.db.shared.read(&[], |reader| Ok(crate::store::blob::waiting(reader.connection(), operation.id.0)?))? { return Ok(Some(SkipReason::Waiting)); }
        Ok(self.is_waiting(operation).then_some(SkipReason::Waiting))
    }

    fn bind(&self, entry: &OutboxOperation) -> Result<Option<Bound>, SyncError> {
        let Some((model, action)) = Operation::split_action_key(&entry.operation.action_key) else { return Ok(None) };
        let spec = self.db.spec(&model)?;
        let Some(declaration) = spec.actions.iter().find(|declared| declared.name == action).cloned() else { return Ok(None) };
        let Some(mutation) = declaration.mutation.clone() else { return Ok(None) };
        let is_id = |field: &str| spec.field(field).is_some_and(|field| matches!(field.kind, crate::schema::FieldKind::Scalar { kind: crate::value::ScalarKind::Id }));
        let Some(input) = self.db.shared.read(&[], |reader| Ok(crate::store::blob::variables(reader.connection(), entry.operation.id.0, &entry.input)?))? else { return Ok(None) };
        let variables = mutation_variables(&declaration, &input, entry.operation.temp_id.as_ref(), &is_id);
        let variables = idempotent_variables(&declaration, &entry.input, variables, &entry.operation.client_mutation_id);
        Ok(Some(Bound { model, action, mutation, variables }))
    }

    async fn send<'a>(&'a self, admitted: &'a crate::lifecycle::Admitted, entry: &OutboxOperation, resumed: Option<Result<Response, TransportError>>, checked: Option<RefusalState>, effects: std::sync::Arc<DeliveryEffects>, reserved: Option<InFlight<'a>>) -> Result<SendResult, SyncError> {
        let Some((Bound { model, action, mutation, .. }, generation, base_revision)) = crate::blocking::blocking_unit(|| -> Result<_, SyncError> {
            let Some(bound) = self.bind(entry)? else { return Ok(None) };
            Ok(Some((bound, self.db.generation(), self.db.revision_seq()?)))
        })? else { return Ok(SendResult::Skipped(SkipReason::NotBound)) };
        let generation = reserved.as_ref().map_or(generation, |held| held.generation);
        let sending = Sending { admitted, effects, entry, model, action, generation, base_revision, checked };
        let operation = locked(&self.operations).get(&mutation).cloned();
        let Some(operation) = operation else {
            let failure = OperationFailure::new(FailureKind::Rejected, format!("operation {mutation} is not registered"));
            return self.fail(&sending, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure));
        };
        let keep = self.keeps_on_refusal(&sending.model, &sending.action)?;
        self.send_reserved(&sending, &operation, resumed, reserved, keep).await
    }

    pub(crate) async fn send_owned<'a>(&'a self, sending: &Sending<'a>, operation: &OperationDef, resumed: Option<Result<Response, TransportError>>, keep: bool) -> Result<SendResult, SyncError> {
        self.send_reserved(sending, operation, resumed, None, keep).await
    }

    fn reserve_send<'a>(&'a self, admitted: &'a crate::lifecycle::Admitted, seq: i64, generation: Generation, effects: std::sync::Arc<DeliveryEffects>, ordered: bool) -> Option<InFlight<'a>> {
        let key = (generation, seq);
        let mut attempts = locked(&self.final_attempts);
        if attempts.active.contains_key(&key) { return None; }
        attempts.next += 1;
        let attempt = attempts.next;
        attempts.active.insert(key, attempt);
        locked(&self.outcomes).begin_send(seq);
        let (before, started) = if ordered {
            let before = attempts.next_start.take();
            let (started, next_start) = tokio::sync::oneshot::channel();
            attempts.next_start = Some(next_start);
            (before, Some(started))
        } else { (None, None) };
        Some(InFlight { admitted, inner: self, effects, seq, generation, attempt, settled: true, before, started })
    }

    async fn send_reserved<'a>(&'a self, sending: &Sending<'a>, operation: &OperationDef, resumed: Option<Result<Response, TransportError>>, reserved: Option<InFlight<'a>>, keep: bool) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let key = (sending.generation, seq);
        let Some(mut in_flight) = reserved.or_else(|| self.reserve_send(sending.admitted, seq, sending.generation, sending.effects.clone(), true)) else { return Ok(SendResult::Discarded) };
        let attempt = in_flight.attempt;
        if let Some(before) = in_flight.before.take() { let _ = before.await; }
        let Some((entry, base_revision)) = self.start(sending)? else { return Ok(SendResult::Discarded) };
        let sending = Sending { admitted: sending.admitted, effects: sending.effects.clone(), entry: &entry, model: sending.model.clone(), action: sending.action.clone(), generation: sending.generation, base_revision, checked: sending.checked };
        in_flight.settled = false;
        let mut request = match self.owned_request(&sending, operation) {
            Ok(request) => request,
            Err(error) => {
                in_flight.settled = true;
                let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
                return self.refuse(&sending, failure, keep).await;
            }
        };
        request.owner = Some(crate::transport::RequestOwner::Operation { seq });
        let admitted = sending.admitted;
        let mut delivery: Pin<Box<dyn Future<Output = Delivery> + Send + '_>> = if sending.entry.operation.upload.is_some() {
            Box::pin(async move {
                match tokio::time::timeout(self.config.upload.step_timeout, self.deliver_or_resume(admitted, request, resumed)).await {
                    Ok(delivery) => delivery,
                    Err(_) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, "the mutation timed out")),
                }
            })
        } else {
            Box::pin(self.deliver_or_resume(admitted, request, resumed))
        };
        let first = std::future::poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
        if let Some(started) = in_flight.started.take() { crate::db::lock::effect(|| { let _ = started.send(()); }); }
        let delivery = match first { Poll::Ready(delivery) => delivery, Poll::Pending => delivery.await };
        if locked(&self.final_attempts).active.get(&key) != Some(&attempt) {
            in_flight.settled = true;
            return Ok(SendResult::Discarded);
        }
        let settled = self.settle_delivery(&sending, operation, delivery, keep).await;
        in_flight.settled = true;
        if let Err(error) = &settled {
            let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
            self.park_operation(&sending, failure, false, false, true)?;
        }
        settled
    }

    /// The next item of a batch below its minimum: it goes through the single mutation as one step of the same operation.
    fn batch_step(&self, sending: &Sending<'_>) -> Result<Option<BatchStep>, SyncError> {
        let entry = sending.entry;
        let Some((_, declared)) = Operation::upload_action(&entry.operation.action_key) else { return Ok(None) };
        if declared == sending.action || !self.db.spec(&sending.model)?.actions.iter().any(|action| action.name == declared && action.batch.is_some()) { return Ok(None) }
        let seq = entry.operation.id.0;
        let read = crate::blocking::blocking_unit(|| self.db.shared.read(&[], |reader| {
            let connection = reader.connection();
            let mut statement = connection.prepare("SELECT position, client_key, row_id, file_id FROM op_item WHERE seq = ?1 ORDER BY position")?;
            let items = statement.query_map([seq], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let confirmed: Vec<(String, String)> = crate::operation::read_value(connection, seq, "confirmed")?.map(serde_json::from_value).transpose().map_err(|error| DbError::Internal(error.to_string()))?.unwrap_or_default();
            Ok((items, confirmed))
        }))?;
        let (items, confirmed) = read;
        let Some((position, key, row, file)) = items.first().cloned() else { return Ok(None) };
        Ok(Some(BatchStep { position, key, row: RowId::new(&row).map_err(|error| SyncError::Malformed(error.to_string()))?, file, last: items.len() == 1, confirmed }))
    }

    fn owned_request(&self, sending: &Sending<'_>, operation: &OperationDef) -> Result<GraphQLRequest, SyncError> {
        let entry = sending.entry;
        if let Some(step) = self.batch_step(sending)? {
            let seq = entry.operation.id.0;
            let spec = self.db.spec(&sending.model)?;
            let declaration = spec.actions.iter().find(|action| action.name == sending.action).ok_or_else(|| SyncError::Malformed("single upload declaration is missing".into()))?;
            let (input, state) = self.db.shared.read(&[], |reader| Ok((crate::operation::read_item_value(reader.connection(), seq, step.position, "input")?, crate::operation::read_operation_uploads(reader.connection(), seq)?.remove(&(seq, step.position)))))?;
            let input: crate::upload::UploadInput = serde_json::from_value(input.unwrap_or(Value::Null)).map_err(|error| SyncError::Malformed(error.to_string()))?;
            let mut state = state.ok_or_else(|| SyncError::Malformed("an upload item has no state".into()))?;
            let signed = state.signed_id.clone().filter(|_| state.is_transferred()).ok_or_else(|| SyncError::Malformed("an upload item is unfinished".into()))?;
            let mut variables = entry.input.as_object().cloned().ok_or_else(|| SyncError::Malformed("upload variables must be an object".into()))?;
            variables.extend(input.variables);
            variables.insert(input.steps.signed_id_variable, Value::String(signed));
            let variables = super::variables::upload_variables(declaration, Value::Object(variables), &format!("{}:{}", entry.operation.client_mutation_id, step.key));
            let mut request = GraphQLRequest::new(&operation.document, &operation.name, variables);
            request.purpose = crate::transport::NetworkPurpose::Transfer;
            state.apply(crate::upload::UploadEvent::MutationBegan { request: crate::upload::UploadRequest { document: request.document.clone(), name: request.operation_name.clone(), variables: request.variables.clone() } }).map_err(|error| SyncError::Malformed(error.to_string()))?;
            self.commit(sending.admitted, sending.generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
                envelope.operation_transitions.push(OperationTransition::Progress { seq, item_position: step.position, state: Box::new(state.clone()) });
                Ok::<_, SyncError>(Some(envelope))
            })?;
            return Ok(request);
        }
        let spec = self.db.spec(&sending.model)?;
        let declaration = spec.actions.iter().find(|action| action.name == sending.action);
        if let Some((declaration, batch)) = declaration.and_then(|action| action.batch.as_ref().map(|batch| (action, batch))) {
            let mut states: Vec<_> = self.db.shared.read(&[], |reader| Ok(crate::operation::read_operation_uploads(reader.connection(), entry.operation.id.0)?))?.into_iter().collect();
            states.sort_by_key(|(key, _)| key.1);
            let mut items = Vec::new();
            for ((seq, position), state) in &states {
                let input = self.db.shared.read(&[], |reader| Ok(crate::operation::read_item_value(reader.connection(), *seq, *position, "input")?))?;
                let input: crate::upload::UploadInput = serde_json::from_value(input.unwrap_or(Value::Null)).map_err(|error| SyncError::Malformed(error.to_string()))?;
                let mut item = input.variables;
                let signed = state.signed_id.as_ref().filter(|_| state.is_transferred()).ok_or_else(|| SyncError::Malformed("an upload item is unfinished".into()))?;
                item.insert(input.steps.signed_id_variable, Value::String(signed.clone()));
                items.push(Value::Object(item));
            }
            let mut variables = entry.input.as_object().cloned().ok_or_else(|| SyncError::Malformed("upload variables must be an object".into()))?;
            variables.insert(batch.items.clone(), Value::Array(items));
            let variables = super::variables::upload_variables(declaration, Value::Object(variables), &entry.operation.client_mutation_id);
            let mut request = GraphQLRequest::new(&operation.document, &operation.name, variables);
            request.purpose = crate::transport::NetworkPurpose::Transfer;
            self.commit(sending.admitted, sending.generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
                for ((seq, position), state) in &states {
                    let mut state = state.clone();
                    state.apply(crate::upload::UploadEvent::MutationBegan { request: crate::upload::UploadRequest { document: request.document.clone(), name: request.operation_name.clone(), variables: request.variables.clone() } }).map_err(|error| SyncError::Malformed(error.to_string()))?;
                    envelope.operation_transitions.push(OperationTransition::Progress { seq: *seq, item_position: *position, state: Box::new(state) });
                }
                Ok(Some(envelope))
            })?;
            return Ok(request);
        }
        let variables = match &entry.operation.upload {
            None => self.bind(entry)?.ok_or_else(|| SyncError::Malformed("the operation has no declaration".into()))?.variables,
            Some(state) => {
                let value = self.db.shared.read(&[], |reader| Ok(crate::operation::read_item_value(reader.connection(), entry.operation.id.0, 0, "input")?))?;
                let input: crate::upload::UploadInput = serde_json::from_value(value.unwrap_or(Value::Null)).map_err(|error| SyncError::Malformed(error.to_string()))?;
                let (Some(signed), Some(_)) = (&state.signed_id, &state.blob_id) else { return Err(SyncError::Malformed("the upload has no stored blob".into())) };
                let mut variables = entry.input.as_object().cloned().ok_or_else(|| SyncError::Malformed("the upload arguments are not an object".into()))?;
                variables.insert(input.steps.signed_id_variable, Value::String(signed.clone()));
                let spec = self.db.spec(&sending.model)?;
                let declaration = spec.actions.iter().find(|action| action.mutation.as_deref() == Some(&operation.name));
                let variables = Value::Object(variables);
                match declaration {
                    Some(declaration) => super::variables::upload_variables(declaration, variables, &entry.operation.client_mutation_id),
                    None => variables,
                }
            }
        };
        let mut request = GraphQLRequest::new(&operation.document, &operation.name, variables);
        if entry.operation.upload.is_some() || self.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM blob_use WHERE consumer_seq = ?1)", [entry.operation.id.0], |row| row.get::<_, bool>(0))?))? {
            request.purpose = crate::transport::NetworkPurpose::Transfer;
        }
        if let Some(mut state) = entry.operation.upload.clone() {
            state.apply(crate::upload::UploadEvent::MutationBegan { request: crate::upload::UploadRequest { document: request.document.clone(), name: request.operation_name.clone(), variables: request.variables.clone() } }).map_err(|error| SyncError::Malformed(error.to_string()))?;
            self.commit(sending.admitted, sending.generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
                envelope.operation_transitions.push(OperationTransition::Progress { seq: entry.operation.id.0, item_position: 0, state: Box::new(state.clone()) });
                Ok(Some(envelope))
            })?;
        }
        Ok(request)
    }

    pub(crate) async fn settle_resumed(self: &std::sync::Arc<Self>, admitted: &crate::lifecycle::Admitted, effects: std::sync::Arc<DeliveryEffects>, upload: Option<super::super::upload::UploadResume>, request_owner: crate::transport::RequestOwner, result: Result<Response, TransportError>) -> Result<(), SyncError> {
        let seq = match &request_owner {
            crate::transport::RequestOwner::Operation { seq } => *seq,
            crate::transport::RequestOwner::Transfer { operation_id, item_id, step } => {
                use rusqlite::OptionalExtension;
                let location: Option<i64> = self.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT seq FROM op_item WHERE client_key = ?1 AND transfer_operation_id = ?2", rusqlite::params![item_id, operation_id], |row| row.get(0)).optional()?))?;
                match location {
                    Some(seq) => seq,
                    None if *step == crate::upload::UploadStage::Abort => {
                        let name = self.db.shared.store.transfer_abort_name(*operation_id, item_id)?;
                        let settled = match (name, result) {
                            (Some(name), Ok(response)) => {
                                let operation = locked(&self.operations).get(&name).cloned().ok_or_else(|| SyncError::Malformed(format!("operation {name} is not registered")))?;
                                super::super::upload::transfer_abort_settled(&name, &operation, response)
                            }
                            _ => false,
                        };
                        if settled {
                            let authority = admitted.authority();
                            self.db.shared.store.settle_transfer_abort_identity(&authority, *operation_id, item_id)?;
                        }
                        return Ok(());
                    }
                    None => return Err(SyncError::Store("the resumed transfer no longer has an item".into())),
                }
            }
            crate::transport::RequestOwner::Unmatched { operation_id } => {
                return Err(SyncError::Store(format!("transfer {operation_id} has no file; select the file again")));
            }
        };
        let settled = crate::blocking::blocking_unit(|| -> Result<_, SyncError> {
            let Some(owner) = self.db.shared.read(&[], |reader| Ok(OutboxOperation::read(reader, seq)?))? else { return Ok(None) };
            if matches!(owner.operation.status, OperationStatus::Failed | OperationStatus::DeliveryUnknown) && owner.operation.upload.as_ref().is_none_or(|state| state.stage != crate::upload::UploadStage::Abort) {
                self.db.retry_operations(admitted, vec![owner.clone()])?;
            }
            Ok(self.db.shared.read(&[], |reader| Ok(OutboxOperation::read(reader, owner.operation.id.0)?))?)
        })?;
        let Some(entry) = settled else { return Ok(()) };
        if let Some(upload) = upload {
            self.resume_upload(upload, result).await;
            return Ok(());
        }
        if matches!(request_owner, crate::transport::RequestOwner::Transfer { .. }) { return Err(SyncError::Store("the resumed transfer is not available".into())); }
        self.send(admitted, &entry, Some(result), None, effects, None).await?;
        Ok(())
    }

    async fn settle_delivery(&self, sending: &Sending<'_>, operation: &OperationDef, delivery: Delivery, keep: bool) -> Result<SendResult, SyncError> {
        let unsettled = match &delivery {
            Delivery::Response(_) => OperationFailure::new(FailureKind::Unrecognized, "the answer arrived after the database was reset"),
            Delivery::Rejected(failure) | Delivery::Unreached(failure) | Delivery::Unknown(failure) => failure.clone(),
        };
        let unreached = matches!(delivery, Delivery::Unreached(_));
        let survives = self.survives_offline(&sending.model, &sending.action);
        let ended_by_network = unreached && !survives;
        let deferred = unreached && survives && self.send_backoff(sending.entry.operation.id.0).allows(sending.entry.operation.attempts + 1);
        let exhausted = unreached && survives && !deferred;
        let keep = !ended_by_network && (keep || exhausted || matches!(&delivery, Delivery::Rejected(OperationFailure { kind: FailureKind::Session(code), .. }) if code.keeps_queue()));
        let rejected = exhausted || ended_by_network || matches!(delivery, Delivery::Rejected(..));
        let result = match delivery {
            Delivery::Response(response) => self.settle_response(sending, operation, response).await?,
            Delivery::Rejected(failure) if sending.action == "destroy" && failure.extensions.as_ref().and_then(|fields| fields.code.as_deref()) == Some("NOT_FOUND") => self.land_response(sending, operation, Value::Null).await?,
            Delivery::Rejected(failure) => self.refuse(sending, failure, keep).await?,
            Delivery::Unreached(failure) if deferred => {
                self.postpone(sending, failure)?
            }
            Delivery::Unreached(failure) => self.refuse(sending, failure, keep).await?,
            Delivery::Unknown(failure) => self.fail(sending, OperationStatus::DeliveryUnknown, failure, SendResult::DeliveryUnknown)?,
        };
        if result == SendResult::Discarded {
            return self.park_operation(sending, unsettled, deferred, rejected, keep || sending.action == "destroy");
        }
        Ok(result)
    }

    fn park_operation(&self, sending: &Sending<'_>, failure: OperationFailure, deferred: bool, rejected: bool, stays_failed: bool) -> Result<SendResult, SyncError> {
        for _ in 0..SETTLE_ATTEMPTS {
            let generation = self.db.generation();
            if rejected && !stays_failed {
                match self.roll_back_refused(sending, generation, &failure) {
                    Ok(()) => return Ok(SendResult::Rejected(failure)),
                    Err(DbError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => continue,
                    Err(DbError::UnknownOperation(_)) => return Ok(SendResult::Discarded),
                    Err(error) => return Err(error.into()),
                }
            }
            let current = Sending { generation, effects: sending.effects.clone(), model: sending.model.clone(), action: sending.action.clone(), ..*sending };
            let result = match (deferred, rejected) {
                (true, _) => self.postpone(&current, failure.clone())?,
                (false, true) => self.fail(&current, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure.clone()))?,
                (false, false) => self.fail(&current, OperationStatus::DeliveryUnknown, failure.clone(), SendResult::DeliveryUnknown)?,
            };
            if result != SendResult::Discarded {
                return Ok(result);
            }
        }
        Ok(SendResult::Discarded)
    }

    async fn refuse(&self, sending: &Sending<'_>, failure: OperationFailure, keep: bool) -> Result<SendResult, SyncError> {
        if keep || sending.action == "destroy" {
            let result = self.fail(sending, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure))?;
            return Ok(result);
        }
        self.roll_back(sending, failure).await
    }

    fn postpone(&self, sending: &Sending<'_>, failure: OperationFailure) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let committed = self.commit(sending.admitted, sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.operation_transitions.push(OperationTransition::Postpone { seq, failure: failure.clone() });
            Ok(Some(envelope))
        })?;
        Ok(if committed.is_some() { SendResult::Deferred } else { SendResult::Discarded })
    }

    fn start(&self, sending: &Sending<'_>) -> Result<Option<(OutboxOperation, i64)>, SyncError> {
        let seq = sending.entry.operation.id.0;
        let spec = self.db.spec(&sending.model)?;
        let batch = spec.actions.iter().find(|action| action.name == sending.action).and_then(|action| action.batch.as_ref());
        let mut frozen = None;
        let committed = self.commit(sending.admitted, sending.generation, |reader| {
            frozen = None;
            let Some(entry) = OutboxOperation::read(reader, seq)? else { return Ok(None) };
            if entry.operation.status != OperationStatus::Pending || entry.operation.transport_started || entry.operation.client_mutation_id != sending.entry.operation.client_mutation_id { return Ok(None); }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            if let Some(checked) = sending.checked {
                let (lost, account, connectivity) = (self.network_lost.clone(), self.account.clone(), self.connectivity.clone());
                envelope.network = Some(crate::commit::NetworkAdmission::new(checked, std::sync::Arc::new(move || (*locked(&lost), locked(&account).1, connectivity.is_online())), self.network_line.clone()));
            }
            if batch.is_some() {
                let items = crate::operation::read_operation_uploads(reader.connection(), seq).map_err(StoreError::from)?;
                if items.iter().any(|(key, state)| key.0 == seq && !state.is_sendable()) { return Ok(None); }
            }
            frozen = Some((entry, reader.revision_seq()?));
            envelope.operation_transitions.push(OperationTransition::Start { seq });
            Ok(Some(envelope))
        })?;
        Ok(committed.filter(|result| !result.is_noop()).and(frozen))
    }

    /// A send that the end of the session or the close of the database cancels. The answer of an ended session is never read.
    pub(crate) async fn mutate_scoped(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> {
        admitted.run(self.transport.mutate(admitted, request)).await?
    }

    async fn deliver(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Delivery {
        locked(&self.counters).record_transport_call();
        let owner = request.owner.clone();
        crate::db::lock::assert_no_library_lock("Transport::mutate");
        let result = self.mutate_scoped(admitted, request).await;
        let replayable = match owner.as_ref().map(|owner| crate::transport::encrypted::pending_matches(&self.db, owner)).transpose() {
            Ok(pending) => pending.unwrap_or(false),
            Err(error) => return Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
        };
        delivery_of(result, replayable)
    }

    async fn deliver_or_resume(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest, resumed: Option<Result<Response, TransportError>>) -> Delivery {
        match resumed {
            Some(result) => {
                let replayable = match request.owner.as_ref().map(|owner| crate::transport::encrypted::pending_matches(&self.db, owner)).transpose() {
                    Ok(pending) => pending.unwrap_or(false),
                    Err(error) => return Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
                };
                delivery_of(result, replayable)
            }
            None => self.deliver(admitted, request).await,
        }
    }

    pub(super) async fn settle_response(&self, sending: &Sending<'_>, operation: &OperationDef, response: Response) -> Result<SendResult, SyncError> {
        let data = response.data.unwrap_or(Value::Null);
        let declared = self.db.spec(&sending.model)?.actions.iter().find(|declared| declared.name == sending.action).cloned();
        let root = declared.as_ref().and_then(|declared| declared.root.as_deref()).unwrap_or(&operation.root_path);
        let landed = declared.iter().flat_map(|declared| &declared.lands).any(|landing| root_value(&data, &landing.root).is_some_and(|value| value.is_object() || value.is_array()));
        let answers_from_payload = declared.as_ref().is_some_and(|declared| !declared.result.is_empty()) && root.rsplit_once('.').is_some_and(|(parent, _)| root_value(&data, parent).is_some_and(Value::is_object));
        let readable = landed
            || match root_value(&data, root) {
                Some(Value::Null) => sending.action == "destroy" || answers_from_payload,
                Some(Value::Object(_) | Value::Array(_)) => true,
                _ => false,
            };
        if !readable {
            return self.quarantine(sending, &data);
        }
        self.land_response(sending, operation, data).await
    }

    async fn land_response(&self, sending: &Sending<'_>, operation: &OperationDef, data: Value) -> Result<SendResult, SyncError> {
        let blob_files = crate::blocking::blocking_unit(|| self.db.shared.read(&[], |reader| Ok(crate::store::blob::files(reader.connection(), sending.entry.operation.id.0)?)))?;
        let entry = sending.entry;
        let temp = entry.operation.temp_id.clone();
        let spec = self.db.spec(&sending.model)?;
        let (normalized, landings) = match self.read_response(spec, sending, operation, &data) {
            Ok(read) => read,
            Err(SyncError::Malformed(_)) => return self.quarantine(sending, &data),
            Err(error) => return Err(error),
        };
        let batch_action = spec.actions.iter().find(|action| action.name == sending.action && action.batch.is_some());
        let mut replacements = Vec::new();
        let mut batch_items = Vec::new();
        if let Some(action) = batch_action {
            let expected = crate::blocking::blocking_unit(|| self.db.shared.read(&[], |reader| {
                let mut statement = reader.connection().prepare("SELECT row_id, client_key FROM op_item WHERE seq = ?1 ORDER BY position")?;
                let rows = statement.query_map([entry.operation.id.0], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            }))?;
            let field = &action.correlate[0];
            let mut seen = HashSet::new();
            for row in &normalized.roots {
                let key = row.get(field).and_then(Value::as_str);
                let real = row.get("id").and_then(read_id);
                let found = expected.iter().find(|(_, expected)| Some(expected.as_str()) == key);
                let (Some((temp, _)), Some(real), Some(key)) = (found, real, key) else { return self.quarantine(sending, &data) };
                if !seen.insert(key) || real.is_temp() || spec.normalize(row).is_err() { return self.quarantine(sending, &data); }
                batch_items.push((key.to_string(), real.clone()));
                replacements.push((RowId::new(temp).map_err(|error| SyncError::Malformed(error.to_string()))?, real));
            }
            if replacements.len() != expected.len() { return self.quarantine(sending, &data); }
        }
        let step = self.batch_step(sending)?;
        let mut step_real = None;
        if let Some(step) = &step {
            let row = normalized.roots.first();
            let matches_key = spec.actions.iter().find(|action| action.name == sending.action).and_then(|action| action.correlate.first()).is_none_or(|field| row.and_then(|row| row.get(field)).and_then(Value::as_str) == Some(step.key.as_str()));
            let Some(real) = row.and_then(|row| row.get("id")).and_then(read_id).filter(|real| !real.is_temp() && normalized.roots.len() == 1 && matches_key) else { return self.quarantine(sending, &data) };
            batch_items.extend(step.confirmed.iter().filter_map(|(item, row)| RowId::new(row).ok().map(|row| (item.clone(), row))));
            batch_items.push((step.key.clone(), real.clone()));
            replacements.push((step.row.clone(), real.clone()));
            step_real = Some(real);
        }
        let real = normalized.roots.first().and_then(|row| row.get("id")).and_then(read_id);
        let creatable = normalized.roots.first().is_some_and(|row| spec.normalize(row).is_ok());
        if temp.is_some() && (normalized.roots.len() != 1 || real.is_none() || real == temp || !creatable) {
            return self.quarantine(sending, &data);
        }
        let answer = self.answer_fields(spec, sending, operation, &data);
        let mark = self.prepare_window_event(super::super::WindowEvent::Mutation { model: sending.model.clone(), action: sending.action.clone() })?;
        let held = Prior::of(entry)?.map(|prior| prior.held_files()).unwrap_or_default();
        let owned: Vec<RowId> = entry.operation.row_ids.iter().chain(&temp).cloned().collect();
        let created = Prior::of(entry)?.map(|prior| prior.created_beyond(&owned)).unwrap_or_default();
        let mut generation = sending.generation;
        let mut committed = None;
        for _ in 0..SETTLE_ATTEMPTS {
            let lands_rows = generation == sending.generation;
            committed = self.commit(sending.admitted, generation, |reader| {
                let ids: BTreeSet<_> = temp.iter().chain(replacements.iter().map(|(temp, _)| temp)).chain(created.iter().map(|(_, temp)| temp)).map(ToString::to_string).collect();
                let outbox = crate::operation::referencing_sequences(reader.connection(), &ids).map_err(StoreError::from)?.into_iter().filter_map(|seq| OutboxOperation::read(reader, seq).transpose()).collect::<Result<Vec<_>, _>>()?;
                let mut drops = 0;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
                envelope.window_debts = mark.as_ref().map(super::super::WindowMark::debts).unwrap_or_default();
                envelope.snapshot = real.as_ref().or_else(|| entry.operation.row_ids.first()).map(|id| RowRef::new(ModelKey::new(&sending.model), id.clone()));
                envelope.item_snapshots = batch_items.iter().map(|(item, id)| (item.clone(), RowRef::new(ModelKey::new(&sending.model), id.clone()))).collect();
                envelope.entity_ops = response_ops(&self.db, reader, sending.base_revision, entry.operation.id.0, &sending.model, &normalized, &mut drops)?;
                if let (Some(temp), Some(real)) = (&temp, &real) {
                    self.db.swap_temp(reader, &mut envelope, &outbox, TempReplacement { model: &sending.model, temp, real }, entry.operation.id.0)?;
                }
                for (temp, real) in &replacements {
                    if reader.point(&sending.model, temp.as_str())?.is_some() {
                        self.db.swap_temp(reader, &mut envelope, &outbox, TempReplacement { model: &sending.model, temp, real }, entry.operation.id.0)?;
                    }
                }
                if step.as_ref().is_none_or(|step| step.last) {
                    envelope.file_ops.extend(held.iter().filter_map(|file| {
                        Some(FileOp::Remove { model: ModelKey::new(&file.model), row_id: RowId::new(&file.row_id).ok()?, field: file.field.clone() })
                    }));
                }
                if !lands_rows {
                    envelope.entity_ops = temp.iter().map(|temp| EntityOp::Destroy { model: ModelKey::new(&sending.model), id: temp.clone(), reason: crate::commit::DestroyReason::Expired })
                        .chain(created.iter().map(|(model, temp)| EntityOp::Destroy { model: ModelKey::new(model), id: temp.clone(), reason: crate::commit::DestroyReason::Expired })).collect();
                } else {
                    for (created_model, created_temp) in &created {
                        if reader.point(created_model, created_temp.as_str())?.is_none() { continue; }
                        let answers: Vec<RowId> = landings.iter().filter(|landed| landed.model.as_str() == created_model.as_str()).map(|landed| landed.id.clone())
                            .chain(normalized.roots.iter().filter(|_| *created_model == sending.model).filter_map(|row| row.get("id").and_then(read_id)))
                            .filter(|answer| answer != created_temp && !owned.contains(answer)).collect();
                        match answers.as_slice() {
                            [answer] => self.db.swap_temp(reader, &mut envelope, &outbox, TempReplacement { model: created_model, temp: created_temp, real: answer }, entry.operation.id.0)?,
                            _ => envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(created_model), id: created_temp.clone(), reason: crate::commit::DestroyReason::Expired }),
                        }
                    }
                    for op in &envelope.entity_ops {
                        let EntityOp::Upsert { model, id, row } = op else { continue };
                        if landings.contains(&RowRef::new(model.clone(), id.clone())) {
                            envelope.scope_ops.extend(self.joins_of(reader, model.as_str(), id, row)?);
                        }
                    }
                }
                match (&step, &step_real) {
                    (Some(step), Some(real)) if !step.last => envelope.operation_transitions.push(OperationTransition::ConfirmUploadItem { seq: entry.operation.id.0, position: step.position, item: step.key.clone(), temp: step.row.to_string(), row: real.to_string() }),
                    _ => envelope.operation_transitions.push(OperationTransition::Close { seq: entry.operation.id.0, status: OperationStatus::Committed }),
                }
                Ok(Some(envelope))
            })?;
            if committed.is_some() {
                break;
            }
            generation = self.db.generation();
        }
        let Some(committed) = committed else { return Ok(SendResult::Discarded) };
        self.queue_window_mark(sending.admitted, &sending.effects, mark);
        if let Some(step) = step.filter(|step| !step.last) {
            self.discard_files(sending.admitted, step.file.into_iter().collect()).await?;
            return Ok(SendResult::Committed);
        }
        let value = committed.snapshot.map(|row| self.db.shared.overlay_snapshot(&sending.model, row)).transpose()?;
        let items = committed.item_snapshots.into_iter().filter_map(|(item, row)| row.map(|row| (item, row))).map(|(item, row)| self.db.shared.overlay_snapshot(&sending.model, row).map(|row| (item, row))).collect::<Result<Vec<_>, _>>()?;
        self.finish_ticket(&sending.effects, entry.operation.id.0, SendResult::Committed, answer, value, items);
        self.discard_files(sending.admitted, held.iter().map(|file| file.id.clone()).collect()).await?;
        self.discard_files(sending.admitted, blob_files).await?;
        Ok(SendResult::Committed)
    }

    fn answer_fields(&self, spec: &crate::schema::ModelSpec, sending: &Sending<'_>, operation: &OperationDef, data: &Value) -> Option<Value> {
        let declared = spec.actions.iter().find(|declared| declared.name == sending.action).cloned()?;
        if declared.result.is_empty() && !declared.upload {
            return None;
        }
        let root = declared.root.as_deref().unwrap_or(&operation.root_path);
        let payload = data.get(root.split('.').next().unwrap_or(root));
        let fields = declared.result.iter().map(|(name, kind)| (name.clone(), payload.and_then(|payload| payload.get(name)).filter(|value| answer_kind_matches(kind, value)).cloned().unwrap_or(Value::Null)));
        Some(Value::Object(fields.collect()))
    }

    fn read_response(&self, spec: &crate::schema::ModelSpec, sending: &Sending<'_>, operation: &OperationDef, data: &Value) -> Result<(Normalized, Vec<RowRef>), SyncError> {
        let ignored = spec.actions.iter().any(|declared| declared.name == sending.action && declared.ignore_response);
        if sending.action == "destroy" || ignored {
            return Ok((Normalized::default(), Vec::new()));
        }
        let declared = spec.actions.iter().find(|declared| declared.name == sending.action).cloned();
        let root = declared.as_ref().and_then(|declared| declared.root.as_deref()).unwrap_or(&operation.root_path);
        let shape = if declared.as_ref().is_some_and(|action| action.batch.is_some()) { RemoteShape::List { coverage: crate::schema::Coverage::Complete } } else { RemoteShape::Single };
        let mut normalized = self.response_rows(&sending.model, data, root, &shape)?;
        let mut landings = Vec::new();
        for landing in declared.iter().flat_map(|declared| &declared.lands) {
            let shape = if root_value(data, &landing.root).is_some_and(Value::is_array) { RemoteShape::List { coverage: crate::schema::Coverage::Complete } } else { RemoteShape::Single };
            let landed = self.response_rows(&landing.model, data, &landing.root, &shape)?;
            let model = ModelKey::new(&landing.model);
            landings.extend(landed.roots.iter().filter_map(|row| row.get("id").and_then(read_id)).map(|id| RowRef::new(model.clone(), id)));
            normalized.related.extend(landed.roots.into_iter().map(|row| (model.clone(), row)));
            normalized.related.extend(landed.related);
            normalized.dropped += landed.dropped;
        }
        normalized.coalesce(&sending.model);
        Ok((normalized, landings))
    }

    fn response_rows(&self, model: &str, data: &Value, root: &str, shape: &RemoteShape) -> Result<Normalized, SyncError> {
        if *shape == RemoteShape::Single && root_value(data, root).is_some_and(Value::is_array) {
            return Err(SyncError::Malformed(format!("{root}: expected a response row")));
        }
        let extracted = extract(data, root, shape, None)?;
        let acknowledgements = extracted.nodes.iter().filter(|row| !row.is_empty() && row.values().all(Value::is_null)).count();
        let normalized = normalize_nodes(&self.db, model, &extracted.nodes)?;
        if normalized.dropped > acknowledgements {
            return Err(SyncError::Malformed(format!("{root}: unreadable response row")));
        }
        Ok(normalized)
    }

    fn quarantine(&self, sending: &Sending<'_>, data: &Value) -> Result<SendResult, SyncError> {
        self.write_connection(sending.admitted, |connection| quarantine_add(connection, Some(&sending.model), data, QuarantineReason::ReplaceNormalizeRejected).map(|_| ()))?;
        let failure = OperationFailure::new(FailureKind::Unrecognized, "the response could not be matched to the record");
        self.fail(sending, OperationStatus::DeliveryUnknown, failure, SendResult::Quarantined)
    }

    fn fail(&self, sending: &Sending<'_>, status: OperationStatus, failure: OperationFailure, result: SendResult) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let mark = if status == OperationStatus::Failed && sending.action == "destroy" { self.prepare_window_event(super::super::WindowEvent::DestroyRefused { model: sending.model.clone() })? } else { None };
        let committed = self.commit(sending.admitted, sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.window_debts = mark.as_ref().map(super::super::WindowMark::debts).unwrap_or_default();
            if status == OperationStatus::Failed && sending.action == "destroy" {
                if let Some(prior) = Prior::of(sending.entry)? {
                    (envelope.entity_ops, envelope.scope_ops) = prior.reappearing();
                }
            }
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status, failure: failure.clone() });
            Ok(Some(envelope))
        })?;
        if committed.is_none() {
            return Ok(SendResult::Discarded);
        }
        self.queue_window_mark(sending.admitted, &sending.effects, mark);
        self.finish_ticket(&sending.effects, seq, result.clone(), None, None, Vec::new());
        Ok(result)
    }

    async fn roll_back(&self, sending: &Sending<'_>, failure: OperationFailure) -> Result<SendResult, SyncError> {
        match self.roll_back_refused(sending, sending.generation, &failure) {
            Ok(()) => {}
            Err(DbError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration)) | DbError::UnknownOperation(_)) => return Ok(SendResult::Discarded),
            Err(error) => return Err(error.into()),
        }
        Ok(SendResult::Rejected(failure))
    }

    pub(crate) fn commit(
        &self,
        admitted: &crate::lifecycle::Admitted,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, SyncError>,
    ) -> Result<Option<CommitResult>, SyncError> {
        let authority = admitted.authority();
        crate::blocking::blocking_unit(|| match self.db.commit_following(&authority, generation, plan) {
            Ok(result) => Ok(Some(result)),
            Err(PlanCommitError::Plan(error)) => Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration | EnvelopeFault::NetworkMoved))) => Ok(None),
            Err(PlanCommitError::Store(error)) => Err(error.into()),
        })
    }

    pub(crate) fn commit_deferred(
        &self,
        admitted: &crate::lifecycle::Admitted,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, SyncError>,
    ) -> Result<Option<crate::store::Committed<'_>>, SyncError> {
        let authority = admitted.authority();
        crate::blocking::blocking_unit(|| match self.db.commit_following_deferred(&authority, generation, plan) {
            Ok((committed, _)) => Ok(Some(committed)),
            Err(PlanCommitError::Plan(error)) => Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration | EnvelopeFault::NetworkMoved))) => Ok(None),
            Err(PlanCommitError::Store(error)) => Err(error.into()),
        })
    }

    fn write_connection(&self, admitted: &crate::lifecycle::Admitted, run: impl FnOnce(&Connection) -> rusqlite::Result<()>) -> Result<(), SyncError> {
        let authority = admitted.authority();
        self.db.shared.store.write(&authority, || SyncError::Store("the session ended while its answer was pending".into()), |inner| run(&inner.connection).map_err(|error| SyncError::Store(error.to_string())))
    }
}
