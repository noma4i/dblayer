use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};

use rusqlite::Connection;
use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, EnvelopeFault, FileOp, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::db::{DbError, OutboxOperation, Prior, TempReplacement};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::operation::{quarantine_add, Operation, QuarantineReason};
use crate::schema::{ActionDeclaration, OnFailure, RemoteShape};
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

enum Delivery {
    Response(Response),
    Rejected(OperationFailure, bool),
    Unreached(OperationFailure),
    Unknown(OperationFailure),
}

pub(crate) struct Sending<'a> {
    pub admitted: &'a crate::lifecycle::Admitted,
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
}

struct Bound {
    model: String,
    action: String,
    declaration: ActionDeclaration,
    mutation: String,
    variables: Value,
}

struct InFlight<'a> {
    admitted: &'a crate::lifecycle::Admitted,
    inner: &'a Inner,
    seq: i64,
    generation: Generation,
    settled: bool,
    attempt: u64,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let mut attempts = locked(&self.inner.final_attempts);
        if attempts.active.get(&(self.generation, self.seq)) != Some(&self.attempt) {
            return;
        }
        attempts.active.remove(&(self.generation, self.seq));
        drop(attempts);
        if self.settled {
            return;
        }
        let failure = OperationFailure::new(FailureKind::Network, "the send was cancelled before an answer was settled");
        let seq = self.seq;
        let generation = self.generation;
        if let Err(error) = self.inner.commit(self.admitted, generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure: failure.clone() });
            Ok(Some(envelope))
        }) { self.inner.fail_commit(self.admitted, error); }
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
            Delivery::Rejected(OperationFailure { kind, message: message_of(&response), extensions: response.extensions() }, false)
        }
        Err(error @ TransportError::Network(_)) => Delivery::Unreached(OperationFailure::new(FailureKind::Network, error.to_string())),
        Err(error @ TransportError::Interrupted(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, error.to_string())),
        Err(error @ TransportError::Http { status, .. }) if status == 408 || status == 429 || replayable && (500..600).contains(&status) => Delivery::Unreached(OperationFailure::new(FailureKind::Server, error.to_string())),
        Err(error @ TransportError::Http { status, .. }) if status >= 500 => Delivery::Unknown(OperationFailure::new(FailureKind::Server, error.to_string())),
        Err(error) if crate::transport::encrypted::permanently_refused(&error) => Delivery::Rejected(OperationFailure::new(FailureKind::Rejected, error.to_string()), true),
        Err(TransportError::Session { code, detail }) => Delivery::Rejected(OperationFailure::new(FailureKind::Session(code), detail), false),
        Err(TransportError::Graphql { messages, extensions }) if retryable => {
            Delivery::Unreached(OperationFailure { kind: FailureKind::Server, message: messages.join("\n"), extensions })
        }
        Err(TransportError::Graphql { messages, extensions }) => {
            let auth = extensions.as_ref().and_then(|fields| fields.code.as_deref()).and_then(crate::transport::SessionCode::from_wire).filter(|code| code.keeps_queue());
            Delivery::Rejected(OperationFailure { kind: auth.map_or(FailureKind::Rejected, FailureKind::Session), message: messages.join("\n"), extensions }, false)
        }
        Err(error @ TransportError::Decode(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
        Err(error) => Delivery::Rejected(OperationFailure::new(FailureKind::Rejected, error.to_string()), false),
    }
}

fn message_of(response: &Response) -> String {
    response.errors.first().map_or("empty response".to_string(), |error| error.message.clone())
}

impl Inner {
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
            admitted.run(async { let pass = self.flush_pass(admitted).await; self.reload_marked(); pass }).await.map_err(SyncError::from)?
        })
    }

    async fn flush_pass(&self, admitted: &crate::lifecycle::Admitted) -> Result<FlushReport, SyncError> {
        let _serial = self.outbox_gate.lock().await;
        let mut report = FlushReport::default();
        if !self.admitted(crate::lifecycle::Origin::Session) { return Ok(report); }
        let mut checked = self.refuse_non_durable(admitted)?;
        locked(self.operation_queue()?).begin_pass();
        let mut cursor = i64::MIN;
        loop {
            if self.refusal_state() != checked {
                checked = self.refuse_non_durable(admitted)?;
            }
            let Some(entry) = crate::blocking::blocking_unit(|| self.next_ready(&mut cursor, &mut report))? else { break };
            if !self.admitted(crate::lifecycle::Origin::Session) { break; }
            if self.refusal_state() != checked {
                checked = self.refuse_non_durable(admitted)?;
                if !locked(self.operation_queue()?).pending.contains(&entry.operation.id.0) { continue; }
            }
            locked(self.operation_queue()?).attempted(entry.operation.id.0);
            locked(&self.backoff_released).remove(&entry.operation.id.0);
            let result = self.send(admitted, &entry, None, Some(checked)).await?; // lock-ok: queue guard ended at statement
            if result == SendResult::Discarded && self.refusal_state() != checked {
                cursor = entry.operation.id.0 - 1;
                continue;
            }
            self.keep_discarded(&entry, &result);
            report.operations.push(SentOperation { sequence: entry.operation.id.0, action_key: entry.operation.action_key.clone(), result });
        }
        locked(self.operation_queue()?).begin_pass();
        Ok(report)
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
    fn refuse_non_durable(&self, admitted: &crate::lifecycle::Admitted) -> Result<RefusalState, SyncError> {
        let state = self.refusal_state();
        let (lost, left, online) = state;
        let pending: Vec<i64> = locked(self.operation_queue()?).pending.iter().copied().collect();
        for seq in pending {
            let of_left_account = seq <= left;
            if !of_left_account && seq > lost && online && seq > self.reopened_through { continue; }
            let Some(entry) = self.outbox_operation(seq)? else { continue };
            let Some((model, action)) = Operation::split_action_key(&entry.operation.action_key) else { continue };
            if self.survives_offline(&model, &action) && !of_left_account { continue; }
            let failure = OperationFailure::new(if of_left_account { FailureKind::Rejected } else { FailureKind::Network }, if of_left_account { format!("{model}.{action} belongs to the account that signed out") } else { format!("{model}.{action} needs the network") });
            let mut kept = self.keep_refusal(&entry, &SendResult::Rejected(failure));
            crate::blocking::blocking_unit(|| self.db.roll_back_at(admitted, seq, self.db.generation()))?;
            kept.landed();
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

    /// The row at the root of an answer is published before the commit that closes the operation, so a waiter that sees the operation closed finds it. A call that waits keeps its result until it reads it or goes away. At most 256 results that nobody waits for stay.
    pub(in crate::sync) fn publish_answer_root(&self, operation: i64, root: Option<RowId>) {
        locked(&self.answers).publish(operation, root);
    }

    fn keep_outcome(&self, entry: &OutboxOperation, result: &SendResult) {
        let Some((model, _)) = Operation::split_action_key(&entry.operation.action_key) else { return };
        let rows = entry.operation.row_ids.iter().cloned().chain(entry.operation.temp_id.clone()).collect();
        locked(&self.outcomes).keep(entry.operation.id.0, &model, &entry.operation.action_key, rows, result);
    }

    fn keep_discarded(&self, entry: &OutboxOperation, result: &SendResult) {
        if *result == SendResult::Discarded {
            self.keep_outcome(entry, result);
        }
    }

    fn keep_refusal(&self, entry: &OutboxOperation, result: &SendResult) -> super::outcomes::KeptRefusal<'_> {
        self.keep_outcome(entry, result);
        super::outcomes::KeptRefusal::new(&self.outcomes, entry.operation.id.0)
    }

    fn refused(&self) {
        #[cfg(test)]
        if let Some(probe) = locked(&self.refused_probe).as_ref() { probe(); }
    }

    fn roll_back_refused(&self, sending: &Sending<'_>, generation: Generation, failure: &OperationFailure) -> Result<(), DbError> {
        let mut kept = self.keep_refusal(sending.entry, &SendResult::Rejected(failure.clone()));
        crate::blocking::blocking_unit(|| self.db.roll_back_at(sending.admitted, sending.entry.operation.id.0, generation))?;
        kept.landed();
        self.refused();
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
        Ok(Some(Bound { model, action, declaration, mutation, variables }))
    }

    async fn send(&self, admitted: &crate::lifecycle::Admitted, entry: &OutboxOperation, resumed: Option<Result<Response, TransportError>>, checked: Option<RefusalState>) -> Result<SendResult, SyncError> {
        let Some((Bound { model, action, declaration, mutation, .. }, generation, base_revision)) = crate::blocking::blocking_unit(|| -> Result<_, SyncError> {
            let Some(bound) = self.bind(entry)? else { return Ok(None) };
            Ok(Some((bound, self.db.generation(), self.db.revision_seq()?)))
        })? else { return Ok(SendResult::Skipped(SkipReason::NotBound)) };
        let sending = Sending { admitted, entry, model, action, generation, base_revision, checked };
        let Some(operation) = locked(&self.operations).get(&mutation).cloned() else {
            let failure = OperationFailure::new(FailureKind::Rejected, format!("operation {mutation} is not registered"));
            return self.fail(&sending, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure));
        };
        self.send_owned(&sending, &operation, resumed, declaration.on_failure == OnFailure::Keep).await
    }

    pub(crate) async fn send_owned(&self, sending: &Sending<'_>, operation: &OperationDef, resumed: Option<Result<Response, TransportError>>, keep: bool) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let key = (sending.generation, seq);
        let attempt = {
            let mut attempts = locked(&self.final_attempts);
            if attempts.active.contains_key(&key) {
                return Ok(SendResult::Discarded);
            }
            attempts.next += 1;
            let attempt = attempts.next;
            attempts.active.insert(key, attempt);
            attempt
        };
        let mut in_flight = InFlight { admitted: sending.admitted, inner: self, seq, generation: sending.generation, attempt, settled: true };
        let Some((entry, base_revision)) = self.start(sending)? else { return Ok(SendResult::Discarded) };
        let sending = Sending { admitted: sending.admitted, entry: &entry, model: sending.model.clone(), action: sending.action.clone(), generation: sending.generation, base_revision, checked: sending.checked };
        in_flight.settled = false;
        let mut request = match self.owned_request(&sending, operation) {
            Ok(request) => request,
            Err(error) => {
                in_flight.settled = true;
                let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
                return self.fail(&sending, OperationStatus::Failed, failure.clone(), SendResult::Rejected(failure));
            }
        };
        request.owner = Some(crate::transport::RequestOwner { seq, item_position: -1, stage: "mutation".into() });
        let delivery = if sending.entry.operation.upload.is_some() {
            match tokio::time::timeout(self.config.upload.step_timeout, self.deliver_or_resume(sending.admitted, request, resumed)).await {
                Ok(delivery) => delivery,
                Err(_) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, "the mutation timed out")),
            }
        } else {
            self.deliver_or_resume(sending.admitted, request, resumed).await
        };
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

    fn owned_request(&self, sending: &Sending<'_>, operation: &OperationDef) -> Result<GraphQLRequest, SyncError> {
        let entry = sending.entry;
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
            request.upload = true;
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
        request.upload = entry.operation.upload.is_some() || self.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM blob_use WHERE consumer_seq = ?1)", [entry.operation.id.0], |row| row.get::<_, bool>(0))?))?;
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

    pub(crate) async fn settle_resumed(self: &std::sync::Arc<Self>, admitted: &crate::lifecycle::Admitted, _serial: &tokio::sync::MutexGuard<'_, ()>, upload: Option<super::super::upload::UploadResume>, request_owner: crate::transport::RequestOwner, result: Result<Response, TransportError>) -> Result<(), SyncError> {
        let settled = crate::blocking::blocking_unit(|| -> Result<_, SyncError> {
            let Some(owner) = self.db.shared.read(&[], |reader| Ok(OutboxOperation::read(reader, request_owner.seq)?))? else { return Ok(None) };
            if matches!(owner.operation.status, OperationStatus::Failed | OperationStatus::DeliveryUnknown) && owner.operation.upload.as_ref().is_none_or(|state| state.stage != crate::upload::UploadStage::Abort) {
                self.db.retry_operations(admitted, vec![owner.clone()])?;
            }
            Ok(self.db.shared.read(&[], |reader| Ok(OutboxOperation::read(reader, owner.operation.id.0)?))?)
        })?;
        let Some(entry) = settled else { return Ok(()) };
        if entry.operation.upload.is_some() {
            if let Some(upload) = upload { self.resume_upload(upload, result).await; }
            return Ok(());
        }
        let sent = self.send(admitted, &entry, Some(result), None).await?;
        self.keep_discarded(&entry, &sent);
        Ok(())
    }

    async fn settle_delivery(&self, sending: &Sending<'_>, operation: &OperationDef, delivery: Delivery, keep: bool) -> Result<SendResult, SyncError> {
        let unsettled = match &delivery {
            Delivery::Response(_) => OperationFailure::new(FailureKind::Unrecognized, "the answer arrived after the database was reset"),
            Delivery::Rejected(failure, _) | Delivery::Unreached(failure) | Delivery::Unknown(failure) => failure.clone(),
        };
        let unreached = matches!(delivery, Delivery::Unreached(_));
        let survives = self.survives_offline(&sending.model, &sending.action);
        let ended_by_network = unreached && !survives;
        let deferred = unreached && survives && self.send_backoff(sending.entry.operation.id.0).allows(sending.entry.operation.attempts + 1);
        let exhausted = unreached && survives && !deferred;
        let keep = !ended_by_network && (keep || exhausted || matches!(&delivery, Delivery::Rejected(_, true)) || matches!(&delivery, Delivery::Rejected(OperationFailure { kind: FailureKind::Session(code), .. }, _) if code.keeps_queue()));
        let rejected = exhausted || ended_by_network || matches!(delivery, Delivery::Rejected(..));
        let result = match delivery {
            Delivery::Response(response) => self.settle_response(sending, operation, response).await?,
            Delivery::Rejected(failure, _) if sending.action == "destroy" && failure.extensions.as_ref().and_then(|fields| fields.code.as_deref()) == Some("NOT_FOUND") => self.land_response(sending, operation, Value::Null).await?,
            Delivery::Rejected(failure, _) => self.refuse(sending, failure, keep).await?,
            Delivery::Unreached(failure) if deferred => {
                self.postpone(sending, failure)?
            }
            Delivery::Unreached(failure) => self.refuse(sending, failure, survives).await?,
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
            let current = Sending { generation, model: sending.model.clone(), action: sending.action.clone(), ..*sending };
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
            if sending.action == "destroy" && result != SendResult::Discarded {
                self.mark_lists_after_refused_destroy(sending.admitted, &sending.model)?;
            }
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
            if let Some(batch) = batch {
                let items = crate::operation::read_operation_uploads(reader.connection(), seq).map_err(StoreError::from)?;
                if items.iter().any(|(key, state)| key.0 == seq && !state.is_sendable()) { return Ok(None); }
                if entry.operation.row_ids.len() < batch.min {
                    let single = spec.actions.iter().find(|action| action.name == batch.single).ok_or_else(|| SyncError::Malformed("single upload declaration is missing".into()))?;
                    let mutation = single.mutation.clone().ok_or_else(|| SyncError::Malformed("single upload mutation is missing".into()))?;
                    envelope.operation_transitions.push(OperationTransition::SeparateUploads { seq, single_action: Operation::upload_action_key(&sending.model, &single.name), single_mutation: mutation });
                    return Ok(Some(envelope));
                }
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
        let replayable = match owner.map(|owner| crate::transport::encrypted::pending_matches(&self.db, owner.seq, owner.item_position, &owner.stage)).transpose() {
            Ok(pending) => pending.unwrap_or(false),
            Err(error) => return Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
        };
        delivery_of(result, replayable)
    }

    async fn deliver_or_resume(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest, resumed: Option<Result<Response, TransportError>>) -> Delivery {
        match resumed {
            Some(result) => {
                let replayable = match request.owner.map(|owner| crate::transport::encrypted::pending_matches(&self.db, owner.seq, owner.item_position, &owner.stage)).transpose() {
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
                replacements.push((RowId::new(temp).map_err(|error| SyncError::Malformed(error.to_string()))?, real));
            }
            if replacements.len() != expected.len() { return self.quarantine(sending, &data); }
        }
        let real = normalized.roots.first().and_then(|row| row.get("id")).and_then(read_id);
        let creatable = normalized.roots.first().is_some_and(|row| spec.normalize(row).is_ok());
        if temp.is_some() && (normalized.roots.len() != 1 || real.is_none() || real == temp || !creatable) {
            return self.quarantine(sending, &data);
        }
        self.keep_answer(spec, sending, operation, &data);
        self.keep_outcome(entry, &SendResult::Committed);
        let mut kept = super::outcomes::KeptAnswer::new(&self.outcomes, entry.operation.id.0);
        let publishes_root = spec.actions.iter().any(|action| action.name == sending.action && action.returns_answer());
        if publishes_root { self.publish_answer_root(entry.operation.id.0, real.clone()); }
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
                envelope.entity_ops = response_ops(&self.db, reader, sending.base_revision, entry.operation.id.0, &sending.model, &normalized, &mut drops)?;
                if let (Some(temp), Some(real)) = (&temp, &real) {
                    self.db.swap_temp(reader, &mut envelope, &outbox, TempReplacement { model: &sending.model, temp, real }, entry.operation.id.0)?;
                }
                for (temp, real) in &replacements {
                    if reader.point(&sending.model, temp.as_str())?.is_some() {
                        self.db.swap_temp(reader, &mut envelope, &outbox, TempReplacement { model: &sending.model, temp, real }, entry.operation.id.0)?;
                    }
                }
                envelope.file_ops.extend(held.iter().filter_map(|file| {
                    Some(FileOp::Remove { model: ModelKey::new(&file.model), row_id: RowId::new(&file.row_id).ok()?, field: file.field.clone() })
                }));
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
                envelope.operation_transitions.push(OperationTransition::Close { seq: entry.operation.id.0, status: OperationStatus::Committed });
                Ok(Some(envelope))
            })?;
            if committed.is_some() {
                break;
            }
            generation = self.db.generation();
        }
        if committed.is_none() {
            if publishes_root { locked(&self.answers).forget(entry.operation.id.0); }
            return Ok(SendResult::Discarded);
        }
        kept.committed();
        #[cfg(test)]
        if let Some(probe) = locked(&self.landed_probe).as_ref() { probe(); }
        self.discard_files(held.iter().map(|file| file.id.clone()).collect()).await?;
        self.discard_files(blob_files).await?;
        self.invalidate_after(sending.admitted, &sending.model, &sending.action)?;
        Ok(SendResult::Committed)
    }

    fn keep_answer(&self, spec: &crate::schema::ModelSpec, sending: &Sending<'_>, operation: &OperationDef, data: &Value) {
        let Some(declared) = spec.actions.iter().find(|declared| declared.name == sending.action).cloned() else { return };
        if declared.result.is_empty() && !declared.upload {
            return;
        }
        let root = declared.root.as_deref().unwrap_or(&operation.root_path);
        let payload = data.get(root.split('.').next().unwrap_or(root));
        let fields = declared.result.iter().map(|(name, kind)| (name.clone(), payload.and_then(|payload| payload.get(name)).filter(|value| answer_kind_matches(kind, value)).cloned().unwrap_or(Value::Null)));
        let entry = sending.entry;
        let rows = entry.operation.row_ids.iter().cloned().chain(entry.operation.temp_id.clone()).collect();
        locked(&self.outcomes).answered(entry.operation.id.0, &sending.model, &entry.operation.action_key, rows, Value::Object(fields.collect()));
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
        let mut kept = self.keep_refusal(sending.entry, &result);
        let committed = self.commit(sending.admitted, sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
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
        kept.landed();
        self.refused();
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
