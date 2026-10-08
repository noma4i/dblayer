use std::collections::HashSet;

use rusqlite::Connection;
use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, EnvelopeFault, FileOp, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::{read_outbox, DbError, OutboxOperation, Prior, ReferencingRow};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::operation::{
    quarantine_add, ready, transport_verdict, Operation, OutboxEntry as GateEntry, OutboxQueue, QuarantineReason, TransportVerdict,
};
use crate::schema::{OnFailure, RemoteShape};
use crate::store::{PlanCommitError, Reader, StoreError};
use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphQLRequest, Response, ResponseOutcome, RetryClass, TransportError};
use crate::value::read_id;

use super::super::error::SyncError;
use super::super::extract::extract;
use super::super::normalize::{normalize_nodes, Normalized};
use super::super::registry::OperationDef;
use super::super::{locked, Inner};
use super::report::{FlushReport, SendResult, SentOperation, SkipReason};
use super::settle::{response_ops, retarget, Swap};
use super::variables::mutation_variables;

const SETTLE_ATTEMPTS: usize = 3;

pub(crate) enum Delivery {
    Response(Response),
    Rejected(OperationFailure),
    Unreached(OperationFailure),
    Unknown(OperationFailure),
}

pub(crate) struct Sending<'a> {
    pub entry: &'a OutboxOperation,
    pub model: String,
    pub action: String,
    pub generation: Generation,
    pub base_revision: i64,
}

struct InFlight<'a> {
    inner: &'a Inner,
    seq: i64,
    generation: Generation,
    settled: bool,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let failure = OperationFailure::new(FailureKind::Network, "the send was cancelled before an answer was settled");
        let seq = self.seq;
        let generation = self.generation;
        let _ = self.inner.commit(generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure });
            Ok(Some(envelope))
        });
    }
}

fn row_of(entry: &OutboxOperation) -> Option<RowRef> {
    let (model, _) = Operation::split_action_key(&entry.operation.action_key)?;
    Some(RowRef::new(ModelKey::new(&model), entry.operation.row_ids.first()?.clone()))
}

fn message_of(response: &Response) -> String {
    response.errors.first().map_or("empty response".to_string(), |error| error.message.clone())
}

impl Inner {
    pub(crate) async fn flush(&self) -> Result<FlushReport, SyncError> {
        let _serial = self.outbox_gate.lock().await;
        let mut report = FlushReport::default();
        let mut attempted = HashSet::new();
        while let Some(entry) = self.next_ready(&mut attempted, &mut report)? {
            attempted.insert(entry.operation.id.0);
            let result = self.send(&entry).await?;
            report.operations.push(SentOperation { sequence: entry.operation.id.0, action_key: entry.operation.action_key.clone(), result });
        }
        Ok(report)
    }

    fn next_ready(&self, attempted: &mut HashSet<i64>, report: &mut FlushReport) -> Result<Option<OutboxOperation>, SyncError> {
        let entries = self.db.outbox()?;
        let queue = OutboxQueue::new(entries.iter().map(|entry| entry.operation.clone()).collect());
        let gate: Vec<GateEntry> = entries
            .iter()
            .map(|entry| GateEntry::new(entry.operation.id.0, entry.operation.status, entry.operation.transport_started))
            .collect();
        for sequence in ready(&gate) {
            if attempted.contains(&sequence) {
                continue;
            }
            let Some(entry) = entries.iter().find(|entry| entry.operation.id.0 == sequence) else { continue };
            if entry.operation.upload.is_some() {
                continue;
            }
            let row = row_of(entry);
            let held_back = row.as_ref().is_some_and(|row| queue.is_held_before(row, sequence, attempted));
            let blocked = held_back || row.as_ref().is_some_and(|row| queue.is_blocked_before(row, sequence));
            let gate_entry = gate.iter().find(|candidate| candidate.sequence == sequence);
            let waiting = if held_back { None } else { self.waiting_reason(&entry.operation) };
            if waiting.is_none() && transport_verdict(gate_entry, false, blocked) == TransportVerdict::Send {
                return Ok(Some(entry.clone()));
            }
            attempted.insert(sequence);
            report.operations.push(SentOperation {
                sequence,
                action_key: entry.operation.action_key.clone(),
                result: SendResult::Skipped(waiting.unwrap_or(SkipReason::BlockedByHead)),
            });
        }
        Ok(None)
    }

    fn waiting_reason(&self, operation: &Operation) -> Option<SkipReason> {
        if !self.connectivity.is_online() {
            return Some(SkipReason::Offline);
        }
        self.is_waiting(operation).then_some(SkipReason::Waiting)
    }

    async fn send(&self, entry: &OutboxOperation) -> Result<SendResult, SyncError> {
        let Some((model, action)) = Operation::split_action_key(&entry.operation.action_key) else {
            return Ok(SendResult::Skipped(SkipReason::NotBound));
        };
        let spec = self.db.spec(&model)?;
        let Some(declaration) = spec.actions.iter().find(|declared| declared.name == action) else {
            return Ok(SendResult::Skipped(SkipReason::NotBound));
        };
        let Some(mutation) = declaration.mutation.clone() else { return Ok(SendResult::Skipped(SkipReason::NotBound)) };
        let Some(operation) = locked(&self.operations).get(&mutation).cloned() else {
            return Ok(SendResult::Skipped(SkipReason::UnknownOperation(mutation)));
        };
        let variables = mutation_variables(declaration, &entry.input, entry.operation.temp_id.as_ref());
        let sending = Sending { entry, model, action, generation: self.db.generation(), base_revision: self.db.revision_seq()? };
        if !self.start(&sending)? {
            return Ok(SendResult::Discarded);
        }
        let request = GraphQLRequest::new(&operation.document, &operation.name, variables);
        let in_flight = InFlight { inner: self, seq: entry.operation.id.0, generation: sending.generation, settled: false };
        let delivery = self.deliver(request).await;
        let settled = self.settle_delivery(&sending, &operation, delivery, declaration.on_failure == OnFailure::Keep).await;
        let mut in_flight = in_flight;
        in_flight.settled = true;
        if let Err(error) = &settled {
            let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
            self.park_operation(&sending, failure, false, false, true)?;
        }
        settled
    }

    pub(crate) async fn settle_delivery(&self, sending: &Sending<'_>, operation: &OperationDef, delivery: Delivery, keep: bool) -> Result<SendResult, SyncError> {
        let unsettled = match &delivery {
            Delivery::Response(_) => OperationFailure::new(FailureKind::Unrecognized, "the answer arrived after the database was reset"),
            Delivery::Rejected(failure) | Delivery::Unreached(failure) | Delivery::Unknown(failure) => failure.clone(),
        };
        let unreached = matches!(delivery, Delivery::Unreached(_));
        let rejected = matches!(delivery, Delivery::Rejected(_));
        let result = match delivery {
            Delivery::Response(response) => self.settle_response(sending, operation, response).await?,
            Delivery::Rejected(failure) => self.refuse(sending, failure, keep).await?,
            Delivery::Unreached(failure) if self.send_backoff(sending.entry.operation.id.0).allows(sending.entry.operation.attempts + 1) => {
                self.postpone(sending, failure)?
            }
            Delivery::Unreached(failure) => self.refuse(sending, failure, keep).await?,
            Delivery::Unknown(failure) => self.fail(sending, OperationStatus::DeliveryUnknown, failure, SendResult::DeliveryUnknown)?,
        };
        if result == SendResult::Discarded {
            return self.park_operation(sending, unsettled, unreached, rejected, keep || sending.action == "destroy");
        }
        Ok(result)
    }

    fn park_operation(&self, sending: &Sending<'_>, failure: OperationFailure, unreached: bool, rejected: bool, stays_failed: bool) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        for _ in 0..SETTLE_ATTEMPTS {
            let generation = self.db.generation();
            if rejected && !stays_failed {
                match self.db.roll_back_at(seq, generation) {
                    Ok(_) => return Ok(SendResult::Rejected(failure.message.clone())),
                    Err(DbError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            let (transition, result) = match (unreached, rejected) {
                (true, _) => (OperationTransition::Postpone { seq, failure: failure.clone() }, SendResult::Deferred),
                (false, true) => (OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: failure.clone() }, SendResult::Rejected(failure.message.clone())),
                (false, false) => (OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure: failure.clone() }, SendResult::DeliveryUnknown),
            };
            let committed = self.commit(generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
                envelope.operation_transitions.push(transition);
                Ok(Some(envelope))
            })?;
            if committed.is_some() {
                return Ok(result);
            }
        }
        Ok(SendResult::Discarded)
    }

    async fn refuse(&self, sending: &Sending<'_>, failure: OperationFailure, keep: bool) -> Result<SendResult, SyncError> {
        if keep || sending.action == "destroy" {
            let message = failure.message.clone();
            let result = self.fail(sending, OperationStatus::Failed, failure, SendResult::Rejected(message))?;
            if sending.action == "destroy" && result != SendResult::Discarded {
                self.invalidate_model(&sending.model).await?;
            }
            return Ok(result);
        }
        self.roll_back(sending, failure.message).await
    }

    fn postpone(&self, sending: &Sending<'_>, failure: OperationFailure) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let committed = self.commit(sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.operation_transitions.push(OperationTransition::Postpone { seq, failure });
            Ok(Some(envelope))
        })?;
        Ok(if committed.is_some() { SendResult::Deferred } else { SendResult::Discarded })
    }

    fn start(&self, sending: &Sending<'_>) -> Result<bool, SyncError> {
        let seq = sending.entry.operation.id.0;
        let committed = self.commit(sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.operation_transitions.push(OperationTransition::Start { seq });
            Ok(Some(envelope))
        })?;
        Ok(committed.is_some_and(|result| !result.is_noop()))
    }

    pub(crate) async fn deliver(&self, request: GraphQLRequest) -> Delivery {
        locked(&self.counters).record_transport_call();
        match self.transport.mutate(request).await {
            Ok(response) if response.shape().outcome() == ResponseOutcome::Commit => Delivery::Response(response),
            Ok(response) => {
                let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
                let kind = if RetryClass::classify(false, None, &codes) == RetryClass::Retriable { FailureKind::Server } else { FailureKind::Rejected };
                Delivery::Rejected(OperationFailure::new(kind, message_of(&response)))
            }
            Err(error @ TransportError::Network(_)) => Delivery::Unreached(OperationFailure::new(FailureKind::Network, error.to_string())),
            Err(error @ TransportError::Interrupted(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, error.to_string())),
            Err(error @ TransportError::Http { status, .. }) if status >= 500 => {
                Delivery::Unknown(OperationFailure::new(FailureKind::Server, error.to_string()))
            }
            Err(error @ TransportError::Decode(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Unrecognized, error.to_string())),
            Err(error) => Delivery::Rejected(OperationFailure::new(FailureKind::Rejected, error.to_string())),
        }
    }

    async fn settle_response(&self, sending: &Sending<'_>, operation: &OperationDef, response: Response) -> Result<SendResult, SyncError> {
        let data = response.data.unwrap_or(Value::Null);
        let entry = sending.entry;
        let temp = entry.operation.temp_id.clone();
        let normalized = self.read_response(sending, operation, &data);
        let real = normalized.roots.first().and_then(|row| row.get("id")).and_then(read_id);
        if temp.is_some() && (normalized.roots.len() != 1 || real.is_none() || real == temp) {
            return self.quarantine(sending, &data);
        }
        let held = Prior::of(entry).map(|prior| prior.held_files()).unwrap_or_default();
        let mut generation = sending.generation;
        let mut committed = None;
        for _ in 0..SETTLE_ATTEMPTS {
            let lands_rows = generation == sending.generation;
            committed = self.commit(generation, |reader| {
                let outbox = read_outbox(reader)?;
                let mut drops = 0;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
                envelope.entity_ops = response_ops(reader, sending.base_revision, entry.operation.id.0, &sending.model, &normalized, &mut drops)?;
                if let (Some(temp), Some(real)) = (&temp, &real) {
                    let own = entry.operation.id.0;
                    let followed = |other: &&OutboxOperation| other.operation.id.0 != own && other.operation.row_ids.contains(temp);
                    if outbox.iter().filter(followed).any(|follower| follower.operation.action_key.ends_with(":destroy")) {
                        envelope.entity_ops.retain(|operation| !matches!(operation, EntityOp::Upsert { model, id, .. } if model.as_str() == sending.model && id == real));
                        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&sending.model), id: real.clone(), reason: crate::commit::DestroyReason::Local });
                    }
                    envelope.entity_ops.push(EntityOp::Destroy {
                        model: ModelKey::new(&sending.model),
                        id: temp.clone(),
                        reason: crate::commit::DestroyReason::Replaced { by: real.clone() },
                    });
                    let references = self.db.references_to(&sending.model);
                    let swap = Swap { model: &sending.model, temp, real, references: &references };
                    envelope.operation_transitions.extend(outbox.iter().filter(|other| other.operation.id.0 != own).filter_map(|other| retarget(other, &swap)));
                    for ReferencingRow { model, field, mut row } in self.db.rows_referencing(reader, &references, temp)? {
                        let Some(child) = row.get("id").and_then(read_id) else { continue };
                        row.insert(field.clone(), Value::String(real.to_string()));
                        let landing = envelope.entity_ops.iter_mut().find_map(|op| match op {
                            EntityOp::Upsert { model: owner, id, row } if owner.as_str() == model && *id == child => Some(row),
                            _ => None,
                        });
                        match landing {
                            Some(landing) => {
                                landing.insert(field, Value::String(real.to_string()));
                            }
                            None => envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(&model), id: child, row }),
                        }
                    }
                }
                envelope.file_ops.extend(held.iter().filter_map(|file| {
                    Some(FileOp::Remove { model: ModelKey::new(&file.model), row_id: RowId::new(&file.row_id).ok()?, field: file.field.clone() })
                }));
                if !lands_rows {
                    envelope.entity_ops = temp.iter().map(|temp| EntityOp::Destroy { model: ModelKey::new(&sending.model), id: temp.clone(), reason: crate::commit::DestroyReason::Expired }).collect();
                }
                envelope.operation_transitions.push(OperationTransition::Close { seq: entry.operation.id.0, status: OperationStatus::Committed, once: false });
                Ok(Some(envelope))
            })?;
            if committed.is_some() {
                break;
            }
            generation = self.db.generation();
        }
        if committed.is_none() {
            return Ok(SendResult::Discarded);
        }
        self.db.shared.files.discard(&held.iter().map(|file| file.id.clone()).collect::<Vec<_>>());
        self.invalidate_after(&sending.model, &sending.action).await?;
        Ok(SendResult::Committed)
    }

    fn read_response(&self, sending: &Sending<'_>, operation: &OperationDef, data: &Value) -> Normalized {
        if sending.action == "destroy" {
            return Normalized::default();
        }
        extract(data, &operation.root_path, &RemoteShape::Single)
            .and_then(|extracted| normalize_nodes(&self.db, &sending.model, &extracted.nodes))
            .unwrap_or_default()
    }

    fn quarantine(&self, sending: &Sending<'_>, data: &Value) -> Result<SendResult, SyncError> {
        let payload = serde_json::to_vec(data).map_err(|error| SyncError::Malformed(error.to_string()))?;
        self.on_connection(|connection| quarantine_add(connection, &sending.model, &payload, QuarantineReason::ReplaceNormalizeRejected).map(|_| ()))?;
        let failure = OperationFailure::new(FailureKind::Unrecognized, "the response could not be matched to the record");
        self.fail(sending, OperationStatus::DeliveryUnknown, failure, SendResult::Quarantined)
    }

    fn fail(&self, sending: &Sending<'_>, status: OperationStatus, failure: OperationFailure, result: SendResult) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let committed = self.commit(sending.generation, |reader| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            if status == OperationStatus::Failed && sending.action == "destroy" {
                if let Some(prior) = Prior::of(sending.entry) {
                    (envelope.entity_ops, envelope.scope_ops) = prior.reappearing(reader)?;
                }
            }
            envelope.operation_transitions.push(OperationTransition::Fail { seq, status, failure });
            Ok(Some(envelope))
        })?;
        Ok(if committed.is_some() { result } else { SendResult::Discarded })
    }

    async fn roll_back(&self, sending: &Sending<'_>, message: String) -> Result<SendResult, SyncError> {
        match self.db.roll_back_at(sending.entry.operation.id.0, sending.generation) {
            Ok(_) => {}
            Err(DbError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => return Ok(SendResult::Discarded),
            Err(error) => return Err(error.into()),
        }
        Ok(SendResult::Rejected(message))
    }

    pub(crate) fn commit(
        &self,
        generation: Generation,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, SyncError>,
    ) -> Result<Option<CommitResult>, SyncError> {
        match self.db.shared.store.commit_with(generation, plan) {
            Ok(result) => Ok(Some(result)),
            Err(PlanCommitError::Plan(error)) => Err(error),
            Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration))) => Ok(None),
            Err(PlanCommitError::Store(error)) => Err(error.into()),
        }
    }

    fn on_connection(&self, run: impl FnOnce(&Connection) -> rusqlite::Result<()>) -> Result<(), SyncError> {
        let inner = self.db.shared.store.lock();
        run(&inner.connection).map_err(|error| SyncError::Store(error.to_string()))
    }
}
