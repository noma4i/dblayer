use std::collections::HashSet;

use rusqlite::Connection;
use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, EnvelopeFault, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::OutboxOperation;
use crate::identity::{Generation, ModelKey, RowRef};
use crate::db::{follows, DbError};
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
use super::settle::{response_ops, retarget};
use super::variables::mutation_variables;

pub(crate) enum Delivery {
    Response(Response),
    Rejected(OperationFailure),
    Unknown(OperationFailure),
}

pub(crate) struct Sending<'a> {
    pub entry: &'a OutboxOperation,
    pub model: String,
    pub action: String,
    pub generation: Generation,
    pub base_revision: i64,
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
            let blocked = row_of(entry).is_some_and(|row| queue.is_blocked(&row));
            let gate_entry = gate.iter().find(|candidate| candidate.sequence == sequence);
            if transport_verdict(gate_entry, false, blocked) == TransportVerdict::Send {
                return Ok(Some(entry.clone()));
            }
            attempted.insert(sequence);
            report.operations.push(SentOperation {
                sequence,
                action_key: entry.operation.action_key.clone(),
                result: SendResult::Skipped(SkipReason::BlockedByHead),
            });
        }
        Ok(None)
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
        let delivery = self.deliver(request).await;
        self.settle_delivery(&sending, &operation, delivery, declaration.on_failure == OnFailure::Keep).await
    }

    pub(crate) async fn settle_delivery(&self, sending: &Sending<'_>, operation: &OperationDef, delivery: Delivery, keep: bool) -> Result<SendResult, SyncError> {
        Ok(match delivery {
            Delivery::Response(response) => self.settle_response(sending, operation, response).await?,
            Delivery::Rejected(failure) if keep => {
                let message = failure.message.clone();
                self.fail(sending, OperationStatus::Failed, failure, SendResult::Rejected(message))?
            }
            Delivery::Rejected(failure) => self.roll_back(sending, failure.message).await?,
            Delivery::Unknown(failure) => self.fail(sending, OperationStatus::DeliveryUnknown, failure, SendResult::DeliveryUnknown)?,
        })
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
            Err(error @ TransportError::Network(_)) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, error.to_string())),
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
        let followers: Vec<OutboxOperation> = match &temp {
            Some(temp) => self.db.outbox()?.into_iter().filter(|other| follows(other, temp, entry.operation.id.0)).collect(),
            None => Vec::new(),
        };
        let committed = self.commit(sending.generation, |reader| {
            let mut drops = 0;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.entity_ops = response_ops(reader, sending.base_revision, &sending.model, &normalized, &mut drops)?;
            if let (Some(temp), Some(real)) = (&temp, &real) {
                envelope.entity_ops.push(crate::commit::EntityOp::Destroy {
                    model: ModelKey::new(&sending.model),
                    id: temp.clone(),
                    reason: crate::commit::DestroyReason::Replaced { by: real.clone() },
                });
                envelope.operation_transitions.extend(followers.iter().map(|follower| retarget(follower, temp, real)));
            }
            envelope.operation_transitions.push(OperationTransition::Close { seq: entry.operation.id.0, status: OperationStatus::Committed, once: false });
            Ok(Some(envelope))
        })?;
        if committed.is_none() {
            return Ok(SendResult::Discarded);
        }
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
        let committed = self.commit(sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
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
        if sending.action == "destroy" {
            self.invalidate_model(&sending.model).await?;
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
