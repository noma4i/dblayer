use std::collections::HashSet;

use rusqlite::Connection;
use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, EnvelopeFault, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::OutboxEntry;
use crate::identity::{Generation, ModelKey, RowRef};
use crate::operation::{
    clear_transport, mark_transport, quarantine_add, ready, transport_verdict, Operation, OutboxEntry as GateEntry, OutboxQueue,
    QuarantineReason, TransportVerdict,
};
use crate::schema::RemoteShape;
use crate::store::{PlanCommitError, Reader, StoreError};
use crate::transport::{retry_delay, GraphQLRequest, Response, ResponseOutcome, RetryClass, TransportError};
use crate::value::read_id;

use super::super::error::SyncError;
use super::super::extract::extract;
use super::super::normalize::{normalize_nodes, Normalized};
use super::super::registry::OperationDef;
use super::super::{locked, Inner};
use super::report::{FlushReport, SendResult, SentOperation, SkipReason};
use super::settle::{follows, response_ops, retarget, rollback_ops};
use super::variables::mutation_variables;

enum Delivery {
    Response(Response),
    Rejected(String),
    Unknown,
    Unreachable,
}

struct Sending<'a> {
    entry: &'a OutboxEntry,
    model: String,
    action: String,
    generation: Generation,
    base_revision: i64,
}

fn row_of(entry: &OutboxEntry) -> Option<RowRef> {
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
            let deferred = result == SendResult::Deferred;
            report.operations.push(SentOperation { sequence: entry.operation.id.0, action_key: entry.operation.action_key.clone(), result });
            if deferred {
                break;
            }
        }
        Ok(report)
    }

    fn next_ready(&self, attempted: &mut HashSet<i64>, report: &mut FlushReport) -> Result<Option<OutboxEntry>, SyncError> {
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

    async fn send(&self, entry: &OutboxEntry) -> Result<SendResult, SyncError> {
        let sequence = entry.operation.id.0;
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
        self.on_connection(|connection| mark_transport(connection, sequence))?;
        let request = GraphQLRequest::new(&operation.document, &operation.name, variables);
        let result = match self.deliver(request).await {
            Delivery::Response(response) => self.settle_response(&sending, &operation, response).await?,
            Delivery::Rejected(message) => self.roll_back(&sending, message).await?,
            Delivery::Unknown => self.close(&sending, OperationStatus::DeliveryUnknown, SendResult::DeliveryUnknown)?,
            Delivery::Unreachable => {
                self.on_connection(|connection| clear_transport(connection, sequence))?;
                SendResult::Deferred
            }
        };
        Ok(result)
    }

    async fn deliver(&self, request: GraphQLRequest) -> Delivery {
        let mut attempt = 0;
        loop {
            locked(&self.counters).record_transport_call();
            let (class, failure) = match self.transport.mutate(request.clone()).await {
                Ok(response) if response.shape().outcome() == ResponseOutcome::Commit => return Delivery::Response(response),
                Ok(response) => {
                    let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
                    (RetryClass::classify(false, None, &codes), Delivery::Rejected(message_of(&response)))
                }
                Err(TransportError::Network(_)) => (RetryClass::Network, Delivery::Unreachable),
                Err(TransportError::Http { status, .. }) if status >= 500 => return Delivery::Unknown,
                Err(TransportError::Interrupted(_) | TransportError::Decode(_)) => return Delivery::Unknown,
                Err(error) => return Delivery::Rejected(error.to_string()),
            };
            if class.allows_retry(attempt, false) {
                tokio::time::sleep(retry_delay(attempt)).await;
                attempt += 1;
                continue;
            }
            return match class {
                RetryClass::Retriable => Delivery::Unreachable,
                _ => failure,
            };
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
        let followers: Vec<OutboxEntry> = match &temp {
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
        self.close(sending, OperationStatus::DeliveryUnknown, SendResult::Quarantined)
    }

    fn close(&self, sending: &Sending<'_>, status: OperationStatus, result: SendResult) -> Result<SendResult, SyncError> {
        let seq = sending.entry.operation.id.0;
        let committed = self.commit(sending.generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, sending.generation);
            envelope.operation_transitions.push(OperationTransition::Close { seq, status, once: false });
            Ok(Some(envelope))
        })?;
        Ok(if committed.is_some() { result } else { SendResult::Discarded })
    }

    async fn roll_back(&self, sending: &Sending<'_>, message: String) -> Result<SendResult, SyncError> {
        let entry = sending.entry;
        let temp = entry.operation.temp_id.clone();
        let followers: Vec<OutboxEntry> = match &temp {
            Some(temp) => self.db.outbox()?.into_iter().filter(|other| follows(other, temp, entry.operation.id.0)).collect(),
            None => Vec::new(),
        };
        let committed = self.commit(sending.generation, |reader| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, sending.generation);
            envelope.entity_ops = rollback_ops(reader, &sending.model, &sending.action, entry)?;
            envelope.operation_transitions.extend(followers.iter().map(|follower| OperationTransition::Remove { seq: follower.operation.id.0, expected: None }));
            envelope.operation_transitions.push(OperationTransition::Close { seq: entry.operation.id.0, status: OperationStatus::RolledBack, once: false });
            Ok(Some(envelope))
        })?;
        if committed.is_none() {
            return Ok(SendResult::Discarded);
        }
        if sending.action == "destroy" {
            self.invalidate_model(&sending.model).await?;
        }
        Ok(SendResult::Rejected(message))
    }

    fn commit(
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
