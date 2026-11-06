//! The calls of the platform that write: `create`, `update`, `act`, `destroy`, `set_local`, `outcome`, `retry` and `discard`.
//! Each call reads the base once (`db/plan_write.rs`), hands the effects to the owner Outbox (a mutation) or to the local writes of the same owner (a local model, an action without a mutation), and waits for the answer of the owner.

use std::sync::Arc;

use serde_json::{json, Map, Value};
use crate::owners::store::{EffectRow, IntentEffects};

use super::database::id;
use super::receipt::{receipt_of, Ticket, WireOperationOutcome, WireOperationReceipt};
use super::{Database, DbError};
use crate::store::Planned;
use crate::owners::life::LifeReply;
use crate::owners::write::outbox::{Fault, LocalWriteKind, NewOp, OpSource, OutboxAnswer, OutboxOutcome, Receipt};
use crate::runtime::Time;
use crate::transport::GraphQLRequest;

/// The bytes of a file with the name and the mime that `attach` stores in the field.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FileBytes {
    pub bytes: Vec<u8>,
    pub name: String,
    pub mime: String,
}

/// The error of a call that an owner refused.
pub(super) fn refused(fault: Fault, what: &str) -> DbError {
    use crate::owners::contract::RejectReason;
    match fault {
        Fault::NoSession => DbError::NoSession { detail: "the database has no session in this epoch".into() },
        Fault::NotFound => DbError::NotFound { detail: format!("{what}: there is no such operation") },
        Fault::InFlight => DbError::Invalid { detail: format!("{what}: the operation is in flight") },
        Fault::Interrupted => super::command::ended(),
        Fault::Offline => DbError::Network { detail: format!("{what} needs the network") },
        Fault::Closed => super::command::closed(),
        Fault::RestoreFailed => DbError::Invalid { detail: format!("{what}: the stored operation cannot be read, discard it") },
        Fault::StateLost => DbError::Session { code: crate::transport::SessionCode::StateLost, detail: "the pending request of the session was lost".into() },
        Fault::Internal => DbError::Database { detail: format!("{what}: internal error") },
        Fault::Invalid => DbError::Invalid { detail: format!("{what}: the owner refused the call") },
        Fault::Copy(detail) => DbError::Database { detail },
        Fault::Rejected(RejectReason::NoSession) => DbError::NoSession { detail: "the session of this write ended".into() },
        Fault::Rejected(RejectReason::Closed) => super::command::closed(),
        Fault::Rejected(RejectReason::Storage(detail)) => DbError::Database { detail },
        Fault::Rejected(RejectReason::StaleGeneration { .. } | RejectReason::Invalid) => DbError::Invalid { detail: format!("{what}: the write was refused") },
    }
}

pub(super) fn failure_of(error: &str) -> super::wire::WireFailure { super::wire::failure(&crate::owners::write::failure::parse(error)) }

impl Database {
    async fn outbox_answer(&self, waiter: crate::runtime::Waiter<LifeReply>, what: &str) -> Result<OutboxAnswer, DbError> {
        match self.await_life(waiter).await? {
            LifeReply::Outbox(OutboxAnswer::Err(fault)) => Err(refused(fault, what)),
            LifeReply::Outbox(answer) => Ok(answer),
            _ => Err(super::command::ended()),
        }
    }

    /// The id a row has now: a temporary id of a `create` that landed gives the real one.
    pub(super) async fn resolved(&self, epoch: u64, model: &str, row: crate::identity::RowId) -> Result<crate::identity::RowId, DbError> {
        if self.life.is_shut() { return Err(super::command::closed()); }
        if !row.is_temp() { return Ok(row); }
        match self.await_life(self.life.resolve_row(epoch, model.to_string(), row.to_string())).await? {
            LifeReply::Outbox(OutboxAnswer::Resolved(resolved)) => id(&resolved),
            _ => Err(super::command::ended()),
        }
    }

    pub(super) fn now(&self) -> Time { Time(u64::try_from(self.db.lock().now_ms()).unwrap_or(0)) }

    pub(super) fn optimistic(&self, model: &str, row: &crate::identity::RowId) -> Result<Option<super::wire::WireRow>, DbError> {
        self.db.find(model, row)?.map(|found| self.wire(model).map(|wire| wire.row(&found))).transpose()
    }

    /// The path and the body of the request of a mutation: the registered document, the variables of the call.
    fn mutation_request(&self, call: &crate::store::MutationCall) -> Result<(String, String), DbError> {
        let path = self.life.graphql_path().ok_or_else(|| DbError::NoSession { detail: "transport is required for server operations".into() })?;
        let operation = self.reads.get(&call.mutation).ok_or_else(|| DbError::Invalid { detail: format!("operation {} is not registered", call.mutation) })?;
        let request = GraphQLRequest::new(operation.document.clone(), call.mutation.clone(), call.variables.clone());
        Ok((path.to_string(), crate::transport::encrypted::graphql_body(&request, true).to_string()))
    }

    /// One call that writes: a mutation goes to Outbox with its receipt, a local write is final when the owner answers.
    async fn write_planned(&self, epoch: u64, action: &str, planned: Planned) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let model = planned.model.clone();
        let row = planned.row_id.clone();
        let shown = planned.shown.clone();
        if planned.noop {
            if self.db.spec(&model)?.is_memory() {
                let renewal = IntentEffects { rows: vec![EffectRow { model: model.clone(), row_id: shown.to_string(), values: json!({ "id": shown.as_str() }).to_string() }], ..Default::default() };
                let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::Model(renewal), row.to_string(), String::new(), self.now());
                self.outbox_answer(waiter, action).await?;
            }
            let value = self.optimistic(&model, &shown)?;
            return Ok(receipt_of(epoch, model, None, self.db.revision_seq()?, value, None, self.life.clone()));
        }
        let destroyed = action == "destroy";
        let call = self.db.mutation_call(&model, action, &planned, &uuid::Uuid::new_v4().to_string())?;
        let Some(call) = call else {
            let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::Model(planned.effects), row.to_string(), String::new(), self.now());
            self.outbox_answer(waiter, action).await?;
            let value = if destroyed { None } else { self.optimistic(&model, &shown)? };
            return Ok(receipt_of(epoch, model, None, self.db.revision_seq()?, value, None, self.life.clone()));
        };
        let (path, body) = self.mutation_request(&call)?;
        let op = NewOp { model: model.clone(), row_id: row.to_string(), action: action.to_string(), on_failure: call.on_failure, origin: OpSource::Api, path, body, effects: planned.effects, durable: call.durable };
        let waiter = self.life.submit(epoch, op);
        let OutboxAnswer::Receipt(held) = self.outbox_answer(waiter, action).await? else { return Err(DbError::Database { detail: "the owner answered a call with no receipt".into() }) };
        let value = if destroyed { None } else { self.optimistic(&model, &shown)? };
        Ok(receipt_of(epoch, model, Some(i64::try_from(held.op_id).unwrap_or(i64::MAX)), held.revision, value, Some(held), self.life.clone()))
    }

    pub(super) async fn create_row(&self, epoch: u64, model: String, values: Map<String, Value>, args: Map<String, Value>) -> Result<WireOperationReceipt, DbError> {
        if self.life.is_shut() { return Err(super::command::closed()); }
        let db = self.db.clone();
        let planned = self.offload(move || Ok(db.plan_create(&model, values, args)?)).await?;
        self.write_planned(epoch, "create", planned).await
    }
}

#[uniffi::export]
impl Database {
    pub async fn create(&self, epoch: u64, model: String, values_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let declared = self.db.spec(&model)?.actions.iter().find(|action| action.name == "create").cloned();
        if declared.is_some_and(|action| action.durable) {
            return Err(DbError::Invalid { detail: format!("{model} sends its create as a durable command: use send") });
        }
        self.create_row(epoch, model, serde_json::from_str(&values_json)?, serde_json::from_str(&args_json)?).await
    }

    pub async fn update(&self, epoch: u64, model: String, row_id: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let (patch, args) = (serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?);
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name) = (self.db.clone(), model.clone());
        let planned = self.offload(move || Ok(db.plan_update(&name, &row, patch, args)?)).await?;
        self.write_planned(epoch, "update", planned).await
    }

    pub async fn act(&self, epoch: u64, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let (patch, args) = (serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?);
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name, verb) = (self.db.clone(), model.clone(), action.clone());
        let planned = self.offload(move || Ok(db.plan_act(&name, &verb, &row, patch, args)?)).await?;
        self.write_planned(epoch, &action, planned).await
    }

    pub async fn destroy(&self, epoch: u64, model: String, row_id: String) -> Result<WireOperationReceipt, DbError> {
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name) = (self.db.clone(), model.clone());
        let planned = self.offload(move || Ok(db.plan_destroy(&name, &row)?)).await?;
        self.write_planned(epoch, "destroy", planned).await
    }

    pub async fn set_local(&self, epoch: u64, model: String, row_id: String, path: String, value_json: String) -> Result<(), DbError> {
        self.account_call(epoch)?;
        let value: Value = serde_json::from_str(&value_json)?;
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name, field) = (self.db.clone(), model.clone(), path.clone());
        let plan = self.offload(move || Ok(db.plan_set_local(&name, &row, &field, value)?)).await?;
        let (kind, row, values) = match plan {
            crate::store::LocalPlan::Unchanged => return Ok(()),
            crate::store::LocalPlan::Row { row_id, values } => (LocalWriteKind::SetLocal, row_id, Value::Object(values).to_string()),
            crate::store::LocalPlan::Pending { row_id, path, value } => (LocalWriteKind::SetLocalPending, row_id, serde_json::json!({ "path": path, "value": value }).to_string()),
        };
        let waiter = self.life.local_write(epoch, model, kind, row.to_string(), values, self.now());
        self.outbox_answer(waiter, "set local").await.map(drop)
    }

    pub async fn set_ephemeral(&self, epoch: u64, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        self.account_call(epoch)?;
        let waiter = self.life.local_write(epoch, model, LocalWriteKind::SetEphemeral { expire_after: None }, id(&row_id)?.to_string(), values_json, self.now());
        self.outbox_answer(waiter, "set ephemeral").await.map(drop)
    }

    pub async fn attach(&self, epoch: u64, model: String, row_id: String, field: String, file: FileBytes) -> Result<String, DbError> {
        let FileBytes { bytes, name, mime } = file;
        self.account_call(epoch)?;
        let row = id(&row_id)?;
        let (checked_model, checked_field) = (model.clone(), field.clone());
        let checked = row.clone();
        self.read_in_epoch(epoch, "attach", move |database| Ok(database.db.file(&checked_model, &checked, &checked_field)?.is_some())).await?;
        let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::AttachBytes { bytes, name, mime }, row.to_string(), field.clone(), self.now());
        self.outbox_answer(waiter, "attach").await?;
        self.read_in_epoch(epoch, "attach", move |database| {
            let attached = database.db.file(&model, &row, &field)?.ok_or_else(|| DbError::Database { detail: "the attached file is not on the row".into() })?;
            Ok(attached.to_value().to_string())
        })
        .await
    }

    pub async fn detach(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<bool, DbError> {
        self.account_call(epoch)?;
        let row = id(&row_id)?;
        let (checked_model, checked_field, checked) = (model.clone(), field.clone(), row.clone());
        let had = self.read_in_epoch(epoch, "detach", move |database| Ok(database.db.file(&checked_model, &checked, &checked_field)?.is_some())).await?;
        let waiter = self.life.local_write(epoch, model, LocalWriteKind::Detach, row.to_string(), field, self.now());
        self.outbox_answer(waiter, "detach").await?;
        Ok(had)
    }

    pub async fn outcome(&self, receipt: WireOperationReceipt) -> Result<WireOperationOutcome, DbError> {
        let held = &receipt.ticket;
        if !Arc::ptr_eq(&held.owner, &self.life) { return Err(DbError::Invalid { detail: "the receipt belongs to another database".into() }); }
        if receipt.epoch != held.epoch || receipt.model != held.model || receipt.operation != held.operation || receipt.revision != held.revision || receipt.optimistic_value != held.optimistic_value {
            return Err(DbError::Invalid { detail: "the receipt was changed".into() });
        }
        if let Some(Ticket::Outbox(ticket)) = &held.ticket {
            if let Some(kept) = crate::lock::lock(&self.outcomes).get(&(ticket.epoch, ticket.op_id, ticket.cycle_id)).cloned() { return Ok(kept); }
        }
        if !self.admits(receipt.epoch) { return Ok(WireOperationOutcome::Interrupted); }
        match held.ticket.clone() {
            None => {
                if receipt.operation.is_some() { return Err(DbError::Invalid { detail: "the operation has no sender".into() }); }
                Ok(WireOperationOutcome::Confirmed { value: receipt.optimistic_value, answer_json: None })
            }
            Some(Ticket::Transfer(ticket)) => self.transfer_outcome(receipt, ticket).await,
            Some(Ticket::Outbox(ticket)) => self.operation_outcome(receipt, ticket).await,
        }
    }

    pub async fn discard(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        self.account_call(epoch)?;
        if self.discard_transfer(epoch, &model, &row_id, action.as_deref()).await? { return Ok(()); }
        let waiter = self.life.discard_operation(epoch, model, row_id);
        self.outbox_answer(waiter, "discard").await.map(drop)
    }
}

impl Database {
    /// Waits until no operation of the owner Outbox is on its way; a database without a session has none.
    #[cfg(test)]
    pub(super) async fn operations_quiet(&self) {
        let _ = self.await_life(self.life.operations_quiet(self.life.published().epoch)).await;
    }

    /// The outcome of a receipt does not change: the first one is kept and given again.
    async fn operation_outcome(&self, receipt: WireOperationReceipt, ticket: Receipt) -> Result<WireOperationOutcome, DbError> {
        let key = (ticket.epoch, ticket.op_id, ticket.cycle_id);
        let outcome = self.read_outcome(receipt, ticket).await?;
        if matches!(outcome, WireOperationOutcome::Confirmed { .. } | WireOperationOutcome::Rejected { .. }) {
            let mut kept = crate::lock::lock(&self.outcomes);
            kept.retain(|(epoch, _, _), _| *epoch == key.0);
            kept.insert(key, outcome.clone());
        }
        Ok(outcome)
    }

    async fn read_outcome(&self, receipt: WireOperationReceipt, ticket: Receipt) -> Result<WireOperationOutcome, DbError> {
        let answer = match self.await_life(self.life.operation_outcome(receipt.epoch, ticket.clone())).await {
            Ok(LifeReply::Outbox(OutboxAnswer::Outcome(outcome))) => outcome,
            Ok(LifeReply::Outbox(OutboxAnswer::Err(Fault::NoSession | Fault::Closed | Fault::Interrupted))) | Err(DbError::NoSession { .. } | DbError::Interrupted { .. } | DbError::Closed { .. }) => return Ok(WireOperationOutcome::Interrupted),
            Ok(LifeReply::Outbox(OutboxAnswer::Err(fault))) => return Err(refused(fault, "operation outcome")),
            Ok(_) => return Err(super::command::ended()),
            Err(_) if !self.admits(receipt.epoch) => return Ok(WireOperationOutcome::Interrupted),
            Err(error) => return Err(error),
        };
        Ok(match answer {
            OutboxOutcome::Confirmed { result, .. } => {
                let landed = match self.await_life(self.life.landed_row(receipt.epoch, ticket.op_id)).await {
                    Ok(LifeReply::Outbox(OutboxAnswer::Resolved(row))) => id(&row).ok(),
                    _ => None,
                };
                let value = match (landed, receipt.optimistic_value.as_ref().map(|row| id(&row.id))) {
                    (Some(row), _) => self.optimistic(&receipt.model, &row)?,
                    (None, Some(Ok(row))) => {
                        let current = self.resolved(receipt.epoch, &receipt.model, row).await?;
                        self.optimistic(&receipt.model, &current)?
                    }
                    _ => None,
                };
                WireOperationOutcome::Confirmed { value: value.or(receipt.optimistic_value), answer_json: result }
            }
            OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error } => WireOperationOutcome::Rejected { failure: failure_of(&error) },
            OutboxOutcome::Discarded | OutboxOutcome::Interrupted => WireOperationOutcome::Interrupted,
        })
    }

    /// `retry` of a failed mutation: the owner gives the receipt of the new cycle.
    pub(super) async fn retry_operation(&self, epoch: u64, model: String, row_id: String) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let waiter = self.life.retry_operation(epoch, model.clone(), row_id.clone());
        let OutboxAnswer::Receipt(held): OutboxAnswer = self.outbox_answer(waiter, "retry").await? else { return Err(DbError::Database { detail: "the owner answered a retry with no receipt".into() }) };
        let value = self.optimistic(&model, &id(&row_id)?)?;
        Ok(receipt_of(epoch, model, Some(i64::try_from(held.op_id).unwrap_or(i64::MAX)), held.revision, value, Some(held), self.life.clone()))
    }
}
