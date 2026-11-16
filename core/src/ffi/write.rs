//! The calls of the platform that write: `create`, `update`, `act`, `destroy`, `set_local`, `outcome`, `retry` and `discard`.
//! Each call reads the base once (`db/plan_write.rs`), hands the effects to the owner Outbox (a mutation) or to the local writes of the same owner (a local model, an action without a mutation), and waits for the answer of the owner.

use std::sync::Arc;

use serde_json::{json, Value};
use crate::owners::store::{EffectRow, IntentEffects};

use super::database::{id, parse_json};
use super::receipt::{receipt_of, Ticket, WireOperationReceipt};
use super::{Database, DbError};
use crate::store::Planned;
use crate::owners::life::LifeReply;
use crate::owners::write::transfer::TransferAnswer;
use crate::owners::write::outbox::{Fault, LocalWriteKind, NewOp, OpSource, OutboxAnswer, OutboxOutcome, Receipt};
use crate::transport::GraphQLRequest;

/// The value of a confirmed operation: the row it shows and the JSON of its answer.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireConfirmed {
    pub value: Option<super::wire::WireRow>,
    pub answer_json: Option<String>,
}

/// The error of a call that an owner refused.
pub(super) fn refused(fault: Fault, what: &str) -> DbError {
    use crate::owners::contract::RejectReason;
    match fault {
        Fault::NoSession => DbError::NoSession { detail: "the database has no session in this epoch".into() },
        Fault::NotFound => DbError::NotFound { detail: format!("{what}: there is no such operation") },
        Fault::NotFoundAt(index) => DbError::NotFound { detail: format!("{what}: the row of the item {index} does not exist") },
        Fault::InFlight => DbError::InFlight { detail: format!("{what}: the operation is in flight") },
        Fault::Ended(cause) => super::cause_error(&cause),
        Fault::RestoreFailed => DbError::Invalid { detail: format!("{what}: the stored operation cannot be read, discard it") },
        Fault::StateLost => DbError::Session { code: crate::transport::SessionCode::StateLost, detail: "the pending request of the session was lost".into() },
        Fault::Runtime(detail) => DbError::Runtime { detail },
        Fault::Internal => DbError::Internal { detail: format!("{what}: internal error") },
        Fault::Invalid => DbError::Invalid { detail: format!("{what}: the owner refused the call") },
        Fault::Copy(detail) => DbError::Copy { detail },
        Fault::Rejected(RejectReason::Storage(detail)) => DbError::Storage { detail },
        Fault::Rejected(RejectReason::Invalid(detail)) => DbError::Invalid { detail: format!("{what}: {detail}") },
        Fault::Rejected(reason @ (RejectReason::NoSession | RejectReason::Closed)) => refused(Fault::of_reject(reason), what),
    }
}

pub(super) fn failure_of(error: &str) -> super::wire::WireFailure { super::wire::failure(&crate::owners::write::failure::parse(error)) }

pub(super) fn fault_error(fault: Fault, what: &str) -> DbError {
    match fault {
        Fault::Ended(cause) => super::cause_error(&cause),
        fault => end_error(refused(fault, what)),
    }
}

/// The error of the wait that the end of the database or of the session gave, as `cause_error` words it.
pub(super) fn end_error(error: DbError) -> DbError {
    match error {
        DbError::Closed { .. } => super::cause_error(&super::EndCause::Closed),
        DbError::NoSession { .. } | DbError::Session { code: crate::transport::SessionCode::SessionEnded, .. } => super::cause_error(&super::EndCause::SessionEnded),
        error => error,
    }
}

impl Database {
    pub(super) async fn outbox_answer(&self, waiter: crate::runtime::Waiter<LifeReply>, what: &str) -> Result<OutboxAnswer, DbError> {
        #[cfg(test)]
        {
            let gate = self.db.answer_gate.lock().unwrap_or_else(|error| error.into_inner()).clone();
            if let Some(gate) = gate {
                gate.0.set();
                let _ = super::database::blocking(move || Ok(gate.1.wait_blocking("the test released the answer of the owner"))).await;
            }
        }
        match self.await_life(waiter).await? {
            LifeReply::Outbox(OutboxAnswer::Err(fault)) => Err(refused(fault, what)),
            LifeReply::Outbox(answer) => Ok(answer),
            _ => Err(super::database::foreign_reply()),
        }
    }

    /// A local write is final when its batch commits: the revision and the row are those of that batch.
    async fn written(&self, waiter: crate::runtime::Waiter<LifeReply>, what: &str, model: &str) -> Result<(i64, Option<super::wire::WireRow>), DbError> {
        match self.outbox_answer(waiter, what).await? {
            OutboxAnswer::Written { revision, shown, .. } => Ok((revision, shown.map(|row| self.wire_of_row(model, &row)).transpose()?)),
            _ => Err(super::database::foreign_reply()),
        }
    }

    /// The id a row has now: a temporary id of a `create` that landed gives the real one.
    pub(super) async fn resolved(&self, epoch: u64, model: &str, row: crate::identity::RowId) -> Result<crate::identity::RowId, DbError> {
        self.account_call(epoch)?;
        if !row.is_temp() { return Ok(row); }
        let (db, model) = (self.db.clone(), model.to_string());
        self.offload(move || Ok(db.resolve(&model, &row)?)).await
    }

    pub(super) async fn file_row(&self, epoch: u64, model: &str, row: &str, field: &str, required: bool) -> Result<crate::identity::RowId, DbError> {
        self.account_call(epoch)?;
        self.db.file_field(model, field)?;
        let row = self.resolved(epoch, model, id(row)?).await?;
        if required {
            let (model, row) = (model.to_string(), row.clone());
            self.read_in_epoch(epoch, move |database| row_present(database, &model, &row)).await?;
        }
        Ok(row)
    }

    pub(super) async fn operation_target(&self, epoch: u64, model: &str, row: &str, action: Option<&str>, upload: bool) -> Result<(String, bool), DbError> {
        self.account_call(epoch)?;
        let spec = self.db.spec(model)?;
        if let Some(action) = action {
            if !spec.actions.iter().any(|declared| declared.name == action) { return Err(DbError::Invalid { detail: format!("{model} declares no action {action}") }); }
        }
        let transfer = action.is_some_and(|action| spec.upload_action(action).is_some());
        if upload && !transfer { return Err(DbError::Invalid { detail: format!("{model} declares no upload action") }); }
        Ok((id(row)?.to_string(), transfer))
    }

    pub(super) fn mutation_needs_transport(&self, model: &str, action: &str) -> Result<(), DbError> {
        let spec = self.db.spec(model)?;
        let sends = !spec.is_local() && !spec.is_memory() && spec.actions.iter().any(|declared| declared.name == action && declared.mutation.is_some());
        if sends { self.remote() } else { Ok(()) }
    }


    pub(super) fn wire_of_row(&self, model: &str, row: &crate::store::Row) -> Result<super::wire::WireRow, DbError> { self.wire(model).map(|wire| wire.row(row)) }

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
            if action != "destroy" && self.db.spec(&model)?.is_memory() {
                let renewal = IntentEffects { rows: vec![EffectRow { model: model.clone(), row_id: shown.to_string(), values: json!({ "id": shown.as_str() }).to_string() }], shown: Some((model.clone(), shown.to_string())), ..Default::default() };
                let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::Model(renewal), row.to_string(), String::new());
                let (revision, value) = self.written(waiter, action, &model).await?;
                return Ok(receipt_of(epoch, model, None, revision, value, None, self.life.clone()));
            }
            let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::Snapshot, shown.to_string(), String::new());
            let (revision, value) = self.written(waiter, action, &model).await?;
            return Ok(receipt_of(epoch, model, None, revision, value, None, self.life.clone()));
        }
        let destroyed = action == "destroy";
        let rowless = row.as_str() == crate::schema::SINGLETON;
        let call = crate::blocking::blocking_unit(|| self.db.mutation_call(&model, action, &planned, &uuid::Uuid::new_v4().to_string()))?;
        let Some(call) = call else {
            let mut effects = planned.effects;
            if !destroyed && !rowless { effects.shown = Some((model.clone(), shown.to_string())); }
            let waiter = self.life.local_write(epoch, model.clone(), LocalWriteKind::Model(effects), row.to_string(), String::new());
            let (revision, value) = self.written(waiter, action, &model).await?;
            return Ok(receipt_of(epoch, model, None, revision, value, None, self.life.clone()));
        };
        let (path, body) = self.mutation_request(&call)?;
        let mut effects = planned.effects;
        if !destroyed && !rowless { effects.shown = Some((model.clone(), shown.to_string())); }
        let op = NewOp { model: model.clone(), row_id: row.to_string(), action: action.to_string(), on_failure: call.on_failure, origin: OpSource::Api, path, body, effects, expires_after: call.expires_after.map(std::time::Duration::from_secs) };
        let waiter = self.life.submit(epoch, op);
        let OutboxAnswer::Receipt(held) = self.outbox_answer(waiter, action).await? else { return Err(DbError::Internal { detail: "the owner answered a call with no receipt".into() }) };
        let value = held.optimistic.as_ref().map(|row| self.wire_of_row(&model, row)).transpose()?;
        Ok(receipt_of(epoch, model, Some(i64::try_from(held.op_id).unwrap_or(i64::MAX)), held.revision, value, Some(held), self.life.clone()))
    }

    pub(super) async fn create_row(&self, epoch: u64, model: String, values_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        self.mutation_needs_transport(&model, "create")?;
        let db = self.db.clone();
        let planned = self.offload(move || Ok(db.plan_create(&model, parse_json(&values_json)?, parse_json(&args_json)?)?)).await?;
        self.write_planned(epoch, "create", planned).await
    }
}

#[uniffi::export]
impl Database {
    pub async fn create(&self, epoch: u64, model: String, values_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.create_row(epoch, model, values_json, args_json).await
    }).await? }

    pub async fn update(&self, epoch: u64, model: String, row_id: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        this_db.mutation_needs_transport(&model, "update")?;
        let (db, name) = (this_db.db.clone(), model.clone());
        let (patch, args) = this_db.offload(move || {
            let patch: serde_json::Map<String, Value> = parse_json(&patch_json)?;
            let args = parse_json(&args_json)?;
            db.validate_update(&name, &patch)?;
            Ok((patch, args))
        }).await?;
        let row = id(&row_id)?;
        let (db, name) = (this_db.db.clone(), model.clone());
        let planned = this_db.offload(move || Ok(db.plan_update(&name, &row, patch, args)?)).await?;
        this_db.write_planned(epoch, "update", planned).await
    }).await? }

    pub async fn act(&self, epoch: u64, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        if !this_db.db.spec(&model)?.actions.iter().any(|declared| declared.name == action) { return Err(DbError::Invalid { detail: format!("{model} declares no action {action}") }); }
        this_db.mutation_needs_transport(&model, &action)?;
        let (db, name) = (this_db.db.clone(), model.clone());
        let (patch, args) = this_db.offload(move || {
            let patch: serde_json::Map<String, Value> = parse_json(&patch_json)?;
            let args = parse_json(&args_json)?;
            db.validate_act(&name, patch.clone(), &args)?;
            Ok((patch, args))
        }).await?;
        let row = id(&row_id)?;
        let (db, name, verb) = (this_db.db.clone(), model.clone(), action.clone());
        let planned = this_db.offload(move || Ok(db.plan_act(&name, &verb, &row, patch, args)?)).await?;
        this_db.write_planned(epoch, &action, planned).await
    }).await? }

    pub async fn destroy(&self, epoch: u64, model: String, row_id: String) -> Result<WireOperationReceipt, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        this_db.mutation_needs_transport(&model, "destroy")?;
        let row = id(&row_id)?;
        let (db, name) = (this_db.db.clone(), model.clone());
        let planned = this_db.offload(move || Ok(db.plan_destroy(&name, &row)?)).await?;
        this_db.write_planned(epoch, "destroy", planned).await
    }).await? }

    pub async fn set_local(&self, epoch: u64, model: String, row_id: String, path: String, value_json: String) -> Result<(), DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let (db, name, field) = (this_db.db.clone(), model.clone(), path.clone());
        let value = this_db.offload(move || {
            let value = parse_json::<Value>(&value_json)?;
            db.local_value(&name, &field, value.clone())?;
            Ok(value)
        }).await?;
        let row = id(&row_id)?;
        let (db, name, field) = (this_db.db.clone(), model.clone(), path.clone());
        let plan = this_db.offload(move || Ok(db.plan_set_local(&name, &row, &field, value)?)).await?;
        let (kind, row, values) = match plan {
            crate::store::LocalPlan::Unchanged => return Ok(()),
            crate::store::LocalPlan::Row { row_id, values } => (LocalWriteKind::SetLocal, row_id, Value::Object(values).to_string()),
            crate::store::LocalPlan::Pending { row_id, path, value } => (LocalWriteKind::SetLocalPending, row_id, serde_json::json!({ "path": path, "value": value }).to_string()),
        };
        let waiter = this_db.life.local_write(epoch, model, kind, row.to_string(), values);
        this_db.outbox_answer(waiter, "set local").await.map(drop)
    }).await? }

    pub async fn set_ephemeral(&self, epoch: u64, model: String, row_id: String, values_json: String) -> Result<(), DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let (db, name, text) = (this_db.db.clone(), model.clone(), values_json.clone());
        this_db.offload(move || Ok(db.plan_set_ephemeral(&name, &parse_json(&text)?)?)).await?;
        let row = this_db.resolved(epoch, &model, id(&row_id)?).await?;
        let waiter = this_db.life.local_write(epoch, model, LocalWriteKind::SetEphemeral { expire_after: None }, row.to_string(), values_json);
        this_db.outbox_answer(waiter, "set ephemeral").await.map(drop)
    }).await? }

    /// Attaches the local file at `source_path`: the library copies it in pieces and the row shows the copy.
    pub async fn attach_file(&self, epoch: u64, model: String, row_id: String, field: String, source_path: String, name: String, mime: String) -> Result<super::wire::WireFile, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let source_path = local_source(&source_path)?;
        let row = this_db.file_row(epoch, &model, &row_id, &field, true).await?;
        let files = this_db.db.files().clone();
        let copied = files.clone();
        let copy = this_db.offload(move || crate::store::files::copy_attachment(&copied, &source_path).map_err(|error| DbError::Storage { detail: format!("the source file could not be copied: {error}") })).await?;
        let id = copy.id.clone();
        let waiter = this_db.life.local_write(epoch, model, LocalWriteKind::AttachCopy { copy, name, mime }, row.to_string(), field.clone());
        let answer = this_db.outbox_answer(waiter, "attach").await;
        let attached = match &answer {
            Ok(OutboxAnswer::Written { shown: Some(shown), .. }) => shown.get(&field).and_then(crate::store::files::FileDescriptor::from_value),
            _ => None,
        };
        if let Some(attached) = attached { return Ok(attached.into()); }
        let _ = files.discard_partial(&id);
        match answer {
            Err(error) => Err(error),
            Ok(_) => Err(DbError::Internal { detail: "the attached file is not on the row".into() }),
        }
    }).await? }

    pub async fn detach(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<bool, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        let row = this_db.file_row(epoch, &model, &row_id, &field, true).await?;
        let waiter = this_db.life.local_write(epoch, model, LocalWriteKind::Detach, row.to_string(), field);
        let OutboxAnswer::Written { files_removed, .. } = this_db.outbox_answer(waiter, "detach").await? else { return Err(super::database::foreign_reply()) };
        Ok(files_removed > 0)
    }).await? }

    /// The one outcome of a receipt (D-286): the value `Confirmed{value, answer_json}`, a refusal is `Rejected{failure, kept}` (`kept` = the failed intent stays for `retry` or `discard`), the end of the wait is the error of `cause_error`.
    pub async fn confirmed(&self, receipt: WireOperationReceipt, discard_on_failure: bool) -> Result<WireConfirmed, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.settle(receipt, discard_on_failure).await
    }).await? }

    pub async fn discard(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        let (row_id, transfer) = this_db.operation_target(epoch, &model, &row_id, action.as_deref(), false).await?;
        if let Some(declared) = action.as_deref().filter(|_| transfer) {
            return this_db.discard_transfer(epoch, &model, &row_id, declared).await;
        }
        let waiter = this_db.life.discard_operation(epoch, (model.clone(), row_id.clone()), action);
        match this_db.await_life(waiter).await? {
            LifeReply::Transfer(TransferAnswer::Err(fault)) | LifeReply::Outbox(OutboxAnswer::Err(fault)) => Err(refused(fault, "discard")),
            LifeReply::Outbox(_) | LifeReply::Transfer(_) => Ok(()),
            _ => Err(super::database::foreign_reply()),
        }
    }).await? }
}

/// The local path of a source: an absolute path or a `file://` address. Another scheme and a relative path are `Invalid`.
fn local_source(source: &str) -> Result<std::path::PathBuf, DbError> {
    let invalid = || DbError::Invalid { detail: "the source of a file must be an absolute path or a file:// address".into() };
    let path = match source.strip_prefix("file://") {
        Some(rest) => {
            let rest = rest.strip_prefix("localhost").unwrap_or(rest);
            let mut bytes = Vec::with_capacity(rest.len());
            let mut raw = rest.bytes();
            while let Some(byte) = raw.next() {
                if byte != b'%' { bytes.push(byte); continue; }
                let (high, low) = (raw.next().and_then(|digit| (digit as char).to_digit(16)), raw.next().and_then(|digit| (digit as char).to_digit(16)));
                bytes.push(u8::try_from(high.zip(low).map(|(high, low)| high * 16 + low).ok_or_else(invalid)?).map_err(|_| invalid())?);
            }
            String::from_utf8(bytes).map_err(|_| invalid())?
        }
        None => source.to_string(),
    };
    if path.starts_with('/') { Ok(path.into()) } else { Err(invalid()) }
}

impl Database {
    /// The outcome of the owner: the value, or the error of a refusal (`kept` = the intent stays) or of the end of the wait.
    async fn settle(&self, receipt: WireOperationReceipt, discard_on_failure: bool) -> Result<WireConfirmed, DbError> {
        self.not_closed()?;
        let held = &receipt.ticket;
        if !Arc::ptr_eq(&held.owner, &self.life) { return Err(DbError::Invalid { detail: "the receipt belongs to another database".into() }); }
        if receipt.epoch != held.epoch || receipt.model != held.model || receipt.operation != held.operation || receipt.revision != held.revision || receipt.optimistic_value != held.optimistic_value {
            return Err(DbError::Invalid { detail: "the receipt was changed".into() });
        }
        let epoch = receipt.epoch;
        if let Some(cause) = self.admission_end(epoch) {
            return Err(super::cause_error(&cause));
        }
        let result = match held.ticket.clone() {
            None => {
                if receipt.operation.is_some() { return Err(DbError::Invalid { detail: "the operation has no sender".into() }); }
                Ok(WireConfirmed { value: receipt.optimistic_value, answer_json: None })
            }
            Some(Ticket::Transfer(ticket)) => self.transfer_outcome(receipt, ticket, discard_on_failure).await,
            Some(Ticket::Outbox(ticket)) => self.operation_outcome(receipt, ticket, discard_on_failure).await,
        };
        result
    }

    /// The row a confirmed receipt names, read from the Store when the answer is given: no row, or a row that is gone, gives no value.
    pub(super) async fn confirmed_row(&self, epoch: u64, model: &str, row_id: Option<String>) -> Result<Option<super::wire::WireRow>, DbError> {
        let Some(row_id) = row_id.filter(|id| !id.is_empty()) else { return Ok(None) };
        let (model, id) = (model.to_string(), super::database::id(&row_id)?);
        self.read_in_epoch(epoch, move |this| {
            let wire = this.wire(&model)?;
            Ok(this.db.find(&model, &id)?.map(|row| wire.row(&row)))
        }).await
    }

    /// The outcome of a receipt is the one the owner Outbox holds for its cycle, or the record of the Store for a cycle that left the memory.
    async fn operation_outcome(&self, receipt: WireOperationReceipt, ticket: Receipt, discard_on_failure: bool) -> Result<WireConfirmed, DbError> {
        let waiter = self.life.operation_outcome(receipt.epoch, ticket.clone(), discard_on_failure);
        let answer = match self.await_life(waiter).await {
            Ok(LifeReply::Outbox(OutboxAnswer::Outcome(outcome))) => outcome,
            Ok(LifeReply::Outbox(OutboxAnswer::Err(fault))) => return Err(fault_error(fault, "operation outcome")),
            Ok(_) => return Err(super::database::foreign_reply()),
            Err(error) => return Err(end_error(error)),
        };
        match answer {
            OutboxOutcome::Confirmed { result, row_id, .. } => Ok(WireConfirmed { value: self.confirmed_row(receipt.epoch, &receipt.model, row_id).await?, answer_json: result }),
            OutboxOutcome::Rejected { error } => Err(DbError::Rejected { failure: failure_of(&error), kept: false }),
            OutboxOutcome::Failed { error } => Err(DbError::Rejected { failure: failure_of(&error), kept: true }),
            OutboxOutcome::Discarded => Err(super::cause_error(&super::EndCause::Discarded)),
            OutboxOutcome::Ended(cause) => Err(super::cause_error(&cause)),
        }
    }

    /// `retry` of a failed mutation: the owner gives the receipt of the new cycle.
    pub(super) async fn retry_operation(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<Retried, DbError> {
        self.account_call(epoch)?;
        let waiter = self.life.retry_operation(epoch, (model.clone(), row_id.clone()), action);
        let held = match self.await_life(waiter).await? {
            LifeReply::Outbox(OutboxAnswer::Receipt(held)) => held,
            LifeReply::Outbox(OutboxAnswer::Transfer { action }) => return Ok(Retried::Transfer(action)),
            LifeReply::Transfer(TransferAnswer::Receipt(held)) => {
                let value = held.optimistic.as_ref().map(|row| self.wire_of_row(&model, row)).transpose()?;
                return Ok(Retried::Receipt(Box::new(self.transfer_receipt(epoch, model, held, value))));
            }
            LifeReply::Outbox(OutboxAnswer::Err(fault)) | LifeReply::Transfer(TransferAnswer::Err(fault)) => return Err(refused(fault, "retry")),
            _ => return Err(DbError::Internal { detail: "the owner answered a retry with no receipt".into() }),
        };
        let value = held.optimistic.as_ref().map(|row| self.wire_of_row(&model, row)).transpose()?;
        Ok(Retried::Receipt(Box::new(receipt_of(epoch, model, Some(i64::try_from(held.op_id).unwrap_or(i64::MAX)), held.revision, value, Some(held), self.life.clone()))))
    }
}

/// What `retry` of the first open operation of a row gave: the receipt of a new cycle, or the action of the transfer that owns the operation.
pub(super) enum Retried {
    Receipt(Box<WireOperationReceipt>),
    Transfer(String),
}

pub(super) fn row_present(database: &Database, model: &str, row: &crate::identity::RowId) -> Result<(), DbError> {
    crate::blocking::blocking_unit(|| database.db.find(model, row))?.map(drop).ok_or_else(|| DbError::NotFound { detail: format!("row {row} of {model} does not exist") })
}
