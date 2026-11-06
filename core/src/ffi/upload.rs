//! The calls of the platform that move files: `upload`, `upload_create_batch`, `cancel_upload`, `retry` with a source and `discard` of a transfer.
//! A call plans the transfer once (the size of the file, the parts, the request of the final mutation) and hands it to the owner Transfer, which copies the file and answers with the receipt.

use serde_json::{Map, Value};

use super::database::id;
use super::receipt::{receipt_of_transfer, ItemId, WireBatchReceipt, WireItemReceipt, WireOperationOutcome, WireOperationReceipt};
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use super::write::refused;
use super::{Database, DbError};
use crate::store::{Planned, Record};
use crate::owners::life::LifeReply;
use crate::owners::store::IntentEffects;
use crate::owners::write::outbox::{Fault, NewOp, OpSource, OutboxOutcome, Receipt};
use crate::owners::write::transfer::{BatchItem, Joint, Renumber, Source, TransferAnswer, SIGNED_ID_SENTINEL};
use crate::owners::write::upload_codec::StepsBody;
use crate::schema::ActionDeclaration;
use crate::transport::GraphQLRequest;
use crate::upload::{mime_of, part_count, scenario_for, UploadConfig, UploadFile, UploadScenario};

/// What one transfer is planned from.
struct Plan<'a> {
    model: &'a str,
    row: &'a str,
    action: &'a str,
    declaration: &'a ActionDeclaration,
    variables: Map<String, Value>,
    file: &'a UploadFile,
    effects: IntentEffects,
}

fn file_name(file: &UploadFile) -> String {
    file.name.clone().unwrap_or_else(|| std::path::Path::new(&file.path).file_name().map_or_else(|| file.path.clone(), |name| name.to_string_lossy().into_owned()))
}

fn invalid(detail: impl Into<String>) -> DbError { DbError::Invalid { detail: detail.into() } }

/// The key of a transfer: the address of the API.
fn key_of(model: &str, row: &str, action: &str) -> String { format!("{model}:{row}:{action}") }

impl Database {
    fn upload_config(&self) -> UploadConfig { UploadConfig::default() }

    /// The size of the file and the number of its steps: a single step for a small file, parts for a large one.
    fn source_of(&self, path: &str) -> Result<(Source, bool), DbError> {
        let total = std::fs::metadata(path).map_err(|error| invalid(format!("the source file is unreadable: {error}")))?.len();
        if total == 0 { return Err(invalid("the source file is empty")); }
        let config = self.upload_config();
        let operations = self.uploads.as_ref().ok_or_else(|| DbError::Sync { detail: "upload operations are not declared: pass UploadOperations to open".into() })?;
        let multipart = operations.multipart.is_some() && (operations.create_blob.is_none() || scenario_for(total, config.multipart_threshold) == UploadScenario::Multipart);
        Ok((Source { path: path.to_string(), parts: if multipart { part_count(total, config.part_size) } else { 1 } }, multipart))
    }

    /// The request of the final mutation: the variables carry a sentinel where the signed id of the file goes.
    fn upload_mutation(&self, plan: &Plan<'_>, mutation: &str) -> Result<NewOp, DbError> {
        let path = self.life.graphql_path().ok_or_else(|| DbError::NoSession { detail: "transport is required for uploads".into() })?;
        let operation = self.reads.get(mutation).ok_or_else(|| invalid(format!("operation {mutation} is not registered")))?;
        let variables = crate::owners::write::plan_variables::upload_variables(plan.declaration, Value::Object(plan.variables.clone()), &uuid::Uuid::new_v4().to_string());
        let request = GraphQLRequest::new(operation.document.clone(), mutation.to_string(), variables);
        let body = crate::transport::encrypted::graphql_body(&request, true).to_string();
        Ok(NewOp { model: plan.model.to_string(), row_id: plan.row.to_string(), action: plan.action.to_string(), on_failure: crate::schema::OnFailure::Keep, origin: OpSource::Api, path: path.to_string(), body, effects: plan.effects.clone(), durable: true })
    }

    fn upload_item(&self, plan: &Plan<'_>, mutation: &str, fields: Map<String, Value>) -> Result<BatchItem, DbError> {
        let (source, multipart) = self.source_of(&plan.file.path)?;
        let name = file_name(plan.file);
        let steps = StepsBody { multipart, mime: plan.file.mime.clone().unwrap_or_else(|| mime_of(&name).to_string()), name, signed_variable: "signedId".into() }.steps();
        Ok(BatchItem { key: key_of(plan.model, plan.row, plan.action), fields, urgent: false, source, mutation: self.upload_mutation(plan, mutation)?, steps })
    }

    fn single_upload(&self, model: &str, row: &str, action: &str, file: &UploadFile, effects: IntentEffects) -> Result<BatchItem, DbError> {
        let spec = self.db.spec(model)?;
        let declaration = spec.upload_action(action).filter(|declared| declared.batch.is_none()).ok_or_else(|| invalid(format!("{model} declares no single upload action {action}")))?;
        let mutation = declaration.mutation.clone().ok_or_else(|| invalid(format!("{model} {action} declares no mutation")))?;
        let mut variables = file.variables.clone();
        variables.insert("signedId".into(), Value::String(SIGNED_ID_SENTINEL.into()));
        self.upload_item(&Plan { model, row, action, declaration, variables, file, effects }, &mutation, Map::new())
    }

    /// One item of a batch: the row of the item is planned like a `create`, its final mutation carries the item alone.
    fn batch_upload(&self, model: &str, action: &str, planned: &Planned, item: &crate::upload::UploadItem, request: &crate::upload::UploadBatchInput) -> Result<BatchItem, DbError> {
        let spec = self.db.spec(model)?;
        let declaration = spec.actions.iter().find(|declared| declared.name == action).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = declaration.batch.as_ref().ok_or_else(|| DbError::Sync { detail: "operation has no upload batch declaration".into() })?;
        let mut fields = item.variables.clone();
        let data = planned.input.as_object().cloned().unwrap_or_default();
        for (target, source) in &batch.input { fields.insert(target.clone(), data.get(source).cloned().unwrap_or(Value::Null)); }
        fields.insert("signedId".into(), Value::String(SIGNED_ID_SENTINEL.into()));
        let shown = fields.clone();
        let single = spec.actions.iter().find(|declared| declared.name == batch.single && declared.mutation.is_some()).filter(|_| batch.min > 1);
        let (declaration, mutation, variables) = match single {
            Some(single) => {
                let mut variables = request.variables.clone();
                variables.extend(fields);
                (single, single.mutation.clone().unwrap_or_default(), variables)
            }
            None => {
                let mut variables = request.variables.clone();
                variables.insert(batch.items.clone(), Value::Array(vec![Value::Object(fields)]));
                (declaration, declaration.mutation.clone().ok_or_else(|| invalid("upload mutation is missing"))?, variables)
            }
        };
        let file = UploadFile { path: item.path.clone(), name: item.name.clone(), mime: item.mime.clone(), variables: Map::new() };
        self.upload_item(&Plan { model, row: planned.row_id.as_str(), action, declaration, variables, file: &file, effects: planned.effects.clone() }, &mutation, shown)
    }

    /// How the elements of a batch go out together: at least `min` of them in one request of the batch action, fewer one by one.
    fn joint_of(&self, model: &str, request: &crate::upload::UploadBatchInput) -> Result<Joint, DbError> {
        let spec = self.db.spec(model)?;
        let declaration = spec.actions.iter().find(|declared| declared.name == request.operation).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = declaration.batch.as_ref().ok_or_else(|| DbError::Sync { detail: "operation has no upload batch declaration".into() })?;
        let mutation = declaration.mutation.clone().ok_or_else(|| invalid("upload mutation is missing"))?;
        let plan = Plan { model, row: "", action: &request.operation, declaration, variables: request.variables.clone(), file: &UploadFile::new(""), effects: IntentEffects::default() };
        let template = self.upload_mutation(&plan, &mutation)?;
        let items_key = match &declaration.wrap {
            Some(wrap) => format!("{wrap}.{}", batch.items),
            None => batch.items.clone(),
        };
        let renumber = batch.renumber.as_ref().map(|fields| {
            let target = |path: &str| batch.input.iter().find(|(_, source)| source.as_str() == path).map(|(target, _)| target.clone()).unwrap_or_else(|| path.to_string());
            Renumber { position: fields.position.clone(), size: fields.size.clone(), position_target: target(&fields.position), size_target: target(&fields.size), start: fields.position_from }
        });
        Ok(Joint { minimum: batch.min, maximum: batch.max, items_key, mutation: template, renumber })
    }

    async fn transfer_answer(&self, waiter: crate::runtime::Waiter<LifeReply>, what: &str) -> Result<TransferAnswer, DbError> {
        match self.await_life(waiter).await? {
            LifeReply::Transfer(TransferAnswer::Err(fault)) => Err(refused(fault, what)),
            LifeReply::Transfer(answer) => Ok(answer),
            _ => Err(super::command::ended()),
        }
    }

    fn transfer_receipt(&self, epoch: u64, model: String, held: Receipt, value: Option<super::wire::WireRow>) -> WireOperationReceipt {
        receipt_of_transfer(epoch, model, held, value, self.life.clone())
    }

    /// The upload actions a model declares: a call without an action names the transfer by them.
    fn upload_actions(&self, model: &str) -> Result<Vec<String>, DbError> {
        Ok(self.db.spec(model)?.actions.iter().filter(|declared| declared.upload).map(|declared| declared.name.clone()).collect())
    }

    /// The row of a transfer after its mutation: a temporary id gives the real one.
    pub(super) async fn transfer_value(&self, epoch: u64, model: &str, row: &str) -> Result<Option<super::wire::WireRow>, DbError> {
        let current = self.resolved(epoch, model, id(row)?).await?;
        self.optimistic(model, &current)
    }
}

#[uniffi::export]
impl Database {
    pub async fn upload(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let file = self.offload(move || request.into_file()).await?;
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name, verb, target, args) = (self.db.clone(), model.clone(), action.clone(), row.clone(), file.variables.clone());
        let effects = self.offload(move || match db.plan_act(&name, &verb, &target, Map::new(), args) {
            Ok(planned) => Ok(planned.effects),
            Err(crate::store::DbError::NotFound { .. }) => Ok(IntentEffects::default()),
            Err(error) => Err(error.into()),
        }).await?;
        let item = self.single_upload(&model, row.as_str(), &action, &file, effects)?;
        let waiter = self.life.accept_upload(epoch, item, self.now());
        let TransferAnswer::Receipt(held) = self.transfer_answer(waiter, "upload").await? else { return Err(DbError::Database { detail: "the owner answered an upload with no receipt".into() }) };
        let value = self.optimistic(&model, &row)?;
        Ok(self.transfer_receipt(epoch, model, held, value))
    }

    pub async fn upload_create_batch(&self, epoch: u64, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<WireBatchReceipt, DbError> {
        self.account_call(epoch)?;
        let (items, request) = self.offload(move || Ok((items.into_iter().map(WireUploadItem::into_item).collect::<Result<Vec<_>, _>>()?, request.into_input()?))).await?;
        let spec = self.db.spec(&model)?;
        let action = spec.actions.iter().find(|declared| declared.name == request.operation).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = action.batch.as_ref().ok_or_else(|| DbError::Sync { detail: "operation has no upload batch declaration".into() })?;
        if items.len() < batch.min || items.len() > batch.max { return Err(invalid(format!("upload batch requires {}..{} items", batch.min, batch.max))); }
        let field = action.correlate.first().cloned().ok_or_else(|| invalid("upload batch has no correlation field"))?;
        let mut keys = std::collections::HashSet::new();
        for item in &items {
            let key = item.values.get(&field).and_then(Value::as_str).filter(|key| !key.trim().is_empty()).ok_or_else(|| invalid("upload correlation key is empty"))?;
            if !keys.insert(key.to_string()) { return Err(invalid("upload correlation keys must be unique")); }
        }
        let (db, name) = (self.db.clone(), model.clone());
        let created = self.offload(move || items.into_iter().map(|item| Ok((db.plan_create(&name, item.values.clone(), Map::new())?, item))).collect::<Result<Vec<_>, DbError>>()).await?;
        let planned = created.into_iter().map(|(planned, item)| {
            let built = self.batch_upload(&model, &request.operation, &planned, &item, &request)?;
            Ok((item, planned, built))
        }).collect::<Result<Vec<_>, DbError>>()?;
        let built: Vec<BatchItem> = planned.iter().map(|(_, _, built)| built.clone()).collect();
        let joint = self.joint_of(&model, &request)?;
        let waiter = self.life.accept_upload_batch(epoch, built, joint, self.now());
        let TransferAnswer::Receipts(held) = self.transfer_answer(waiter, "upload batch").await? else { return Err(DbError::Database { detail: "the owner answered a batch with no receipts".into() }) };
        let wire = self.wire(&model)?.clone();
        let items = planned.into_iter().zip(held).map(|((item, planned, _), held)| {
            let key = item.values.get(&field).and_then(Value::as_str).unwrap_or_default().to_string();
            let record = Record { id: planned.row_id.clone(), revision: 0, data: planned.input.as_object().cloned().unwrap_or_default(), sync: Default::default() };
            let value = wire.row(&record);
            WireItemReceipt { item: ItemId { value: key }, optimistic_value: value.clone(), receipt: self.transfer_receipt(epoch, model.clone(), held, Some(value)) }
        }).collect();
        Ok(WireBatchReceipt { epoch, items })
    }

    pub async fn cancel_upload(&self, epoch: u64, model: String, row_id: String, action: String) -> Result<bool, DbError> {
        self.account_call(epoch)?;
        if self.uploads.is_none() { return Ok(false); }
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let waiter = self.life.cancel_upload(epoch, key_of(&model, row.as_str(), &action));
        match self.transfer_answer(waiter, "cancel upload").await? {
            TransferAnswer::Cancelled(cancelled) => Ok(cancelled),
            _ => Err(DbError::Database { detail: "the owner answered a cancel with no result".into() }),
        }
    }
}

impl Database {
    /// `send` with a file: the row is written like a `create`, the final mutation is the declared upload action.
    pub(super) async fn send_upload(&self, request: super::command::CommandRequest) -> Result<WireOperationReceipt, DbError> {
        let epoch = request.epoch;
        self.account_call(epoch)?;
        let Some(upload) = request.upload else { return Err(invalid("a command without a file goes through the owners")) };
        let action = upload.action.clone();
        let (db, name, texts) = (self.db.clone(), request.model.clone(), (request.values_json.clone(), request.args_json.clone()));
        let (file, planned) = self.offload(move || Ok((upload.request.into_file()?, db.plan_create(&name, serde_json::from_str(&texts.0)?, serde_json::from_str(&texts.1)?)?))).await?;
        let item = self.single_upload(&request.model, planned.row_id.as_str(), &upload.action, &file, planned.effects.clone())?;
        let waiter = self.life.accept_upload(epoch, item, self.now());
        let TransferAnswer::Receipt(held) = self.transfer_answer(waiter, "send").await? else { return Err(DbError::Database { detail: "the owner answered a send with no receipt".into() }) };
        let queued = crate::upload::UploadInfo { status: crate::upload::UploadStatus::Queued, stage: crate::upload::UploadStage::Prepare, sent_bytes: 0, total_bytes: 0, wait_reason: None, attempts: 0, error: None, local_url: None, processing: None, copy_file: None };
        let sync = crate::value::sync::SyncInfo { uploads: std::collections::BTreeMap::from([(action, queued)]), ..Default::default() };
        let record = Record { id: planned.row_id.clone(), revision: 0, data: planned.input.as_object().cloned().unwrap_or_default(), sync };
        let value = self.wire(&request.model)?.row(&record);
        Ok(self.transfer_receipt(epoch, request.model, held, Some(value)))
    }

    /// `retry` of a transfer with a new source of a file that is gone.
    pub(super) async fn retry_transfer(&self, epoch: u64, model: String, row_id: String, action: Option<String>, source: Option<WireUploadRequest>) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let action = match action {
            Some(action) => action,
            None => {
                let declared = self.upload_actions(&model)?;
                match declared.as_slice() {
                    [only] => only.clone(),
                    _ => return Err(invalid(format!("{model} {row} has more than one upload, name its action"))),
                }
            }
        };
        let source = match source {
            Some(request) => {
                let path = request.into_file()?.path;
                Some(self.source_of(&path)?.0)
            }
            None => None,
        };
        let waiter = self.life.retry_upload(epoch, key_of(&model, row.as_str(), &action), source);
        let TransferAnswer::Receipt(held) = self.transfer_answer(waiter, "retry").await? else { return Err(DbError::Database { detail: "the owner answered a retry with no receipt".into() }) };
        let value = self.optimistic(&model, &row)?;
        Ok(self.transfer_receipt(epoch, model, held, value))
    }

    /// `retry` with no source: the transfers of the row first, then the mutations. `None` when no transfer has the row.
    pub(super) async fn retry_declared_transfer(&self, epoch: u64, model: &str, row: &str) -> Result<Option<WireOperationReceipt>, DbError> {
        if self.uploads.is_none() { return Ok(None); }
        for action in self.upload_actions(model)? {
            let waiter = self.life.retry_upload(epoch, key_of(model, row, &action), None);
            match self.await_life(waiter).await? {
                LifeReply::Transfer(TransferAnswer::Err(Fault::NotFound)) => continue,
                LifeReply::Transfer(TransferAnswer::Err(fault)) => return Err(refused(fault, "retry")),
                LifeReply::Transfer(TransferAnswer::Receipt(held)) => {
                    let value = self.optimistic(model, &id(row)?)?;
                    return Ok(Some(self.transfer_receipt(epoch, model.to_string(), held, value)));
                }
                _ => return Err(super::command::ended()),
            }
        }
        Ok(None)
    }

    /// `discard` of a transfer: `false` when no transfer of the row exists, the caller asks the mutations then.
    pub(super) async fn discard_transfer(&self, epoch: u64, model: &str, row: &str, action: Option<&str>) -> Result<bool, DbError> {
        if self.uploads.is_none() { return Ok(false); }
        let actions = match action {
            Some(action) => vec![action.to_string()],
            None => self.upload_actions(model)?,
        };
        for action in actions {
            let waiter = self.life.discard_upload(epoch, key_of(model, row, &action));
            match self.await_life(waiter).await? {
                LifeReply::Transfer(TransferAnswer::Err(Fault::NotFound)) => continue,
                LifeReply::Transfer(TransferAnswer::Err(fault)) => return Err(refused(fault, "discard")),
                LifeReply::Transfer(_) => return Ok(true),
                _ => return Err(super::command::ended()),
            }
        }
        Ok(false)
    }

    /// The outcome of the receipt of a transfer.
    pub(super) async fn transfer_outcome(&self, receipt: WireOperationReceipt, held: Receipt) -> Result<WireOperationOutcome, DbError> {
        let answer = match self.await_life(self.life.upload_outcome(receipt.epoch, held.clone())).await {
            Ok(LifeReply::Transfer(TransferAnswer::Outcome(outcome))) => outcome,
            Ok(LifeReply::Transfer(TransferAnswer::Err(Fault::NoSession | Fault::Closed | Fault::Interrupted))) | Err(DbError::NoSession { .. } | DbError::Interrupted { .. } | DbError::Closed { .. }) => return Ok(WireOperationOutcome::Interrupted),
            Ok(LifeReply::Transfer(TransferAnswer::Err(fault))) => return Err(refused(fault, "upload outcome")),
            Ok(_) => return Err(super::command::ended()),
            Err(_) if !self.admits(receipt.epoch) => return Ok(WireOperationOutcome::Interrupted),
            Err(error) => return Err(error),
        };
        Ok(match answer {
            OutboxOutcome::Confirmed { .. } => {
                let row = receipt.optimistic_value.as_ref().map(|row| row.id.clone());
                let value = match row {
                    Some(row) => self.transfer_value(receipt.epoch, &receipt.model, &row).await?,
                    None => None,
                };
                WireOperationOutcome::Confirmed { value: value.or(receipt.optimistic_value), answer_json: None }
            }
            OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error } => WireOperationOutcome::Rejected { failure: super::write::failure_of(&error) },
            OutboxOutcome::Discarded | OutboxOutcome::Interrupted => WireOperationOutcome::Interrupted,
        })
    }
}
