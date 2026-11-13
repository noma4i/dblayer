//! The calls of the platform that move files: `upload`, `upload_create_batch`, `cancel_upload`, `retry` with a source and `discard` of a transfer.
//! A call plans the transfer once (the size of the file, the parts, the request of the final mutation) and hands it to the owner Transfer, which copies the file and answers with the receipt.

use serde_json::{Map, Value};

use super::database::id;
use super::receipt::{receipt_of_transfer, ItemId, WireBatchReceipt, WireItemReceipt, WireOperationReceipt};
use super::upload_wire::{WireSource, WireStagedUpload, WireUploadBatch, WireUploadItem, WireUploadRequest, WireUploadSource};
use super::write::refused;
use super::{Database, DbError};
use crate::store::Planned;
use crate::owners::life::LifeReply;
use crate::owners::store::IntentEffects;
use crate::owners::write::outbox::{NewOp, OpSource, OutboxOutcome, Receipt};
use crate::owners::write::transfer::{batch_size_admitted, BatchItem, CommitRequest, Joint, Renumber, Source, SuppliedSource, TransferAnswer, SIGNED_ID_SENTINEL};
use crate::owners::write::upload_codec::StepsBody;
use crate::schema::ActionDeclaration;
use crate::transport::GraphQLRequest;
use crate::owners::write::upload::{mime_of, UploadFile};

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

struct Single {
    declaration: ActionDeclaration,
    mutation: String,
    source: Option<(Source, bool)>,
}

fn file_name(file: &UploadFile, path: &str) -> String {
    file.source.name().map(str::to_string).unwrap_or_else(|| std::path::Path::new(path).file_name().map_or_else(|| path.to_string(), |name| name.to_string_lossy().into_owned()))
}

fn invalid(detail: impl Into<String>) -> DbError { DbError::Invalid { detail: detail.into() } }

/// The key of a transfer: the address of the API.
fn key_of(model: &str, row: &str, action: &str) -> String { format!("{model}:{row}:{action}") }

impl Database {
    fn upload_operations(&self) -> Result<&crate::owners::write::upload::UploadOperations, DbError> {
        self.uploads.as_ref().ok_or_else(|| invalid("upload operations are not declared: pass UploadOperations to open"))
    }

    /// The size of the file and the number of its steps: a single step for a small file, parts for a large one.
    fn source_of(&self, path: &str, held: bool) -> Result<(Source, bool), DbError> {
        #[cfg(test)] self.db.files().record_source_read();
        let total = std::fs::metadata(path).map_err(|error| invalid(format!("the source file is unreadable: {error}")))?.len();
        if total == 0 { return Err(invalid(crate::store::files::EMPTY_SOURCE)); }
        let sizing = self.upload_operations()?.sizing(&self.upload_config);
        if held && !sizing.multipart_declared { return Err(invalid("a staged upload needs the multipart steps")); }
        let (parts, multipart) = sizing.of(total, held);
        Ok((Source { path: path.to_string(), parts }, multipart))
    }

    fn checked_prepare(&self, prepare: Option<&super::upload_wire::WirePrepare>) -> Result<(), DbError> {
        let Some(prepare) = prepare else { return Ok(()) };
        crate::prepare::check_settings(prepare)?;
        if self.preparer.get().is_none() { return Err(invalid("a source with prepare settings needs a preparer: pass one to open")); }
        Ok(())
    }

    /// The source of a call: the prepare settings are checked before the file is read.
    fn checked_source(&self, source: &WireSource, held: bool) -> Result<Option<(Source, bool)>, DbError> {
        self.checked_prepare(source.prepare())?;
        source.path().map(|path| self.source_of(path, held)).transpose()
    }

    fn supplied_source(&self, model: &str, row: &crate::identity::RowId, action: &str, source: WireUploadSource) -> Result<SuppliedSource, DbError> {
        self.checked_prepare(source.prepare.as_ref())?;
        let values = super::upload_wire::object(&source.values_json)?;
        let variables = super::upload_wire::object(&source.variables_json)?;
        let (planned, local) = self.db.plan_supplied(model, row, values)?;
        let effects = planned.effects;
        let (file, multipart) = self.source_of(&source.path, false)?;
        let name = source.name.unwrap_or_else(|| std::path::Path::new(&source.path).file_name().map_or_else(|| source.path.clone(), |name| name.to_string_lossy().into_owned()));
        let spec = self.db.spec(model)?;
        let declaration = spec.upload_action(action).ok_or_else(|| invalid("upload action is not declared"))?;
        let current = self.db.read(&[model], |reader| reader.point(model, row.as_str()))?;
        let correlation = declaration.correlate.first().and_then(|field| current.as_ref().and_then(|row| row.get(field)).and_then(Value::as_str)).map_or_else(|| row.to_string(), str::to_string);
        let steps = StepsBody { multipart, mime: source.mime.unwrap_or_else(|| mime_of(&name).to_string()), name, signed_variable: "signedId".into(), item: correlation }.steps();
        let wrap = match &declaration.batch {
            Some(batch) => spec.actions.iter().find(|action| action.name == batch.single).and_then(|action| action.wrap.clone()),
            None => declaration.wrap.clone(),
        };
        Ok(SuppliedSource { source: file, prepare: source.prepare, steps, values: effects, local: Value::Object(local).to_string(), variables, wrap })
    }

    /// The request of the final mutation: the variables carry a sentinel where the signed id of the file goes.
    fn upload_mutation(&self, plan: &Plan<'_>, mutation: &str) -> Result<NewOp, DbError> {
        let path = self.life.graphql_path().ok_or_else(|| DbError::NoSession { detail: "transport is required for uploads".into() })?;
        let operation = self.reads.get(mutation).ok_or_else(|| invalid(format!("operation {mutation} is not registered")))?;
        let variables = crate::owners::write::plan_variables::upload_variables(plan.declaration, Value::Object(plan.variables.clone()), &uuid::Uuid::new_v4().to_string());
        let request = GraphQLRequest::new(operation.document.clone(), mutation.to_string(), variables);
        let body = crate::transport::encrypted::graphql_body(&request, true).to_string();
        Ok(NewOp { model: plan.model.to_string(), row_id: plan.row.to_string(), action: plan.action.to_string(), on_failure: crate::schema::OnFailure::Keep, origin: OpSource::Api, path: path.to_string(), body, effects: plan.effects.clone(), expires_after: None })
    }

    fn upload_item(&self, plan: &Plan<'_>, mutation: &str, (fields, item): (Map<String, Value>, &str), source: Option<(Source, bool)>) -> Result<BatchItem, DbError> {
        let steps = source.as_ref().map(|(file, multipart)| {
            let name = file_name(plan.file, &file.path);
            StepsBody { multipart: *multipart, mime: plan.file.source.mime().map_or_else(|| mime_of(&name).to_string(), str::to_string), name, signed_variable: "signedId".into(), item: item.to_string() }.steps()
        });
        Ok(BatchItem { key: key_of(plan.model, plan.row, plan.action), held: false, fields, urgent: false, source: source.map(|(source, _)| source), prepare: plan.file.source.prepare().cloned(), mutation: self.upload_mutation(plan, mutation)?, steps })
    }

    fn single_of(&self, model: &str, action: &str, file: &UploadFile, held: bool) -> Result<Single, DbError> {
        self.upload_operations()?;
        let spec = self.db.spec(model)?;
        let declaration = spec.upload_action(action).filter(|declared| declared.batch.is_none()).cloned().ok_or_else(|| invalid(format!("{model} declares no single upload action {action}")))?;
        let mutation = declaration.mutation.clone().ok_or_else(|| invalid(format!("{model} {action} declares no mutation")))?;
        Ok(Single { declaration, mutation, source: self.checked_source(&file.source, held)? })
    }

    fn single_upload(&self, (model, row, action): (&str, &str, &str), single: Single, file: &UploadFile, effects: IntentEffects) -> Result<BatchItem, DbError> {
        let mut variables = file.variables.clone();
        variables.insert("signedId".into(), Value::String(SIGNED_ID_SENTINEL.into()));
        self.upload_item(&Plan { model, row, action, declaration: &single.declaration, variables, file, effects }, &single.mutation, (Map::new(), row), single.source)
    }

    /// One item of a batch: the row of the item is planned like a `create`, its final mutation carries the item alone.
    fn batch_upload(&self, model: &str, action: &str, planned: &Planned, item: &crate::owners::write::upload::UploadItem, request: &crate::owners::write::upload::UploadBatchInput) -> Result<BatchItem, DbError> {
        let spec = self.db.spec(model)?;
        let declaration = spec.actions.iter().find(|declared| declared.name == action).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = declaration.batch.as_ref().ok_or_else(|| invalid("operation has no upload batch declaration"))?;
        let mut fields = item.variables.clone();
        let data = planned.input.as_object().cloned().unwrap_or_default();
        for (target, source) in &batch.input { fields.insert(target.clone(), data.get(source).cloned().unwrap_or(Value::Null)); }
        fields.insert("signedId".into(), Value::String(SIGNED_ID_SENTINEL.into()));
        let shown = fields.clone();
        let single = spec.actions.iter().find(|declared| declared.name == batch.single && declared.mutation.is_some()).filter(|_| batch.min > 1);
        let single_name = single.map(|declared| declared.name.clone());
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
        let file = UploadFile { source: item.source.clone(), variables: Map::new() };
        let correlate = spec.actions.iter().find(|declared| declared.name == action).and_then(|declared| declared.correlate.first()).and_then(|field| item.values.get(field)).and_then(Value::as_str).unwrap_or(planned.row_id.as_str()).to_string();
        let source = self.checked_source(&file.source, false)?;
        let mut built = self.upload_item(&Plan { model, row: planned.row_id.as_str(), action, declaration, variables, file: &file, effects: planned.effects.clone() }, &mutation, (shown, &correlate), source)?;
        if let Some(name) = single_name { built.mutation.action = name; }
        Ok(built)
    }

    /// How the elements of a batch go out together: at least `min` of them in one request of the batch action, fewer one by one.
    fn joint_of(&self, model: &str, request: &crate::owners::write::upload::UploadBatchInput) -> Result<Joint, DbError> {
        let spec = self.db.spec(model)?;
        let declaration = spec.actions.iter().find(|declared| declared.name == request.operation).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = declaration.batch.as_ref().ok_or_else(|| invalid("operation has no upload batch declaration"))?;
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

    async fn transfer_answer(&self, epoch: u64, waiter: crate::runtime::Waiter<LifeReply>, what: &str) -> Result<TransferAnswer, DbError> {
        match self.in_epoch(epoch, "upload", self.await_life(waiter)).await? {
            LifeReply::Transfer(TransferAnswer::Err(fault)) => Err(refused(fault, what)),
            LifeReply::Transfer(answer) => Ok(answer),
            _ => Err(super::database::foreign_reply()),
        }
    }

    pub(super) fn transfer_receipt(&self, epoch: u64, model: String, held: Receipt, value: Option<super::wire::WireRow>) -> WireOperationReceipt {
        receipt_of_transfer(epoch, model, held, value, self.life.clone())
    }
}

#[uniffi::export]
impl Database {
    pub async fn supply_source(&self, epoch: u64, model: String, row_id: String, action: String, source: WireUploadSource) -> Result<(), DbError> {
        let (row, _) = self.operation_target(epoch, &model, &row_id, Some(&action), true).await?;
        let row = id(&row)?;
        let database = self.me.upgrade().ok_or_else(super::command::closed)?;
        let (name, verb, target) = (model.clone(), action.clone(), row.clone());
        let source = self.offload(move || database.supplied_source(&name, &target, &verb, source)).await?;
        match self.transfer_answer(epoch, self.life.supply_source(epoch, key_of(&model, row.as_str(), &action), source), "supply source").await? {
            TransferAnswer::Ok => Ok(()),
            _ => Err(super::database::foreign_reply()),
        }
    }

    pub async fn fail_source(&self, epoch: u64, model: String, row_id: String, action: String, message: String) -> Result<(), DbError> {
        let (row, _) = self.operation_target(epoch, &model, &row_id, Some(&action), true).await?;
        match self.transfer_answer(epoch, self.life.fail_source(epoch, key_of(&model, row.as_str(), &action), message), "fail source").await? {
            TransferAnswer::Ok => Ok(()),
            _ => Err(super::database::foreign_reply()),
        }
    }

    pub async fn upload(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let file = self.offload(move || request.into_file()).await?;
        let single = self.single_of(&model, &action, &file, false)?;
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let (db, name, verb, target, args) = (self.db.clone(), model.clone(), action.clone(), row.clone(), file.variables.clone());
        let effects = self.offload(move || {
            db.validate_act(&name, Map::new(), &args)?;
            match db.plan_act(&name, &verb, &target, Map::new(), args) {
                Ok(planned) => Ok(planned.effects),
                Err(crate::store::CoreError::NotFound { .. }) => Ok(IntentEffects::default()),
                Err(error) => Err(error.into()),
            }
        }).await?;
        let item = self.single_upload((&model, row.as_str(), &action), single, &file, effects)?;
        let waiter = self.life.accept_upload(epoch, item);
        let TransferAnswer::Receipt(held) = self.transfer_answer(epoch, waiter, "upload").await? else { return Err(DbError::Internal { detail: "the owner answered an upload with no receipt".into() }) };
        let value = held.optimistic.as_ref().map(|shown| self.wire_of_row(&model, shown)).transpose()?;
        Ok(self.transfer_receipt(epoch, model, held, value))
    }

    /// The file goes up before its row exists: `commit_staged` sends the final mutation (D-372).
    pub async fn stage(&self, epoch: u64, model: String, action: String, request: WireUploadRequest) -> Result<WireStagedUpload, DbError> {
        self.account_call(epoch)?;
        let file = self.offload(move || request.into_file()).await?;
        let single = self.single_of(&model, &action, &file, true)?;
        if single.source.is_none() { return Err(invalid("a staged upload needs a file")); }
        let row = crate::identity::RowId::generate_temp(None);
        let mut item = self.single_upload((&model, row.as_str(), &action), single, &file, IntentEffects::default())?;
        item.held = true;
        let waiter = self.life.accept_upload(epoch, item);
        let TransferAnswer::Receipt(held) = self.transfer_answer(epoch, waiter, "stage").await? else { return Err(DbError::Internal { detail: "the owner answered a stage with no receipt".into() }) };
        Ok(WireStagedUpload { row_id: row.to_string(), receipt: self.transfer_receipt(epoch, model, held, None) })
    }

    pub async fn commit_staged(&self, epoch: u64, model: String, row_id: String, action: String, values_json: String, variables_json: String) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let (db, name, row) = (self.db.clone(), model.clone(), row_id.clone());
        let (planned, variables) = self.offload(move || {
            let mut values = super::upload_wire::object(&values_json)?;
            values.insert("id".into(), Value::String(row));
            let variables = super::upload_wire::object(&variables_json)?;
            match db.plan_create(&name, values, Map::new()) {
                Ok(planned) => Ok((Some(planned.effects), variables)),
                Err(crate::store::CoreError::AlreadyExists { .. }) => Ok((None, variables)),
                Err(error) => Err(error.into()),
            }
        }).await?;
        let wrap = self.db.spec(&model)?.upload_action(&action).ok_or_else(|| invalid("upload action is not declared"))?.wrap.clone();
        let refused = planned.is_none();
        let waiter = self.life.commit_upload(epoch, key_of(&model, &row_id, &action), CommitRequest { effects: planned.unwrap_or_default(), variables, wrap, refused });
        let TransferAnswer::Receipt(held) = self.transfer_answer(epoch, waiter, "commit").await? else { return Err(DbError::Internal { detail: "the owner answered a commit with no receipt".into() }) };
        let value = held.optimistic.as_ref().map(|shown| self.wire_of_row(&model, shown)).transpose()?;
        Ok(self.transfer_receipt(epoch, model, held, value))
    }

    pub async fn upload_create_batch(&self, epoch: u64, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<WireBatchReceipt, DbError> {
        self.account_call(epoch)?;
        let (items, request) = self.offload(move || Ok((items.into_iter().map(WireUploadItem::into_item).collect::<Result<Vec<_>, _>>()?, request.into_input()?))).await?;
        self.upload_operations()?;
        let spec = self.db.spec(&model)?;
        let action = spec.actions.iter().find(|declared| declared.name == request.operation).ok_or_else(|| invalid("unknown upload operation"))?;
        let batch = action.batch.as_ref().ok_or_else(|| invalid("operation has no upload batch declaration"))?;
        if !batch_size_admitted((batch.min, batch.max), items.len()) { return Err(invalid(format!("upload batch requires {}..{} items", batch.min, batch.max))); }
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
        let waiter = self.life.accept_upload_batch(epoch, built, joint);
        let TransferAnswer::Receipts(held) = self.transfer_answer(epoch, waiter, "upload batch").await? else { return Err(DbError::Internal { detail: "the owner answered a batch with no receipts".into() }) };
        let mut items = Vec::new();
        for ((item, planned, _), held) in planned.into_iter().zip(held) {
            let key = item.values.get(&field).and_then(Value::as_str).unwrap_or_default().to_string();
            let value = held.optimistic.as_ref().map(|shown| self.wire_of_row(&model, shown)).transpose()?.ok_or_else(|| DbError::Internal { detail: format!("the accepted row {} of {model} is not shown", planned.row_id) })?;
            items.push(WireItemReceipt { item: ItemId { value: key }, optimistic_value: value.clone(), receipt: self.transfer_receipt(epoch, model.clone(), held, Some(value)) });
        }
        Ok(WireBatchReceipt { epoch, items })
    }

    pub async fn cancel_upload(&self, epoch: u64, model: String, row_id: String, action: String) -> Result<bool, DbError> {
        let (row, _) = self.operation_target(epoch, &model, &row_id, Some(&action), true).await?;
        if self.uploads.is_none() { return Err(DbError::NotFound { detail: "cancel upload: there is no such operation".into() }); }
        let waiter = self.life.cancel_upload(epoch, key_of(&model, &row, &action));
        match self.transfer_answer(epoch, waiter, "cancel upload").await? {
            TransferAnswer::Cancelled(cancelled) => Ok(cancelled),
            _ => Err(DbError::Internal { detail: "the owner answered a cancel with no result".into() }),
        }
    }
}

impl Database {
    /// Answers when the writes of the epoch have no work that runs without a new external event: no transfer step in the process, no batch in flight, no operation on its way. A backoff, the network and a slot are waits for an event. The end of the epoch and `close` answer too.
    /// Transfer answers, Outbox answers, Transfer answers again: when Transfer stepped no input in between, the first answer still held at the Outbox answer, so both owners were quiet at one moment. Otherwise the work Transfer handed over is waited for in the next round.
    #[cfg(test)]
    pub(crate) async fn writes_quiet(&self, epoch: u64) {
        loop {
            let Some(before) = self.transfers_quiet_at(epoch).await else { return };
            if self.await_life(self.life.operations_quiet(epoch)).await.is_err() { return }
            if before.is_none() || self.transfers_quiet_at(epoch).await == Some(before) { return }
        }
    }

    #[cfg(test)]
    async fn transfers_quiet_at(&self, epoch: u64) -> Option<Option<u64>> {
        match self.await_life(self.life.transfers_quiet(epoch)).await {
            Ok(LifeReply::Transfer(TransferAnswer::Quiet(handled))) => Some(Some(handled)),
            Ok(LifeReply::Transfer(TransferAnswer::Err(crate::owners::write::outbox::Fault::NoSession))) => Some(None),
            _ => None,
        }
    }

    /// `send` with a file: the row is written like a `create`, the final mutation is the declared upload action.
    pub(super) async fn send_upload(&self, request: super::command::CommandRequest) -> Result<WireOperationReceipt, DbError> {
        let epoch = request.epoch;
        self.account_call(epoch)?;
        let Some(upload) = request.upload else { return Err(invalid("a command without a file goes through the owners")) };
        let (db, name, texts) = (self.db.clone(), request.model.clone(), (request.values_json.clone(), request.args_json.clone()));
        let (file, planned) = self.offload(move || Ok((upload.request.into_file()?, db.plan_create(&name, serde_json::from_str(&texts.0)?, serde_json::from_str(&texts.1)?)?))).await?;
        let single = self.single_of(&request.model, &upload.action, &file, false)?;
        let item = self.single_upload((&request.model, planned.row_id.as_str(), &upload.action), single, &file, planned.effects.clone())?;
        let waiter = self.life.accept_upload(epoch, item);
        let TransferAnswer::Receipt(held) = self.transfer_answer(epoch, waiter, "send").await? else { return Err(DbError::Internal { detail: "the owner answered a send with no receipt".into() }) };
        let value = held.optimistic.as_ref().map(|shown| self.wire_of_row(&request.model, shown)).transpose()?.ok_or_else(|| DbError::Internal { detail: format!("the accepted row {} of {} is not shown", planned.row_id, request.model) })?;
        Ok(self.transfer_receipt(epoch, request.model, held, Some(value)))
    }

    /// `retry` of a transfer with a new source of a file that is gone.
    pub(super) async fn retry_transfer(&self, epoch: u64, model: String, row_id: String, action: String, source: Option<WireUploadSource>) -> Result<WireOperationReceipt, DbError> {
        self.account_call(epoch)?;
        let row = self.resolved(epoch, &model, id(&row_id)?).await?;
        let source = if let Some(source) = source {
            let database = self.me.upgrade().ok_or_else(super::command::closed)?;
            let (name, verb, target) = (model.clone(), action.clone(), row.clone());
            Some(self.offload(move || database.supplied_source(&name, &target, &verb, source)).await?)
        } else { None };
        let waiter = self.life.retry_upload(epoch, key_of(&model, row.as_str(), &action), source);
        let TransferAnswer::Receipt(held) = self.transfer_answer(epoch, waiter, "retry").await? else { return Err(DbError::Internal { detail: "the owner answered a retry with no receipt".into() }) };
        let value = held.optimistic.as_ref().map(|shown| self.wire_of_row(&model, shown)).transpose()?;
        Ok(self.transfer_receipt(epoch, model, held, value))
    }

    /// `discard` of the transfer of an action.
    pub(super) async fn discard_transfer(&self, epoch: u64, model: &str, row: &str, action: &str) -> Result<(), DbError> {
        let waiter = self.life.discard_upload(epoch, key_of(model, row, action));
        self.transfer_answer(epoch, waiter, "discard").await.map(drop)
    }

    /// The outcome of the receipt of a transfer.
    pub(super) async fn transfer_outcome(&self, receipt: WireOperationReceipt, held: Receipt, discard_on_failure: bool) -> Result<super::WireConfirmed, DbError> {
        let waiter = self.life.upload_outcome(receipt.epoch, held.clone(), discard_on_failure);
        let (answer, landed) = match self.await_life(waiter).await {
            Ok(LifeReply::Transfer(TransferAnswer::Outcome(outcome, row))) => (outcome, row),
            Ok(LifeReply::Transfer(TransferAnswer::Err(fault))) => return Err(super::write::fault_error(fault, "upload outcome")),
            Ok(_) => return Err(super::database::foreign_reply()),
            Err(error) => return Err(super::write::end_error(error)),
        };
        match answer {
            OutboxOutcome::Confirmed { result, .. } => {
                let value = landed.map(|row| self.wire_of_row(&receipt.model, &row)).transpose()?;
                Ok(super::WireConfirmed { value, answer_json: result })
            }
            OutboxOutcome::Rejected { error } => Err(DbError::Rejected { failure: super::write::failure_of(&error), kept: false }),
            OutboxOutcome::Failed { error } => Err(DbError::Rejected { failure: super::write::failure_of(&error), kept: true }),
            OutboxOutcome::Discarded => Err(super::cause_error(&super::EndCause::Discarded)),
            OutboxOutcome::Ended(cause) => Err(super::cause_error(&cause)),
        }
    }
}
