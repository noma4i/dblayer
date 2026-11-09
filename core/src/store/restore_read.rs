//! The intents of the queue as `Restored` gives them to the owner Outbox.

use crate::owners::write::operation_status::OperationStatus;
use crate::owners::write::outbox::{NewOp, OpSource, StoredOp, TransferMember};
use crate::schema::OnFailure;
use crate::transport::RequestOwner;

use super::db_error::CoreError;
use super::plan::read_outbox;
use super::Store;
use super::upload_rows::UploadRaw;

#[derive(Default)]
struct Origins {
    valid: std::collections::BTreeMap<i64, OpSource>,
    invalid_ops: std::collections::BTreeSet<i64>,
    invalid_transfers: std::collections::BTreeSet<u64>,
    invalid_groups: std::collections::BTreeSet<u64>,
}

fn read_origins(connection: &rusqlite::Connection) -> Result<Origins, super::StoreError> {
    let rows = connection.prepare(include_str!("sql/restore_read/read_origins_select_op.sql"))?.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, Option<i64>>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut origins = Origins::default();
    for (seq, kind, transfer, batch) in rows {
        let member = |id: i64| -> Result<TransferMember, super::StoreError> {
            let (cycle, row_id): (i64, String) = connection.query_row(include_str!("sql/restore_read/read_origins_select_sync_upload.sql"), [id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok(TransferMember { transfer_id: u64::try_from(id).map_err(|_| super::StoreError::Constraint("invalid transfer id".into()))?, cycle_id: u64::try_from(cycle).map_err(|_| super::StoreError::Constraint("invalid transfer cycle".into()))?, row_id })
        };
        let origin = match (kind.as_str(), transfer, batch) {
            ("api", None, None) => Some(OpSource::Api),
            ("transfer", Some(id), None) => match member(id) {
                Ok(found) => Some(OpSource::Transfer { transfer_id: found.transfer_id, cycle_id: found.cycle_id }),
                Err(_) => { if let Ok(id) = u64::try_from(id) { origins.invalid_transfers.insert(id); } None },
            },
            ("transfer_group", None, Some(batch_id)) => {
                let entries = connection.prepare(include_str!("sql/restore_read/read_origins_select_op_transfer.sql"))?.query_map([seq], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                let ordered = entries.iter().enumerate().all(|(index, (position, _))| *position == index as i64);
                let ids: Vec<_> = entries.into_iter().map(|(_, id)| id).collect();
                let found = ids.iter().map(|id| member(*id)).collect::<Result<Vec<_>, _>>();
                let valid_group: bool = connection.query_row(include_str!("sql/restore_read/read_origins_select_sync_upload_batch.sql"), rusqlite::params![batch_id, seq], |row| row.get(0))?;
                let mut belongs = true;
                for id in &ids {
                    let matched: bool = connection.query_row(include_str!("sql/restore_read/read_origins_select_sync_upload_2.sql"), rusqlite::params![id, batch_id], |row| row.get(0))?;
                    belongs &= matched;
                }
                match (u64::try_from(batch_id), found) {
                    (Ok(batch_id), Ok(members)) if valid_group && belongs && ordered && !members.is_empty() && members.iter().map(|member| member.transfer_id).collect::<std::collections::BTreeSet<_>>().len() == members.len() => Some(OpSource::TransferGroup { batch_id, members }),
                    (group, _) => {
                        if let Ok(group) = group { origins.invalid_groups.insert(group); }
                        origins.invalid_transfers.extend(ids.into_iter().filter_map(|id| u64::try_from(id).ok()));
                        None
                    }
                }
            }
            _ => None,
        };
        if let Some(origin) = origin { origins.valid.insert(seq, origin); } else { origins.invalid_ops.insert(seq); }
    }
    Ok(origins)
}

pub(crate) struct Restore {
    pub aliases: std::collections::BTreeMap<(String, String), String>,
    pub ops: Result<Vec<StoredOp>, CoreError>,
    pub transfers: Result<Vec<crate::owners::write::transfer::StoredTransfer>, CoreError>,
    pub transfer_outcomes: std::collections::BTreeMap<u64, std::collections::BTreeMap<u64, crate::owners::write::outbox::OutboxOutcome>>,
    /// `(upload_id, key)` of the multipart uploads of removed transfers that wait for the answer of `abort`.
    pub orphans: Vec<(String, String)>,
    /// The rule of every batch that still has an element in the base.
    pub batches: std::collections::BTreeMap<u64, crate::owners::write::transfer::StoredGroup>,
    /// The highest `op_id` a transfer reserved: Outbox gives no `op_id` at or below it.
    pub reserved: Option<u64>,
    /// The saved answers that no operation of the queue owns.
    pub orphan_answers: Vec<u64>,
    pub transfer_pending_orphan: bool,
}

fn transfer_asking_again<'a>(rows: &'a [crate::owners::write::transfer::StoredTransfer], owner: &crate::transport::RequestOwner) -> Option<&'a crate::owners::write::transfer::StoredTransfer> {
    use crate::owners::write::upload::UploadStage;
    let crate::transport::RequestOwner::Transfer { item_id, step, .. } = owner else { return None };
    rows.iter().find(|row| {
        let open = row.restore_failed.is_none() && row.failed.is_none() && !row.completed && !row.aborted && !row.copying;
        match step {
            UploadStage::Create | UploadStage::Init | UploadStage::Presign => open && row.source.as_ref().is_some_and(|source| row.confirmed_parts < source.parts) && row.steps.as_ref().is_some_and(|steps| serde_json::from_str::<crate::owners::write::upload_codec::StepsBody>(&steps.body).is_ok_and(|body| body.item == *item_id)),
            UploadStage::Complete => open && row.source.as_ref().is_some_and(|source| row.confirmed_parts >= source.parts) && row.key == *item_id,
            UploadStage::Prepare | UploadStage::Put | UploadStage::Part | UploadStage::Abort | UploadStage::Mutation => false,
        }
    })
}

impl Store {
    pub(crate) fn read_restore(&self) -> Result<Restore, CoreError> {
        let (entries, origins, cycles, effects_only, pending, uploads, outcomes, answers, orphans, batches, orphan_answers, aliases) = self.read_models(&[], |reader| {
            let connection = reader.connection();
            let aliases = connection.prepare(include_str!("sql/restore_read/read_aliases.sql"))?.query_map([], |row| Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), row.get::<_, String>(2)?)))?.collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
            let pending = crate::store::session_state::pending(connection).map_err(crate::store::StoreError::from)?;
            let answers = crate::store::session_state::answers_of_operations(connection).map_err(crate::store::StoreError::from)?;
            let effects_only = connection.prepare(include_str!("sql/restore_read/read_restore_select_op.sql")).and_then(|mut statement| statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let cycles = connection.prepare(include_str!("sql/restore_read/read_restore_select_op_2.sql")).and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?.collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()).map_err(crate::store::StoreError::from)?;
            let uploads = super::upload_rows::upload_rows(connection).map_err(crate::store::StoreError::from)?;
            let origins = read_origins(connection)?;
            let outcomes = connection.prepare(include_str!("sql/restore_read/read_restore_select_transfer_outcome.sql")).and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<i64>>(3)?, row.get::<_, Option<String>>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let orphans = connection.prepare(include_str!("sql/restore_read/read_restore_select_upload_orphan.sql")).and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let batches = connection.prepare(include_str!("sql/restore_read/read_restore_select_sync_upload_batch.sql")).and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<i64>>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let orphan_answers = connection.prepare(include_str!("sql/restore_read/read_restore_select_encrypted_answer.sql")).and_then(|mut statement| statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            Ok((read_outbox(reader)?, origins, cycles, effects_only, pending, uploads, outcomes, answers, orphans, batches, orphan_answers, aliases))
        })?;
        let owned: Vec<i64> = uploads.iter().filter_map(UploadRaw::owned_op).collect();
        let reserved = uploads.iter().filter_map(UploadRaw::reserved_op).chain(batches.iter().filter_map(|(_, _, op)| *op)).max().and_then(|op| u64::try_from(op).ok());
        let entries = entries.into_iter().filter(|entry| !effects_only.contains(&entry.operation.id.0) && !owned.contains(&entry.operation.id.0)).collect();
        let ops = self.stored_intents(entries, pending.clone(), answers, &origins.valid, &origins.invalid_ops, &cycles);
        let intent_ids: std::collections::BTreeSet<u64> = origins.valid.values().flat_map(|origin| match origin {
            OpSource::Transfer { transfer_id, .. } => vec![*transfer_id],
            OpSource::TransferGroup { members, .. } => members.iter().map(|member| member.transfer_id).collect(),
            OpSource::Api => Vec::new(),
        }).collect();
        let transfers = self.stored_transfers(uploads).map(|mut rows| {
            for row in &mut rows {
                row.has_intent = intent_ids.contains(&row.transfer_id);
                if origins.invalid_transfers.contains(&row.transfer_id) || row.batch.is_some_and(|group| origins.invalid_groups.contains(&group)) { row.restore_failed = Some("invalid intent origin".into()); }
            }
            rows
        });
        let mut transfer_outcomes = std::collections::BTreeMap::<u64, std::collections::BTreeMap<u64, crate::owners::write::outbox::OutboxOutcome>>::new();
        for (id, cycle, kind, revision, value) in outcomes {
            use crate::owners::write::outbox::OutboxOutcome;
            let id = u64::try_from(id).map_err(|_| CoreError::Internal("invalid transfer outcome id".into()))?;
            let cycle = u64::try_from(cycle).map_err(|_| CoreError::Internal("invalid transfer outcome cycle".into()))?;
            let outcome = match (kind.as_str(), revision, value) {
                ("confirmed", Some(revision), result) => OutboxOutcome::Confirmed { revision, result, row_id: None },
                ("rejected", None, Some(error)) => OutboxOutcome::Rejected { error },
                ("failed", None, Some(error)) => OutboxOutcome::Failed { error },
                ("discarded", None, None) => OutboxOutcome::Discarded,
                _ => return Err(CoreError::Internal("invalid transfer outcome".into())),
            };
            transfer_outcomes.entry(id).or_default().insert(cycle, outcome);
        }
        let batches = batches.into_iter().filter_map(|(id, text, op_id)| {
            let id = u64::try_from(id).ok()?;
            let intent_members = origins.valid.values().find_map(|origin| match origin { OpSource::TransferGroup { batch_id, members } if *batch_id == id => Some(members.iter().map(|member| member.transfer_id).collect()), _ => None }).unwrap_or_default();
            Some((id, crate::owners::write::transfer::StoredGroup { joint: crate::owners::write::transfer::Joint::from_text(&text)?, op_id: op_id.and_then(|op| u64::try_from(op).ok()), intent_members }))
        }).collect();
        let orphan_answers = orphan_answers.into_iter().filter_map(|id| u64::try_from(id).ok()).collect();
        let transfer_pending_orphan = match (&pending, &transfers) {
            (Some(held), Ok(rows)) => held.owner.as_ref().is_some_and(|owner| matches!(owner, crate::transport::RequestOwner::Transfer { .. }) && transfer_asking_again(rows, owner).is_none()),
            _ => false,
        };
        let transfers = transfers.map(|mut rows| {
            let asking = pending.as_ref().and_then(|held| held.owner.as_ref().and_then(|owner| transfer_asking_again(&rows, owner)).map(|row| (row.transfer_id, held.req_id)));
            if let Some((id, req_id)) = asking {
                if let Some(row) = rows.iter_mut().find(|row| row.transfer_id == id) { row.pending_step = Some(req_id); }
            }
            rows
        });
        Ok(Restore { aliases, ops, transfers, transfer_outcomes, orphans, batches, reserved, orphan_answers, transfer_pending_orphan })
    }

    fn stored_intents(&self, entries: Vec<super::plan::OutboxOperation>, pending: Option<crate::store::session_state::Pending>, mut answers: std::collections::BTreeMap<i64, (u64, crate::store::session_state::StoredAnswer)>, origins: &std::collections::BTreeMap<i64, OpSource>, invalid: &std::collections::BTreeSet<i64>, cycles: &std::collections::BTreeMap<i64, i64>) -> Result<Vec<StoredOp>, CoreError> {
        let mut stored = Vec::with_capacity(entries.len());
        for entry in entries {
            let operation = &entry.operation;
            let Some((model, action)) = operation.action_key.split_once(':') else {
                return Err(CoreError::Internal(format!("the action key `{}` is not model:action", operation.action_key)));
            };
            let Some(row_id) = operation.temp_id.as_ref().or_else(|| operation.row_ids.first()) else {
                return Err(CoreError::Internal(format!("the operation {} has no row", operation.id.0)));
            };
            let declared = self.spec(model)?.actions.iter().find(|declaration| declaration.name == action);
            let mutation = declared.and_then(|declaration| declaration.mutation.as_deref());
            if mutation.is_none() {
                return Err(CoreError::Internal(format!("the operation {} ({model}.{action}) declares no mutation to restore it with", operation.id.0)));
            }
            let op = NewOp {
                model: model.to_string(),
                row_id: row_id.as_str().to_string(),
                action: action.to_string(),
                on_failure: declared.map_or(OnFailure::Rollback, |declaration| declaration.on_failure),
                origin: if invalid.contains(&operation.id.0) { OpSource::Api } else { origins.get(&operation.id.0).cloned().ok_or_else(|| CoreError::Internal("missing intent origin".into()))? },
                path: String::new(),
                body: entry.input.to_string(),
                effects: Default::default(),
                created_at: u64::try_from(operation.created_at).unwrap_or(0),
                expires_after: declared.and_then(|declaration| declaration.expires_after).map(std::time::Duration::from_secs),
            };
            let seq = operation.id.0;
            let resume = pending.as_ref().filter(|pending| pending.owner == Some(RequestOwner::Operation { seq })).and_then(|pending| Some((pending.req_id, u64::try_from(pending.sequence).ok()?)));
            let answered = answers.remove(&seq);
            let restore_failed = invalid.contains(&seq).then(|| "invalid intent origin".to_string());
            let restore_element = restore_failed.as_ref().map(|_| ("op".to_string(), seq.to_string()));
            let cycle_id = cycles.get(&seq).copied().and_then(|cycle| u64::try_from(cycle).ok()).ok_or_else(|| CoreError::Internal("invalid operation cycle".into()))?;
            stored.push(StoredOp { answered, op_id: u64::try_from(seq).map_err(|_| CoreError::UnknownOperation(seq))?, op, error_mark: operation.status == OperationStatus::Failed, pending: resume, attempts: operation.attempts, cycle_id, restore_failed, restore_element });
        }
        stored.extend(self.dropped_intents()?);
        Ok(stored)
    }

    /// The operations the layout move could not carry: each stays in the queue as `Failed{RestoreFailed}` until its element is discarded.
    fn dropped_intents(&self) -> Result<Vec<StoredOp>, CoreError> {
        let mut dropped = Vec::new();
        for row in self.restore_failed()?.into_iter().filter(|row| row.table == "op") {
            let Ok(columns) = serde_json::from_str::<serde_json::Value>(&row.columns) else { continue };
            let Some(op_id) = columns.get("seq").and_then(serde_json::Value::as_u64) else { continue };
            let Some((model, action)) = columns.get("action_key").and_then(serde_json::Value::as_str).and_then(|key| key.split_once(':')) else { continue };
            let row_id = columns.get("temp_id").and_then(serde_json::Value::as_str).unwrap_or(&row.key).to_string();
            let op = NewOp { model: model.to_string(), row_id, action: action.to_string(), on_failure: OnFailure::Keep, origin: OpSource::Api, path: String::new(), body: String::new(), effects: Default::default(), created_at: 0, expires_after: None };
            dropped.push(StoredOp { answered: None, op_id, op, error_mark: false, pending: None, attempts: 1, cycle_id: 0, restore_failed: Some(row.reason.clone()), restore_element: Some((row.table.clone(), row.key.clone())) });
        }
        Ok(dropped)
    }
}
