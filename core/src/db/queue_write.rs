//! The executor `Write` of `store.md` for the ops of the queue: `Intent`, `RollBack`, `RemoveIntent`, `MarkError`, `Rows`, `LocalRows`, `RemoveRows`,
//! `Upload`, `RemoveUpload`. The owners Outbox and Transfer decide, this code turns their ops into SQL through the commit path of `Db`.

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, OperationRecord, OperationTransition, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ListKey, ModelKey, RowId};
use crate::operation::{Operation, OperationStatus};
use crate::files::Location;
use crate::owners::store::{EffectRow, IntentEffects, RowValue, StoreOp, UploadKind, UploadRow};
use crate::owners::write::outbox::{NewOp, Origin};
use crate::owners::write::transfer::{Source, Steps, StoredTransfer};
use crate::runtime::Time;
use crate::schema::OnFailure;

use crate::lifecycle::authority::Authority;
use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;
use super::prior::{Prior, PriorRow};

fn json_fault(error: serde_json::Error) -> DbError { DbError::Internal(error.to_string()) }

fn row_id(value: &str) -> Result<RowId, DbError> { RowId::new(value).map_err(|error| DbError::InvalidUpload(error.to_string())) }

fn values(text: &str) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(text).map_err(json_fault)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::InvalidUpload("the values of a row are not an object".into())),
    }
}

fn upserts(model: &str, rows: &[RowValue]) -> Result<Vec<EntityOp>, DbError> {
    rows.iter().map(|row| Ok(EntityOp::Upsert { model: ModelKey::new(model), id: row_id(&row.row_id)?, row: values(&row.values)? })).collect()
}

impl Db {
    /// Applies the ops of one batch. The batch of an operation holds the ops of one commit kind, so each op is one commit.
    pub(crate) fn write_queue_ops(&self, ops: &[StoreOp]) -> Result<(), DbError> {
        let generation = self.generation();
        let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
        let mut rolled_back = Vec::new();
        for op in ops {
            match op {
                StoreOp::RollBack { op_id } => rolled_back.push(i64::try_from(*op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?),
                StoreOp::RemoveIntent { op_id } if rolled_back.contains(&i64::try_from(*op_id).unwrap_or(i64::MAX)) => {}
                StoreOp::RemoveIntent { op_id } => envelope.operation_transitions.push(OperationTransition::Close { seq: i64::try_from(*op_id).unwrap_or(i64::MAX), status: OperationStatus::Committed }),
                StoreOp::Rows { model, rows } => {
                    envelope.origin = WriteOrigin::Response;
                    envelope.entity_ops.extend(upserts(model, rows)?);
                }
                StoreOp::LocalRows { model, rows } => {
                    envelope.writes_local = true;
                    envelope.entity_ops.extend(upserts(model, rows)?);
                }
                StoreOp::RemoveRows { model, ids } => {
                    for id in ids {
                        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: row_id(id)?, reason: DestroyReason::Local });
                    }
                }
                StoreOp::MarkError { op_id, error } => {
                    let seq = i64::try_from(*op_id).unwrap_or(i64::MAX);
                    envelope.operation_transitions.push(match error {
                        Some(message) => OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, message.clone()) },
                        None => OperationTransition::Retry { seq },
                    });
                }
                StoreOp::Upload { row, kind } => self.write_upload_row(row, *kind)?,
                StoreOp::RemoveUpload { key } => self.remove_upload_row(key)?,
                StoreOp::Intent { op_id, model, row_id: id, action, payload, effects } => return self.write_intent(*op_id, (model, id, action), payload, effects),
                StoreOp::Pending { .. } | StoreOp::RemovePending | StoreOp::Session { .. } => {}
            }
        }
        for seq in rolled_back {
            self.roll_back_gated(&Authority::Owner, seq, generation, None)?;
        }
        if envelope.entity_ops.is_empty() && envelope.operation_transitions.is_empty() {
            return Ok(());
        }
        self.commit_following(&Authority::Owner, generation, |_| Ok::<_, DbError>(Some(envelope.clone()))).map_err(plan_fault)?;
        Ok(())
    }

    /// `Intent`: the optimistic rows, the membership, the tombstones and the operation, in one commit. The prior state the rollback restores is read here.
    fn write_intent(&self, op_id: u64, (model, id, action): (&str, &str, &str), payload: &str, effects: &IntentEffects) -> Result<(), DbError> {
        let generation = self.generation();
        let seq = i64::try_from(op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?;
        let created_at = self.shared.store.lock().now_ms();
        let input: Value = serde_json::from_str(payload).map_err(json_fault)?;
        let owner = row_id(id)?;
        let (result, _) = self
            .commit_following_deferred(&Authority::Owner, generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                let mut prior = Prior::default();
                for EffectRow { model: row_model, row_id: row_key, values: text } in &effects.rows {
                    let next = values(text)?;
                    prior.rows.push(match reader.point(row_model, row_key)? {
                        Some(current) => PriorRow::edited(reader, row_model, row_key, &current, &next)?,
                        None => PriorRow::Created { model: row_model.clone(), id: row_key.clone() },
                    });
                    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(row_model), id: row_id(row_key)?, row: next });
                }
                for tombstone in &effects.tombstones {
                    if let Some(destroyed) = PriorRow::destroyed(reader, &tombstone.model, &tombstone.row_id)? {
                        prior.rows.push(destroyed);
                    }
                    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&tombstone.model), id: row_id(&tombstone.row_id)?, reason: DestroyReason::Local });
                }
                for member in &effects.members {
                    envelope.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), ListKey::from_raw(&member.list)), row: row_id(&member.row_id)?, order_key: format!("{:020}", member.revision) });
                }
                envelope.snapshot = Some(crate::identity::RowRef::new(ModelKey::new(model), owner.clone()));
                envelope.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
                    seq: Some(seq),
                    action_key: Operation::action_key(model, action),
                    status: OperationStatus::Pending,
                    row_ids: vec![owner.clone()],
                    temp_id: None,
                    input: input.clone(),
                    transport_started: false,
                    base_revision: Some(reader.revision_seq()?),
                    created_at,
                    dedupe_key: None,
                    rollback: Some(serde_json::to_value(&prior).map_err(json_fault)?),
                    upload: None,
                })));
                Ok::<_, DbError>(Some(envelope))
            })
            .map_err(plan_fault)?;
        result.publish();
        Ok(())
    }

    /// `Upload{row, kind}`: the whole value of a transfer in `sync_upload` for the key `model:row_id:action`. The kind of the batch names the status.
    fn write_upload_row(&self, row: &UploadRow, kind: UploadKind) -> Result<(), DbError> {
        let [model, id, action] = key_parts(&row.key)?;
        let (status, stage) = match kind {
            UploadKind::Failed => ("failed", "failed"),
            UploadKind::Completed => ("uploading", "mutating"),
            UploadKind::SyncUpload | UploadKind::ConfirmedParts | UploadKind::Retrying => ("uploading", "uploading"),
        };
        let on_failure = match row.on_failure {
            OnFailure::Rollback => "rollback",
            OnFailure::Keep => "keep",
        };
        self.shared
            .store
            .write_store(&Authority::Owner, |stored| {
                stored.connection.execute(
                    "INSERT OR REPLACE INTO sync_upload (model, row_id, action, status, stage, sent, total, wait, attempts, error_kind, error_message, seq, source, batch, upload_id, completed, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure, steps_path, steps_body) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 0, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)",
                    rusqlite::params![
                        model, id, action, status, stage, row.confirmed_parts, row.parts, row.failed.as_ref().map(|_| "failed"), row.failed,
                        i64::try_from(row.seq).unwrap_or(i64::MAX), row.source, row.batch, row.upload_id, row.completed, i64::try_from(row.created_at).unwrap_or(i64::MAX), row.inline,
                        i64::try_from(row.size).unwrap_or(i64::MAX), row.sha256, row.path, row.body, on_failure, row.steps_path, row.steps_body
                    ],
                )?;
                Ok(())
            })
            .map_err(DbError::from)
    }

    /// The transfers kept in the base, whole, for `Restored` (`transfer.md`). A row of the display without the value of a transfer (`seq` empty) is not one.
    pub(crate) fn read_transfers(&self) -> Result<Vec<StoredTransfer>, DbError> {
        let rows = self
            .shared
            .store
            .read_locked(|connection| {
                let mut statement = connection.prepare(
                    "SELECT model, row_id, action, seq, source, total, batch, upload_id, sent, completed, error_message, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure, steps_path, steps_body \
                     FROM sync_upload WHERE seq IS NOT NULL ORDER BY seq",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        [row.get::<_, String>(0)?, row.get(1)?, row.get(2)?],
                        row.get::<_, i64>(3)?, row.get::<_, String>(4)?, row.get::<_, i64>(5)?, row.get::<_, bool>(6)?, row.get::<_, Option<String>>(7)?, row.get::<_, i64>(8)?,
                        row.get::<_, bool>(9)?, row.get::<_, Option<String>>(10)?, row.get::<_, i64>(11)?, row.get::<_, bool>(12)?, row.get::<_, i64>(13)?, row.get::<_, String>(14)?,
                        row.get::<_, String>(15)?, row.get::<_, String>(16)?, row.get::<_, String>(17)?, row.get::<_, String>(18)?, row.get::<_, String>(19)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .map_err(|error| DbError::Internal(error.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|([model, id, action], seq, source, total, batch, upload_id, sent, completed, failed, created_at, inline, size, sha256, path, body, on_failure, steps_path, steps_body)| {
                let key = format!("{model}:{id}:{action}");
                let place = if inline { Location::Table } else { Location::Directory };
                let file_id = crate::owners::write::transfer::file_id(&key);
                let file_present = match place {
                    Location::Table => self.shared.store.read_locked(|connection| connection.query_row("SELECT COUNT(*) FROM file WHERE id = ?1", [&file_id], |row| row.get::<_, i64>(0))).map(|count| count > 0),
                    Location::Directory => Ok(self.shared.files.path(&file_id).is_some_and(|path| path.exists())),
                }
                .map_err(|error| DbError::Internal(error.to_string()))?;
                Ok(StoredTransfer {
                    key,
                    seq: unsigned(seq, "seq")?,
                    source: Source { path: source, parts: unsigned(total, "total")? },
                    batch,
                    upload_id,
                    confirmed_parts: unsigned(sent, "sent")?,
                    completed,
                    failed,
                    file_present,
                    created_at: Time(unsigned(created_at, "created_at")?),
                    place,
                    size: unsigned(size, "size")?,
                    sha256,
                    mutation: NewOp {
                        model: model.clone(),
                        row_id: id.clone(),
                        action: action.clone(),
                        on_failure: if on_failure == "keep" { OnFailure::Keep } else { OnFailure::Rollback },
                        origin: Origin::Api,
                        path,
                        body,
                        effects: IntentEffects::default(),
                    },
                    steps: Steps { path: steps_path, body: steps_body },
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?)
    }

    fn remove_upload_row(&self, key: &str) -> Result<(), DbError> {
        let [model, id, action] = key_parts(key)?;
        self.shared
            .store
            .write_store(&Authority::Owner, |stored| {
                stored.connection.execute("DELETE FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND action = ?3", rusqlite::params![model, id, action])?;
                Ok(())
            })
            .map_err(DbError::from)
    }
}

fn unsigned<T: TryFrom<i64>>(value: i64, column: &str) -> Result<T, DbError> {
    T::try_from(value).map_err(|_| DbError::Internal(format!("sync_upload.{column} holds {value}")))
}

fn key_parts(key: &str) -> Result<[&str; 3], DbError> {
    let mut parts = key.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(model), Some(id), Some(action)) => Ok([model, id, action]),
        _ => Err(DbError::InvalidUpload(format!("transfer key `{key}` is not model:row_id:action"))),
    }
}
