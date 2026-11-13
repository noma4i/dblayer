//! The rows of `sync_upload` and `sync_upload_part`: the whole value of a transfer, read back for `Restored`.

use std::collections::BTreeMap;

use crate::store::files::Location;
use crate::owners::store::IntentEffects;
use crate::owners::write::outbox::{NewOp, OpSource};
use crate::owners::write::transfer::{Source, Steps, StoredTransfer};
use crate::runtime::Time;
use crate::schema::OnFailure;

use super::db_error::CoreError;
use super::Store;

type BatchRow = (crate::identity::RowId, serde_json::Map<String, serde_json::Value>);

/// A row of `sync_upload` as the columns give it.
pub(crate) struct UploadRaw {
    key: [String; 3],
    seq: i64,
    source: Option<String>,
    total: i64,
    batch: Option<i64>,
    upload_id: Option<String>,
    sent: i64,
    progress: i64,
    weight: Option<i64>,
    weighed_prepare: bool,
    batch_progress: i64,
    attempts: i64,
    completed: bool,
    failed: Option<String>,
    created_at: i64,
    inline: bool,
    size: i64,
    sha256: String,
    path: String,
    body: String,
    on_failure: String,
    steps_path: Option<String>,
    steps_body: Option<String>,
    transfer_id: i64,
    cycle_id: Option<i64>,
    signed_id: Option<String>,
    aborted: bool,
    op_id: Option<i64>,
    fields: Option<String>,
    etags: Vec<Option<String>>,
    staged: bool,
    failed_at: Option<i64>,
    blob_created_at: Option<i64>,
    held: bool,
    prepare: Option<crate::ffi::upload_wire::WirePrepare>,
    max_bytes: Option<u64>,
}

impl UploadRaw {
    /// The operation of the optimistic effects of a transfer that has not handed its final mutation to Outbox: Transfer owns it.
    pub(crate) fn owned_op(&self) -> Option<i64> { self.op_id.filter(|_| !self.completed) }

    pub(crate) fn reserved_op(&self) -> Option<i64> { self.op_id }
}

pub(crate) fn upload_rows(connection: &rusqlite::Connection) -> rusqlite::Result<Vec<UploadRaw>> {
    let mut parts: BTreeMap<[String; 3], Vec<Option<String>>> = BTreeMap::new();
    let mut statement = connection.prepare(include_str!("sql/upload_rows/upload_rows_select_sync_upload_part.sql"))?;
    for found in statement.query_map([], |row| Ok(([row.get::<_, String>(0)?, row.get(1)?, row.get(2)?], row.get::<_, Option<String>>(3)?)))? {
        let (key, etag) = found?;
        parts.entry(key).or_default().push(etag);
    }
    let mut prepares = super::prepare_rows::read_prepares(connection)?;
    let mut statement = connection.prepare(
        include_str!("sql/upload_rows/upload_rows_select_sync_upload.sql"),
    )?;
    let rows = statement.query_map([], |row| {
        let key = [row.get::<_, String>(0)?, row.get(1)?, row.get(2)?];
        Ok(UploadRaw {
            etags: parts.get(&key).cloned().unwrap_or_default(),
            key,
            seq: row.get(3)?,
            source: row.get(4)?,
            total: row.get(5)?,
            batch: row.get(6)?,
            upload_id: row.get(7)?,
            sent: row.get(8)?,
            completed: row.get(9)?,
            failed: row.get(10)?,
            created_at: row.get(11)?,
            inline: row.get(12)?,
            size: row.get(13)?,
            sha256: row.get(14)?,
            path: row.get(15)?,
            body: row.get(16)?,
            on_failure: row.get(17)?,
            steps_path: row.get(18)?,
            steps_body: row.get(19)?,
            prepare: prepares.remove(&row.get::<_, i64>(20)?),
            transfer_id: row.get(20)?,
            cycle_id: row.get(21)?,
            signed_id: row.get(22)?,
            aborted: row.get::<_, Option<bool>>(23)?.unwrap_or(false),
            op_id: row.get(24)?,
            fields: row.get(25)?,
            attempts: row.get(26)?,
            staged: row.get::<_, Option<bool>>(27)?.unwrap_or(false),
            failed_at: row.get(28)?,
            blob_created_at: row.get(29)?,
            progress: row.get(30)?,
            held: row.get::<_, Option<bool>>(31)?.unwrap_or(false),
            max_bytes: row.get::<_, Option<i64>>(32)?.map(|limit| limit as u64),
            weight: row.get(33)?,
            weighed_prepare: row.get(34)?,
            batch_progress: row.get(35)?,
        })
    })?;
    rows.collect()
}

fn unsigned<T: TryFrom<i64>>(value: i64, column: &str) -> Result<T, CoreError> { T::try_from(value).map_err(|_| CoreError::Internal(format!("sync_upload.{column} holds {value}"))) }


impl Store {
    pub(crate) fn failed_transfer(&self, row: &super::restore::RestoreFailedRow, id: u64) -> Result<Option<StoredTransfer>, CoreError> {
        let columns: serde_json::Value = serde_json::from_str(&row.columns).map_err(|error| CoreError::Internal(error.to_string()))?;
        let (Some(model), Some(row_id), Some(action)) = (columns.get("model").and_then(|value| value.as_str()), columns.get("row_id").and_then(|value| value.as_str()), columns.get("action").and_then(|value| value.as_str())) else { return Ok(None) };
        let number = |name: &str| columns.get(name).and_then(|value| value.as_u64());
        let text = |name: &str| columns.get(name).and_then(|value| value.as_str()).map(str::to_string);
        let holder = row.transfer_id();
        let source = text("source").zip(number("total").and_then(|total| u32::try_from(total).ok())).map(|(path, parts)| Source { path, parts });
        let file_present = holder.map(|id| self.file_present(id, Location::Directory)).transpose()?.unwrap_or(false);
        let mutation = NewOp { model: model.into(), row_id: row_id.into(), action: action.into(), on_failure: OnFailure::Keep, origin: OpSource::Api, path: text("mutation_path").unwrap_or_default(), body: text("mutation_body").unwrap_or_default(), effects: IntentEffects::default(), expires_after: None };
        Ok(Some(StoredTransfer {
            transfer_id: id, key: transfer_key(model, row_id, action), seq: number("seq").unwrap_or(id), source, awaiting_source: false,
            batch: number("batch"), fields: Default::default(), upload_id: text("upload_id"), confirmed_parts: 0, floor: 0, weight: None, weighed_prepare: false, batch_floor: 0, attempts: 0,
            etags: Vec::new(), completed: false, signed_id: None, failed: None, aborted: false, op_id: number("op_id"), copying: false,
            file_present, created_at: Time(number("created_at").unwrap_or(0)), place: Location::Directory, size: number("size").unwrap_or(0),
            sha256: text("sha256").unwrap_or_default(), cycle_id: number("cycle_id").unwrap_or(1).max(1), has_intent: false, has_operation: false,
            mutation, steps: None, staged: false, failed_at: None, blob_created_at: None, held: false, restore_failed: Some(row.reason.clone()), prepare: None, max_bytes: number("max_bytes"),
            restore_element: Some((row.key.clone(), holder)), pending_step: None, saved_answer: None,
        }))
    }

    fn file_present(&self, transfer_id: u64, place: Location) -> Result<bool, CoreError> {
        let file_id = crate::owners::write::transfer::file_id(transfer_id);
        match place {
            Location::Table => self.read_locked(|connection| connection.query_row(include_str!("sql/upload_rows/file_present.sql"), [&file_id], |row| row.get::<_, i64>(0))).map(|count| count > 0),
            Location::Directory => Ok(self.files().path(&file_id).is_some_and(|path| path.exists())),
        }
        .map_err(|error| CoreError::Internal(error.to_string()))
    }

    pub(crate) fn stored_transfers(&self, rows: Vec<UploadRaw>) -> Result<Vec<StoredTransfer>, CoreError> {
        rows.into_iter().map(|raw| self.stored_transfer(raw)).collect()
    }

    fn stored_transfer(&self, raw: UploadRaw) -> Result<StoredTransfer, CoreError> {
        let [model, id, action] = raw.key;
        let key = transfer_key(&model, &id, &action);
        let place = if raw.inline { Location::Table } else { Location::Directory };
        let awaiting_source = raw.source.is_none() && raw.failed.is_none();
        let copying = raw.size == 0 && raw.source.is_some();
        let seq: u64 = unsigned(raw.seq, "seq")?;
        let transfer_id: u64 = unsigned(raw.transfer_id, "transfer_id")?;
        let file_present = if copying { true } else if awaiting_source { false } else { self.file_present(transfer_id, place)? };
        Ok(StoredTransfer {
            transfer_id,
            key,
            seq,
            source: raw.source.map(|path| Ok::<_, CoreError>(Source { path, parts: unsigned(raw.total, "total")? })).transpose()?,
            awaiting_source,
            batch: raw.batch.and_then(|group| u64::try_from(group).ok()).filter(|group| *group > 0),
            upload_id: raw.upload_id,
            fields: raw.fields.and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok()).and_then(|value| value.as_object().cloned()).unwrap_or_default(),
            confirmed_parts: unsigned(raw.sent, "sent")?,
            floor: unsigned(raw.progress, "progress")?,
            weight: raw.weight.map(|weight| unsigned(weight, "weight")).transpose()?,
            weighed_prepare: raw.weighed_prepare,
            batch_floor: unsigned(raw.batch_progress, "batch_progress")?,
            attempts: unsigned(raw.attempts, "attempts")?,
            etags: raw.etags,
            completed: raw.completed,
            signed_id: raw.signed_id,
            failed: raw.failed,
            aborted: raw.aborted,
            staged: raw.staged,
            failed_at: raw.failed_at.and_then(|at| u64::try_from(at).ok()).map(Time),
            blob_created_at: raw.blob_created_at.and_then(|at| u64::try_from(at).ok()).map(Time),
            op_id: raw.op_id.map(|op| unsigned(op, "op_id")).transpose()?,
            copying,
            pending_step: None,
            saved_answer: None,
            file_present,
            created_at: Time(unsigned(raw.created_at, "created_at")?),
            place,
            size: unsigned(raw.size, "size")?,
            sha256: raw.sha256,
            cycle_id: raw.cycle_id.map_or(Ok(1), |value| unsigned(value, "cycle_id"))?,
            has_intent: false,
            has_operation: false,
            mutation: NewOp {
                model: model.clone(),
                row_id: id.clone(),
                action: action.clone(),
                on_failure: if raw.on_failure == "keep" { OnFailure::Keep } else { OnFailure::Rollback },
                origin: OpSource::Api,
                path: raw.path,
                body: raw.body,
                effects: IntentEffects::default(),
                expires_after: None,
            },
            prepare: raw.prepare,
            max_bytes: raw.max_bytes,
            held: raw.held,
            steps: raw.steps_path.zip(raw.steps_body).map(|(path, body)| Steps { path, body }),
            restore_failed: None,
            restore_element: None,
        })
    }

    /// The cache entry of a file with this content: `Fresh` while the blob of the server lives (7 days from `createBlob`), `Stale` after it.
    pub(crate) fn blob_cache_lookup(&self, content_sha256: &str) -> crate::owners::write::transfer::CachedBlob {
        use crate::owners::write::transfer::{CachedBlob, EXPIRY_MS};
        let now = u64::try_from(self.lock().now_ms()).unwrap_or(0);
        let found = self.read_locked(|connection| {
            rusqlite::OptionalExtension::optional(connection.query_row(include_str!("sql/upload_rows/blob_cache_lookup.sql"), [content_sha256], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))))
        });
        match found {
            Ok(Some((blob_id, created_at))) if now < u64::try_from(created_at).unwrap_or(0).saturating_add(EXPIRY_MS) => CachedBlob::Fresh { blob_id },
            Ok(Some(_)) => CachedBlob::Stale,
            _ => CachedBlob::Miss,
        }
    }

    /// The elements of the batch `group` that still wait for the answer: the row id and the fields the element sends.
    pub(crate) fn batch_rows(&self, model: &str, action: &str, group: u64) -> Result<Vec<BatchRow>, CoreError> {
        let found = self
            .read_locked(|connection| {
                let mut statement = connection.prepare_cached(include_str!("sql/upload_rows/batch_rows.sql"))?;
                let rows = statement.query_map(rusqlite::params![model, action, i64::try_from(group).unwrap_or(i64::MAX)], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)))?.collect::<Result<Vec<_>, _>>();
                rows
            })
            .map_err(|error| CoreError::Internal(error.to_string()))?;
        Ok(found
            .into_iter()
            .filter_map(|(id, fields)| {
                let fields = fields.and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok()).and_then(|value| value.as_object().cloned())?;
                Some((crate::identity::RowId::new(&id).ok()?, fields))
            })
            .collect())
    }

}

/// The key of the transfer of `uploads[action]` of a row.
pub(crate) fn transfer_key(model: &str, id: &str, action: &str) -> String {
    format!("{model}:{id}:{action}")
}

/// The model, the row id and the action of a transfer key.
pub(crate) fn split_transfer_key(key: &str) -> Option<(&str, &str, &str)> {
    let (model, rest) = key.split_once(':')?;
    let (id, action) = rest.rsplit_once(':')?;
    Some((model, id, action))
}
