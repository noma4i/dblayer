//! The rows of `sync_upload` and `sync_upload_part`: the whole value of a transfer, written by the batches of the owner Transfer and read back for `Restored`.

use std::collections::BTreeMap;

use crate::files::Location;
use crate::lifecycle::authority::Authority;
use crate::owners::store::{IntentEffects, UploadKind, UploadRow};
use crate::owners::write::outbox::{NewOp, OpOrigin};
use crate::owners::write::transfer::{Source, Steps, StoredTransfer};
use crate::runtime::Time;
use crate::schema::OnFailure;

use super::error::DbError;
use super::open::Db;

/// A row of `sync_upload` as the columns give it.
pub(crate) struct UploadRaw {
    key: [String; 3],
    seq: i64,
    source: String,
    total: i64,
    batch: bool,
    upload_id: Option<String>,
    sent: i64,
    completed: bool,
    failed: Option<String>,
    created_at: i64,
    inline: bool,
    size: i64,
    sha256: String,
    path: String,
    body: String,
    on_failure: String,
    steps_path: String,
    steps_body: String,
    transfer_id: Option<i64>,
    cycle_id: Option<i64>,
    signed_id: Option<String>,
    aborted: bool,
    op_id: Option<i64>,
    etags: Vec<Option<String>>,
}

impl UploadRaw {
    /// The operation of the optimistic effects of a transfer that has not handed its final mutation to Outbox: Transfer owns it.
    pub(crate) fn owned_op(&self) -> Option<i64> { self.op_id.filter(|_| !self.completed) }

    pub(crate) fn reserved_op(&self) -> Option<i64> { self.op_id }
}

pub(crate) fn upload_rows(connection: &rusqlite::Connection) -> rusqlite::Result<Vec<UploadRaw>> {
    let mut parts: BTreeMap<[String; 3], Vec<Option<String>>> = BTreeMap::new();
    let mut statement = connection.prepare("SELECT model, row_id, action, etag FROM sync_upload_part ORDER BY model, row_id, action, number")?;
    for found in statement.query_map([], |row| Ok(([row.get::<_, String>(0)?, row.get(1)?, row.get(2)?], row.get::<_, Option<String>>(3)?)))? {
        let (key, etag) = found?;
        parts.entry(key).or_default().push(etag);
    }
    let mut statement = connection.prepare(
        "SELECT model, row_id, action, seq, source, total, batch, upload_id, sent, completed, error_message, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure, steps_path, steps_body, \
         transfer_id, cycle_id, signed_id, aborted, op_id FROM sync_upload WHERE seq IS NOT NULL ORDER BY seq",
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
            transfer_id: row.get(20)?,
            cycle_id: row.get(21)?,
            signed_id: row.get(22)?,
            aborted: row.get::<_, Option<bool>>(23)?.unwrap_or(false),
            op_id: row.get(24)?,
        })
    })?;
    rows.collect()
}

fn unsigned<T: TryFrom<i64>>(value: i64, column: &str) -> Result<T, DbError> { T::try_from(value).map_err(|_| DbError::Internal(format!("sync_upload.{column} holds {value}"))) }

pub(super) fn key_parts(key: &str) -> Result<[&str; 3], DbError> {
    let mut parts = key.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(model), Some(id), Some(action)) => Ok([model, id, action]),
        _ => Err(DbError::InvalidUpload(format!("transfer key `{key}` is not model:row_id:action"))),
    }
}

impl Db {
    /// The row of a transfer shows `uploads[action]`: a write of the transfer touches it, so its streams carry the new state.
    pub(super) fn upload_touch(&self, key: &str) -> Result<crate::commit::EntityOp, DbError> {
        let [model, id, _] = key_parts(key)?;
        Ok(crate::commit::EntityOp::Touch { model: crate::identity::ModelKey::new(model), id: crate::identity::RowId::new(id).map_err(|error| DbError::InvalidUpload(error.to_string()))? })
    }

    /// `Upload{row, kind}`: the whole value of a transfer in `sync_upload` for the key `model:row_id:action`, and its confirmed parts. The kind of the batch names the status.
    pub(super) fn write_upload_row(&self, row: &UploadRow, kind: UploadKind) -> Result<(), DbError> {
        let [model, id, action] = key_parts(&row.key)?;
        let (status, stage) = match kind {
            UploadKind::Failed => ("failed", "prepare"),
            UploadKind::Completed => ("sending", "mutation"),
            UploadKind::ConfirmedParts => ("uploading", "part"),
            UploadKind::SyncUpload | UploadKind::Retrying => ("queued", "prepare"),
        };
        let on_failure = match row.on_failure {
            OnFailure::Rollback => "rollback",
            OnFailure::Keep => "keep",
        };
        self.shared
            .store
            .write_store(&Authority::Owner, |stored| {
                stored.connection.execute(
                    "INSERT OR REPLACE INTO sync_upload (model, row_id, action, status, stage, sent, total, wait, attempts, error_kind, error_message, seq, source, batch, upload_id, completed, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure, steps_path, steps_body, transfer_id, cycle_id, signed_id, aborted, op_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 0, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28)",
                    rusqlite::params![
                        model, id, action, status, stage, row.confirmed_parts, row.parts, row.failed.as_ref().map(|_| "unrecognized"), row.failed,
                        i64::try_from(row.seq).unwrap_or(i64::MAX), row.source, row.batch, row.upload_id, row.completed, i64::try_from(row.created_at).unwrap_or(i64::MAX), row.inline,
                        i64::try_from(row.size).unwrap_or(i64::MAX), row.sha256, row.path, row.body, on_failure, row.steps_path, row.steps_body,
                        i64::try_from(row.transfer_id).unwrap_or(i64::MAX), i64::try_from(row.cycle_id).unwrap_or(i64::MAX), row.signed_id, row.aborted, row.op_id.and_then(|op| i64::try_from(op).ok())
                    ],
                )?;
                stored.connection.execute("DELETE FROM sync_upload_part WHERE model = ?1 AND row_id = ?2 AND action = ?3", rusqlite::params![model, id, action])?;
                for (index, etag) in row.etags.iter().enumerate() {
                    stored.connection.execute("INSERT INTO sync_upload_part (model, row_id, action, number, etag) VALUES (?1, ?2, ?3, ?4, ?5)", rusqlite::params![model, id, action, index as i64 + 1, etag])?;
                }
                Ok(())
            })
            .map_err(DbError::from)
    }

    /// The transfers kept in the base, whole, for `Restored`. A row of the display without the value of a transfer (`seq` empty) is not one.
    #[cfg(test)]
    pub(crate) fn read_transfers(&self) -> Result<Vec<StoredTransfer>, DbError> {
        let rows = self.shared.store.read_locked(upload_rows).map_err(|error| DbError::Internal(error.to_string()))?;
        self.stored_transfers(rows)
    }

    fn file_present(&self, key: &str, place: Location) -> Result<bool, DbError> {
        let file_id = crate::owners::write::transfer::file_id(key);
        match place {
            Location::Table => self.shared.store.read_locked(|connection| connection.query_row("SELECT COUNT(*) FROM file WHERE id = ?1", [&file_id], |row| row.get::<_, i64>(0))).map(|count| count > 0),
            Location::Directory => Ok(self.shared.files.path(&file_id).is_some_and(|path| path.exists())),
        }
        .map_err(|error| DbError::Internal(error.to_string()))
    }

    pub(crate) fn stored_transfers(&self, rows: Vec<UploadRaw>) -> Result<Vec<StoredTransfer>, DbError> {
        rows.into_iter().map(|raw| self.stored_transfer(raw)).collect()
    }

    fn stored_transfer(&self, raw: UploadRaw) -> Result<StoredTransfer, DbError> {
        let [model, id, action] = raw.key;
        let key = format!("{model}:{id}:{action}");
        let place = if raw.inline { Location::Table } else { Location::Directory };
        let copying = raw.size == 0;
        let file_present = if copying { std::path::Path::new(&raw.source).exists() } else { self.file_present(&key, place)? };
        let seq: u64 = unsigned(raw.seq, "seq")?;
        Ok(StoredTransfer {
            transfer_id: raw.transfer_id.map_or(Ok(seq), |value| unsigned(value, "transfer_id"))?,
            key,
            seq,
            source: Source { path: raw.source, parts: unsigned(raw.total, "total")? },
            batch: raw.batch,
            upload_id: raw.upload_id,
            confirmed_parts: unsigned(raw.sent, "sent")?,
            etags: raw.etags,
            completed: raw.completed,
            signed_id: raw.signed_id,
            failed: raw.failed,
            aborted: raw.aborted,
            op_id: raw.op_id.map(|op| unsigned(op, "op_id")).transpose()?,
            copying,
            file_present,
            created_at: Time(unsigned(raw.created_at, "created_at")?),
            place,
            size: unsigned(raw.size, "size")?,
            sha256: raw.sha256,
            cycle_id: raw.cycle_id.map_or(Ok(1), |value| unsigned(value, "cycle_id"))?,
            has_intent: false,
            mutation: NewOp {
                model: model.clone(),
                row_id: id.clone(),
                action: action.clone(),
                on_failure: if raw.on_failure == "keep" { OnFailure::Keep } else { OnFailure::Rollback },
                origin: OpOrigin::Api,
                path: raw.path,
                body: raw.body,
                effects: IntentEffects::default(),
                durable: true,
            },
            steps: Steps { path: raw.steps_path, body: raw.steps_body },
            restore_failed: None,
        })
    }

    /// `BlobCachePut` and `BlobCacheRemove`.
    pub(super) fn write_blob_cache(&self, content_sha256: &str, entry: Option<(&str, u64)>) -> Result<(), DbError> {
        self.shared
            .store
            .write_store(&Authority::Owner, |stored| {
                match entry {
                    Some((blob_id, created_at)) => stored.connection.execute("INSERT OR REPLACE INTO blob_cache (content_sha256, blob_id, created_at) VALUES (?1, ?2, ?3)", rusqlite::params![content_sha256, blob_id, i64::try_from(created_at).unwrap_or(i64::MAX)])?,
                    None => stored.connection.execute("DELETE FROM blob_cache WHERE content_sha256 = ?1", [content_sha256])?,
                };
                Ok(())
            })
            .map_err(DbError::from)
    }

    /// The cache entry of a file with this content: `Fresh` while the blob of the server lives (7 days from `createBlob`), `Stale` after it.
    pub(crate) fn blob_cache_lookup(&self, content_sha256: &str) -> crate::owners::write::transfer::CachedBlob {
        use crate::owners::write::transfer::{CachedBlob, EXPIRY_MS};
        let now = u64::try_from(self.shared.store.lock().now_ms()).unwrap_or(0);
        let found = self.shared.store.read_locked(|connection| {
            rusqlite::OptionalExtension::optional(connection.query_row("SELECT blob_id, created_at FROM blob_cache WHERE content_sha256 = ?1", [content_sha256], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))))
        });
        match found {
            Ok(Some((blob_id, created_at))) if now < u64::try_from(created_at).unwrap_or(0).saturating_add(EXPIRY_MS) => CachedBlob::Fresh { blob_id },
            Ok(Some(_)) => CachedBlob::Stale,
            _ => CachedBlob::Miss,
        }
    }

    pub(super) fn remove_upload_row(&self, key: &str) -> Result<(), DbError> {
        let [model, id, action] = key_parts(key)?;
        self.shared
            .store
            .write_store(&Authority::Owner, |stored| {
                stored.connection.execute("DELETE FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND action = ?3", rusqlite::params![model, id, action])?;
                stored.connection.execute("DELETE FROM sync_upload_part WHERE model = ?1 AND row_id = ?2 AND action = ?3", rusqlite::params![model, id, action])?;
                Ok(())
            })
            .map_err(DbError::from)
    }
}
