use rusqlite::{params, OptionalExtension};
use super::{PendingCloudRecord, StoreError};
use crate::owners::cloud::CloudScope;
use crate::ffi::{CloudIssue, CloudIssueCause};
use crate::identity::RowId;

const TOO_LARGE: i64 = 2;
const REJECTED: i64 = 3;

fn eligible(info: &super::super::ModelInfo) -> bool { !info.spec.is_memory() }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloudRecordDisposition {
    Excluded, Held, Waiting, Missing, Queued, QueuedDeletion, Synchronized, Deleted, TooLarge, Rejected,
}

impl super::super::Store {
    pub(crate) fn cloud_models(&self) -> Vec<(String, bool)> {
        let models = self.models.read().unwrap_or_else(|error| error.into_inner());
        models.iter().map(|(name, info)| (name.clone(), eligible(info))).collect()
    }

    pub(crate) fn cloud_model_eligible(&self, model: &str) -> Result<bool, StoreError> {
        let models = self.models.read().unwrap_or_else(|error| error.into_inner());
        models.get(model).map(eligible).ok_or_else(|| StoreError::UnknownModel(model.into()))
    }

    pub(crate) fn cloud_record_disposition(&self, scope: &CloudScope, model: &str, row_id: &RowId) -> Result<CloudRecordDisposition, StoreError> {
        let check_scope = || {
            let inner = self.lock();
            if inner.cloud_scope.as_ref().is_some_and(|attachment| attachment.mode != super::super::CloudAttachmentMode::Erased && attachment.scope == *scope) {
                Ok(())
            } else {
                Err(StoreError::Invalid("stale cloud scope".into()))
            }
        };
        check_scope()?;
        let disposition = self.read(&[model], |reader| {
            let info = reader.models().get(model).ok_or_else(|| StoreError::UnknownModel(model.into()))?;
            if !eligible(info) || row_id.is_temp() { return Ok(CloudRecordDisposition::Excluded); }
            let connection = reader.connection();
            if !info.spec.is_local() && super::has_open_row_operation(connection, model, row_id.as_str())? {
                return Ok(CloudRecordDisposition::Held);
            }
            let state: Option<(i64, bool)> = connection.query_row(
                "SELECT pending, deleted FROM cloud_record WHERE domain = ?1 AND model = ?2 AND row_id = ?3",
                params![scope.domain, model, row_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?;
            match state {
                Some((0, false)) => Ok(CloudRecordDisposition::Synchronized),
                Some((0, true)) => Ok(CloudRecordDisposition::Deleted),
                Some((1, false)) => Ok(CloudRecordDisposition::Queued),
                Some((1, true)) => Ok(CloudRecordDisposition::QueuedDeletion),
                Some((TOO_LARGE, _)) => Ok(CloudRecordDisposition::TooLarge),
                Some((REJECTED, _)) => Ok(CloudRecordDisposition::Rejected),
                Some(_) => Err(StoreError::Invalid("invalid cloud record disposition".into())),
                None => {
                    let sql = format!("SELECT EXISTS(SELECT 1 FROM {} WHERE id = ?1)", info.table);
                    let exists: bool = connection.query_row(&sql, [row_id.as_str()], |row| row.get(0))?;
                    Ok(if exists { CloudRecordDisposition::Waiting } else { CloudRecordDisposition::Missing })
                }
            }
        })?;
        check_scope()?;
        Ok(disposition)
    }

    pub(crate) fn cloud_issues(&self, scope: &CloudScope) -> Result<Vec<CloudIssue>, StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        let mut query = inner.connection.prepare_cached("SELECT model, row_id, pending FROM cloud_record WHERE domain = ?1 AND pending IN (2, 3) ORDER BY model, row_id")?;
        let rows = query.query_map([&scope.domain], |row| {
            let cause = if row.get::<_, i64>(2)? == TOO_LARGE { CloudIssueCause::TooLarge } else { CloudIssueCause::Rejected };
            Ok(CloudIssue { model: row.get(0)?, row_id: row.get(1)?, cause })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn block_cloud_send(&self, scope: &CloudScope, record: &PendingCloudRecord, cause: CloudIssueCause) -> Result<(), StoreError> {
        let disposition = match cause {
            CloudIssueCause::TooLarge => TOO_LARGE,
            CloudIssueCause::Rejected => REJECTED,
        };
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        inner.connection.prepare_cached(include_str!("../sql/cloud_block_send.sql"))?
            .execute(params![scope.domain, record.model, record.row_id, record.version, disposition])?;
        Ok(())
    }
}
