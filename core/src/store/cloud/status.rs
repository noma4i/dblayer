use rusqlite::params;
use super::{PendingCloudRecord, StoreError};
use crate::owners::cloud::CloudScope;
use crate::ffi::{CloudIssue, CloudIssueCause};

const TOO_LARGE: i64 = 2;
const REJECTED: i64 = 3;

impl super::super::Store {
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
