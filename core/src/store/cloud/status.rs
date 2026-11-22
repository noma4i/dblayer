use rusqlite::params;
use super::{PendingCloudRecord, StoreError};
use crate::owners::cloud::CloudScope;
use crate::ffi::{CloudIssue, CloudIssueCause};

impl super::super::Store {
    pub(crate) fn cloud_issues(&self, scope: &CloudScope) -> Result<Vec<CloudIssue>, StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        let mut query = inner.connection.prepare_cached("SELECT model, row_id, pending FROM cloud_record WHERE domain = ?1 AND pending IN (2, 3) ORDER BY model, row_id")?;
        let rows = query.query_map([&scope.domain], |row| Ok(CloudIssue { model: row.get(0)?, row_id: row.get(1)?, cause: if row.get::<_, i64>(2)? == 2 { CloudIssueCause::TooLarge } else { CloudIssueCause::Rejected } }))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn reject_cloud_send(&self, scope: &CloudScope, record: &PendingCloudRecord) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        inner.connection.execute("UPDATE cloud_record SET pending = 3 WHERE domain = ?1 AND model = ?2 AND row_id = ?3 AND version = ?4 AND pending = 1", params![scope.domain, record.model, record.row_id, record.version])?;
        Ok(())
    }
}
