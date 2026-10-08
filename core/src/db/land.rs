use crate::commit::{CommitEnvelope, CommitResult, WriteOrigin};

use super::error::DbError;
use super::open::{locked, Db};

impl Db {
    pub fn land(&self, envelope: CommitEnvelope) -> Result<CommitResult, DbError> {
        if envelope.origin == WriteOrigin::Optimistic {
            return Err(DbError::OriginNotLandable(envelope.origin));
        }
        let result = self.shared.store.commit(envelope)?;
        self.publish(&result);
        Ok(result)
    }

    pub(crate) fn publish(&self, result: &CommitResult) {
        if result.is_noop() {
            return;
        }
        {
            let mut cache = locked(&self.shared.cache);
            for change in &result.rows {
                cache.evict(change.row.model.as_str(), change.row.id.as_str());
            }
        }
        self.shared.notify(&result.changed_models());
    }
}
