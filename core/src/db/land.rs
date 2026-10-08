use crate::commit::{CommitEnvelope, CommitResult, WriteOrigin};

use super::error::DbError;
use super::open::Db;

impl Db {
    pub fn land(&self, envelope: CommitEnvelope) -> Result<CommitResult, DbError> {
        if envelope.origin == WriteOrigin::Optimistic {
            return Err(DbError::OriginNotLandable(envelope.origin));
        }
        Ok(self.shared.store.commit(envelope)?)
    }
}
