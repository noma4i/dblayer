use crate::commit::{CommitEnvelope, CommitResult, WriteOrigin};

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

impl Db {
    pub fn land(&self, envelope: CommitEnvelope) -> Result<CommitResult, DbError> {
        if envelope.origin == WriteOrigin::Optimistic {
            return Err(DbError::OriginNotLandable(envelope.origin));
        }
        let generation = envelope.generation;
        self.commit_following(generation, |_| Ok(Some(envelope)))
            .map_err(plan_fault)
    }
}
