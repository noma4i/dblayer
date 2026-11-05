use crate::commit::{CommitEnvelope, CommitResult, WriteOrigin};

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

impl Db {
    pub fn land(&self, admitted: &crate::ffi::legacy::Admitted, envelope: CommitEnvelope) -> Result<CommitResult, DbError> {
        if !std::sync::Arc::ptr_eq(&self.shared.store.lifecycle, admitted.life()) {
            return Err(DbError::NoSession { detail: "the admission belongs to another database".into() });
        }
        if envelope.origin == WriteOrigin::Optimistic {
            return Err(DbError::OriginNotLandable(envelope.origin));
        }
        let generation = envelope.generation;
        self.commit_following(&crate::lifecycle::authority::Authority::Admitted(admitted.clone()), generation, |_| Ok(Some(envelope.clone())))
            .map_err(|error| if admitted.is_current() { plan_fault(error) } else { DbError::NoSession { detail: "the session of this write ended".into() } })
    }
}
