use crate::commit::{CommitEnvelope, CommitResult, WriteOrigin};
use crate::identity::Generation;
use crate::store::{PlanCommitError, Reader};

use super::error::DbError;
use super::open::Db;
use super::parent_pointer::Followed;

impl Db {
    pub(crate) fn commit_following<E: From<DbError>>(
        &self,
        generation: Generation,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        self.commit_following_report(generation, plan).map(|(result, _)| result)
    }

    pub(crate) fn commit_following_report<E: From<DbError>>(
        &self,
        generation: Generation,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<(CommitResult, Followed), PlanCommitError<E>> {
        let mut followed = Followed::default();
        let result = self.shared.store.commit_with(generation, |reader| {
            let Some(mut envelope) = plan(reader)? else { return Ok(None) };
            if matches!(envelope.origin, WriteOrigin::Event | WriteOrigin::Snapshot) {
                self.follow_correlations(reader, &mut envelope)?;
            }
            followed = self.follow_parent_pointers(reader, &mut envelope)?;
            Ok(Some(envelope))
        })?;
        Ok((result, followed))
    }
}
