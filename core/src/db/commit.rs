use std::collections::HashSet;

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, OperationTransition, WriteOrigin};
use crate::identity::Generation;
use crate::lifecycle::authority::Authority;
use crate::store::{Committed, PlanCommitError, Reader};

use super::error::DbError;
use super::merge::Admission;
use super::open::Db;
use super::parent_pointer::Followed;

impl Db {
    /// The admission of the epoch an owner decided in. `Session` origin also needs a session in that epoch.
    pub(crate) fn admit_in(&self, origin: crate::ffi::admission::Origin, target: &'static str, epoch: u64) -> Result<crate::ffi::admission::Admitted, DbError> {
        self.shared.store.lifecycle.admit_in(origin, target, epoch)
            .map_err(|_| DbError::NoSession { detail: "the epoch of the request ended or has no session".into() })
    }

    pub fn admit(&self) -> Result<crate::ffi::admission::Admitted, DbError> {
        self.shared.store.lifecycle.admit(crate::ffi::admission::Origin::Explicit, "core call")
            .map_err(|_| DbError::NoSession { detail: "the database does not admit work".into() })
    }

    /// Plans on a read connection while the writer stays free and writes when no commit landed in between, otherwise plans again under the writer. `plan` runs once or twice and must only read and build from data it can build again: it must not consume captures, write files or announce anything.
    pub(crate) fn commit_following<E: From<DbError>>(
        &self,
        authority: &Authority,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        self.commit_following_report(authority, generation, plan).map(|(result, _)| result)
    }

    pub(crate) fn commit_following_report<E: From<DbError>>(
        &self,
        authority: &Authority,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<(CommitResult, Followed), PlanCommitError<E>> {
        self.commit_following_deferred(authority, generation, plan).map(|(committed, followed)| (committed.publish(), followed))
    }

    pub(crate) fn commit_following_deferred<E: From<DbError>>(
        &self,
        authority: &Authority,
        generation: Generation,
        mut plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<(Committed<'_>, Followed), PlanCommitError<E>> {
        let mut followed = Followed::default();
        let result = self.shared.store.commit_prepared_deferred(authority, generation, |reader| {
            followed = Followed::default();
            let Some(envelope) = plan(reader)? else { return Ok(None) };
            let (envelope, found) = self.follow(reader, envelope)?;
            followed = found;
            Ok(Some(envelope))
        })?;
        Ok((result, followed))
    }

    fn follow(&self, reader: &Reader<'_>, mut envelope: CommitEnvelope) -> Result<(CommitEnvelope, Followed), DbError> {
    if envelope.origin == WriteOrigin::Optimistic {
            for op in &mut envelope.entity_ops {
                if let EntityOp::Upsert { model, id, row } = op {
                    row.insert("id".into(), serde_json::Value::String(id.to_string()));
                    if let Some(admitted) = self.admitted(reader, envelope.origin, Admission::default(), model.as_str(), row, &mut 0)? { *row = admitted; }
                }
            }
        }
        if matches!(envelope.origin, WriteOrigin::Event | WriteOrigin::Snapshot) {
            self.follow_correlations(reader, &mut envelope)?;
        }
        self.follow_removed_by(reader, &mut envelope)?;
        self.follow_destroys(reader, &mut envelope)?;
        let followed = self.follow_parent_pointers(reader, &mut envelope)?;
        let changed_uploads: HashSet<i64> = envelope.operation_transitions.iter().filter_map(|transition| match transition {
            OperationTransition::RemoveUploadItem { seq, .. } => Some(*seq),
            _ => None,
        }).collect();
        for transition in &mut envelope.operation_transitions {
            let record = match transition {
                OperationTransition::Begin(record) | OperationTransition::BeginUpload { record, .. } if record.seq.is_none_or(|seq| changed_uploads.contains(&seq)) => record,
                _ => continue,
            };
            record.rollback = record.rollback.take().map(|prior| super::prior::Prior::with_edits(prior, reader, &followed.edits)).transpose().map_err(DbError::from)?;
        }
        Ok((envelope, followed))
    }
}
