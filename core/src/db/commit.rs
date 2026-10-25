use std::collections::HashSet;

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, OperationTransition, WriteOrigin};
use crate::identity::Generation;
use crate::store::{PlanCommitError, Reader};

use super::error::DbError;
use super::open::Db;
use super::parent_pointer::Followed;

impl Db {
    /// Plans on a read connection while the writer stays free and writes when no commit landed in between, otherwise plans again under the writer. `plan` runs once or twice and must only read and build from data it can build again: it must not consume captures, write files or announce anything.
    pub(crate) fn commit_following<E: From<DbError>>(
        &self,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        self.commit_following_report(generation, plan).map(|(result, _)| result)
    }

    pub(crate) fn commit_following_report<E: From<DbError>>(
        &self,
        generation: Generation,
        mut plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<(CommitResult, Followed), PlanCommitError<E>> {
        let mut followed = Followed::default();
        let result = self.shared.store.commit_prepared(generation, |reader| {
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
                    if let Some(admitted) = self.admitted(reader, envelope.origin, None, None, model.as_str(), row, &mut 0)? { *row = admitted; }
                }
            }
        }
        if envelope.origin == WriteOrigin::Optimistic {
            self.resolve_cloud_conflicts(reader, &mut envelope)?;
        }
        if matches!(envelope.origin, WriteOrigin::Event | WriteOrigin::Snapshot) {
            self.follow_correlations(reader, &mut envelope)?;
        }
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
