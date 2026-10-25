use std::collections::{HashMap, HashSet};

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, OperationTransition, ScopeOp, WriteOrigin};
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
            followed = self.follow_parent_pointers(reader, &mut envelope)?;
            let changed_uploads: HashSet<i64> = envelope.operation_transitions.iter().filter_map(|transition| match transition {
                OperationTransition::RemoveUploadItem { seq, .. } => Some(*seq),
                _ => None,
            }).collect();
            for transition in &mut envelope.operation_transitions {
                let record = match transition {
                    OperationTransition::Begin(record) | OperationTransition::BeginUploadBatch { record, .. } if record.seq.is_none_or(|seq| changed_uploads.contains(&seq)) => record,
                    _ => continue,
                };
                record.rollback = record.rollback.take().map(|prior| super::prior::Prior::with_edits(prior, reader, &followed.edits)).transpose().map_err(DbError::from)?;
            }
            let mut taken: HashMap<_, HashSet<String>> = HashMap::new();
            let mut vacated: HashMap<_, HashSet<String>> = HashMap::new();
            for op in &envelope.scope_ops {
                match op {
                    ScopeOp::Move { scope, row, order_key } => {
                        taken.entry(scope.clone()).or_default().insert(order_key.clone());
                        vacated.entry(scope.clone()).or_default().insert(row.to_string());
                    }
                    ScopeOp::Detach { scope, row } => {
                        vacated.entry(scope.clone()).or_default().insert(row.to_string());
                    }
                    ScopeOp::Append { .. } => {}
                }
            }
            for op in &mut envelope.scope_ops {
                if let ScopeOp::Append { scope, row, order_key } = op {
                    if self.shared.spec(scope.model.as_str())?.stable_order(&scope.key).is_some() { continue; }
                    let claimed = taken.entry(scope.clone()).or_default();
                    let freed = vacated.entry(scope.clone()).or_default();
                    *order_key = reader.free_order_key(scope.model.as_str(), scope.key.as_str(), row.as_str(), order_key, claimed, freed).map_err(DbError::from)?;
                    claimed.insert(order_key.clone());
                }
            }
            Ok(Some(envelope))
        })?;
        Ok((result, followed))
    }
}
