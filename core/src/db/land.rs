use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, CommitResult, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::store::Reader;
use crate::value::read_id;


use super::error::DbError;
use super::local_write::plan_fault;
use super::merge::Admission;
use super::open::Db;

impl Db {
    pub fn land(&self, admitted: &crate::ffi::admission::Admitted, envelope: CommitEnvelope) -> Result<CommitResult, DbError> {
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

    /// Admits the rows of a server answer against the stored versions: `AcceptedRows` holds the rows that stand, `invalid` counts the rows no rule could admit.
    /// The origin is `Response` for the answer of an operation (`closing`) and `Snapshot` for a read.
    pub(crate) fn admit_rows(&self, reader: &Reader<'_>, base: i64, closing: Option<i64>, model: &str, rows: &[Map<String, Value>], drops: &mut usize) -> Result<AcceptedRows, DbError> {
        self.spec(model)?;
        let mut admitted = AcceptedRows { rows: Vec::with_capacity(rows.len()), invalid: 0 };
        for row in rows {
            if row.get("id").and_then(read_id).is_none() {
                continue;
            }
            let origin = if closing.is_some() { WriteOrigin::Response } else { WriteOrigin::Snapshot };
            let before = *drops;
            match self.admitted(reader, origin, Admission { base: Some(base), closing }, model, row, drops)? {
                Some(row) => admitted.rows.push(row),
                None if *drops == before => admitted.invalid += 1,
                None => {}
            }
        }
        Ok(admitted)
    }

    /// The rows of the models an answer names besides its roots, as upserts, except the rows a planned op writes already.
    pub(crate) fn related_upserts(&self, reader: &Reader<'_>, base_revision: i64, related: &[(ModelKey, Map<String, Value>)], planned: &BTreeSet<(ModelKey, RowId)>, drops: &mut usize) -> Result<Vec<EntityOp>, DbError> {
        let mut ops = Vec::new();
        for (model, row) in related {
            for row in self.admit_rows(reader, base_revision, None, model.as_str(), std::slice::from_ref(row), drops)?.rows {
                let Some(id) = row.get("id").and_then(read_id) else { continue };
                if !planned.contains(&(model.clone(), id.clone())) {
                    ops.push(EntityOp::Upsert { model: model.clone(), id, row });
                }
            }
        }
        Ok(ops)
    }
}

pub(crate) struct AcceptedRows {
    pub rows: Vec<Map<String, Value>>,
    pub invalid: usize,
}
