use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::identity::RowRef;
use crate::schema::{AssociationKind, ModelSpec};
use crate::store::{BatchPlan, CommitResult, EntityOp};
use crate::value::read_id;

use super::row_write::RowWrite;
use super::{Reader, StoreError};

type Row = Map<String, Value>;

fn touching(spec: &ModelSpec) -> impl Iterator<Item = &crate::schema::AssociationSpec> {
    spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && association.touches_parent)
}

/// The one writer of `Touch` (P-DATA-115): every resident parent that a changed child names through a `touch: true` link gets one `Touch` in the batch, and the touch goes up the links to the root.
/// A row changed by the batch is a child whatever path wrote it; a parent the batch changed itself is not touched again.
pub(super) fn touch_ancestors(writer: &mut RowWrite<'_>) -> Result<Option<CommitResult>, StoreError> {
    let reader = Reader::new(writer.connection, writer.models);
    let mut seen = BTreeSet::new();
    let mut level: Vec<(RowRef, Vec<Row>)> = Vec::new();
    for (row, before) in writer.originals.iter() {
        let spec = &writer.models.get(row.model.as_str()).ok_or_else(|| StoreError::UnknownModel(row.model.to_string()))?.spec;
        if touching(spec).next().is_none() { continue; }
        let after = reader.point(row.model.as_str(), row.id.as_str())?;
        if *before == after { continue; }
        level.push((row.clone(), before.iter().cloned().chain(after).collect()));
    }
    seen.extend(level.iter().map(|(row, _)| row.clone()));
    let mut touches = Vec::new();
    while !level.is_empty() {
        let mut next = Vec::new();
        for (child, rows) in &level {
            let spec = &writer.models.get(child.model.as_str()).ok_or_else(|| StoreError::UnknownModel(child.model.to_string()))?.spec;
            for association in touching(spec) {
                for row in rows {
                    let Some(id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
                    let parent = RowRef::new(association.target.clone(), id);
                    if !seen.insert(parent.clone()) { continue; }
                    let Some(stored) = reader.point(parent.model.as_str(), parent.id.as_str())? else { continue };
                    if writer.originals.get(&parent).is_some_and(|before| before.as_ref() != Some(&stored)) { continue; }
                    touches.push(EntityOp::Touch { model: parent.model.clone(), id: parent.id.clone() });
                    next.push((parent, vec![stored]));
                }
            }
        }
        level = next;
    }
    if touches.is_empty() { return Ok(None); }
    let plan = BatchPlan { entity_ops: touches, ..BatchPlan::from_source(writer.generation, crate::schema::RowSource::Derived) };
    Ok(Some(writer.apply(&plan)?))
}
