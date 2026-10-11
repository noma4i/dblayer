use std::collections::HashSet;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, RowRef};

pub fn cascade_destroys(parent: &RowRef, children: &[RowRef], parent_is_resident: bool) -> Vec<RowRef> {
    if !parent_is_resident {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut result: Vec<RowRef> = children.iter().filter(|child| *child != parent && seen.insert((*child).clone())).cloned().collect();
    result.push(parent.clone());
    result
}

pub fn cascade_envelope(destroys: &[RowRef], reason: DestroyReason, origin: WriteOrigin, generation: Generation) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(origin, generation);
    envelope.entity_ops = destroys
        .iter()
        .map(|row| EntityOp::Destroy { model: row.model.clone(), id: row.id.clone(), reason: reason.clone() })
        .collect();
    envelope
}
