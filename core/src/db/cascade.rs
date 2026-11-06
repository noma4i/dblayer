use std::collections::HashSet;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp};
use crate::identity::RowRef;
use crate::store::Reader;

use super::{Db, DbError};

impl Db {
    pub(super) fn follow_destroys(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        let mut seen: HashSet<RowRef> = envelope.entity_ops.iter().filter_map(|op| match op {
            EntityOp::Destroy { model, id, .. } => Some(RowRef::new(model.clone(), id.clone())),
            _ => None,
        }).collect();
        let parents: Vec<_> = envelope.entity_ops.iter().filter_map(|op| match op {
            EntityOp::Destroy { model, id, reason } if !matches!(reason, DestroyReason::Replaced { .. }) => Some((RowRef::new(model.clone(), id.clone()), reason.clone())),
            _ => None,
        }).collect();
        for (parent, reason) in parents {
            for child in self.destroy_targets(reader, &parent)? {
                if seen.insert(child.clone()) {
                    envelope.entity_ops.push(EntityOp::Destroy { model: child.model, id: child.id, reason: reason.clone() });
                }
            }
        }
        Ok(())
    }
}
