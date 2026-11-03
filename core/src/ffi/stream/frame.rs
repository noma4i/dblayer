use crate::db::Change;
use super::super::wire::{WireModel, WireRow};
use super::{Placement, Replacement};

/// The rows part of one observer read, before the stream adds its state and revision.
#[derive(Clone, Debug)]
pub(in crate::ffi) struct Frame {
    pub revision: i64,
    pub full: bool,
    pub size: u32,
    pub rows: Vec<WireRow>,
    pub row_indexes: Vec<u32>,
    pub removed: Vec<String>,
    pub placed: Vec<Placement>,
    pub replaced: Vec<Replacement>,
    pub renamed: Vec<Replacement>,
    pub count: Option<i64>,
}

impl Frame {
    pub(in crate::ffi) fn rows(change: &Change, wire: &WireModel) -> Self {
        let diff = &change.diff;
        let row_indexes = if change.full { Vec::new() } else { change.rows.iter().map(|row| diff.at[&row.id] as u32).collect() };
        Self {
            revision: change.revision, full: change.full, size: change.size as u32, rows: change.rows.iter().map(|row| wire.row(row)).collect(), row_indexes,
            removed: diff.removed.iter().map(ToString::to_string).collect(),
            placed: diff.placed().into_iter().map(|(id, index)| Placement { id: id.to_string(), index: index as u32 }).collect(),
            replaced: diff.replaced.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            renamed: diff.renamed.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            count: None,
        }
    }

    pub(super) fn counted(revision: i64, count: i64) -> Self {
        Self { revision, full: false, size: 0, rows: Vec::new(), row_indexes: Vec::new(), removed: Vec::new(), placed: Vec::new(), replaced: Vec::new(), renamed: Vec::new(), count: Some(count) }
    }

    pub(super) fn moves_rows(&self) -> bool {
        self.full || !(self.rows.is_empty() && self.removed.is_empty() && self.placed.is_empty() && self.replaced.is_empty() && self.renamed.is_empty())
    }
}

#[cfg(test)]
impl Frame {
    pub(in crate::ffi) fn whole(revision: i64, rows: Vec<WireRow>) -> Self {
        Self { revision, full: true, size: rows.len() as u32, rows, row_indexes: Vec::new(), removed: Vec::new(), placed: Vec::new(), replaced: Vec::new(), renamed: Vec::new(), count: None }
    }
}
