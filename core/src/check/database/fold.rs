use crate::ffi::{ReadState, ReadValue, RowEdit};

/// What one `pull` shows of the unread journal of a stream.
#[derive(Debug, PartialEq)]
pub(super) enum Folded {
    Delta { base: i64, revision: i64, rows: Vec<String>, removed: Vec<String>, state: ReadState },
    Value(ReadValue),
}

pub(super) fn fold(values: Vec<ReadValue>) -> Vec<Folded> {
    let mut folded: Vec<Folded> = Vec::new();
    for value in values {
        match value {
            ReadValue::Delta { base, revision: next, edits, state: latest } => {
                if let Some(Folded::Delta { revision, rows, removed, state, .. }) = folded.last_mut() {
                    *revision = next;
                    *state = latest;
                    apply(rows, removed, &edits);
                } else {
                    let (mut rows, mut removed) = (Vec::new(), Vec::new());
                    apply(&mut rows, &mut removed, &edits);
                    folded.push(Folded::Delta { base, revision: next, rows, removed, state: latest });
                }
            }
            ReadValue::Snapshot { .. } | ReadValue::Count { .. } => {
                folded.retain(|kept| !matches!(kept, Folded::Delta { .. } | Folded::Value(ReadValue::State { .. } | ReadValue::Snapshot { .. } | ReadValue::Count { .. })));
                folded.push(Folded::Value(value));
            }
            ReadValue::State { .. } => {
                folded.retain(|kept| !matches!(kept, Folded::Value(ReadValue::State { .. })));
                folded.push(Folded::Value(value));
            }
            other => folded.push(Folded::Value(other)),
        }
    }
    folded
}

fn apply(rows: &mut Vec<String>, removed: &mut Vec<String>, edits: &[RowEdit]) {
    for edit in edits {
        match edit {
            RowEdit::Insert { row, .. } | RowEdit::Rewrite { row, .. } => {
                removed.retain(|id| *id != row.id);
                if !rows.contains(&row.id) { rows.push(row.id.clone()); }
            }
            RowEdit::Remove { id, .. } => {
                rows.retain(|kept| kept != id);
                if !removed.contains(id) { removed.push(id.clone()); }
            }
            RowEdit::Move { .. } => {}
        }
    }
}
