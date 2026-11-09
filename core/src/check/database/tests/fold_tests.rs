use super::super::fold::{fold, Folded};
use crate::ffi::{EndCause, ReadPhase, ReadState, ReadValue, RowEdit, WireRow, WireSync, WireSyncState};

fn state(loaded: i64) -> ReadState {
    ReadState { phase: ReadPhase::Loaded, error: None, loaded: Some(loaded), has_more: false, settled: true, total: None }
}

fn row(id: &str) -> WireRow {
    WireRow { id: id.into(), revision: 1, estimated_bytes: 0, layout: String::new(), cells: Vec::new(), sync: WireSync { state: WireSyncState::Synced, actions: Vec::new(), dirty_fields: Vec::new(), error: None, attempts: 0, since: 0 }, uploads: Vec::new() }
}

fn delta(base: i64, revision: i64, edits: Vec<RowEdit>) -> ReadValue {
    ReadValue::Delta { base, revision, edits, state: state(revision) }
}

fn insert(id: &str) -> RowEdit { RowEdit::Insert { index: 0, row: row(id) } }

fn rewrite(id: &str) -> RowEdit { RowEdit::Rewrite { index: 0, row: row(id), temporary: None } }

fn remove(id: &str) -> RowEdit { RowEdit::Remove { index: 0, id: id.into() } }

fn folded(base: i64, revision: i64, rows: &[&str], removed: &[&str]) -> Folded {
    Folded::Delta { base, revision, rows: rows.iter().map(|id| id.to_string()).collect(), removed: removed.iter().map(|id| id.to_string()).collect(), state: state(revision) }
}

/// Requirement: corpus.md runner step, item 5 (D-198), a `pull` shows the unread journal of a stream folded by "the last wins".
/// Observable: the folded values. Assert: equal to the expected list for each journal.
#[test]
fn the_unread_journal_folds_by_the_last_wins() {
    let snapshot = |revision| ReadValue::Snapshot { revision, rows: vec![row("a")], edits: Vec::new(), state: state(revision) };
    let ended = ReadValue::Ended { cause: EndCause::Closed };
    let table: Vec<(&str, Vec<ReadValue>, Vec<Folded>)> = vec![
        ("a chain of deltas", vec![delta(2, 3, vec![insert("a")]), delta(3, 4, vec![rewrite("a"), insert("b")])], vec![folded(2, 4, &["a", "b"], &[])]),
        ("a removed row leaves the rows", vec![delta(2, 3, vec![insert("a")]), delta(3, 4, vec![remove("a"), remove("c")])], vec![folded(2, 4, &[], &["a", "c"])]),
        ("a delta, a gap, a delta", vec![delta(2, 3, vec![insert("a")]), ReadValue::Gap, delta(3, 4, vec![insert("b")])], vec![folded(2, 3, &["a"], &[]), Folded::Value(ReadValue::Gap), folded(3, 4, &["b"], &[])]),
        ("a snapshot after a delta", vec![delta(2, 3, vec![insert("a")]), snapshot(4)], vec![Folded::Value(snapshot(4))]),
        ("the last count", vec![ReadValue::Count { revision: 2, count: 1, state: state(2) }, ReadValue::Count { revision: 3, count: 2, state: state(3) }], vec![Folded::Value(ReadValue::Count { revision: 3, count: 2, state: state(3) })]),
        ("the last state", vec![ReadValue::State { revision: 2, state: state(2) }, ReadValue::State { revision: 3, state: state(3) }], vec![Folded::Value(ReadValue::State { revision: 3, state: state(3) })]),
        ("ready and ended in the order of the journal", vec![ReadValue::Ready, ended.clone()], vec![Folded::Value(ReadValue::Ready), Folded::Value(ended)]),
        ("an empty journal", Vec::new(), Vec::new()),
    ];
    for (name, values, expected) in table {
        assert_eq!(fold(values), expected, "{name}");
    }
}
