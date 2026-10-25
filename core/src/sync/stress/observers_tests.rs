use std::sync::Arc;

use crate::db::{Change, Diff, Record};
use crate::identity::RowId;
use crate::syncstate::SyncInfo;

use super::findings::Findings;
use super::observers::check_step;

#[tokio::test]
async fn inv_stress_01_observers_report_unreadable_lists_counts_and_rows() {
    let rig = crate::sync::rig::rig();
    let findings = Findings::default();
    let watchers = super::observers::Watchers::attach(
        &rig.db, &findings, &Default::default(), &Default::default(), &["m1".into()], super::scope_of(&rig.db, "feed"),
    );
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    rig.db.execute_for_test("DROP TABLE row_moments");
    watchers.finish(&rig.db, &findings);
    for name in ["all moments", "moment count", "moment m1"] {
        assert!(findings.all().iter().any(|entry| entry.starts_with("[observer-read]") && entry.contains(name)), "{name}: {:?}", findings.all());
    }
}

fn row(id: &str, revision: i64) -> Arc<Record> {
    Arc::new(Record { id: RowId::new(id).unwrap(), revision, data: Default::default(), sync: SyncInfo::default(), ephemeral: 0 })
}

fn change(revision: i64, rows: Vec<Arc<Record>>, diff: Diff) -> Change {
    Change { revision, size: rows.len(), rows, related: Default::default(), diff, full: true, window: None, keys: Default::default() }
}

fn inserted(ids: &[&str]) -> Diff {
    Diff { inserted: ids.iter().map(|id| RowId::new(id).unwrap()).collect(), ..Diff::default() }
}

fn run(previous: Option<&Change>, next: &Change) -> Vec<String> {
    let findings = Findings::default();
    check_step("probe", previous, next, &findings);
    findings.all()
}

#[test]
fn a_consistent_step_is_silent() {
    let first = change(1, vec![row("a", 1)], inserted(&["a"]));
    let second = change(2, vec![row("a", 1), row("b", 2)], inserted(&["b"]));
    assert_eq!(run(None, &first), Vec::<String>::new());
    assert_eq!(run(Some(&first), &second), Vec::<String>::new());
}

#[test]
fn a_revision_that_goes_back_turns_red() {
    let first = change(5, vec![row("a", 1)], inserted(&["a"]));
    let second = change(4, vec![row("a", 1)], Diff::default());
    assert!(run(Some(&first), &second).iter().any(|finding| finding.starts_with("[observer-monotonic]")));
}

#[test]
fn rows_that_the_diff_does_not_explain_turn_red() {
    let first = change(1, vec![row("a", 1)], inserted(&["a"]));
    let second = change(2, vec![row("a", 1), row("b", 2)], Diff::default());
    assert!(run(Some(&first), &second).iter().any(|finding| finding.starts_with("[observer-diff]")));
}

#[test]
fn a_changed_row_that_is_not_marked_updated_turns_red() {
    let first = change(1, vec![row("a", 1)], inserted(&["a"]));
    let second = change(2, vec![row("a", 2)], Diff::default());
    assert!(run(Some(&first), &second).iter().any(|finding| finding.starts_with("[observer-diff]")));
}

#[test]
fn a_row_listed_twice_turns_red() {
    let first = change(1, vec![row("a", 1), row("a", 1)], inserted(&["a", "a"]));
    assert!(run(None, &first).iter().any(|finding| finding.starts_with("[observer-duplicates]")));
}
