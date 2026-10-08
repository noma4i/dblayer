use super::*;
use crate::identity::RowId;

fn names(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

fn record(row: &str, order: &str) -> ScopeRecord {
    ScopeRecord::new(RowId::new(row).unwrap(), order)
}

#[test]
fn page_adds_moves_and_never_removes() {
    let added = MembershipPlan::page(&names(&["a"]), &names(&["a", "b"]));
    let reordered = MembershipPlan::page(&names(&["a", "b"]), &names(&["b", "a"]));
    let missing = MembershipPlan::page(&names(&["a", "b"]), &names(&["a"]));
    let repeated = MembershipPlan::page(&names(&["a", "b", "a"]), &names(&["a"]));
    assert_eq!(added.inserted, names(&["b"]));
    assert!(added.moved.is_empty() && added.removed.is_empty());
    assert_eq!(reordered.moved, names(&["b", "a"]));
    assert!(reordered.inserted.is_empty());
    assert!(missing.is_empty());
    assert!(repeated.inserted.is_empty() && repeated.moved.is_empty());
}

#[test]
fn complete_removes_ids_missing_from_incoming() {
    assert_eq!(MembershipPlan::complete(&names(&["a", "b"]), &names(&["a"])).removed, names(&["b"]));
}

#[test]
fn empty_and_touched_changes_match_their_contents() {
    let empty = MembershipChange::default();
    let touched = MembershipChange { inserted: names(&["b"]), moved: names(&["a"]), removed: names(&["c", "b"]) };
    assert!(empty.is_empty());
    assert_eq!(touched.touched_ids(), names(&["b", "a", "c"]));
}

#[test]
fn visible_sorts_by_order_key_then_row_and_skips_dangling() {
    let unsorted = vec![record("b", "b"), record("a", "a"), record("c", "c")];
    let ids = |records: &[ScopeRecord]| ScopeRecord::visible(records).iter().map(|id| id.to_string()).collect::<Vec<_>>();
    assert_eq!(ids(&unsorted), vec!["a", "b", "c"]);
    let with_dangling = vec![record("1", "a"), ScopeRecord::dangling(RowId::new("2").unwrap(), "b"), record("3", "c")];
    assert_eq!(ids(&with_dangling), vec!["1", "3"]);
    assert_eq!(ScopeRecord::count(&with_dangling), 2);
    assert_eq!(ids(&[record("2", "a"), record("1", "a")]), vec!["1", "2"]);
}

#[test]
fn windowed_limits_rows_and_count_ignores_window() {
    let records = vec![record("1", "a"), record("2", "b"), record("3", "c")];
    assert_eq!(ScopeRecord::windowed(&records, Some(2)).len(), 2);
    assert_eq!(ScopeRecord::windowed(&records, None).len(), 3);
    assert!(ScopeRecord::windowed(&records, Some(0)).is_empty());
    assert!(ScopeRecord::count_ignores_window(&records));
}
