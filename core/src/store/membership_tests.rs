use super::*;

fn names(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
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
    assert_eq!(missing, MembershipChange::default());
    assert!(repeated.inserted.is_empty() && repeated.moved.is_empty());
}

#[test]
fn complete_removes_ids_missing_from_incoming() {
    assert_eq!(MembershipPlan::complete(&names(&["a", "b"]), &names(&["a"])).removed, names(&["b"]));
}
