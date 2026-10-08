use super::*;
use std::collections::BTreeSet;

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| item.to_string()).collect()
}

fn ids(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

#[test]
fn verdict_follows_members_and_live_rows() {
    let chain = ids(&["a", "b"]);
    assert_eq!(ChainVerdict::of(&chain, &set(&["a", "b"]), &set(&["a", "b"])), ChainVerdict::Intact);
    assert_eq!(ChainVerdict::of(&chain, &set(&["a", "b"]), &set(&["a"])), ChainVerdict::LostRows);
    assert_eq!(ChainVerdict::of(&chain, &set(&["a"]), &set(&["a", "b"])), ChainVerdict::LostMembership);
    assert_eq!(ChainVerdict::of(&[], &set(&[]), &set(&[])), ChainVerdict::Empty);
}

#[test]
fn lost_rows_win_over_lost_membership() {
    assert_eq!(ChainVerdict::of(&ids(&["a", "b"]), &set(&["a"]), &set(&["a"])), ChainVerdict::LostRows);
}

#[test]
fn only_intact_is_not_stale() {
    assert!(!ChainVerdict::Intact.is_stale());
    assert!(ChainVerdict::LostRows.is_stale());
    assert!(ChainVerdict::LostMembership.is_stale());
    assert!(ChainVerdict::Empty.is_stale());
}

#[test]
fn survivors_keep_order_and_shrink_reflects_partial_loss() {
    let chain = ids(&["a", "b", "c"]);
    assert_eq!(surviving_ids(&chain, &set(&["c", "a"])), ["a", "c"]);
    assert!(chain_shrinks(&chain, &set(&["c", "a"])));
    assert!(!chain_shrinks(&chain, &set(&["a", "b", "c"])));
    assert!(!chain_shrinks(&chain, &set(&[])));
}
