use super::*;

#[test]
fn the_opening_token_is_issued_once_per_database() {
    let life = Lifecycle::default();
    assert!(life.opening().is_some());
    assert!(life.opening().is_none());
}
