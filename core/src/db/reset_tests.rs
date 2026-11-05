use super::fixtures::*;

use crate::query::Query;

#[test]
fn inv_row_11_reset_all_wipes_the_rows_and_the_row_cache() {
    let db = booted();
    seed_tree(&db);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    db.clear_account().unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert_eq!(db.count(&Query::new("users")).unwrap(), 0);
}

#[test]
fn inv_row_11_reset_of_models_keeps_the_other_models() {
    let db = booted();
    seed_tree(&db);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    assert!(db.peek_row("moments", &rid("m1")).is_some());
    db.clear_models(&["moments"]).unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert!(db.query(&Query::new("moments")).unwrap().is_empty());
    assert!(db.find("users", &rid("u1")).unwrap().is_some());
}

#[test]
fn reset_advances_the_revision() {
    let db = booted();
    seed_tree(&db);
    let before = db.revision_seq().unwrap();
    db.clear_models(&["moments"]).unwrap();
    assert!(db.revision_seq().unwrap() > before);
}

#[test]
fn reset_of_an_unknown_model_fails() {
    let db = booted();
    assert!(db.clear_models(&["ghosts"]).is_err());
}
