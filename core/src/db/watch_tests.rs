use super::dispatch::Event;
use super::support::*;
use super::watch::Subject;
use crate::commit::ScopeRef;
use crate::identity::{ModelKey, RowRef, ScopeKey};
use crate::query::Query;
use std::collections::BTreeSet;

fn model_event(model: &str) -> Event {
    Event { revision: 1, models: BTreeSet::from([ModelKey::new(model)]), ..Event::default() }
}

fn deps_of(db: &super::Db, subject: Subject) -> super::watch::Deps {
    db.shared.deps(&subject).unwrap()
}

#[test]
fn a_query_depends_on_its_model_and_the_models_it_includes() {
    let db = booted();
    let deps = deps_of(&db, Subject::Rows(Query::new("moments").including(&["author"])));
    assert!(deps.hit(&model_event("moments")));
    assert!(deps.hit(&model_event("users")));
    assert!(!deps.hit(&model_event("chats")));
}

#[test]
fn a_scope_query_depends_on_its_scope_not_on_its_model() {
    let db = booted();
    let scope = ScopeRef::new(ModelKey::new("moments"), ScopeKey::from_raw("feed"));
    let deps = deps_of(&db, Subject::Rows(Query::new("moments").in_scope(scope.key.clone())));
    assert!(!deps.hit(&model_event("moments")));
    assert!(deps.hit(&Event { revision: 1, scopes: BTreeSet::from([scope]), ..Event::default() }));
}

#[test]
fn a_row_subject_depends_on_that_row_only() {
    let db = booted();
    let row = RowRef::new(ModelKey::new("moments"), rid("m1"));
    let deps = deps_of(&db, Subject::Row(row.clone()));
    assert!(!deps.hit(&model_event("moments")));
    assert!(deps.hit(&Event { revision: 1, rows: BTreeSet::from([row]), ..Event::default() }));
}

#[test]
fn a_reset_hits_the_models_it_wipes() {
    let db = booted();
    let deps = deps_of(&db, Subject::Count(Query::new("moments")));
    assert!(deps.hit(&Event { revision: 1, reset_all: true, ..Event::default() }));
    assert!(deps.hit(&Event { revision: 1, reset_models: BTreeSet::from([ModelKey::new("moments")]), ..Event::default() }));
    assert!(!deps.hit(&Event { revision: 1, reset_models: BTreeSet::from([ModelKey::new("users")]), ..Event::default() }));
}
