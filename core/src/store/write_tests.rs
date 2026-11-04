use std::sync::Arc;

use super::fixtures::*;
use crate::lifecycle::{Authority, Origin};

fn stale(store: &super::Store) -> Authority {
    let admitted = store.lifecycle.admit(Origin::Explicit, "test write").unwrap();
    assert!(store.lifecycle.session_from(Some("other".into()), store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(admitted.value())).unwrap()).is_ok());
    admitted.authority()
}

#[test]
fn a_sql_write_outside_an_envelope_is_refused_once_its_epoch_has_gone() {
    let (store, _) = booted_store();
    let key = crate::identity::ListKey::compose(&["messages", "thread", "1"]).as_str().to_string();
    store.save_window(&super::windows::WindowRecord { key, cursor: None, has_next: false, total: None, pages: 1, issued: 0, applied: 0, generation: 0 }).unwrap();
    assert!(store.invalidate_family(&stale(&store), "messages", "thread", 1).is_err());
    assert!(store.stale_window_keys().unwrap().is_empty(), "a refused write changed the window");
    let current = store.lifecycle.admit(Origin::Explicit, "test write").unwrap().authority();
    assert_eq!(store.invalidate_family(&current, "messages", "thread", 1).unwrap(), 1);
}

#[test]
fn a_sql_write_outside_an_envelope_keeps_the_epoch_until_it_is_done() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let held = store.lifecycle.snapshot().session_epoch;
    let authority = store.lifecycle.admit(Origin::Explicit, "test write").unwrap().authority();
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    let mut starting = None;
    store.write(&authority, || (), |_| {
        let life = store.lifecycle.clone();
        starting = Some(std::thread::spawn(move || life.session_from(Some("other".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(held)).unwrap()).is_ok()));
        crate::test_wait::spin_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits);
        assert_eq!(store.lifecycle.snapshot().session_epoch, held, "the epoch changed while the write held the commit line");
        Ok::<_, ()>(())
    }).unwrap();
    assert!(crate::test_wait::join_within(starting.unwrap()));
    assert_ne!(store.lifecycle.snapshot().session_epoch, held);
}
