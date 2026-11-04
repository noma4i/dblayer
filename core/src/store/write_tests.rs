use std::sync::Arc;

use super::fixtures::*;
use crate::lifecycle::{Authority, Epoch};

fn stale(store: &super::Store) -> Authority {
    Authority::Epoch(Epoch::new(&store.lifecycle, store.lifecycle.epoch_now().value() + 1))
}

#[test]
fn a_sql_write_outside_an_envelope_is_refused_once_its_epoch_has_gone() {
    let (store, _) = booted_store();
    let key = crate::identity::ListKey::compose(&["messages", "thread", "1"]).as_str().to_string();
    store.save_window(&super::windows::WindowRecord { key, cursor: None, has_next: false, total: None, pages: 1, issued: 0, applied: 0, generation: 0 }).unwrap();
    assert!(store.invalidate_family(&stale(&store), "messages", "thread").is_err());
    assert!(store.stale_window_keys().unwrap().is_empty(), "a refused write changed the window");
    let current = Authority::Epoch(store.lifecycle.epoch_now());
    assert_eq!(store.invalidate_family(&current, "messages", "thread").unwrap(), 1);
}

#[test]
fn a_sql_write_outside_an_envelope_keeps_the_epoch_until_it_is_done() {
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let held = store.lifecycle.epoch_now().value();
    let authority = Authority::Epoch(store.lifecycle.epoch_now());
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    let mut starting = None;
    store.write(&authority, || (), |_| {
        let life = store.lifecycle.clone();
        starting = Some(std::thread::spawn(move || life.start_session(Some("other".into()), held)));
        crate::test_wait::spin_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits);
        assert_eq!(store.lifecycle.epoch_now().value(), held, "the epoch changed while the write held the commit line");
        Ok::<_, ()>(())
    }).unwrap();
    assert!(crate::test_wait::join_within(starting.unwrap()));
    assert_ne!(store.lifecycle.epoch_now().value(), held);
}
