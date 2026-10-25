use super::fixtures::*;
use super::*;

use crate::identity::ScopeKey;
use crate::schema::PageDirection;

#[test]
fn saving_a_page_has_constant_sql_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use crate::commit::{WindowSave, WindowTail};
    let measure = |size| {
        let (store, _) = booted_store();
        let target = scope("messages", "thread");
        let rows = (0..size).map(|n| upsert("messages", message(&format!("m{n:05}"), "1"))).collect();
        let members = (0..size).map(|n| append(&target, &format!("m{n:05}"), &format!("{n:05}"))).collect();
        store.commit(envelope(&store, rows, members)).unwrap();
        let mut page = envelope(&store, vec![upsert("messages", message("new", "1"))], vec![append(&target, "new", "z")]);
        page.window = Some(WindowSave { key: "page".into(), scope: target, cursor: Some("new".into()), has_next: false, total: None, pages: 1,
            tail: Some(WindowTail { anchor: "m00000".into(), cursor: Some("kept".into()), has_next: true }), issued: 1, applied: 1, cleared_at: None });
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = steps.clone();
        store.lock().connection.progress_handler(1, Some(move || { counter.fetch_add(1, Ordering::SeqCst); false })).unwrap();
        store.commit(page).unwrap();
        let writes = steps.swap(0, Ordering::SeqCst);
        assert_eq!(store.window("page").unwrap().unwrap().cursor.as_deref(), Some("kept"));
        store.lock().connection.progress_handler(0, None::<fn() -> bool>).unwrap();
        (writes, steps.load(Ordering::SeqCst))
    };
    let (small, large) = (measure(200), measure(2000));
    eprintln!("page SQL steps (write, read): 200={small:?}, 2000={large:?}");
    assert_eq!(small, large);
}

fn window(key: &str) -> WindowRecord {
    WindowRecord { key: key.into(), cursor: None, has_next: false, total: None, pages: 1, issued: 0, applied: 0, generation: 0 }
}

fn key(model_name: &str, relation: &str, param: &str) -> String {
    ScopeKey::compose(&[model_name, relation, param]).as_str().to_string()
}

#[test]
fn save_and_load_return_equal_record() {
    let (store, _) = booted_store();
    let record = WindowRecord {
        key: "messages:thread:1".into(),
        cursor: Some("c1".into()),
        has_next: true,
        total: Some(5),
        pages: 3,
        issued: 1,
        applied: 2,
        generation: 3,
    };
    store.save_window(&record).unwrap();
    assert_eq!(store.window(&record.key).unwrap(), Some(record));
}

#[test]
fn load_unknown_key_returns_none() {
    let (store, _) = booted_store();
    assert_eq!(store.window("unknown").unwrap(), None);
}

#[test]
fn repeated_save_replaces_record_and_keeps_one_row() {
    let (store, _) = booted_store();
    let first = WindowRecord { cursor: Some("c1".into()), has_next: true, issued: 1, ..window("k") };
    let second = WindowRecord { cursor: Some("c2".into()), issued: 4, applied: 5, generation: 6, ..window("k") };
    store.save_window(&first).unwrap();
    store.save_window(&second).unwrap();
    assert_eq!(store.window("k").unwrap(), Some(second));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window"), 1);
}

#[test]
fn save_keeps_landed_at_and_invalidation_of_existing_window() {
    let (store, clock) = booted_store();
    let thread = key("messages", "thread", "1");
    let landed = clock.now().as_millis() as i64;
    store.save_window(&window(&thread)).unwrap();
    store.invalidate_family("messages", "thread").unwrap();
    clock.advance(std::time::Duration::from_millis(5_000));
    store.save_window(&WindowRecord { issued: 2, ..window(&thread) }).unwrap();
    assert_eq!(store.window_freshness(&thread).unwrap(), Some(WindowFreshness { landed_at: landed, invalidated: true }));
    assert!(store.stale_window_keys().unwrap().contains(&thread));
    assert_eq!(store.window(&thread).unwrap().unwrap().issued, 2);
}

#[test]
fn freshness_needs_landing_inside_window_and_no_invalidation() {
    let fresh = WindowFreshness { landed_at: 100, invalidated: false };
    assert!(fresh.is_fresh(110, 30));
    assert!(!WindowFreshness { invalidated: true, ..fresh }.is_fresh(110, 30));
    assert!(!fresh.is_fresh(140, 30));
}

#[test]
fn mark_family_marks_only_thread_keys() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    let media = key("messages", "media", "1");
    store.save_window(&window(&thread)).unwrap();
    store.save_window(&window(&media)).unwrap();
    assert_eq!(store.invalidate_family("messages", "thread").unwrap(), 1);
    assert_eq!(store.stale_window_keys().unwrap(), vec![thread]);
}

#[test]
fn stale_keys_are_empty_on_clean_database() {
    let (store, _) = booted_store();
    store.save_window(&window(&key("messages", "thread", "1"))).unwrap();
    assert!(store.stale_window_keys().unwrap().is_empty());
}

#[test]
fn window_key_carries_direction() {
    assert_eq!(window_key("inbox", PageDirection::Forward), "inbox:forward");
    assert_eq!(window_key("inbox", PageDirection::Backward), "inbox:backward");
}
