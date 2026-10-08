use super::fixtures::*;
use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::identity::ScopeKey;
use crate::schema::PageDirection;

fn window(key: &str) -> WindowRecord {
    WindowRecord { key: key.into(), cursor: None, has_next: false, ids: vec![], issued: 0, applied: 0, generation: 0 }
}

fn key(model_name: &str, relation: &str, param: &str) -> String {
    ScopeKey::compose(&[model_name, relation, param]).as_str().to_string()
}

fn ids(names: &[&str]) -> Vec<RowId> {
    names.iter().map(|name| id(name)).collect()
}

use crate::identity::RowId;

#[test]
fn save_and_load_return_equal_record() {
    let (store, _) = booted_store();
    let record = WindowRecord {
        key: "messages:thread:1".into(),
        cursor: Some("c1".into()),
        has_next: true,
        ids: vec!["m1".into(), "m2".into()],
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
    let first = WindowRecord { cursor: Some("c1".into()), has_next: true, ids: vec!["m1".into()], issued: 1, ..window("k") };
    let second = WindowRecord { cursor: Some("c2".into()), ids: vec!["m3".into()], issued: 4, applied: 5, generation: 6, ..window("k") };
    store.save_window(&first).unwrap();
    store.save_window(&second).unwrap();
    assert_eq!(store.window("k").unwrap(), Some(second));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window"), 1);
    assert_eq!(count(&store, "SELECT count FROM query_window"), 1);
}

#[test]
fn drop_removes_record() {
    let (store, _) = booted_store();
    store.save_window(&window("k")).unwrap();
    store.drop_window("k").unwrap();
    assert_eq!(store.window("k").unwrap(), None);
}

#[test]
fn save_keeps_landed_at_and_invalidation_of_existing_window() {
    let (store, clock) = booted_store();
    let landed = clock.now().as_millis() as i64;
    store.save_window(&window("k")).unwrap();
    store.invalidate_window("k").unwrap();
    clock.advance(std::time::Duration::from_millis(5_000));
    store.save_window(&WindowRecord { ids: vec!["m2".into()], issued: 2, ..window("k") }).unwrap();
    assert_eq!(store.window_freshness("k").unwrap(), Some(WindowFreshness { landed_at: landed, invalidated: true }));
    assert!(store.stale_window_keys().unwrap().contains(&"k".to_string()));
    assert_eq!(store.window("k").unwrap().unwrap().issued, 2);
}

#[test]
fn freshness_needs_landing_inside_window_and_no_invalidation() {
    let fresh = WindowFreshness { landed_at: 100, invalidated: false };
    assert!(fresh.is_fresh(110, 30));
    assert!(!WindowFreshness { invalidated: true, ..fresh }.is_fresh(110, 30));
    assert!(!fresh.is_fresh(140, 30));
}

#[test]
fn freshness_of_stored_window_is_read_back() {
    let (store, clock) = booted_store();
    store.save_window(&window("k")).unwrap();
    let now = clock.now().as_millis() as i64;
    assert!(store.is_window_fresh("k", now + 10, 30).unwrap());
    assert!(!store.is_window_fresh("k", now + 40, 30).unwrap());
    assert!(!store.is_window_fresh("missing", now, 30).unwrap());
}

#[test]
fn keys_are_empty_without_windows() {
    let (store, _) = booted_store();
    assert!(store.window_keys("vibes").unwrap().is_empty());
}

#[test]
fn keys_include_only_windows_of_requested_model() {
    let (store, _) = booted_store();
    let thread = key("vibes", "thread", "1");
    let media = key("vibes", "media", "1");
    let chats = key("chats", "thread", "1");
    for window_key in [&thread, &media, &chats] {
        store.save_window(&window(window_key)).unwrap();
    }
    let mut expected = vec![thread, media];
    expected.sort();
    assert_eq!(store.window_keys("vibes").unwrap(), expected);
    assert_eq!(store.window_keys("chats").unwrap(), vec![chats]);
}

#[test]
fn dropping_model_windows_removes_only_that_model() {
    let (store, _) = booted_store();
    assert_eq!(store.drop_model_windows("vibes").unwrap(), 0);
    let vibes1 = key("vibes", "thread", "1");
    let vibes2 = key("vibes", "thread", "2");
    let chats = key("chats", "thread", "1");
    for window_key in [&vibes1, &vibes2, &chats] {
        store.save_window(&window(window_key)).unwrap();
    }
    assert_eq!(store.drop_model_windows("vibes").unwrap(), 2);
    assert!(store.window(&vibes1).unwrap().is_none());
    assert!(store.window(&chats).unwrap().is_some());
}

#[test]
fn model_prefix_does_not_match_longer_model_names() {
    let (store, _) = booted_store();
    store.save_window(&window(&key("ab", "thread", "1"))).unwrap();
    store.save_window(&window(&key("abc", "thread", "1"))).unwrap();
    assert_eq!(store.window_keys("ab").unwrap().len(), 1);
}

#[test]
fn mark_exact_marks_only_requested_key() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    let media = key("messages", "media", "1");
    store.save_window(&window(&thread)).unwrap();
    store.save_window(&window(&media)).unwrap();
    store.invalidate_window(&thread).unwrap();
    assert_eq!(store.stale_window_keys().unwrap(), vec![thread]);
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
fn clear_flag_removes_stale_key_and_sets_landing_time() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    store.save_window(&window(&thread)).unwrap();
    store.invalidate_window(&thread).unwrap();
    store.clear_invalidation(&thread, 10).unwrap();
    assert!(store.stale_window_keys().unwrap().is_empty());
    assert_eq!(store.window_freshness(&thread).unwrap().unwrap().landed_at, 10);
}

#[test]
fn stale_keys_are_empty_on_clean_database() {
    let (store, _) = booted_store();
    store.save_window(&window(&key("messages", "thread", "1"))).unwrap();
    assert!(store.stale_window_keys().unwrap().is_empty());
}

#[test]
fn invalidation_with_no_requests_leaves_no_stale_keys() {
    let (store, _) = booted_store();
    assert_eq!(store.apply_invalidation(&[]).unwrap(), 0);
    assert!(store.stale_window_keys().unwrap().is_empty());
}

#[test]
fn family_request_marks_matching_window_stale() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "thread-1");
    store.save_window(&window(&thread)).unwrap();
    assert_eq!(store.apply_invalidation(&[InvalidationRequest::family("messages", "thread")]).unwrap(), 1);
    assert!(store.stale_window_keys().unwrap().contains(&thread));
}

#[test]
fn family_request_collapses_exact_request_of_same_relation() {
    let (store, _) = booted_store();
    let exact = key("messages", "thread", "1");
    store.save_window(&window(&exact)).unwrap();
    let requests = vec![InvalidationRequest::family("messages", "thread"), InvalidationRequest::exact(&exact)];
    assert_eq!(store.apply_invalidation(&requests).unwrap(), 1);
    let other = vec![InvalidationRequest::exact(&exact), InvalidationRequest::exact(&exact)];
    assert_eq!(InvalidationRequest::deduplicate(&other).len(), 1);
}

#[test]
fn family_prefix_follows_length_prefixed_encoding() {
    assert_eq!(family_prefix("messages", "thread"), "8:messages6:thread");
    assert_eq!(family_prefix("chats", "all"), "5:chats3:all");
}

#[test]
fn window_key_carries_direction() {
    assert_eq!(window_key("inbox", PageDirection::Forward), "inbox:forward");
    assert_eq!(window_key("inbox", PageDirection::Backward), "inbox:backward");
}

#[test]
fn load_more_needs_cursor_flag_and_page_room() {
    let base = QueryWindow::new("inbox:forward", PageDirection::Forward);
    let no_next = QueryWindow { cursor: Some("cursor".into()), ..base.clone() };
    let no_cursor = QueryWindow { has_next: true, ..base.clone() };
    let within = QueryWindow { cursor: Some("cursor".into()), has_next: true, count: 2, ..base.clone() };
    let at_limit = QueryWindow { count: 3, ..within.clone() };
    assert!(!Pagination::can_load_more(None, None));
    assert!(!Pagination::can_load_more(Some(&no_next), None));
    assert!(!Pagination::can_load_more(Some(&no_cursor), None));
    assert!(Pagination::can_load_more(Some(&within), Some(3)));
    assert!(!Pagination::can_load_more(Some(&at_limit), Some(3)));
    assert!(Pagination::can_load_more(Some(&at_limit), None));
}

#[test]
fn advance_appends_rows_skips_duplicates_and_rejects_malformed_page_info() {
    let window = QueryWindow { row_ids: ids(&["row-1"]), ..QueryWindow::new("inbox:forward", PageDirection::Forward) };
    let first = Pagination::advance(
        &window,
        &PageInfo { has_next_page: true, end_cursor: Some("cursor-1".into()) },
        &ids(&["row-2", "row-3", "row-2"]),
    )
    .unwrap();
    let second =
        Pagination::advance(&first, &PageInfo { has_next_page: false, end_cursor: None }, &ids(&["row-4"])).unwrap();
    assert_eq!(first.cursor.as_deref(), Some("cursor-1"));
    assert!(first.has_next);
    assert_eq!(first.count, 1);
    assert_eq!(first.row_ids, ids(&["row-1", "row-2", "row-3"]));
    assert_eq!(second.cursor, None);
    assert!(!second.has_next);
    assert_eq!(second.count, 2);
    assert_eq!(second.row_ids, ids(&["row-1", "row-2", "row-3", "row-4"]));
    let malformed = Pagination::advance(&window, &PageInfo { has_next_page: true, end_cursor: None }, &[]);
    assert!(matches!(malformed, Err(StoreError::Constraint(_))));
}

#[test]
fn replace_keeps_chain_age_and_visible_rows_respect_limit() {
    let window = QueryWindow {
        cursor: Some("cursor-4".into()),
        has_next: true,
        count: 4,
        row_ids: ids(&["row-old"]),
        ..QueryWindow::new("inbox:forward", PageDirection::Forward)
    };
    let rows = ids(&["row-old", "row-new", "row-3"]);
    let replaced = Pagination::replace_rows(&window, ids(&["row-new"]));
    assert_eq!(replaced.row_ids, ids(&["row-new"]));
    assert_eq!(replaced.count, 4);
    assert_eq!(replaced.cursor.as_deref(), Some("cursor-4"));
    assert!(replaced.has_next);
    assert_eq!(Pagination::visible_rows(&rows, None), rows);
    assert_eq!(Pagination::visible_rows(&rows, Some(2)), ids(&["row-old", "row-new"]));
    assert_eq!(Pagination::visible_rows(&rows, Some(5)), rows);
    assert!(Pagination::visible_rows(&rows, Some(-1)).is_empty());
}

#[test]
fn survivors_drop_removed_ids_preserving_order() {
    let removed: BTreeSet<String> = ["b".to_string()].into();
    let source = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    assert_eq!(Pagination::survivors(&source, &removed), vec!["a", "c"]);
    assert_eq!(Pagination::survivors(&source, &BTreeSet::new()), source);
}

#[test]
fn affected_keys_give_only_intersecting_keys_sorted() {
    let windows: BTreeMap<String, Vec<String>> = BTreeMap::from([
        ("z".into(), vec!["a".into(), "b".into()]),
        ("a".into(), vec!["c".into(), "d".into()]),
        ("m".into(), vec!["e".into()]),
    ]);
    let touched: BTreeSet<String> = ["b".to_string(), "d".to_string()].into();
    assert_eq!(Pagination::affected_keys(&windows, &touched), vec!["a", "z"]);
    assert!(Pagination::affected_keys(&windows, &BTreeSet::new()).is_empty());
}

#[test]
fn window_ids_decode_filters_non_strings() {
    let (store, _) = booted_store();
    store.save_window(&window("k")).unwrap();
    exec(&store, "UPDATE query_window SET ids = CAST('[\"a\",1]' AS BLOB) WHERE key = 'k'");
    assert_eq!(store.window("k").unwrap().unwrap().ids, vec!["a"]);
}
