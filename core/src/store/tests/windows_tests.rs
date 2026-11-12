use super::fixtures::*;
use super::*;

use crate::identity::ListKey;
use crate::schema::PageDirection;

#[test]
fn win_store_landed_at_uses_response_time() {
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::StoreOp;
    use std::sync::Arc;
    use std::time::Duration;
    let clock = Arc::new(crate::runtime::ManualClock::new(Duration::from_millis(100)));
    let db = Store::open_models(None, vec![crate::schema_fixtures::messages_spec(false)], None, clock.clone()).unwrap();
    let params = serde_json::json!({"chatId":"c1"}).as_object().unwrap().clone();
    let key = WindowKey::relation("messages", "thread", &params);
    let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 100, key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
    clock.advance(Duration::from_millis(500));
    db.write_queue_ops(&[page]).unwrap();
    let read = || db.lock().connection.query_row("SELECT landed_at FROM query_window", [], |row| row.get::<_, i64>(0)).unwrap();
    assert_eq!(read(), 100);
    let marker = crate::store::WindowMarker { sequence: 5, values: [("revision".into(), 1)].into() };
    let update = StoreOp::WindowMarker { landed_at: Some(700), key: key.name.clone(), model: key.model.clone(), marker };
    clock.advance(Duration::from_millis(500));
    db.write_queue_ops(&[update]).unwrap();
    assert_eq!(read(), 700);
    let marker = crate::store::WindowMarker { sequence: 6, values: [("revision".into(), 2)].into() };
    clock.advance(Duration::from_millis(500));
    db.write_queue_ops(&[StoreOp::WindowMarker { landed_at: None, key: key.name.clone(), model: key.model.clone(), marker }]).unwrap();
    assert_eq!(read(), 700);
}

#[test]
fn win_page_cover_to_end_server_order_only() {
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreOp};
    use crate::schema::Sort;
    use serde_json::json;
    for local_order in [false, true] {
        let mut spec = crate::schema_fixtures::messages_spec(false);
        if !local_order { spec.relations[0].sort = Sort::ServerOrder; }
        let db = Store::open_models(None, vec![spec], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        let params = json!({"chatId":"c1"}).as_object().unwrap().clone();
        let key = WindowKey::relation("messages", "thread", &params);
        let scope = db.spec("messages").unwrap().selection_key("thread", &params).unwrap();
        let page = |page, ids: &[(&str, i64)], cursor: Option<&str>| StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0,
            key: key.name.clone(), model: key.model.clone(), page, replace_all: false, cursor: cursor.map(str::to_string),
            roots: ids.iter().map(|(id, seq)| RowValue { row_id: (*id).into(), values: json!({"id": id, "chatId":"c1", "sequenceNumber":seq}).to_string() }).collect(),
            related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap(),
        };
        db.write_queue_ops(&[page(1, &[("a", 1), ("b", 2), ("c", 3)], None)]).unwrap();
        db.write_queue_ops(&[page(1, &[("a", 1)], Some("a"))]).unwrap();
        db.write_queue_ops(&[page(2, &[("b", 2)], None)]).unwrap();
        let members: Vec<_> = db.read(&["messages"], |reader| reader.scope_members("messages", scope.as_str())).unwrap().into_iter().map(|(id, _)| id).collect();
        assert_eq!(members, if local_order { vec!["a", "b", "c"] } else { vec!["a", "b"] }, "local_order={local_order}");
    }
}

#[test]
fn store_ordered_page_open_interval() {
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreOp};
    use serde_json::json;
    for (seed, page_rows, number, cursor, expected) in [
        (vec![("old", 2)], vec![("new1", 2), ("new2", 2)], 1, Some("next"), vec!["new1", "new2", "old"]),
        (vec![("low", 1), ("middle", 2), ("high", 3)], vec![("new_low", 1), ("new_high", 3)], 1, Some("next"), vec!["high", "low", "new_high", "new_low"]),
        (vec![("old", 2)], vec![("new", 2)], 1, Some("next"), vec!["new", "old"]),
        (vec![("old", 2)], vec![("new", 2)], 1, None, vec!["new"]),
        (vec![("old", 2)], vec![("new", 2)], 2, None, vec!["new", "old"]),
    ] {
        let db = Store::open_models(None, vec![crate::schema_fixtures::messages_spec(false)], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        let params = json!({"chatId":"c1"}).as_object().unwrap().clone();
        let key = WindowKey::relation("messages", "thread", &params);
        let scope = db.spec("messages").unwrap().selection_key("thread", &params).unwrap();
        let page = |rows: &[(&str, i64)], page, cursor: Option<&str>| StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0,
            key: key.name.clone(), model: key.model.clone(), page, replace_all: false, cursor: cursor.map(str::to_string),
            roots: rows.iter().map(|(id, seq)| RowValue { row_id: (*id).into(), values: json!({"id": id, "chatId":"c1", "sequenceNumber":seq}).to_string() }).collect(),
            related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap(),
        };
        db.write_queue_ops(&[page(&seed, 1, Some("next"))]).unwrap();
        db.write_queue_ops(&[page(&page_rows, number, cursor)]).unwrap();
        let mut members: Vec<_> = db.read(&["messages"], |reader| reader.scope_members("messages", scope.as_str())).unwrap().into_iter().map(|(id, _)| id).collect();
        members.sort();
        assert_eq!(members, expected, "seed={seed:?}, page={page_rows:?}");
    }
}

#[test]
fn saving_a_page_has_constant_sql_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use crate::store::WindowSave;
    let measure = |size| {
        let (store, _) = booted_store();
        let target = scope("messages", "thread");
        let rows = (0..size).map(|n| upsert("messages", message(&format!("m{n:05}"), "1"))).collect();
        let members = (0..size).map(|n| append(&target, &format!("m{n:05}"), &format!("{n:05}"))).collect();
        store.commit(envelope(&store, rows, members)).unwrap();
        let mut page = envelope(&store, vec![upsert("messages", message("new", "1"))], vec![append(&target, "new", "z")]);
        page.window = Some(WindowSave { marker: None, stale_since: None, key: "page".into(), scope: target, cursor: Some("new".into()), has_next: false, total: None, pages: 1,
            anchor: Some("new".into()), issued: 1, applied: 1, cleared_at: None });
        let steps = Arc::new(AtomicUsize::new(0));
        let counter = steps.clone();
        store.lock().connection.progress_handler(1, Some(move || { counter.fetch_add(1, Ordering::SeqCst); false })).unwrap();
        store.commit(page).unwrap();
        let writes = steps.swap(0, Ordering::SeqCst);
        assert_eq!(store.window("page").unwrap().unwrap().cursor.as_deref(), Some("new"));
        store.lock().connection.progress_handler(0, None::<fn() -> bool>).unwrap();
        (writes, steps.load(Ordering::SeqCst))
    };
    let (small, large) = (measure(200), measure(2000));
    eprintln!("page SQL steps (write, read): 200={small:?}, 2000={large:?}");
    assert_eq!(small, large);
}

fn window(key: &str) -> WindowRecord {
    WindowRecord { key: key.into(), cursor: None, anchor: None, has_next: false, total: None, pages: 1, issued: 0, applied: 0, generation: 0 }
}

fn key(model_name: &str, relation: &str, param: &str) -> String {
    ListKey::compose(&[model_name, relation, param]).as_str().to_string()
}

#[test]
fn save_and_load_return_equal_record() {
    let (store, _) = booted_store();
    let record = WindowRecord {
        key: "messages:thread:1".into(),
        cursor: Some("c1".into()),
        anchor: Some("a1".into()),
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
    store.invalidate_family_now("messages", "thread").unwrap();
    clock.advance(std::time::Duration::from_millis(5_000));
    store.save_window(&WindowRecord { issued: 2, ..window(&thread) }).unwrap();
    assert_eq!(store.window_freshness(&thread).unwrap(), Some(WindowFreshness { landed_at: landed, stale_since: Some(1) }));
    assert!(store.stale_window_keys().unwrap().contains(&thread));
    assert_eq!(store.window(&thread).unwrap().unwrap().issued, 2);
}

#[test]
fn mark_family_marks_only_thread_keys() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    let media = key("messages", "media", "1");
    store.save_window(&window(&thread)).unwrap();
    store.save_window(&window(&media)).unwrap();
    assert_eq!(store.invalidate_family_now("messages", "thread").unwrap(), 1);
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

#[test]
fn mark_stale_op_sets_stale_since_for_models_and_keys_in_the_batch_transaction() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{StoreBatch, StoreOp};
    let (store, _) = booted_store();
    let (messages, items) = (key("messages", "thread", "1"), key("items", "list", "1"));
    store.save_window(&window(&messages)).unwrap();
    store.save_window(&window(&items)).unwrap();
    let write = |models: Vec<String>, keys: Vec<String>| {
        let batch = StoreBatch { author: BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::MarkStale { models, keys }] };
        store.write_batch(&batch, store.files(), None).unwrap();
    };
    write(vec!["messages".into()], Vec::new());
    assert_eq!(store.stale_window_keys().unwrap(), vec![messages.clone()], "a model marks the windows of its lists");
    write(Vec::new(), vec![items.clone()]);
    let mut stale = store.stale_window_keys().unwrap();
    stale.sort();
    let mut wanted = vec![messages, items];
    wanted.sort();
    assert_eq!(stale, wanted, "a key marks its own window");
}

thread_local! { static PAGE_ADMISSION_QUERIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn count_page_admission(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.contains("FROM op_effect e JOIN op") { PAGE_ADMISSION_QUERIES.with(|count| count.set(count.get() + 1)); }
    }
}

#[test]
fn win_page_admission_query_work() {
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreOp};
    use serde_json::json;
    let measure = |size: usize| {
        let db = Store::open_models(None, vec![crate::schema_fixtures::messages_spec(false)], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        let params = json!({"chatId":"c1"}).as_object().unwrap().clone();
        let key = WindowKey::relation("messages", "thread", &params);
        let roots = (0..size).map(|i| RowValue { row_id: format!("m{i}"), values: json!({"id":format!("m{i}"),"chatId":"c1","sequenceNumber":i}).to_string() }).collect();
        let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
        PAGE_ADMISSION_QUERIES.with(|count| count.set(0));
        db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_page_admission));
        db.write_queue_ops(&[page]).unwrap();
        db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        let scope = db.spec("messages").unwrap().selection_key("thread", &params).unwrap();
        assert_eq!(db.read(&["messages"], |reader| reader.scope_members("messages", scope.as_str())).unwrap().len(), size);
        PAGE_ADMISSION_QUERIES.with(|count| count.get())
    };
    assert_eq!(measure(2), measure(40), "admission queries must not grow with page size");
}

thread_local! { static PAGE_TOMBSTONE_QUERIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn count_page_tombstones(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.starts_with("SELECT") && sql.contains("FROM tombstone WHERE model") { PAGE_TOMBSTONE_QUERIES.with(|count| count.set(count.get() + 1)); }
    }
}

#[test]
fn win_page_tombstone_query_work() {
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreOp};
    use serde_json::json;
    let measure = |size: usize| {
        let db = Store::open_models(None, vec![crate::schema_fixtures::messages_spec(false)], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        let params = json!({"chatId":"c1"}).as_object().unwrap().clone();
        let key = WindowKey::relation("messages", "thread", &params);
        let roots = (0..size).map(|i| RowValue { row_id: format!("m{i}"), values: json!({"id":format!("m{i}"),"chatId":"c1","sequenceNumber":i}).to_string() }).collect();
        let page = StoreOp::Window { source: crate::schema::RowSource::Page, landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
        PAGE_TOMBSTONE_QUERIES.with(|count| count.set(0));
        db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_page_tombstones));
        db.write_queue_ops(&[page]).unwrap();
        db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        let scope = db.spec("messages").unwrap().selection_key("thread", &params).unwrap();
        assert_eq!(db.read(&["messages"], |reader| reader.scope_members("messages", scope.as_str())).unwrap().len(), size);
        PAGE_TOMBSTONE_QUERIES.with(|count| count.get())
    };
    assert_eq!(measure(2), measure(40), "tombstone queries must not grow with page size");
}
