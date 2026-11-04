use super::load::LoadOptions;
use super::rig::*;
use super::load_machine::{LoadPhase, RelationState};
use crate::transport::Reply;
use serde_json::Map;
use std::time::Duration;
use crate::test_wait::{join_within, within};

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test]
async fn stored_seed_read_failures_remain_visible_with_and_without_rows() {
    for rows in [false, true] {
        for case in ["window", "freshness", "scope"] {
            let rig = rig();
            let ids: &[&str] = if rows { &["m1"] } else { &[] };
            rig.transport.enqueue("MomentsFeed", feed_reply(ids, false, "c"));
            rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
            let watch = if case == "scope" { Some(rig.sync.relation_state("moments", "feed", &none()).await.unwrap()) } else { None };
            {
                let store = rig.db.shared.store.lock();
                let sql = match case {
                    "window" => "ALTER TABLE query_window RENAME COLUMN cursor TO broken_cursor",
                    "freshness" => "ALTER TABLE query_window RENAME COLUMN landed_at TO broken_landed_at",
                    _ => "DROP TABLE scope_member",
                };
                store.connection.execute_batch(sql).unwrap();
            }
            let watch = match watch { Some(watch) => watch, None => rig.sync.relation_state("moments", "feed", &none()).await.unwrap() };
            let resolved = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "feed", &none()).unwrap();
            assert!(rig.sync.inner.stored(&resolved).is_err(), "{case}");
            let state = watch.borrow();
            assert_eq!(state.phase, if rows { LoadPhase::Loaded } else { LoadPhase::Failed }, "{case}");
            assert_eq!(state.error.as_ref().unwrap().kind, "database", "{case}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_relation_never_loaded_is_idle_with_nothing_more() {
    let rig = rig();
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    assert_eq!(*state.borrow(), RelationState { complete: false, phase: LoadPhase::NotLoaded, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None, revision: 0, needs_reload: None });
}

#[tokio::test(start_paused = true)]
async fn the_state_follows_a_load_from_loading_to_idle_with_the_page_flag() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2").after(Duration::from_secs(1)));
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    let load = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.load(&sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await }
    });
    rig.transport.await_calls("MomentsFeed", 1).await;
    assert_eq!(state.borrow().phase, LoadPhase::Loading);
    load.await.unwrap().unwrap();
    let done = state.borrow().clone();
    assert_eq!((done.phase, done.has_more, done.error.is_none(), done.loaded_at.is_some()), (LoadPhase::Loaded, true, true, true));
}

#[tokio::test(start_paused = true)]
async fn load_more_clears_the_page_flag_on_the_last_page() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2"));
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert!(state.borrow().has_more);
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert!(!state.borrow().has_more);
}

#[tokio::test(start_paused = true)]
async fn the_state_carries_the_total_the_server_reports_for_the_whole_list() {
    let rig = rig();
    let page = |ids: &[&str], cursor: &str, total: i64| {
        let mut data = feed_page(ids.iter().map(|id| moment(id, id)).collect(), true, cursor);
        data["feed"]["totalCount"] = serde_json::json!(total);
        Reply::data(data)
    };
    rig.transport.enqueue("MomentsFeed", page(&["a"], "c1", 7));
    rig.transport.enqueue("MomentsFeed", page(&["b"], "c2", 8));
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    assert_eq!(state.borrow().total, None);
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(state.borrow().total, Some(7));
    rig.sync.load_more(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(state.borrow().total, Some(8));
    let reopened = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    assert_eq!(reopened.borrow().total, Some(8));
}

#[tokio::test(start_paused = true)]
async fn a_failed_load_reports_its_kind_and_the_next_success_clears_it() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "no".to_string() }));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1"));
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    assert!(rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.is_err());
    let failed = state.borrow().clone();
    assert_eq!(failed.phase, LoadPhase::Failed);
    assert!(failed.error.is_some());
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    let healed = state.borrow().clone();
    assert_eq!((healed.phase, healed.error), (LoadPhase::Loaded, None));
}

#[tokio::test(start_paused = true)]
async fn a_second_receiver_starts_from_the_stored_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let mut fresh = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    within(fresh.changed()).await.unwrap();
    assert!(fresh.borrow().has_more && fresh.borrow().loaded_at.is_some());
}

#[tokio::test(start_paused = true)]
async fn the_state_tells_whether_the_running_load_is_the_first_one_a_refresh_or_the_next_page() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1").after(Duration::from_secs(1)));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], true, "c1").after(Duration::from_secs(1)));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["b"], false, "c2").after(Duration::from_secs(1)));
    let state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    let running = |state: &RelationState| (state.initial, state.refreshing, state.fetching_more);
    let first = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.load(&sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await }
    });
    rig.transport.await_calls("MomentsFeed", 1).await;
    assert_eq!(running(&state.borrow()), (true, false, false));
    first.await.unwrap().unwrap();
    assert_eq!(running(&state.borrow()), (false, false, false));
    let refresh = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.load(&sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions { force: true }).await }
    });
    rig.transport.await_calls("MomentsFeed", 2).await;
    assert_eq!(running(&state.borrow()), (false, true, false));
    refresh.await.unwrap().unwrap();
    let more = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.load_more(&sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await }
    });
    rig.transport.await_calls("MomentsFeed", 3).await;
    assert_eq!(running(&state.borrow()), (false, false, true));
    more.await.unwrap().unwrap();
    assert_eq!(running(&state.borrow()), (false, false, false));
}

#[tokio::test]
async fn watching_a_relation_state_does_not_wait_for_the_store_writer() {
    let rig = rig();
    let held = std::sync::Arc::new(crate::test_wait::Rendezvous::new(2));
    let release = std::sync::Arc::new(crate::test_wait::Rendezvous::new(2));
    let holder = {
        let (db, held, release) = (rig.db.clone(), held.clone(), release.clone());
        std::thread::spawn(move || {
            let _writer = db.shared.store.lock();
            held.meet();
            release.meet();
        })
    };
    held.meet();
    let (sender, receiver) = std::sync::mpsc::channel();
    let caller = {
        let (sync, runtime) = (rig.sync.clone(), tokio::runtime::Handle::current());
        std::thread::spawn(move || {
            let _entered = runtime.enter();
            let params = none();
            let mut call = std::pin::pin!(sync.relation_state("moments", "feed", &params));
            let _ = std::future::Future::poll(call.as_mut(), &mut std::task::Context::from_waker(std::task::Waker::noop()));
            let _ = sender.send(());
        })
    };
    let returned = receiver.recv_timeout(Duration::from_secs(2));
    release.meet();
    join_within(holder);
    join_within(caller);
    assert!(returned.is_ok(), "relation_state waited for the store lock");
}
