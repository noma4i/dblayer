use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;
use crate::identity::RowId;
use crate::transport::Reply;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

async fn rereads_finished(rig: &Rig, operation: &str, calls: usize) {
    rig.transport.await_calls(operation, calls).await;
    crate::test_wait::pass(std::time::Duration::from_millis(1)).await;
    crate::test_wait::yield_until("a load of a window is still in flight", || super::super::locked(&rig.sync.inner.flights).waiting.is_empty()).await;
}

#[test]
fn a_refused_reread_leaves_the_call_of_the_mutation_successful() { crate::test_wait::paused(async {
    for case in ["read", "reload"] {
        let rig = rig();
        let _feed = rig.sync.watch("moments", "feed", &none()).unwrap();
        let _history = rig.sync.watch("moments", "history", &none()).unwrap();
        rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
        rig.transport.enqueue("MomentsHistory", history_reply(&["m1"]));
        if case == "reload" {
            rig.transport.enqueue("MomentsFeed", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "feed rejected".into() }));
            rig.transport.enqueue("MomentsHistory", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "history rejected".into() }));
        }
        load(&rig, "feed").await;
        load(&rig, "history").await;
        rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "t"}))).unwrap();
        rig.transport.enqueue("CreateMoment", created());
        if case == "read" {
            rig.db.shared.store.lock().connection.execute("ALTER TABLE query_window RENAME COLUMN landed_at TO broken_landed_at", []).unwrap();
        }
        let report = crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        rereads_finished(&rig, "MomentsHistory", if case == "read" { 1 } else { 2 }).await;
        assert_eq!(report.committed(), 1, "{case}");
        assert_eq!(rig.transport.call_count("MomentsFeed"), if case == "read" { 1 } else { 2 }, "{case}");
        assert_eq!(rig.transport.call_count("MomentsHistory"), if case == "read" { 1 } else { 2 }, "{case}");
        if case == "reload" {
            assert!(is_stale(&rig, "feed") && is_stale(&rig, "history"), "the refused reread left the lists fresh");
        }
    }
}) }

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn created() -> Reply {
    Reply::data(json!({"createMoment": {"id": "m9", "text": "t", "userId": null}}))
}

fn is_stale(rig: &Rig, relation: &str) -> bool {
    let key = rig.sync.window_key("moments", relation, &none()).unwrap();
    rig.db.shared.store.window_freshness(&key).unwrap().is_some_and(|freshness| freshness.stale_since.is_some())
}

async fn load(rig: &Rig, relation: &str) {
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", relation, &none(), LoadOptions::default()).await.unwrap();
}

fn history_reply(ids: &[&str]) -> Reply {
    Reply::data(json!({"history": feed_page(ids.iter().map(|id| moment(id, id)).collect(), false, "c")["feed"]}))
}

#[test]
fn inv_fresh_10_created_moment_marks_the_open_feed_stale_and_the_next_load_reads_it() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(is_stale(&rig, "feed"));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert!(!is_stale(&rig, "feed"));
}) }

#[test]
fn watched_feed_is_reread_right_after_create() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m9", "m1"], false, "c2"));
    load(&rig, "feed").await;
    rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    rereads_finished(&rig, "MomentsFeed", 2).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert!(!is_stale(&rig, "feed"));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m9", "m1"]);
}) }

#[test]
fn feed_without_an_open_window_is_not_touched() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
}) }

#[test]
fn inv_fresh_10_refetch_marks_only_the_named_relations() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsHistory", history_reply(&["m1"]));
    load(&rig, "feed").await;
    load(&rig, "history").await;
    rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(is_stale(&rig, "history"));
    assert!(!is_stale(&rig, "feed"));
}) }

#[test]
fn refetch_of_a_relation_of_another_model_marks_that_relation() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.transport.enqueue("CurrentUser", Reply::data(json!({"me": {"id": "u1", "name": "Ann"}})));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &none(), LoadOptions::default()).await.unwrap();
    let key = rig.sync.window_key("users", "current", &none()).unwrap();
    let stale = || rig.db.shared.store.window_freshness(&key).unwrap().is_some_and(|freshness| freshness.stale_since.is_some());
    assert!(!stale());
    rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(stale());
}) }

#[test]
fn update_without_refetch_marks_nothing() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(!is_stale(&rig, "feed"));
}) }

#[test]
fn destroy_success_marks_the_feed_stale() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    load(&rig, "feed").await;
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"ok": true}})));
    assert_eq!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap().committed(), 1);
    assert!(is_stale(&rig, "feed"));
}) }

#[test]
fn refresh_rereads_a_fresh_window() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m2"], false, "c2"));
    load(&rig, "feed").await;
    rig.sync.refresh(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m2"]);
}) }

#[test]
fn reload_after_a_commit_is_issued_after_the_load_that_was_in_flight_at_the_commit() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    let feed_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = feed_calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name != "MomentsFeed" {
            return None;
        }
        Some(match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => feed_reply(&["m1"], false, "c"),
            1 => feed_reply(&["m1"], false, "c").after(std::time::Duration::from_secs(1)),
            _ => feed_reply(&["m9", "m1"], false, "c2"),
        })
    });
    load(&rig, "feed").await;
    rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created());
    let no_params = none();
    let admitted = rig.db.admit().unwrap();
    let older = rig.sync.refresh(&admitted, "moments", "feed", &no_params);
    let commit = async {
        crate::test_wait::pass(std::time::Duration::from_millis(10)).await;
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    };
    let (older, ()) = tokio::join!(older, commit);
    older.unwrap();
    rereads_finished(&rig, "MomentsFeed", 3).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 3);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m9", "m1"]);
}) }

#[test]
fn a_confirmed_create_does_not_reload_a_watched_list_that_takes_events() { crate::test_wait::paused(async {
    use crate::sync::stable_order::rig::*;
    let rig = chat_rig(true);
    enqueue_page(&rig, vec![message("m1", Some(1), 1)], false, "c");
    let _watch = rig.sync.watch("messages", "thread", &chat_params()).unwrap();
    load_first_page(&rig).await;
    rig.db.create(&rig.db.admit().unwrap(), "messages", params(json!({"chatId": CHAT, "text": "mine", "createdAt": "2026-01-01T00:00:09.000Z"}))).unwrap();
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": message("m9", Some(2), 9)})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("MessagesThread"), 1);
}) }

/// A call that waits for its own answer does not wait for a load of the refetched list that was already in flight, and the next mutation is sent meanwhile.
#[test]
fn a_mutation_does_not_wait_for_a_load_of_its_refetched_list_that_is_in_flight() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "history", &none()).unwrap();
    put(&rig, "moments", moment("m1", "t"));
    let history_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = history_calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name != "MomentsHistory" {
            return None;
        }
        Some(match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            1 => history_reply(&["m1"]).after(std::time::Duration::from_secs(3600)),
            _ => history_reply(&["m1"]),
        })
    });
    load(&rig, "history").await;
    let no_params = none();
    let admitted = rig.db.admit().unwrap();
    let held = rig.sync.refresh(&admitted, "moments", "history", &no_params);
    let liking = async {
        crate::test_wait::pass(std::time::Duration::from_millis(10)).await;
        let (_, liked) = act_claimed(&rig.sync, "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({})));
        rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
        rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
        let first = crate::test_wait::timeout(std::time::Duration::from_secs(60), async { flushed(&rig).await; liked.result().await.result }).await;
        let (_, liked_again) = act_claimed(&rig.sync, "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({})));
        let second = crate::test_wait::timeout(std::time::Duration::from_secs(60), async { flushed(&rig).await; liked_again.result().await.result }).await;
        (first, second)
    };
    let (held, (first, second)) = tokio::join!(held, liking);
    held.unwrap();
    assert_eq!(first.expect("the answer of the first mutation waited for the load that was in flight"), crate::sync::SendResult::Committed);
    assert_eq!(second.expect("the second mutation waited behind the refetch of the first"), crate::sync::SendResult::Committed);
}) }

#[test]
fn a_mutation_returns_with_its_own_answer_while_its_reread_is_held() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "history", &none()).unwrap();
    put(&rig, "moments", moment("m1", "t"));
    let history_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = history_calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name != "MomentsHistory" {
            return None;
        }
        Some(match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => history_reply(&["m1"]),
            _ => history_reply(&["m1", "m2"]).after(std::time::Duration::from_secs(3600)),
        })
    });
    load(&rig, "history").await;
    let (_, liked) = act_claimed(&rig.sync, "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({})));
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
    let outcome = crate::test_wait::timeout(std::time::Duration::from_secs(60), async { flushed(&rig).await; liked.result().await.result }).await;
    assert_eq!(outcome.expect("the call of the mutation waited for its reread"), crate::sync::SendResult::Committed);
    rig.transport.await_calls("MomentsHistory", 2).await;
    assert!(is_stale(&rig, "history"), "the reread has not been answered yet");
    crate::test_wait::pass(std::time::Duration::from_secs(3601)).await;
    assert!(!is_stale(&rig, "history"));
    assert_eq!(member_ids(&rig, "moments", "history", &none()), ["m1", "m2"]);
}) }

#[test]
fn an_older_answer_landing_after_the_mark_still_leads_to_the_reread() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "history", &none()).unwrap();
    put(&rig, "moments", moment("m1", "t"));
    put(&rig, "moments", moment("m2", "t"));
    let history_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = history_calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name != "MomentsHistory" {
            return None;
        }
        Some(match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => history_reply(&["m1", "m2"]),
            1 => history_reply(&["m1", "m2"]).after(std::time::Duration::from_secs(5)),
            _ => history_reply(&["m3", "m1", "m2"]),
        })
    });
    load(&rig, "history").await;
    let no_params = none();
    let admitted = rig.db.admit().unwrap();
    let older = rig.sync.refresh(&admitted, "moments", "history", &no_params);
    let commits = async {
        crate::test_wait::pass(std::time::Duration::from_millis(10)).await;
        rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
        rig.db.act(&rig.db.admit().unwrap(), "moments", "poke", &rid("m2"), params(json!({})), params(json!({}))).unwrap();
        rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
        rig.transport.enqueue("PokeMoment", Reply::data(json!({"pokeMoment": {"id": "m2", "text": "t", "userId": null}})).after(std::time::Duration::from_secs(10)));
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    };
    let (older, ()) = tokio::join!(older, commits);
    older.unwrap();
    rereads_finished(&rig, "MomentsHistory", 3).await;
    assert_eq!(rig.transport.call_count("MomentsHistory"), 3);
    assert_eq!(member_ids(&rig, "moments", "history", &none()), ["m3", "m1", "m2"]);
}) }

#[test]
fn a_mutation_committed_during_the_first_load_of_its_refetched_list_leads_to_the_reread() { crate::test_wait::paused(async {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "history", &none()).unwrap();
    put(&rig, "moments", moment("m1", "t"));
    let history_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = history_calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name != "MomentsHistory" {
            return None;
        }
        Some(match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => history_reply(&["m1"]).after(std::time::Duration::from_secs(5)),
            _ => history_reply(&["m1", "m2"]),
        })
    });
    let no_params = none();
    let admitted = rig.db.admit().unwrap();
    let first = rig.sync.load(&admitted, "moments", "history", &no_params, LoadOptions::default());
    let commit = async {
        crate::test_wait::pass(std::time::Duration::from_millis(10)).await;
        rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
        rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    };
    let (first, ()) = tokio::join!(first, commit);
    first.unwrap();
    rereads_finished(&rig, "MomentsHistory", 2).await;
    assert_eq!(rig.transport.call_count("MomentsHistory"), 2);
    assert_eq!(member_ids(&rig, "moments", "history", &none()), ["m1", "m2"]);
}) }

#[test]
fn a_reread_queued_before_a_session_change_writes_nothing_in_the_next_session() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    put(&rig, "moments", moment("m2", "t"));
    rig.transport.enqueue("MomentsHistory", history_reply(&["m1"]));
    load(&rig, "history").await;
    rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.db.act(&rig.db.admit().unwrap(), "moments", "like", &rid("m2"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null, "likedByMe": true}})));
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m2", "text": "t", "userId": null, "likedByMe": true}})).after(std::time::Duration::from_secs(3600)));
    let sync = rig.sync.clone();
    let flushing = tokio::spawn(async move { crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await });
    rig.transport.await_calls("LikeMoment", 2).await;
    assert!(is_stale(&rig, "history"), "the first commit left the list fresh");
    rig.sync.end_session_for_test();
    assert!(matches!(crate::test_wait::within(flushing).await.unwrap(), Err(crate::sync::SyncError::Transport(crate::transport::TransportError::Interrupted(_)))));
    rig.sync.commit_session_for_test();
    let _watch = rig.sync.watch("moments", "history", &none()).unwrap();
    rig.transport.enqueue("MomentsHistory", history_reply(&["m1"]));
    load(&rig, "history").await;
    rereads_finished(&rig, "MomentsHistory", 2).await;
    rig.db.shared.store.invalidate_family_now("moments", "history").unwrap();
    let before = rig.transport.call_count("MomentsHistory");
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    rereads_finished(&rig, "MomentsHistory", before).await;
    assert_eq!(rig.transport.call_count("MomentsHistory"), before, "the reread of the ended epoch ran in the next session");
    assert!(is_stale(&rig, "history"));
}) }
