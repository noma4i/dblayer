use super::error::SyncError;
use super::load::{LoadOptions, LoadStatus};
use super::rig::*;
use crate::store::WindowRecord;
use crate::transport::{Reply, TransportError};
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn window(rig: &Rig, relation: &str, params: &Map<String, serde_json::Value>) -> Option<WindowRecord> {
    let key = rig.sync.window_key("moments", relation, params).unwrap();
    rig.db.shared.store.window(&key).unwrap()
}

#[tokio::test]
async fn freshness_read_does_not_hold_the_flight_lock() {
    let rig = rig();
    let inner = std::sync::Arc::downgrade(&rig.sync.inner);
    let (sent, received) = std::sync::mpsc::channel();
    *rig.db.shared.store.before_freshness.lock().unwrap() = Some(Box::new(move || {
        sent.send(crate::test_wait::mutex_free(&inner.upgrade().unwrap().flights)).unwrap();
    }));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    crate::test_wait::within(rig.sync.load("moments", "feed", &none(), LoadOptions::default())).await.unwrap();
    assert!(crate::test_wait::recv_within(&received));
}

#[tokio::test(start_paused = true)]
async fn first_page_lands_rows_members_and_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], true, "c2"));
    let outcome = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::Loaded);
    assert_eq!((outcome.rows, outcome.has_next, outcome.attempts), (2, true, 1));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m1", "m2"]);
    let stored = window(&rig, "feed", &none()).unwrap();
    assert_eq!((stored.cursor.as_deref(), stored.has_next), (Some("c2"), true));
    assert_eq!(rig.transport.calls()[0].variables, json!({"first": 2}));
}

#[tokio::test(start_paused = true)]
async fn nested_author_lands_in_users() {
    let rig = rig();
    let node = json!({"id": "m1", "text": "hi", "author": {"id": "u1", "name": "Ann"}});
    rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![node], false, "c1")));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    assert_eq!(user.text("name"), Some("Ann"));
    assert_eq!(text_of(&rig, "moments", "m1"), Some("hi".to_string()));
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_01_fresh_window_is_skipped_until_stale_time_passes() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(299));
    let skipped = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(skipped.status, LoadStatus::SkippedFresh);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.clock.advance(Duration::from_secs(2));
    let again = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(again.status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn force_reloads_a_fresh_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let forced = rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    assert_eq!(forced.status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn invalidated_window_loads_again() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.db.shared.store.invalidate_family("moments", "feed").unwrap();
    let again = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(again.status, LoadStatus::Loaded);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn empty_result_uses_the_shorter_empty_window() {
    let rig = rig();
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": []})));
    rig.sync.load("moments", "empty_aware", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(5));
    let early = rig.sync.load("moments", "empty_aware", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(early.status, LoadStatus::SkippedFresh);
    rig.clock.advance(Duration::from_secs(6));
    let again = rig.sync.load("moments", "empty_aware", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(again.status, LoadStatus::Loaded);
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_01_parallel_identical_loads_share_one_transport_call() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c2").after(Duration::from_secs(1)));
    let nothing = none();
    let (rig_ref, nothing_ref) = (&rig, &nothing);
    let returned = |options: LoadOptions| async move {
        let outcome = rig_ref.sync.load("moments", "feed", nothing_ref, options).await;
        (outcome, member_ids(rig_ref, "moments", "feed", nothing_ref))
    };
    let ((first, first_rows), (second, second_rows)) = tokio::join!(returned(LoadOptions::default()), returned(LoadOptions { force: true }));
    assert_eq!(first.unwrap().status, LoadStatus::Loaded);
    assert_eq!(second.unwrap().status, LoadStatus::Loaded);
    assert_eq!(first_rows, ["m1", "m2"], "the first caller returned before the rows landed");
    assert_eq!(second_rows, ["m1", "m2"], "the second caller returned before the rows landed");
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    assert_eq!(rig.sync.counters().transport_calls, 1);
}

#[tokio::test(start_paused = true)]
async fn network_error_is_retried_with_delay() {
    let mut delays = Vec::new();
    for _ in 0..3 {
        let rig = rig();
        rig.transport.enqueue("MomentsFeed", Reply::fail(TransportError::Network("down".to_string())));
        rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
        let started = tokio::time::Instant::now();
        let outcome = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
        assert_eq!((outcome.status, outcome.attempts), (LoadStatus::Loaded, 2));
        delays.push(started.elapsed());
        assert!((Duration::from_millis(800)..=Duration::from_secs(1)).contains(&started.elapsed()));
        assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
        assert_eq!(rig.sync.counters().transport_calls, 2);
    }
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]), "clients repeated together: {delays:?}");
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_08_offline_load_makes_no_call_and_online_loads_again() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.set_online(false);
    let result = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await;
    assert_eq!(result, Err(SyncError::Offline));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 0);
    rig.sync.set_online(true);
    let outcome = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::Loaded);
    rig.sync.set_online(false);
    assert_eq!(rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await, Err(SyncError::Offline));
}

#[tokio::test]
async fn offline_interrupts_a_load_without_waiting_for_http() {
    use std::{future::Future, task::{Context, Poll, Waker}};
    for cached in [false, true] {
    let rig = rig();
    if cached {
        rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
        rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
        rig.clock.advance(Duration::from_secs(301));
    }
    rig.transport.respond_with(|_| Some(feed_reply(&["m1"], false, "c1").after(Duration::from_secs(60))));
    let params = none();
    let mut load = Box::pin(rig.sync.load("moments", "feed", &params, LoadOptions::default()));
    assert!(load.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    rig.sync.set_online(false);
    assert!(matches!(load.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(SyncError::Offline))));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1 + usize::from(cached));
    }
}

#[tokio::test(start_paused = true)]
async fn retry_budget_is_spent_then_the_error_is_returned() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::fail(TransportError::Network("down".to_string())));
    let result = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await;
    assert_eq!(result, Err(SyncError::Transport(TransportError::Network("down".to_string()))));
    assert_eq!(rig.transport.call_count("MomentsFeed"), 4);
}

#[tokio::test(start_paused = true)]
async fn server_code_is_retried_and_fatal_code_is_not() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("busy", "SERVICE_UNAVAILABLE"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let outcome = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(outcome.attempts, 2);
    rig.transport.enqueue("MomentsByUser", Reply::graphql_error("nope", "FORBIDDEN"));
    let fatal = rig.sync.load("moments", "by_user", &params(json!({"userId": "u1"})), LoadOptions::default()).await;
    assert!(matches!(fatal, Err(SyncError::Graphql { messages, .. }) if messages == ["nope"]));
    assert_eq!(rig.transport.call_count("MomentsByUser"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_05_inv_merge_10_failed_refresh_keeps_stale_data_unless_forced() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("nope", "FORBIDDEN"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let (row, window_before) = (rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap(), window(&rig, "feed", &none()));
    rig.clock.advance(Duration::from_secs(400));
    let kept = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(kept.status, LoadStatus::KeptStale);
    let forced = rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await;
    assert!(matches!(forced, Err(SyncError::Graphql { messages, .. }) if messages == ["nope"]));
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m1"]);
    assert_eq!(rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap(), row);
    assert_eq!(window(&rig, "feed", &none()), window_before);
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_05_inv_merge_10_failed_first_load_without_window_is_an_error() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", Reply::graphql_error("nope", "FORBIDDEN"));
    let result = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await;
    assert!(matches!(result, Err(SyncError::Graphql { messages, .. }) if messages == ["nope"]));
}

#[tokio::test(start_paused = true)]
async fn older_response_does_not_overwrite_newer_event_data() {
    let rig = rig();
    put(&rig, "moments", json!({"id": "m1", "text": "old", "userId": null}));
    let page = Reply::data(feed_page(vec![moment("m1", "from server"), moment("m2", "m2")], false, "c2"));
    rig.transport.enqueue("MomentsFeed", page.after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.load("moments", "feed", &Map::new(), LoadOptions::default()).await });
    rig.transport.await_calls("MomentsFeed", 1).await;
    put(&rig, "moments", json!({"id": "m1", "text": "event wins", "userId": null}));
    task.await.unwrap().unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("event wins".to_string()));
    assert_eq!(text_of(&rig, "moments", "m2"), Some("m2".to_string()));
    assert!(rig.sync.counters().stale_version_drops >= 1);
}

#[tokio::test(start_paused = true)]
async fn inv_merge_05_row_destroyed_during_flight_is_not_resurrected() {
    use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
    let rig = rig();
    put(&rig, "moments", json!({"id": "m1", "text": "old", "userId": null}));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c2").after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.load("moments", "feed", &Map::new(), LoadOptions::default()).await });
    rig.transport.await_calls("MomentsFeed", 1).await;
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    let id = crate::identity::RowId::new("m1").unwrap();
    envelope.entity_ops.push(EntityOp::Destroy { model: crate::identity::ModelKey::new("moments"), id, reason: DestroyReason::Server });
    rig.db.land(envelope).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m2"]);
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_03_response_after_a_reset_is_discarded() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.load("moments", "feed", &Map::new(), LoadOptions::default()).await });
    rig.transport.await_calls("MomentsFeed", 1).await;
    rig.db.shared.store.reset_cache_only(&["moments"]).unwrap();
    let outcome = task.await.unwrap();
    assert!(matches!(&outcome, Err(SyncError::Transport(TransportError::Interrupted(_)))), "the explicit call straddling the reset must end with its outcome: {outcome:?}");
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn list_load_replaces_membership_completely() {
    let rig = rig();
    let mine = params(json!({"userId": "u1"}));
    let list = |ids: &[&str]| Reply::data(json!({"moments": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>()}));
    rig.transport.enqueue("MomentsByUser", list(&["a", "b"]));
    rig.transport.enqueue("MomentsByUser", list(&["b", "c"]));
    rig.sync.load("moments", "by_user", &mine, LoadOptions::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "by_user", &mine), ["a", "b"]);
    rig.sync.load("moments", "by_user", &mine, LoadOptions { force: true }).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "by_user", &mine), ["b", "c"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"userId": "u1"}));
}

#[tokio::test(start_paused = true)]
async fn null_param_loads_its_own_list_and_sends_null() {
    let rig = rig();
    let unowned = params(json!({"userId": null}));
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": [moment("a", "a")]})));
    let outcome = rig.sync.load("moments", "by_user", &unowned, LoadOptions::default()).await.unwrap();
    assert_eq!(outcome.status, LoadStatus::Loaded);
    assert_eq!(member_ids(&rig, "moments", "by_user", &unowned), ["a"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"userId": null}));
}

#[tokio::test(start_paused = true)]
async fn singleton_load_and_refresh_store_without_observers() {
    for force in [false, true] {
        let rig = rig();
        rig.transport.enqueue("CurrentUser", Reply::data(json!({"me": {"id": "u1", "name": "Ann"}})));
        let outcome = if force {
            rig.sync.refresh("users", "current", &none()).await
        } else {
            rig.sync.load("users", "current", &none(), LoadOptions::default()).await
        }.unwrap();
        assert_eq!(outcome.rows, 1, "force={force}");
        assert_eq!(member_ids(&rig, "users", "current", &none()), ["u1"]);
        let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
        assert_eq!(user.text("name"), Some("Ann"));
    }
}

#[tokio::test(start_paused = true)]
async fn refreshing_the_first_page_keeps_the_loaded_tail_when_the_anchor_is_found() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "c2"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["c", "d"], false, "c4"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a", "b"], true, "r2"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.load_more("moments", "feed", &none()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b", "c", "d"]);
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["a", "b", "c", "d"]);
    assert_eq!(window(&rig, "feed", &none()).unwrap().cursor.as_deref(), Some("c4"));
}

#[tokio::test(start_paused = true)]
async fn rows_failing_normalization_are_counted_not_fatal() {
    let rig = rig();
    let page = feed_page(vec![moment("m1", "ok"), json!({"text": "no id"})], false, "c1");
    rig.transport.enqueue("MomentsFeed", Reply::data(page));
    let outcome = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!((outcome.rows, outcome.dropped), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn unknown_names_are_reported() {
    let rig = rig();
    let load = |model: &'static str, relation: &'static str| {
        let sync = rig.sync.clone();
        async move { sync.load(model, relation, &Map::new(), LoadOptions::default()).await }
    };
    assert!(matches!(load("ghosts", "feed").await, Err(SyncError::UnknownModel(_))));
    assert!(matches!(load("moments", "nope").await, Err(SyncError::UnknownRelation { .. })));
    assert!(matches!(load("moments", "recent").await, Err(SyncError::NotRemote { .. })));
}

#[tokio::test(start_paused = true)]
async fn missing_operation_registration_is_reported() {
    let rig = rig();
    rig.sync.inner.operations.lock().unwrap().clear();
    let result = rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await;
    assert_eq!(result, Err(SyncError::UnknownOperation("MomentsFeed".to_string())));
}

#[tokio::test(start_paused = true)]
async fn invalid_params_are_reported() {
    let rig = rig();
    let result = rig.sync.load("moments", "by_user", &params(json!({"bogus": "x"})), LoadOptions::default()).await;
    assert!(matches!(result, Err(SyncError::Params(_))));
}

#[tokio::test(start_paused = true)]
async fn inv_observe_05_forced_reload_of_a_complete_feed_replaces_the_members_in_one_delivery() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c1"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let spec = rig.db.spec("moments").unwrap();
    let relation = spec.relations.iter().find(|candidate| candidate.name == "feed").unwrap();
    let scope = relation.scope_key("moments", &Default::default()).unwrap();
    type Seen = Vec<(Vec<String>, usize)>;
    let seen: std::sync::Arc<std::sync::Mutex<Seen>> = Default::default();
    let sink = seen.clone();
    let whole = crate::db::fixtures::Whole::default();
    let _subscription = rig
        .db
        .observe(crate::query::Query::new("moments").in_scope(scope), move |change| {
            sink.lock().unwrap().push((whole.of(change).iter().map(|row| row.id.to_string()).collect(), change.diff.removed.len()));
        })
        .unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m2", "m3"], false, "c2"));
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m2", "m3"], false, "c2"));
    rig.sync.load("moments", "feed", &none(), LoadOptions { force: true }).await.unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let all = seen.lock().unwrap().clone();
    assert_eq!(all, vec![(vec!["m1".to_string(), "m2".to_string()], 0), (vec!["m2".to_string(), "m3".to_string()], 1)]);
}

#[tokio::test(start_paused = true)]
async fn rows_without_the_scope_column_get_the_param() {
    let rig = rig();
    let mine = params(json!({"userId": "u1"}));
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": [{"id": "a", "text": "a"}]})));
    rig.sync.load("moments", "by_user", &mine, LoadOptions::default()).await.unwrap();
    let row = rig.db.find("moments", &crate::identity::RowId::new("a").unwrap()).unwrap().unwrap();
    assert_eq!(row.text("userId"), Some("u1"));
}

#[tokio::test(start_paused = true)]
async fn text_param_keys_its_own_window_without_a_column() {
    let rig = rig();
    let cats = params(json!({"query": "cats"}));
    let dogs = params(json!({"query": "dogs"}));
    rig.transport.enqueue("MomentsSearch", Reply::data(json!({"moments": [{"id": "a", "text": "a"}]})));
    rig.transport.enqueue("MomentsSearch", Reply::data(json!({"moments": [{"id": "b", "text": "b"}]})));
    rig.sync.load("moments", "search", &cats, LoadOptions::default()).await.unwrap();
    rig.sync.load("moments", "search", &dogs, LoadOptions::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "search", &cats), ["a"]);
    assert_eq!(member_ids(&rig, "moments", "search", &dogs), ["b"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"query": "cats"}));
}

#[tokio::test(start_paused = true)]
async fn enum_column_groups_a_relation_and_fills_rows() {
    let rig = rig();
    let calm = params(json!({"mood": "calm"}));
    rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "a", "text": "a"}]})));
    rig.sync.load("moments", "by_mood", &calm, LoadOptions::default()).await.unwrap();
    assert_eq!(member_ids(&rig, "moments", "by_mood", &calm), ["a"]);
    let row = rig.db.find("moments", &crate::identity::RowId::new("a").unwrap()).unwrap().unwrap();
    assert_eq!(row.text("mood"), Some("calm"));
}

async fn rig_with_user_list(size: usize) -> (Rig, Map<String, serde_json::Value>) {
    let rig = rig();
    let params = json!({"userId": "u1"}).as_object().unwrap().clone();
    let ids: Vec<String> = (0..size).map(|index| format!("m{index:06}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>()})));
    rig.sync.load("moments", "by_user", &params, LoadOptions::default()).await.unwrap();
    (rig, params)
}

fn reads_during(work: impl FnOnce()) -> usize {
    let before = crate::identity::touches::member_reads();
    work();
    crate::identity::touches::member_reads() - before
}

#[tokio::test(start_paused = true)]
async fn reading_the_stored_window_for_a_load_reads_the_same_members_at_every_list_size() {
    let mut costs = Vec::new();
    for size in [100, 10_000] {
        let (rig, params) = rig_with_user_list(size).await;
        let resolved = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "by_user", &params).unwrap();
        costs.push(reads_during(|| assert!(rig.sync.inner.stored(&resolved).unwrap().unwrap().has_rows)));
    }
    assert_eq!(costs[0], costs[1], "member reads at 100 and 10000 rows");
}

#[tokio::test(start_paused = true)]
async fn creating_a_row_in_a_loaded_list_reads_the_same_members_at_every_list_size() {
    let mut costs = Vec::new();
    for size in [100, 10_000] {
        let (rig, params) = rig_with_user_list(size).await;
        costs.push(reads_during(|| drop(rig.db.create("moments", crate::db::fixtures::obj(json!({"userId": "u1", "text": "new"}))).unwrap())));
        assert_eq!(member_ids(&rig, "moments", "by_user", &params).len(), size + 1);
    }
    assert_eq!(costs[0], costs[1], "member reads at 100 and 10000 rows");
}

#[test]
fn resolving_twice_shares_the_operation_and_the_relation_spec() {
    let rig = rig();
    let first = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "feed", &Map::new()).unwrap();
    let second = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "feed", &Map::new()).unwrap();
    let operations: [&super::registry::OperationDef; 2] = [&first.operation, &second.operation];
    let remotes: [&crate::schema::RemoteSpec; 2] = [first.remote, second.remote];
    assert!(std::ptr::eq(operations[0], operations[1]), "the operation document is cloned for each resolve");
    assert!(std::ptr::eq(remotes[0], remotes[1]), "the remote spec is cloned for each resolve");
}
