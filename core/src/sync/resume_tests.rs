use super::load::LoadOptions;
use super::rig::*;
use super::SyncConfig;
use crate::pacing::ResumeDrain;
use crate::test_wait::within;
use serde_json::{json, Map};
use std::time::Duration;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[derive(Clone, Copy)]
struct Window {
    name: &'static str,
    loaded_before: bool,
    watched: bool,
    invalidated: bool,
    idle_secs: u64,
    chunks: usize,
    loaded: usize,
    calls: usize,
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_07_resume_reloads_a_watched_window_that_is_stale_invalidated_or_never_loaded() {
    let cases = [
        Window { name: "stale", loaded_before: true, watched: true, invalidated: false, idle_secs: 120, chunks: 1, loaded: 1, calls: 2 },
        Window { name: "recent", loaded_before: true, watched: true, invalidated: false, idle_secs: 30, chunks: 0, loaded: 0, calls: 1 },
        Window { name: "no readers", loaded_before: true, watched: false, invalidated: false, idle_secs: 120, chunks: 0, loaded: 0, calls: 1 },
        Window { name: "invalidated while recent", loaded_before: true, watched: true, invalidated: true, idle_secs: 0, chunks: 1, loaded: 1, calls: 2 },
        Window { name: "never loaded", loaded_before: false, watched: true, invalidated: false, idle_secs: 0, chunks: 1, loaded: 1, calls: 1 },
    ];
    for case in cases {
        for by_event in [false, true] {
            let rig = rig();
            rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
            let watch = rig.sync.watch("moments", "feed", &none()).unwrap();
            if case.loaded_before {
                rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
            }
            if !case.watched {
                drop(watch);
            }
            if case.invalidated {
                rig.db.shared.store.invalidate_family("moments", "feed").unwrap();
            }
            rig.clock.advance(Duration::from_secs(case.idle_secs));
            if by_event {
                rig.sync.enter_foreground();
                within(rig.sync.resume_settled()).await;
            } else {
                let report = within(rig.sync.resume()).await.unwrap();
                assert_eq!((report.chunks, report.loaded, report.failed), (case.chunks, case.loaded, 0), "{}", case.name);
            }
            assert_eq!(rig.transport.call_count("MomentsFeed"), case.calls, "{} by_event={by_event}", case.name);
        }
    }
}

fn stale_loaded_feed(rig: &Rig) -> crate::sync::Watch {
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.watch("moments", "feed", &none()).unwrap()
}

#[tokio::test(start_paused = true)]
async fn foreground_and_online_together_reload_a_stale_window_once() {
    let rig = rig();
    let _watch = stale_loaded_feed(&rig);
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.set_online(false);
    rig.clock.advance(Duration::from_secs(120));
    rig.sync.enter_foreground();
    rig.sync.set_online(true);
    rig.sync.enter_foreground();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn offline_foreground_is_ignored_and_the_return_online_reloads() {
    let rig = rig();
    let _watch = stale_loaded_feed(&rig);
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.sync.set_online(false);
    rig.clock.advance(Duration::from_secs(120));
    rig.sync.enter_foreground();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.sync.set_online(true);
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn an_event_during_a_run_reloads_the_window_it_missed() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10)));
    rig.transport.enqueue("MomentsByUser", rig_list(&["a"]));
    let _feed = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.enter_foreground();
    within(rig.transport.await_calls("MomentsFeed", 1)).await;
    let _by_user = rig.sync.watch("moments", "by_user", &params(json!({"userId": "u1"}))).unwrap();
    rig.sync.enter_foreground();
    within(rig.sync.resume_settled()).await;
    assert_eq!((rig.transport.call_count("MomentsFeed"), rig.transport.call_count("MomentsByUser")), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn the_foreground_keeps_a_session_refusal_and_the_manual_resume_clears_it() {
    use crate::transport::{SessionCode, SubscriptionEvent, SubscriptionFailure, TransportError};
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    let _watch = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "v1"}))).unwrap();
    within(rig.transport.await_calls("VibeEvents", 1)).await;
    let error = TransportError::Session { code: SessionCode::Unauthenticated, detail: "session_unknown".into() };
    rig.transport.emit("VibeEvents", SubscriptionEvent::Failed(SubscriptionFailure::from_error(error)));
    let refused = || super::locked(&rig.sync.inner.lives).values().any(|live| live.subscription.as_ref().is_some_and(|subscription| subscription.status().stopped));
    for _ in 0..1000 {
        if refused() { break; }
        tokio::task::yield_now().await;
    }
    assert!(refused());
    rig.sync.enter_foreground();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("VibeEvents"), 1);
    within(rig.sync.resume()).await.unwrap();
    within(rig.transport.await_calls("VibeEvents", 2)).await;
}

#[tokio::test(start_paused = true)]
async fn a_session_refusal_from_a_normal_load_keeps_events_away_until_the_manual_resume() {
    use crate::transport::{SessionCode, TransportError};
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", crate::transport::Reply::fail(TransportError::Session { code: SessionCode::Unauthenticated, detail: "session_unknown".into() }));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    let _state = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    assert!(rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.is_err());
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    rig.sync.enter_foreground();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
    within(rig.sync.resume()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_session_failure_is_selected_among_the_failures_of_every_observed_subscription() {
    use crate::transport::{SessionCode, SubscriptionEvent, SubscriptionFailure, TransportError};
    let session = || TransportError::Session { code: SessionCode::Unauthenticated, detail: "session_unknown".into() };
    let denied = || TransportError::Http { status: 401, body: "denied".into() };
    let cases = [
        ("session first", [Some(session()), Some(denied())], true),
        ("session last", [Some(denied()), Some(session())], true),
        ("no session", [Some(denied()), Some(denied())], false),
        ("session alone", [None, Some(session())], true),
    ];
    for (name, errors, expects_session) in cases {
        let rig = rig();
        let expected = errors.iter().flatten().count();
        let mut watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
        for (operation, error) in ["MomentEvents", "MomentRemoved"].into_iter().zip(errors) {
            within(rig.transport.await_calls(operation, 1)).await;
            let Some(error) = error else { continue };
            rig.transport.emit(operation, SubscriptionEvent::Failed(SubscriptionFailure::from_error(error)));
            within(rig.transport.await_consumed(operation)).await;
        }
        let recorded = || super::locked(&rig.sync.inner.lives).values().filter(|live| live.subscription.as_ref().is_some_and(|subscription| subscription.status().failure.is_some())).count();
        assert_eq!(recorded(), expected, "{name}");
        let failures = watch.failures().unwrap();
        for found in [failures.current.clone(), (failures.recheck)()] {
            assert_eq!(found.map(|failure| failure.error), expects_session.then(session), "{name}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_manual_resume_gets_the_report_of_the_pass_that_started_after_it_while_events_arrive() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", crate::transport::Reply::graphql_error("nope", "FORBIDDEN").after(Duration::from_secs(10)));
    rig.transport.enqueue("MomentsByUser", rig_list(&["a"]));
    let _feed = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.enter_foreground();
    within(rig.transport.await_calls("MomentsFeed", 1)).await;
    let (sender, report) = tokio::sync::oneshot::channel();
    assert!(rig.sync.inner.begin_resume(super::resume_machine::Cause::Explicit, Some(sender)).is_none());
    let _by_user = rig.sync.watch("moments", "by_user", &params(json!({"userId": "u1"}))).unwrap();
    within(rig.transport.await_calls("MomentsFeed", 2)).await;
    for _ in 0..3 {
        rig.sync.enter_foreground();
    }
    let report = within(report).await.unwrap().unwrap();
    assert_eq!((report.loaded, report.failed), (1, 1));
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 3);
}

#[tokio::test(start_paused = true)]
async fn a_runner_dropped_before_its_first_poll_fails_the_waiting_resume() {
    let rig = rig();
    let (sender, mut report) = tokio::sync::oneshot::channel();
    let runner = rig.sync.inner.begin_resume(super::resume_machine::Cause::Explicit, Some(sender)).unwrap();
    drop(runner.run());
    assert_eq!(report.try_recv(), Ok(Err(crate::sync::SyncError::Cancelled)));
    within(rig.sync.resume_settled()).await;
}

#[tokio::test(start_paused = true)]
async fn a_runner_aborted_during_a_pass_fails_every_waiting_resume_and_frees_the_owner() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10)));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    let (first, mut first_report) = tokio::sync::oneshot::channel();
    let runner = rig.sync.inner.begin_resume(super::resume_machine::Cause::Explicit, Some(first)).unwrap();
    let task = tokio::spawn(runner.run());
    within(rig.transport.await_calls("MomentsFeed", 1)).await;
    let (queued, mut queued_report) = tokio::sync::oneshot::channel();
    assert!(rig.sync.inner.begin_resume(super::resume_machine::Cause::Explicit, Some(queued)).is_none());
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert_eq!(first_report.try_recv(), Ok(Err(crate::sync::SyncError::Cancelled)));
    assert_eq!(queued_report.try_recv(), Ok(Err(crate::sync::SyncError::Cancelled)));
    assert!(within(rig.sync.resume()).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn closing_cancels_the_run_an_event_started() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1").after(Duration::from_secs(10)));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.sync.enter_foreground();
    within(rig.transport.await_calls("MomentsFeed", 1)).await;
    let db = rig.db.clone();
    drop(rig.sync);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(db.query(&crate::query::Query::new("moments")).unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_07_windows_are_reloaded_in_chunks() {
    let rig = rig_with(SyncConfig { resume: ResumeDrain::new(Duration::from_secs(60), 2), ..SyncConfig::default() });
    rig.transport.enqueue("MomentsByUser", rig_list(&["a"]));
    let mut watches = Vec::new();
    for user in ["u1", "u2", "u3", "u4", "u5"] {
        watches.push(rig.sync.watch("moments", "by_user", &params(json!({"userId": user}))).unwrap());
    }
    let report = within(rig.sync.resume()).await.unwrap();
    assert_eq!((report.chunks, report.loaded, report.failed), (3, 5, 0));
    assert_eq!(rig.transport.call_count("MomentsByUser"), 5);
}

#[tokio::test(start_paused = true)]
async fn failed_reload_is_counted() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", crate::transport::Reply::graphql_error("nope", "FORBIDDEN"));
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    let report = within(rig.sync.resume()).await.unwrap();
    assert_eq!((report.loaded, report.failed), (0, 1));
}

#[tokio::test(start_paused = true)]
async fn a_dropped_watch_leaves_the_db_registry_and_resume_skips_it() {
    let rig = rig();
    let watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    assert_eq!(rig.db.observed_reads().len(), 1);
    drop(watch);
    assert!(rig.db.observed_reads().is_empty());
    let report = within(rig.sync.resume()).await.unwrap();
    assert_eq!((report.loaded, report.failed, rig.transport.call_count("MomentsFeed")), (0, 0, 0));
}

fn rig_list(ids: &[&str]) -> crate::transport::Reply {
    crate::transport::Reply::data(json!({"moments": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>()}))
}

#[tokio::test(start_paused = true)]
async fn a_committed_credential_resumes_once_and_the_same_credential_does_not() {
    let rig = rig();
    let _watch = stale_loaded_feed(&rig);
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    rig.clock.advance(Duration::from_secs(120));
    rig.transport.set_session(None);
    rig.sync.session_committed();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1, "no credential");
    rig.transport.set_session(Some("first"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c2"));
    rig.sync.session_committed();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2, "new credential");
    rig.clock.advance(Duration::from_secs(120));
    rig.sync.session_committed();
    within(rig.sync.resume_settled()).await;
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2, "same credential");
}
