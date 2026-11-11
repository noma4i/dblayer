//! moment.dbl `by_user` windows and `changed_by "REACTION", via: :node` against feed.md l.22 (`gap`) and l.194 (ChannelConfirmed -> Stale of the observed windows).
use serde_json::json;

use super::stand::Stand;
use crate::ffi::ReadFetch;

fn page() -> serde_json::Value { json!({"user": {"moments": {"edges": [], "pageInfo": {"hasNextPage": false, "endCursor": null}}}}) }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_window_freshness_survives_restart() {
    use std::sync::Arc;
    use crate::ffi::selftest::rig_tests::bound;
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::tape::Rows;

    let directory = tempfile::tempdir().unwrap();
    let query = r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#;
    let answer = json!({"user": {"moments": {"edges": [{"cursor": "c1", "node": super::stand::moment("mo1", 0)}], "pageInfo": {"hasNextPage": false, "endCursor": null}}}});
    for delay in [0, 400_000] {
        let path = directory.path().join(format!("fresh-{delay}.sqlite")).to_string_lossy().into_owned();
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_at(Some(path.clone()), credentials.clone(), None, true, 1_000_000).await;
        stand.answer("userMoments", answer.clone());
        stand.sign_in().await;
        stand.hold(&["userMoments"]);
        let mark = stand.request_mark();
        let rows = Arc::new(Rows::default());
        let observed = stand.database.watch_query(query, rows.clone());
        stand.receive_after(mark, "userMoments", Some(("userId", "u1")), 1).await;
        stand.clock.advance(delay);
        stand.release(&["userMoments"]);
        bound(observed.wait_for_initial_load()).await.unwrap();
        stand.settle().await;
        assert_eq!(stand.requests_with("userMoments", "userId", "u1"), 1);
        assert_eq!(rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), ["mo1"]);
        bound(stand.database.close_database()).await.unwrap();
        drop(stand);

        let fresh = Stand::prepare_at(Some(path.clone()), credentials.clone(), None, true, 1_000_000 + delay + 300_000).await;
        fresh.answer("userMoments", answer.clone());
        let rows = Arc::new(Rows::default());
        let observed = fresh.database.watch_query(query, rows.clone());
        bound(observed.wait_for_initial_load()).await.unwrap();
        fresh.settle().await;
        assert_eq!(fresh.requests_with("userMoments", "userId", "u1"), 0, "fresh after response delay {delay}");
        assert_eq!(rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), ["mo1"]);
        bound(fresh.database.close_database()).await.unwrap();
        drop(fresh);

        let expired = Stand::prepare_at(Some(path), credentials, None, true, 1_000_000 + delay + 600_001).await;
        expired.answer("userMoments", answer.clone());
        let rows = Arc::new(Rows::default());
        let observed = expired.database.watch_query(query, rows.clone());
        bound(observed.wait_for_initial_load()).await.unwrap();
        expired.settle().await;
        assert_eq!(expired.requests_with("userMoments", "userId", "u1"), 1, "expired after response delay {delay}");
        assert_eq!(rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), ["mo1"]);
        bound(expired.database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_profile_refresh_failure_does_not_reread_without_gap() {
    use crate::ffi::{DbError, ReadPhase};
    use crate::ffi::selftest::rig_tests::bound;
    use crate::ffi::tape::Rows;
    use std::sync::Arc;

    let stand = Stand::prepare().await;
    let fixture: serde_json::Value = serde_json::from_str(include_str!("../../../../tests/corpus/consumer/my_profile.json")).unwrap();
    stand.answer("myProfile", fixture["data"].clone());
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let rows = Arc::new(Rows::default());
    let _stream = stand.database.watch_query(r#"{"model":"CurrentUser","relation":"profile","params":{}}"#, rows.clone());
    bound(stand.database.refresh(epoch, "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
    stand.settle().await;
    assert_eq!(stand.requests("myProfile"), 1);
    assert_eq!(rows.len(), 1);
    let original = rows.0.lock().unwrap()[0].id.clone();
    stand.fail_next("myProfile", DbError::Network { detail: "offline".into() });
    stand.fail_next("myProfile", DbError::Http { status: 400, detail: "refused".into() });
    let database = stand.database.clone();
    let mut armed = stand.clock.armed();
    let mut refresh = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), async move { database.refresh(epoch, "CurrentUser".into(), Some("profile".into()), "{}".into()).await }));
    let result = bound(async {
        loop {
            tokio::select! {
                result = &mut refresh => break result.unwrap(),
                () = stand.clock.await_armed(armed) => { armed = stand.clock.armed(); stand.clock.advance(5_000); }
            }
        }
    }).await;
    assert!(matches!(result, Err(DbError::Http { status: 400, .. })), "{result:?}");
    stand.settle().await;
    assert_eq!(stand.requests("myProfile"), 3);
    assert_eq!(rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), vec![original]);
    let state = rows.last().expect("profile snapshot");
    assert_eq!(state.phase, Some(ReadPhase::Loaded));
    assert!(matches!(state.error, Some(DbError::Http { status: 400, .. })), "{:?}", state.error);
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_cable_drop_resubscribes_and_reloads_only_observed() {
    let stand = Stand::open().await;
    stand.answer("userMoments", page());
    stand.answer("MomentNode", json!({"node": super::stand::moment("mo1", 0)}));
    let (events, token) = stand.events("Moment").await;
    let epoch = stand.database.session_epoch();
    let unobserved = stand.database.begin_load(epoch, "Moment".into(), Some("by_user".into()), json!({"userId": "u2"}).to_string()).unwrap();
    crate::ffi::selftest::rig_tests::bound(unobserved.result()).await.unwrap();
    drop(unobserved);
    let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
    let observed = stand.database.watch_query(r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#, rows.clone());
    crate::ffi::selftest::rig_tests::bound(observed.wait_for_initial_load()).await.unwrap();
    stand.settle().await;
    assert_eq!((stand.requests_with("userMoments", "userId", "u1"), stand.requests_with("userMoments", "userId", "u2")), (1, 1));

    stand.drop_cable(&token);
    stand.database.set_online(false).unwrap();
    stand.database.set_online(true).unwrap();
    let token = stand.next_subscription().await;
    while !matches!(crate::ffi::selftest::rig_tests::bound(events.pull_next()).await, Some(crate::ffi::ReadValue::Gap)) {}
    stand.settle().await;
    assert_eq!((stand.requests_with("userMoments", "userId", "u1"), stand.requests_with("userMoments", "userId", "u2")), (2, 1));

    let _node = stand.entity("Moment", "mo1", ReadFetch::Stale);
    stand.settle().await;
    assert_eq!(stand.requests_with("MomentNode", "momentId", "mo1"), 1);
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    stand.settle().await;
    assert_eq!((stand.requests_with("MomentNode", "momentId", "mo1"), stand.requests("MomentNode")), (2, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_cable_gaps_reread_each_observed_key_once() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node": super::stand::moment("mo1", 0)}));
    let (events, mut token) = stand.events("Moment").await;
    let _node = stand.entity("Moment", "mo1", ReadFetch::Stale);
    stand.settle().await;
    assert_eq!(stand.requests("MomentNode"), 1);
    let mut expected = None;
    for drop in 1..=3 {
        let before = stand.asked().len();
        stand.drop_cable(&token);
        token = stand.next_subscription().await;
        while !matches!(crate::ffi::selftest::rig_tests::bound(events.pull_next()).await, Some(crate::ffi::ReadValue::Gap)) {}
        stand.settle().await;
        let asked = stand.asked();
        let mut added: Vec<_> = asked[before..].iter().map(|(name, variables)| (name.clone(), variables.to_string())).collect();
        added.sort();
        assert!(added.windows(2).all(|pair| pair[0] != pair[1]), "cable drop {drop} duplicated a key: {added:?}");
        assert_eq!(added.iter().filter(|(name, _)| name == "MomentNode").count(), 1, "cable drop {drop}: {added:?}");
        if let Some(first) = &expected { assert_eq!(&added, first, "cable drop {drop} changed the observed keys"); } else { expected = Some(added); }
    }

    let before = stand.requests("MomentNode");
    stand.push(&token, json!({"area": "MOMENT", "kind": "UPDATED", "entityId": "mo1"}));
    stand.settle().await;
    assert_eq!(stand.requests("MomentNode"), before + 1, "the update rereads the node once");
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_subscription_overflow_gaps_and_rereads() {
    use std::sync::Arc;
    use crate::ffi::selftest::rig_tests::bound;
    use crate::ffi::tape::Rows;
    use crate::ffi::ReadValue;

    let stand = Stand::open().await;
    stand.answer("userMoments", page());
    stand.answer("MomentNode", json!({"node": super::stand::moment("mo1", 0)}));
    let (events, token) = stand.events("Moment").await;
    let rows = Arc::new(Rows::default());
    let observed = stand.database.watch_query(r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#, rows);
    bound(observed.wait_for_initial_load()).await.unwrap();
    stand.settle().await;
    assert_eq!(stand.requests_with("userMoments", "userId", "u1"), 1);

    stand.overflow_cable(&token, json!({"area":"REACTION", "kind":"UPDATED", "entityId":"mo1", "parentId":null, "chat":null, "message":null}));
    assert!(stand.cable_close_codes(&token).contains(&1013), "the input mailbox must overflow");

    let token = stand.next_subscription().await;
    let gap = bound(events.pull_next()).await;
    assert!(matches!(gap, Some(ReadValue::Gap)), "the unknown loss must produce a gap: {gap:?}");
    stand.settle().await;
    assert_eq!(stand.requests_with("userMoments", "userId", "u1"), 2, "the observed window rereads once after the loss");
    stand.push(&token, json!({"area":"REACTION", "kind":"UPDATED", "entityId":"mo1"}));
    let next = bound(events.pull_next()).await;
    assert!(matches!(next, Some(ReadValue::Event { ref id, position: 1, .. }) if id == "mo1"), "events continue after the gap: {next:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_dropped_stale_survives_crash() {
    use std::sync::Arc;
    use crate::ffi::selftest::rig_tests::bound;
    use crate::ffi::tests::session_guards_tests::WatchedHeld;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let image = directory.path().join("crash.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.answer("userMoments", page());
    stand.sign_in().await;
    let (events, token) = stand.events("Moment").await;
    let epoch = stand.database.session_epoch();
    let params = json!({"userId":"u1"});
    let load = stand.database.begin_load(epoch, "Moment".into(), Some("by_user".into()), params.to_string()).unwrap();
    bound(load.result()).await.unwrap();
    drop(load);
    stand.settle().await;
    let spec = stand.database.db.spec("Moment").unwrap();
    let scope = spec.selection_key("by_user", params.as_object().unwrap()).unwrap();
    let key = crate::store::window_key(scope.as_str(), crate::schema::PageDirection::Forward);
    assert!(stand.database.db.window_freshness(&key).unwrap().unwrap().stale_since.is_none());

    stand.drop_cable(&token);
    stand.settle().await;
    stand.database.set_online(false).unwrap();
    stand.database.set_online(true).unwrap();
    let _token = stand.next_subscription().await;
    assert!(matches!(bound(events.pull_next()).await, Some(crate::ffi::ReadValue::Gap)));
    stand.settle().await;
    let stale = stand.database.db.window_freshness(&key).unwrap().unwrap().stale_since;
    assert_eq!(stale, Some(3), "the first loss must commit stale debt");
    stand.database.set_online(false).unwrap();
    stand.settle().await;
    let second_stale = stand.database.db.window_freshness(&key).unwrap().unwrap().stale_since;
    assert_eq!(second_stale, Some(4), "the second loss must commit a later stale mark");
    for suffix in ["", "-wal"] {
        let source = format!("{}{suffix}", path.display());
        let target = format!("{}{suffix}", image.display());
        if std::path::Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }
    let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials, None, false).await;
    reopened.settle().await;
    assert_eq!(reopened.database.db.window_freshness(&key).unwrap().unwrap().stale_since, second_stale);
    reopened.answer("userMoments", page());
    let rows = Arc::new(crate::ffi::tape::Rows::default());
    let observed = reopened.database.watch_query(r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#, rows);
    reopened.database.set_online(true).unwrap();
    bound(observed.wait_for_initial_load()).await.unwrap();
    reopened.settle().await;
    assert_eq!(reopened.requests_with("userMoments", "userId", "u1"), 1);
    bound(reopened.database.close_database()).await.unwrap();
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_gap_debt_survives_crash_before_ready() {
    use std::sync::Arc;
    use crate::ffi::selftest::rig_tests::bound;
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::tape::Rows;

    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let first_image = directory.path().join("first.sqlite");
    let second_image = directory.path().join("second.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(live.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.answer("userMoments", page());
    stand.sign_in().await;
    let (_events, token) = stand.events("Moment").await;
    let query = r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#;
    let observed = stand.database.watch_query(query, Arc::new(Rows::default()));
    bound(observed.wait_for_initial_load()).await.unwrap();
    stand.settle().await;
    assert_eq!(stand.requests_with("userMoments", "userId", "u1"), 1);

    stand.hold_cable_confirmations();
    stand.drop_cable(&token);
    let _pending = stand.next_subscription().await;
    stand.settle().await;
    assert_eq!(stand.cable_connects(), 2, "the new subscription must wait for confirmation");
    for suffix in ["", "-wal"] {
        let source = format!("{}{suffix}", live.display());
        let target = format!("{}{suffix}", first_image.display());
        if std::path::Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }

    let reopened = Stand::prepare_online(Some(first_image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    reopened.answer("userMoments", page());
    reopened.database.set_online(true).unwrap();
    let (_events, _token) = reopened.events("Moment").await;
    let observed = reopened.database.watch_query(query, Arc::new(Rows::default()));
    bound(observed.wait_for_initial_load()).await.unwrap();
    reopened.settle().await;
    assert_eq!(reopened.requests_with("userMoments", "userId", "u1"), 1, "the fresh window must reread the debt after the crash");
    for suffix in ["", "-wal"] {
        let source = format!("{}{suffix}", first_image.display());
        let target = format!("{}{suffix}", second_image.display());
        if std::path::Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }
    let again = Stand::prepare_online(Some(second_image.to_string_lossy().into_owned()), credentials, None, false).await;
    again.answer("userMoments", page());
    again.database.set_online(true).unwrap();
    let (_events, _token) = again.events("Moment").await;
    let observed = again.database.watch_query(query, Arc::new(Rows::default()));
    bound(observed.wait_for_initial_load()).await.unwrap();
    again.settle().await;
    assert_eq!(again.requests_with("userMoments", "userId", "u1"), 0, "a landed reread clears the debt");
    bound(again.database.close_database()).await.unwrap();
    bound(reopened.database.close_database()).await.unwrap();
    bound(stand.database.close_database()).await.unwrap();
}
