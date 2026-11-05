use serde_json::json;

use super::rig_tests::{bound, Fault, Held, Rig};
use crate::ffi::lifecycle_tests::named;
use crate::ffi::{Database, DbError, SessionCode};
use crate::test_wait::yield_until;

fn session_code(error: DbError) -> SessionCode {
    let DbError::Session { code, .. } = error else { panic!("expected a session error, got {error:?}") };
    code
}

async fn ends(database: &Database) { bound(crate::ffi::settled(database)).await.unwrap(); }

/// C05, C07, R-5, R-8: the app opens its handles signed out, then logs in. Both login orders end with every handle showing the account.
#[tokio::test(flavor = "multi_thread")]
async fn app_login_in_the_ios_and_android_order_fills_the_handles_held_since_launch() {
    for android in [false, true] {
        let rig = Rig::encrypted().await;
        let held = Held::open(&rig.database);
        if android { rig.login_android("A").await } else { rig.login_ios("A").await }
        ends(&rig.database).await;
        held.follow(&rig.database);
        held.shows("A").await;
        held.lists_show("A").await;
        assert!(rig.world.count("MyProfile") >= 1, "android={android}: the profile was never loaded");
        assert_eq!(rig.world.count("SessionProfile"), usize::from(!android), "android={android}");
        rig.database.close_now();
    }
}

/// C06, R-6: the iOS exit with a held singleton and a wait for the initial load that the reset left pending. Every call returns, the wait ends, no read follows the exit and account A never reaches the next login.
#[tokio::test(flavor = "multi_thread")]
async fn app_logout_ios_order_ends_a_pending_initial_wait_and_hides_the_account_from_the_next_login() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    bound(held.query().wait_for_initial_load()).await.unwrap();
    let gate = rig.world.hold_next("Users");
    let _late = { let database = rig.database.clone(); let epoch = database.session_epoch(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "all".into(), "{}".into()).await }) };
    gate.entered().await;
    let waiting = held.query();
    let pending = tokio::spawn(async move { waiting.wait_for_initial_load().await });

    rig.exit(&held, true, false).await;
    let marks = held.deliveries();

    drop(bound(pending).await.unwrap());
    assert!(held.entities_empty(), "the held singleton and entity still show a row after the exit");
    rig.world.forbid(true);
    ends(&rig.database).await;
    assert!(rig.world.violations().is_empty(), "a read followed the exit: {:?}", rig.world.violations());
    rig.world.forbid(false);
    gate.release();
    rig.login_ios("B").await;
    ends(&rig.database).await;
    held.follow(&rig.database);
    held.shows("B").await;
    assert!(held.quiet_since(marks, "A"), "account A reached a held stream after the exit");
    rig.database.close_now();
}

/// C08, R-9: the Android exit cancels the collectors after the session reset. Then the next login reads through fresh handles.
#[tokio::test(flavor = "multi_thread")]
async fn app_logout_android_order_cancels_the_streams_and_the_next_login_reads_again() {
    let rig = Rig::encrypted().await;
    rig.login_android("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;

    rig.exit(&held, true, true).await;

    assert!(held.entities_empty());
    let before = rig.world.count("Users");
    rig.login_android("B").await;
    let fresh = Held::open(&rig.database);
    ends(&rig.database).await;
    fresh.shows("B").await;
    fresh.lists_show("B").await;
    held.follow(&rig.database);
    held.shows("B").await;
    assert!(rig.world.count("Users") > before);
    assert!(fresh.never_showed("A"));
    rig.database.close_now();
}

/// C09, R-7: the local-only exit sends neither unregister nor logout. The server session stays and no request follows.
#[tokio::test(flavor = "multi_thread")]
async fn app_logout_local_only_skips_the_revoke_and_stops_every_read() {
    for cancel in [false, true] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let held = Held::open(&rig.database);
        held.shows("A").await;
        held.lists_show("A").await;

        rig.exit(&held, false, cancel).await;
        let mark = held.deliveries();

        assert!(rig.world.signed(), "cancel={cancel}: the local-only exit revoked the server session");
        assert_eq!(rig.world.count("UnregisterDevice"), 0);
        rig.world.forbid(true);
        ends(&rig.database).await;
        assert!(rig.world.violations().is_empty(), "cancel={cancel}: a read followed the local exit: {:?} log {:?}", rig.world.violations(), rig.world.log().iter().rev().take(6).collect::<Vec<_>>());
        assert!(held.entities_empty());
        assert!(held.quiet_since(mark, "A"));
        rig.database.close_now();
    }
}

/// R-9: a refused unregister stops the chain. The session, the credential and the held data stay.
#[tokio::test(flavor = "multi_thread")]
async fn app_logout_stops_when_the_unregister_is_refused() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    rig.world.fail_next("UnregisterDevice", Fault::Refuse { code: "UNAUTHENTICATED", confirmed: true });

    let refused = bound(rig.database.run(rig.database.session_epoch(), "UnregisterDevice".into(), "{}".into())).await.unwrap_err();

    assert_eq!(session_code(refused), SessionCode::Unauthenticated);
    assert!(rig.world.signed());
    assert_eq!(rig.world.count("UnregisterDevice"), 0);
    assert!(rig.database.session.as_ref().unwrap().signed_in().unwrap().is_some(), "the refusal cleared the credential");
    held.shows("A").await;
    rig.database.close_now();
}

/// C12: A, B, A with every kind of handle held. After each exit nothing of the previous account is delivered, a late answer of the old account changes nothing, the history ends on the new account.
#[tokio::test(flavor = "multi_thread")]
async fn account_switch_a_b_a_never_delivers_the_previous_account_and_drops_its_late_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    let mut previous = "A";
    for account in ["B", "A"] {
        let gate = rig.world.hold_next("Users");
        let _late = { let database = rig.database.clone(); let epoch = database.session_epoch(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "all".into(), "{}".into()).await }) };
        gate.entered().await;

        rig.exit(&held, true, false).await;
        let marks = held.deliveries();
        rig.login_ios(account).await;
        ends(&rig.database).await;
        held.follow(&rig.database);
        held.shows(account).await;
        held.lists_show(account).await;
        gate.release();
        ends(&rig.database).await;

        assert!(held.quiet_since(marks, previous), "{previous} reached a held stream while {account} logs in");
        previous = account;
    }
    rig.database.close_now();
}

/// Exit and login of the same account: the handles show it again, the reads repeat, and the server sees one more login.
#[tokio::test(flavor = "multi_thread")]
async fn logout_then_login_of_the_same_account_reads_it_again() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    let reads = rig.world.count("MyProfile");

    rig.exit(&held, true, false).await;
    assert!(held.entities_empty());
    rig.login_ios("A").await;
    ends(&rig.database).await;

    held.follow(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    assert!(rig.world.count("MyProfile") > reads, "the same account was not read again");
    rig.database.close_now();
}

/// C19, R-5: login is refused as already authenticated, the confirmed sign-out follows, the login repeats and starts the session. A refused sign-out does not repeat the login.
#[tokio::test(flavor = "multi_thread")]
async fn login_link_already_authenticated_signs_out_then_repeats_once() {
    let rig = Rig::encrypted().await;
    let body = json!({"account":"A"}).to_string();
    rig.world.fail_next("login_link", Fault::Refuse { code: "ALREADY_AUTHENTICATED", confirmed: true });
    let refused = bound(rig.database.session_call("login_link".into(), body.clone())).await.unwrap_err();
    assert_eq!(session_code(refused), SessionCode::AlreadyAuthenticated);
    assert!(!rig.world.signed());
    let logins = rig.world.log().iter().filter(|entry| entry.operation == "login_link").count();

    let logout = bound(rig.database.session_call("logout".into(), "{}".into())).await.unwrap_err();
    assert_eq!(session_code(logout), SessionCode::NoCredential);
    bound(rig.database.session_call("login_link".into(), body.clone())).await.unwrap();
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();

    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "login_link").count(), logins + 1);
    assert!(rig.world.signed());

    rig.world.fail_next("logout", Fault::Refuse { code: "SESSION_ENDED", confirmed: true });
    let refused = bound(rig.database.session_call("logout".into(), "{}".into())).await.unwrap_err();
    assert_eq!(session_code(refused), SessionCode::SessionEnded);
    assert!(rig.world.signed(), "a refused sign-out ended the server session");
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "login_link").count(), logins + 1, "a refused sign-out repeated the login");
    rig.database.close_now();
}

/// S5 and G-06: a list opened after the exit belongs to the epoch of no account: its wait returns at once and the next login ends it with one outcome, and a list opened after the login loads and shows B.
#[tokio::test(flavor = "multi_thread")]
async fn a_list_opened_after_the_exit_loads_after_the_next_login_and_its_wait_ends() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    rig.exit(&held, true, true).await;
    let early = std::sync::Arc::new(crate::ffi::stream::tape::Rows::default());
    let late = rig.database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, early.clone());
    let waiting = late.clone();
    let pending = tokio::spawn(async move { waiting.wait_for_initial_load().await });

    rig.login_ios("B").await;

    let waited = bound(pending).await.unwrap();
    assert!(waited.is_ok() || matches!(waited, Err(DbError::Interrupted { .. })), "the wait of a list of the anonymous epoch ended as {waited:?}");
    yield_until("the list of the anonymous epoch ended", || !early.ended().is_empty()).await;
    assert_eq!(early.ended(), vec![crate::ffi::StreamEnd::SessionEnded], "the login ends a list that no account owned, with one outcome");
    let rows = std::sync::Arc::new(crate::ffi::stream::tape::Rows::default());
    let fresh = rig.database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, rows.clone());
    bound(fresh.wait_for_initial_load()).await.unwrap();
    yield_until("the fresh list shows B", || rows.0.lock().unwrap().iter().any(|row| named(row, "B"))).await;
    assert!(rows.0.lock().unwrap().iter().all(|row| !named(row, "A")));
    rig.database.close_now();
}
