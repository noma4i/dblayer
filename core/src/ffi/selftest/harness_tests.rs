use std::sync::Arc;

use super::rig_tests::{bound, Rig, World};
use super::rig_held::Held;
use crate::ffi::lifecycle_tests::{named, Rows};
use crate::ffi::tape::Watched;
use crate::ffi::{DbError, Transport};
use crate::test_wait::yield_until;

const ALL: &str = r#"{"model":"User","relation":"all","params":{}}"#;

fn first_error(rows: &Rows) -> Option<String> { rows.1.lock().unwrap().iter().find_map(|change| change.error.as_ref().map(ToString::to_string)) }

fn trace(rows: &Rows) -> Vec<String> { rows.1.lock().unwrap().iter().map(|change| format!("rev={} size={} phase={:?} error={:?}", change.revision, change.size, change.phase, change.error)).collect() }

fn never_listed(rows: &Rows, account: &str) -> bool { rows.1.lock().unwrap().iter().all(|change| change.rows.iter().all(|row| !named(row, account))) }

async fn window(rig: &Rig) -> (Arc<Watched>, Arc<Rows>) {
    let rows = Arc::new(Rows::default());
    let live = rig.database.watch_query(ALL, rows.clone());
    (live, rows)
}

async fn shows(rows: &Rows, account: &str) {
    yield_until(&format!("the window shows {account}"), || {
        let rows = rows.0.lock().unwrap();
        !rows.is_empty() && rows.iter().all(|row| named(row, account))
    })
    .await;
}

/// C01, C02, R-1, R-3, G-01: one openTesting, the target unset (Android: Interrupted, iOS: Invalid), reset, settle, the next target, then new windows of the same key. The reset sends nothing to either transport and the new window inherits no error.
#[tokio::test(flavor = "multi_thread")]
async fn closing_with_the_target_unset_sends_nothing_and_leaves_the_next_database_clean() {
    for invalid in [false, true] {
        let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
        let mut rig = Rig::bare(a.clone(), invalid).await;
        let held = Held::open(&rig.database);
        held.shows("A").await;
        held.lists_show("A").await;
        yield_until("the forced entity read reached A", || a.count("UserNode") >= 1).await;
        rig.settle().await;
        let before = a.hits();
        held.cancel_all();

        rig.harness_cycle(Some(b.clone())).await;

        assert_eq!(a.hits(), before, "invalid={invalid}: work of the reset reached the transport after its target was unset");
        assert_eq!(b.hits(), 0, "invalid={invalid}: work of the reset reached the next transport before the next test asked");
        let (live, rows) = window(&rig).await;
        bound(live.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("invalid={invalid}: a new window inherited {error:?}"));
        shows(&rows, "B").await;
        assert!(first_error(&rows).is_none(), "invalid={invalid}: the new window delivered {:?}; deliveries {:?}", first_error(&rows), trace(&rows));
        assert_eq!(b.count("Users"), 1, "invalid={invalid}: the window of the next test was read more than once");
        assert!(never_listed(&rows, "A"));
        live.cancel();
        rig.database.close_now();
    }
}

/// C03, G-04: a request of transport A is in flight, the target is unset, reset, settle, B is set and answers, then A answers late. The late answer reaches no window and no storage.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_answer_of_the_previous_transport_reaches_no_window_of_the_next_test() {
    let (a, b) = (World::new("A", "A-late", true), World::new("B", "B", true));
    let gate = a.hold_next("Users");
    let mut rig = Rig::bare(a.clone(), false).await;
    let (old, old_rows) = window(&rig).await;
    gate.entered().await;

    rig.harness_cycle(Some(b.clone())).await;
    let (fresh, rows) = window(&rig).await;
    bound(fresh.wait_for_initial_load()).await.unwrap();
    shows(&rows, "B").await;
    gate.release();
    rig.settle().await;

    assert!(never_listed(&rows, "A-late") && never_listed(&old_rows, "A-late"), "the late answer of A reached a window");
    let (again, again_rows) = window(&rig).await;
    bound(again.wait_for_initial_load()).await.unwrap();
    shows(&again_rows, "B").await;
    assert!(never_listed(&again_rows, "A-late"), "the late answer of A reached the storage");
    old.cancel();
    fresh.cancel();
    again.cancel();
    rig.database.close_now();
}

/// C04, R-2, R-4: the second open is refused while the database is open. After the close the same opener opens again without a transport (iOS reopen), then with one (Android direct), and a window of the last open reads its transport.
#[tokio::test(flavor = "multi_thread")]
async fn reopening_through_the_same_opener_after_a_close_gives_a_clean_database() {
    let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
    let rig = Rig::bare(a.clone(), false).await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    assert!(rig.open_again(None).await.is_err(), "a second open succeeded while the first database was open");

    held.cancel_all();
    rig.database.close_now();
    let local = rig.open_again(None).await.expect("the opener refused a reopen after the close");
    let rows = Arc::new(Rows::default());
    let live = local.watch_query(r#"{"model":"User"}"#, rows.clone());
    assert!(matches!(bound(live.wait_for_initial_load()).await, Err(DbError::Interrupted { .. })), "a database without a session shows no rows of a server model");
    assert!(rows.0.lock().unwrap().is_empty(), "the reopened database holds rows of the closed one");
    live.cancel();
    local.close_now();

    let switch = super::rig_tests::Switchable::new(Some(b.clone()), false);
    let remote = rig.open_again(Some(switch as Arc<dyn Transport>)).await.expect("the opener refused the second reopen");
    remote.register_operations(r#"[{"name":"Users","document":"query Users { users { id name } }"}]"#.into()).await.unwrap();
    remote.set_online(true).unwrap();
    let rows = Arc::new(Rows::default());
    let live = remote.watch_query(ALL, rows.clone());
    bound(live.wait_for_initial_load()).await.unwrap();
    shows(&rows, "B").await;
    assert!(never_listed(&rows, "A"));
    live.cancel();
    remote.close_now();
}

/// R-1 across tests: each test leaves something behind (an offline flag, a logout, a held window, a server subscription) and the harness cycle runs between them. Every cycle returns, no request of the old test reaches the next transport, and each next test reads its own transport once.
#[tokio::test(flavor = "multi_thread")]
async fn harness_cycles_survive_tests_that_leave_offline_logout_and_subscriptions_behind() {
    let leftovers = ["offline", "logout", "held window"];
    let worlds: Vec<Arc<World>> = (0..=leftovers.len()).map(|index| World::new(&format!("T{index}"), &format!("T{index}"), true)).collect();
    let mut rig = Rig::bare(worlds[0].clone(), false).await;
    let mut kept = Vec::new();
    for (index, leftover) in leftovers.iter().enumerate() {
        let world = &worlds[index];
        bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
        let (live, rows) = window(&rig).await;
        bound(live.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("test {index} after {:?}: {error:?}", index.checked_sub(1).map(|before| leftovers[before])));
        shows(&rows, &format!("T{index}")).await;
        assert!(first_error(&rows).is_none(), "test {index} inherited {:?}", first_error(&rows));
        assert_eq!(world.count("Users"), 1, "test {index} read its transport {} times", world.count("Users"));
        match *leftover {
            "offline" => rig.database.set_online(false).unwrap(),
            "logout" => bound(rig.database.session_reset()).await.unwrap(),
            _ => {}
        }
        kept.push((live, rows));
        let hits: Vec<usize> = worlds.iter().map(|world| world.hits()).collect();

        rig.harness_cycle(Some(worlds[index + 1].clone())).await;
        rig.database.set_online(true).unwrap();

        assert_eq!(worlds.iter().map(|world| world.hits()).collect::<Vec<_>>(), hits, "the cycle after {leftover} sent a request");
    }
    for (live, _) in kept { live.cancel(); }
    rig.database.close_now();
}
