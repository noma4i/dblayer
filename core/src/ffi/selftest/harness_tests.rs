use std::sync::Arc;

use super::rig_tests::{bound, Held, Rig, World};
use crate::ffi::lifecycle_tests::{named, Rows};
use crate::ffi::{DbError, LiveSubscription, Transport};
use crate::test_wait::yield_until;

const ALL: &str = r#"{"model":"User","relation":"all","params":{}}"#;

fn first_error(rows: &Rows) -> Option<DbError> { rows.1.lock().unwrap().iter().find_map(|change| change.error.clone()) }

fn trace(rows: &Rows) -> Vec<String> { rows.1.lock().unwrap().iter().map(|change| format!("rev={} size={} phase={:?} kind={:?} error={:?}", change.revision, change.size, change.phase, change.error_kind, change.error)).collect() }

fn never_listed(rows: &Rows, account: &str) -> bool { rows.1.lock().unwrap().iter().all(|change| change.rows.iter().all(|row| !named(row, account))) }

async fn window(rig: &Rig) -> (Arc<LiveSubscription>, Arc<Rows>) {
    let rows = Arc::new(Rows::default());
    let live = rig.database.live_query(ALL.into(), rows.clone());
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
async fn harness_reset_with_the_target_unset_sends_nothing_and_leaves_new_windows_clean() {
    for invalid in [false, true] {
        let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
        let rig = Rig::plain(a.clone(), invalid).await;
        let held = Held::open(&rig.database);
        held.shows("A").await;
        held.lists_show("A").await;
        let before = a.hits();

        rig.harness_cycle(Some(b.clone())).await;

        assert_eq!(a.hits(), before, "invalid={invalid}: work of the reset reached the transport after its target was unset");
        assert_eq!(b.hits(), 0, "invalid={invalid}: work of the reset reached the next transport before the next test asked");
        rig.database.set_listening(true).unwrap();
        let (live, rows) = window(&rig).await;
        bound(live.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("invalid={invalid}: a new window inherited {error:?}"));
        shows(&rows, "B").await;
        assert!(first_error(&rows).is_none(), "invalid={invalid}: the new window delivered {:?}; deliveries {:?}", first_error(&rows), trace(&rows));
        assert_eq!(b.count("Users"), 1, "invalid={invalid}: the window of the next test was read more than once");
        assert!(never_listed(&rows, "A"));
        for (id, lease) in [("", 1_u64), ("user", 2_u64)] {
            bound(held.entities.load_initial(id.into(), lease)).await.unwrap_or_else(|error| panic!("invalid={invalid}: the first-load wait of entity {id:?} inherited {error:?}"));
        }
        let singleton = held.log.latest(1).unwrap();
        assert!(!matches!(singleton.read, crate::ffi::EntityRead::Failed { .. }), "invalid={invalid}: the held singleton kept {:?} of the reset after the target came back", singleton.read);
        held.cancel_all();
        live.cancel();
        rig.database.shutdown();
    }
}

/// C03, G-04: a request of transport A is in flight, the target is unset, reset, settle, B is set and answers, then A answers late. The late answer reaches no window and no storage.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_answer_of_the_previous_transport_reaches_no_window_of_the_next_test() {
    let (a, b) = (World::new("A", "A-late", true), World::new("B", "B", true));
    let gate = a.hold_next("Users");
    let rig = Rig::plain(a.clone(), false).await;
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
    rig.database.shutdown();
}

/// C04, R-2, R-4: the second open is refused while the database is open. After the close the same opener opens again without a transport (iOS reopen), then with one (Android direct), and a window of the last open reads its transport.
#[tokio::test(flavor = "multi_thread")]
async fn reopening_through_the_same_opener_after_a_close_gives_a_clean_database() {
    let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
    let rig = Rig::plain(a.clone(), false).await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    assert!(rig.open_again(None).await.is_err(), "a second open succeeded while the first database was open");

    held.cancel_all();
    rig.database.shutdown();
    let local = rig.open_again(None).await.expect("the opener refused a reopen after the close");
    let rows = Arc::new(Rows::default());
    let live = local.live_query(r#"{"model":"User"}"#.into(), rows.clone());
    bound(live.wait_for_initial_load()).await.unwrap();
    assert!(rows.0.lock().unwrap().is_empty(), "the reopened database holds rows of the closed one");
    live.cancel();
    local.shutdown();

    let switch = super::rig_tests::Switchable::new(Some(b.clone()), false);
    let remote = rig.open_again(Some(switch as Arc<dyn Transport>)).await.expect("the opener refused the second reopen");
    remote.register_operations(r#"[{"name":"Users","document":"query Users { users { id name } }"}]"#.into()).unwrap();
    let rows = Arc::new(Rows::default());
    let live = remote.live_query(ALL.into(), rows.clone());
    bound(live.wait_for_initial_load()).await.unwrap();
    shows(&rows, "B").await;
    assert!(never_listed(&rows, "A"));
    live.cancel();
    remote.shutdown();
}

/// R-1 across tests: each test leaves something behind (an offline flag, a logout, a held window, a server subscription) and the harness cycle runs between them. Every cycle returns, no request of the old test reaches the next transport, and each next test reads its own transport once.
#[tokio::test(flavor = "multi_thread")]
async fn harness_cycles_survive_tests_that_leave_offline_logout_and_subscriptions_behind() {
    let leftovers = ["offline", "logout", "subscription", "held window"];
    let worlds: Vec<Arc<World>> = (0..=leftovers.len()).map(|index| World::new(&format!("T{index}"), &format!("T{index}"), true)).collect();
    let rig = Rig::plain(worlds[0].clone(), false).await;
    let mut kept = Vec::new();
    for (index, leftover) in leftovers.iter().enumerate() {
        let world = &worlds[index];
        bound(rig.database.refresh("User".into(), "profile".into(), "{}".into())).await.unwrap();
        let (live, rows) = window(&rig).await;
        bound(live.wait_for_initial_load()).await.unwrap_or_else(|error| panic!("test {index} after {:?}: {error:?}", index.checked_sub(1).map(|before| leftovers[before])));
        shows(&rows, &format!("T{index}")).await;
        assert!(first_error(&rows).is_none(), "test {index} inherited {:?}", first_error(&rows));
        assert_eq!(world.count("Users"), 1, "test {index} read its transport {} times", world.count("Users"));
        match *leftover {
            "offline" => rig.database.set_online(false).unwrap(),
            "logout" => bound(rig.database.logout()).await.unwrap(),
            "subscription" => {
                kept.push((rig.database.live_query(r#"{"model":"Notice","relation":"recent","params":{}}"#.into(), Arc::new(Rows::default())), Arc::new(Rows::default())));
                yield_until("the server subscription opened", || rig.switch.opened() > 0).await;
            }
            _ => {}
        }
        kept.push((live, rows));
        let hits: Vec<usize> = worlds.iter().map(|world| world.hits()).collect();

        rig.harness_cycle(Some(worlds[index + 1].clone())).await;
        rig.database.set_online(true).unwrap();

        assert_eq!(worlds.iter().map(|world| world.hits()).collect::<Vec<_>>(), hits, "the cycle after {leftover} sent a request");
    }
    for (live, _) in kept { live.cancel(); }
    rig.database.shutdown();
}

/// G-01, I20: the harness cycle releases the token of the old target exactly once, with either error the unset target throws.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_cycle_releases_the_token_of_a_held_server_subscription_exactly_once() {
    for invalid in [false, true] {
        let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
        let rig = Rig::plain(a.clone(), invalid).await;
        let rows = Arc::new(Rows::default());
        let live = rig.database.live_query(r#"{"model":"Notice","relation":"recent","params":{}}"#.into(), rows.clone());
        bound(live.wait_for_initial_load()).await.unwrap();
        yield_until("the subscription opened on A", || rig.switch.opened() == 1).await;

        rig.harness_cycle(Some(b.clone())).await;
        live.cancel();
        rig.settle().await;

        assert_eq!(rig.switch.unsubscribed(), vec!["A-0".to_string()], "invalid={invalid}: the token of the old target was not released exactly once");
        rig.database.shutdown();
    }
}

/// G-01, S1: the held server subscription meets the unset target, waits for its retry on the test clock, and reopens exactly once on the next target when the harness moves the clock.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_server_subscription_reopens_once_on_the_next_target_when_the_clock_moves() {
    for invalid in [false, true] {
        let (a, b) = (World::new("A", "A", true), World::new("B", "B", true));
        let rig = Rig::plain(a.clone(), invalid).await;
        let rows = Arc::new(Rows::default());
        let live = rig.database.live_query(r#"{"model":"Notice","relation":"recent","params":{}}"#.into(), rows.clone());
        bound(live.wait_for_initial_load()).await.unwrap();
        yield_until("the subscription opened on A", || rig.switch.opened() == 1).await;

        rig.harness_cycle(Some(b.clone())).await;

        let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async { while rig.switch.opened() < 2 { rig.clock.advance(60_000); tokio::task::yield_now().await; } }).await.is_ok();
        let mut probes = Vec::new();
        if !reopened {
            bound(rig.database.resume()).await.unwrap();
            probes.push(format!("resume -> opened {}", rig.switch.opened()));
            rig.database.set_online(false).unwrap();
            rig.database.set_online(true).unwrap();
            rig.settle().await;
            probes.push(format!("offline/online -> opened {}", rig.switch.opened()));
            rig.database.set_listening(false).unwrap();
            rig.database.set_listening(true).unwrap();
            rig.settle().await;
            probes.push(format!("listening toggle -> opened {}", rig.switch.opened()));
        }
        assert!(reopened, "invalid={invalid}: probes {probes:?}: the subscription did not reopen on the next target (subscribe attempts: {}, opened: {}, failures: {:?})", rig.switch.attempts(), rig.switch.opened(), rig.switch.failures());
        rig.settle().await;
        assert_eq!(rig.switch.opened(), 2, "invalid={invalid}: the subscription opened more than once on the next target");
        live.cancel();
        rig.database.shutdown();
    }
}
