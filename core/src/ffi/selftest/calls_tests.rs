use std::sync::Arc;

use serde_json::json;
use tokio::task::JoinHandle;

use super::rig_tests::{bound, Fault, Held, Rig, BOUND};
use crate::ffi::{Database, DbError};
use crate::test_wait::within_secs;

#[derive(Clone, Copy, Debug)]
enum Call {
    Run,
    RefreshList,
    RefreshProfile,
    LoadAll,
    Node,
    SignIn,
}

const CALLS: [Call; 6] = [Call::Run, Call::RefreshList, Call::RefreshProfile, Call::LoadAll, Call::Node, Call::SignIn];

impl Call {
    fn operation(self) -> &'static str {
        match self {
            Call::Run | Call::RefreshProfile => "MyProfile",
            Call::RefreshList | Call::LoadAll => "Users",
            Call::Node => "UserNode",
            Call::SignIn => "login_link",
        }
    }

    fn start(self, database: Arc<Database>) -> JoinHandle<Result<(), DbError>> {
        tokio::spawn(async move {
            match self {
                Call::Run => database.run("MyProfile".into(), "{}".into()).await.map(|_| ()),
                Call::RefreshList => database.refresh("User".into(), "all".into(), "{}".into()).await,
                Call::RefreshProfile => database.refresh("User".into(), "profile".into(), "{}".into()).await,
                Call::LoadAll => database.load_all("User".into(), "all".into(), "{}".into(), true).await,
                Call::Node => database.node("User".into(), "node".into(), "user".into(), true).await.map(|_| ()),
                Call::SignIn => database.session_call("login_link".into(), json!({"account":"B"}).to_string()).await.map(|_| ()),
            }
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    Reset,
    SessionReset,
    Close,
}

const TRANSITIONS: [Transition; 3] = [Transition::Reset, Transition::SessionReset, Transition::Close];

impl Transition {
    async fn apply(self, database: &Database) {
        match self {
            Transition::Reset => bound(database.reset_for_testing()).await.unwrap(),
            Transition::SessionReset => database.session_reset().await.unwrap(),
            Transition::Close => database.close_now(),
        }
    }
}

/// C10, G-02, G-09: every explicit call is in flight at the server when reset, sessionReset or close arrives. The call ends within the bound, the late answer (success or sealed refusal) writes nothing into held handles, and the next request of the same key reads the next account.
#[tokio::test(flavor = "multi_thread")]
async fn every_explicit_call_survives_every_transition_with_a_late_answer() {
    for call in CALLS {
        for transition in TRANSITIONS {
            for refusal in [false, true] {
                let case = format!("{call:?} x {transition:?} x refusal={refusal}");
                let rig = Rig::encrypted().await;
                rig.login_ios("A").await;
                let held = Held::open(&rig.database);
                held.shows("A").await;
                held.lists_show("A").await;
                rig.settle().await;
                let gate = if refusal { rig.world.hold_then(call.operation(), Fault::Refuse { code: "UNAUTHENTICATED", confirmed: false }) } else { rig.world.hold_next(call.operation()) };
                gate.as_account("late");
                let running = call.start(rig.database.clone());
                gate.entered_for(&case).await;
                rig.world.set_account("B");

                transition.apply(&rig.database).await;

                if matches!(transition, Transition::Close) {
                    gate.release();
                    let result = tokio::time::timeout(std::time::Duration::from_secs(BOUND), running).await.unwrap_or_else(|_| panic!("{case}: the call outlived the close")).unwrap();
                    assert!(result.is_err(), "{case}: the call ended the close with {result:?}");
                    continue;
                }
                match transition {
                    Transition::Reset => {}
                    _ => rig.login_ios("B").await,
                }
                let result = bound(rig.database.refresh("User".into(), "all".into(), "{}".into())).await;
                assert!(result.is_ok(), "{case}: the next read of the same key failed with {result:?}");
                gate.release();
                let late = bound(running).await.unwrap();
                rig.settle().await;
                held.shows("B").await;
                assert!(held.never_showed("late"), "{case}: the late answer ({late:?}) reached a held handle");
                bound(rig.database.refresh("User".into(), "all".into(), "{}".into())).await.unwrap_or_else(|error| panic!("{case}: the read after the late answer failed with {error:?}"));
                held.lists_show("B").await;
                rig.database.close_now();
            }
        }
    }
}

/// C11, I24: a call that the server never answers ends when the database closes, without the answer, and a second close returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_that_is_never_answered_ends_when_the_database_closes() {
    for call in CALLS {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        rig.settle().await;
        let gate = rig.world.hold_next(call.operation());
        let running = call.start(rig.database.clone());
        gate.entered_for(&format!("{call:?}")).await;

        rig.database.close_now();

        let result = tokio::time::timeout(std::time::Duration::from_secs(BOUND), running).await.unwrap_or_else(|_| panic!("{call:?}: the call outlived the close")).unwrap();
        assert!(result.is_err(), "{call:?}: the call ended the close with {result:?}");
        rig.database.close_now();
    }
}

/// C11: a held list, a window, a changes feed and an initial-load wait end at close with nothing delivered after it.
#[tokio::test(flavor = "multi_thread")]
async fn close_ends_every_pending_stream_wait_of_a_held_handle() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    let gate = rig.world.hold_next("Users");
    bound(rig.database.reset_for_testing()).await.unwrap();
    gate.entered().await;
    let (query, window) = (held.query.clone(), held.window.clone());
    let waits = [tokio::spawn(async move { query.wait_for_initial_load().await }), tokio::spawn(async move { window.wait_for_initial_load().await })];
    let changes = held.changes.clone();
    let next = tokio::spawn(async move { within_secs(BOUND, changes.next()).await });

    rig.database.close_now();

    for wait in waits {
        assert!(bound(wait).await.unwrap().is_err(), "a wait for the initial load outlived the close");
    }
    let mut last = bound(next).await.unwrap();
    while last.is_some() { last = within_secs(BOUND, held.changes.next()).await; }
    assert!(within_secs(BOUND, held.changes.next()).await.is_none(), "the changes feed delivered after the close");
    rig.database.close_now();
}
