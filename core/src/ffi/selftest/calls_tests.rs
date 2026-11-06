use std::sync::Arc;

use serde_json::json;
use tokio::task::JoinHandle;

use super::rig_tests::{bound, Fault, Held, Rig, BOUND};
use crate::ffi::lifecycle_tests::Rows;
use crate::ffi::{Database, DbError};

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
                Call::Run => database.run(database.session_epoch(), "MyProfile".into(), "{}".into()).await.map(|_| ()),
                Call::RefreshList => database.refresh(database.session_epoch(), "User".into(), "all".into(), "{}".into()).await,
                Call::RefreshProfile => database.refresh(database.session_epoch(), "User".into(), "profile".into(), "{}".into()).await,
                Call::LoadAll => database.load_all(database.session_epoch(), "User".into(), "all".into(), "{}".into(), true).await,
                Call::Node => database.node(database.session_epoch(), "User".into(), "node".into(), "user".into(), true).await.map(|_| ()),
                Call::SignIn => database.session_call("login_link".into(), json!({"account":"B"}).to_string()).await.map(|_| ()),
            }
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    SessionReset,
    Close,
}

const TRANSITIONS: [Transition; 2] = [Transition::SessionReset, Transition::Close];

impl Transition {
    async fn apply(self, database: &Database) {
        match self {
            Transition::SessionReset => database.session_reset().await.unwrap(),
            Transition::Close => database.close_now(),
        }
    }
}

/// C10, G-02, G-09: every explicit call is in flight at the server when reset, sessionReset or close arrives. The call ends within the bound, the late answer (success or sealed refusal) writes nothing into held handles, and the next request of the same key reads the next account.
async fn every_explicit_call_survives_every_transition_with_a_late_answer(refusal: bool) {
    let runs: Vec<_> = CALLS.into_iter().flat_map(|call| TRANSITIONS.into_iter().map(move |transition| tokio::spawn(late_answer_case(call, transition, refusal)))).collect();
    for run in runs {
        run.await.expect("a case did not panic");
    }
}

async fn late_answer_case(call: Call, transition: Transition, refusal: bool) {
    let case = format!("{call:?} x {transition:?} x refusal={refusal}");
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    rig.settle().await;
    let gate = if refusal { rig.world.hold_then(call.operation(), Fault::Refuse { code: "UNAUTHENTICATED", confirmed: false }) } else { rig.world.hold_next(call.operation()) };
    gate.as_account("late");
    let mut running = call.start(rig.database.clone());
    let arrival_gate = gate.clone();
    let arrival = tokio::spawn(async move { crate::test_wait::within_secs(BOUND, arrival_gate.entered()).await });
    tokio::select! {
        result = arrival => if result.is_err() {
            let outcome = if running.is_finished() { Some((&mut running).await) } else { None };
            panic!("{case}: the request never reached the transport; call: {outcome:?}; requests: {:?}", rig.world.log());
        },
        result = &mut running => panic!("{case}: the call ended before reaching the transport: {result:?}; requests: {:?}", rig.world.log()),
    }
    rig.world.set_account("B");

    transition.apply(&rig.database).await;

    if matches!(transition, Transition::Close) {
        gate.release();
        let result = tokio::time::timeout(std::time::Duration::from_secs(BOUND), running).await.unwrap_or_else(|_| panic!("{case}: the call outlived the close")).unwrap();
        assert!(result.is_err(), "{case}: the call ended the close with {result:?}");
        return;
    }
    rig.login_ios("B").await;
    let result = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(result.is_ok(), "{case}: the next read of the same key failed with {result:?}");
    gate.release();
    let late = bound(running).await.unwrap();
    rig.settle().await;
    held.follow(&rig.database);
    held.shows("B").await;
    assert!(held.never_showed("late"), "{case}: the late answer ({late:?}) reached a held handle");
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await.unwrap_or_else(|error| panic!("{case}: the read after the late answer failed with {error:?}"));
    held.lists_show("B").await;
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn every_explicit_call_survives_every_transition_with_a_late_answer_that_succeeds() {
    every_explicit_call_survives_every_transition_with_a_late_answer(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_explicit_call_survives_every_transition_with_a_late_sealed_refusal() {
    every_explicit_call_survives_every_transition_with_a_late_answer(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_session_answer_without_restored_call_returns_none() {
    let rig = Rig::encrypted().await;
    assert_eq!(bound(rig.database.interrupted_session_answer()).await.unwrap(), None);
    rig.database.close_database().await.unwrap();
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

/// C11: a held list, a window and an initial-load wait end at close with nothing delivered after it.
#[tokio::test(flavor = "multi_thread")]
async fn close_ends_every_pending_stream_wait_of_a_held_handle() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    let gate = rig.world.hold_next("Users");
    let _late = { let database = rig.database.clone(); let epoch = database.session_epoch(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "all".into(), "{}".into()).await }) };
    gate.entered().await;
    let pending = rig.database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, Arc::new(Rows::default()));
    let waits = [tokio::spawn(async move { pending.wait_for_initial_load().await })];
    rig.database.close_now();

    for wait in waits {
        assert!(bound(wait).await.unwrap().is_err(), "a wait for the initial load outlived the close");
    }
    assert!(held.rows.ended().iter().all(|end| *end == crate::ffi::ReadEnd::Closed) && held.rows.ended().len() == 1, "the held list ends once at the close: {:?}", held.rows.ended());
    rig.database.close_now();
}
