//! Owner-level tests of the `session` machine: a refused write of the pair or of the landing (R14, R17), `Release` and `Cancel` of a queued call (R16) and the retry wait cut by the network (R18).

use super::*;

fn phase(twin: &Twin) -> &SessionPhase { twin.det.state(twin.addr).expect("the owner state").phase() }

fn request_id(twin: &Twin) -> u64 {
    match phase(twin) {
        SessionPhase::Persisting { pending, .. } | SessionPhase::Calling { pending, .. } | SessionPhase::Landing { pending, .. } => pending.request_id,
        _ => 0,
    }
}

fn batch(twin: &Twin) -> u64 {
    match phase(twin) {
        SessionPhase::Persisting { batch, .. } | SessionPhase::Landing { batch, .. } => *batch,
        _ => 0,
    }
}

fn kind(twin: &Twin) -> &'static str {
    match phase(twin) {
        SessionPhase::Persisting { .. } => "Persisting",
        SessionPhase::Landing { .. } => "Landing",
        SessionPhase::Calling { phase: owner::AttemptPhase::InFlight(_), .. } => "InFlight",
        SessionPhase::Calling { phase: owner::AttemptPhase::WaitingRetry, .. } => "WaitingRetry",
        SessionPhase::Calling { phase: owner::AttemptPhase::WaitingNetwork, .. } => "WaitingNetwork",
        SessionPhase::Idle => "Idle",
        SessionPhase::Sealing { .. } => "Sealing",
        _ => "Other",
    }
}

fn post(twin: &mut Twin, input: SessionInput) -> String {
    twin.det.post(twin.addr, EpochStamp::Control, input);
    twin.det.run_until_quiet(100).expect("the owner is quiet");
    twin.raw_text = format!("{:?}", take(&twin.log));
    twin.raw_text.clone()
}

fn timers_set(twin: &mut Twin, input: Input) -> usize {
    let before = twin.det.trace().len();
    twin.feed(input);
    twin.det.trace()[before..].iter().filter(|entry| entry.effects.contains("Schedule {") || entry.effects.contains("Backoff {")).count()
}

fn answers_heard(twin: &Twin) -> usize { twin.raw_text.matches("Answer { author").count() }

fn mutate_persisting() -> Twin {
    let mut twin = Twin::new();
    twin.feed(mutate(7, 11));
    finish_seal(&mut twin);
    twin
}

#[test]
fn sess_persisting_storage_retried() {
    for refusals in 1..=5 {
        let mut twin = mutate_persisting();
        let pair = request_id(&twin);
        for _ in 0..refusals {
            let timers = timers_set(&mut twin, rejected(Reason::Storage));
            assert_eq!((kind(&twin), answers_heard(&twin), timers, request_id(&twin)), ("Persisting", 0, 1, pair), "refusals={refusals}: {}", twin.raw_text);
            twin.feed(Input::Tick);
            assert_eq!((kind(&twin), twin.raw_text.matches("CommitPending").count()), ("Persisting", 1), "the same pair is written again: {}", twin.raw_text);
        }
        twin.feed(committed());
        assert_eq!((kind(&twin), twin.raw_text.matches("HttpRequest").count(), answers_heard(&twin), request_id(&twin)), ("InFlight", 1, 0, pair), "refusals={refusals}: {}", twin.raw_text);
    }
}

#[test]
fn sess_persisting_rejected_ended() {
    for reason in [Reason::NoSession, Reason::Closed] {
        let mut twin = mutate_persisting();
        twin.feed(rejected(reason));
        assert_eq!((kind(&twin), answers_heard(&twin)), ("Persisting", 0), "{reason:?}: {}", twin.raw_text);
    }
}

#[test]
fn sess_persisting_tick() {
    let mut twin = mutate_persisting();
    twin.feed(rejected(Reason::Storage));
    let first = batch(&twin);
    twin.feed(Input::Tick);
    assert_eq!((kind(&twin), twin.raw_text.matches("CommitPending").count()), ("Persisting", 1), "{}", twin.raw_text);
    assert_eq!(batch(&twin), first, "the same batch of the pair goes again");
}

#[test]
fn sess_persisting_tick_stray() {
    let mut twin = mutate_persisting();
    twin.feed(Input::Tick);
    assert_eq!((kind(&twin), twin.raw_text.as_str()), ("Persisting", "[]"), "a batch is in flight: the Tick is foreign");
}

#[test]
fn sess_landing_storage_retried() {
    for refusals in 1..=5 {
        let mut twin = super::session_owner_release_tests::rejected_landing(1);
        for _ in 0..refusals {
            let before = batch(&twin);
            let timers = timers_set(&mut twin, rejected(Reason::Storage));
            assert_eq!((kind(&twin), answers_heard(&twin), timers), ("Landing", 0, 1), "refusals={refusals}: {}", twin.raw_text);
            twin.feed(Input::Tick);
            assert_eq!((kind(&twin), twin.raw_text.matches("CommitLanding").count()), ("Landing", 1), "{}", twin.raw_text);
            assert_ne!(batch(&twin), before, "the landing goes again with a new batch");
        }
        twin.feed(committed());
        assert_eq!((kind(&twin), answers_heard(&twin), twin.raw_text.matches("Answer { author: 7, req_id: 11").count()), ("Idle", 1, 1), "the author hears the outcome once: {}", twin.raw_text);
        assert_eq!(twin.det.state(twin.addr).and_then(|state| state.credential().map(|credential| credential.sequence)), Some(1));
    }
}

#[test]
fn sess_landing_invalid() {
    let mut twin = super::session_owner_release_tests::rejected_landing(1);
    let at = batch(&twin);
    let text = post(&mut twin, SessionInput::Rejected(Rejected { batch: at, reason: RejectReason::Invalid("bad".into()) }));
    assert_eq!(text.matches("Answer { author: 7, req_id: 11").count(), 1, "the server outcome reaches the author: {text}");
    assert!(format!("{:?}", phase(&twin)).contains("Invalid"), "the pair stays with the reason Invalid: {:?}", phase(&twin));
    assert_eq!(text.matches("CommitLanding").count(), 0, "no repeat of the write");
}

#[test]
fn sess_landing_rejected_ended() {
    for reason in [Reason::NoSession, Reason::Closed] {
        let mut twin = super::session_owner_release_tests::rejected_landing(1);
        twin.feed(rejected(reason));
        assert_eq!((kind(&twin), answers_heard(&twin)), ("Landing", 0), "{reason:?}: {}", twin.raw_text);
    }
}

#[test]
fn sess_landing_tick() {
    let mut twin = super::session_owner_release_tests::rejected_landing(1);
    twin.feed(rejected(Reason::Storage));
    twin.feed(Input::Tick);
    assert_eq!((kind(&twin), twin.raw_text.matches("CommitLanding").count()), ("Landing", 1), "{}", twin.raw_text);
}

#[test]
fn sess_landing_tick_stray() {
    let mut twin = super::session_owner_release_tests::rejected_landing(1);
    twin.feed(Input::Tick);
    assert_eq!((kind(&twin), twin.raw_text.as_str()), ("Landing", "[]"), "a batch is in flight: the Tick is foreign");
}

#[test]
fn sess_release_queued() {
    let mut twin = persisting();
    twin.feed(mutate(7, 12));
    twin.feed(Input::Release { req_id: 12 });
    assert!(twin.raw_text.contains("NotSent") && twin.raw_text.matches("Answer { author: 7, req_id: 12").count() == 1, "{}", twin.raw_text);
    assert_eq!((twin.det.state(twin.addr).map(SessionState::queued), kind(&twin)), (Some(0), "Persisting"), "the started request goes on");
}

#[test]
fn sess_release_sealing() {
    let mut twin = Twin::new();
    twin.feed(mutate(7, 11));
    let SessionPhase::Sealing { seal_id, .. } = phase(&twin) else { panic!("sealing") };
    let seal_id = *seal_id;
    let text = post(&mut twin, SessionInput::Release { req_id: 11 });
    assert!(text.contains("NotSent") && text.matches("Answer { author: 7, req_id: 11").count() == 1, "no pair was created: {text}");
    assert_eq!(kind(&twin), "Idle");
    let late = post(&mut twin, SessionInput::Sealed { seal_id, size: 1 });
    assert_eq!((late.matches("CommitPending").count(), kind(&twin)), (0, "Idle"), "the late Sealed is foreign: {late}");
}

#[test]
fn sess_release_pair_started() {
    for mut twin in [mutate_persisting(), calling_mutate()] {
        let pair = request_id(&twin);
        twin.feed(Input::Release { req_id: pair });
        assert!(twin.raw_text.contains("Dropped") && twin.raw_text.matches("Answer { author: 7").count() == 1, "the author hears: unknown outcome: {}", twin.raw_text);
        assert!(format!("{:?}", phase(&twin)).contains("Detached"), "the pair goes on without the author");
    }
}

#[test]
fn sess_release_started_stray() {
    for mut twin in [mutate_persisting(), calling_mutate()] {
        let before = kind(&twin);
        twin.feed(Input::Release { req_id: 99 });
        assert_eq!((kind(&twin), twin.raw_text.as_str()), (before, "[]"));
    }
    let mut sealing = Twin::new();
    sealing.feed(mutate(7, 11));
    let text = post(&mut sealing, SessionInput::Release { req_id: 99 });
    assert_eq!((kind(&sealing), text.as_str()), ("Sealing", "[]"));
}

#[test]
fn sess_calling_offline_retry_waits_network() {
    let mut twin = calling();
    let input = twin.http_error(true, false);
    twin.feed(input);
    assert_eq!(kind(&twin), "WaitingRetry");
    twin.feed(Input::Online(false));
    assert_eq!((kind(&twin), twin.raw_text.as_str()), ("WaitingNetwork", "[]"), "the wait is cut by the network, no request");
}

#[test]
fn sess_calling_tick_offline() {
    let mut twin = calling();
    let input = twin.http_error(true, false);
    twin.feed(input);
    twin.feed(Input::Online(false));
    twin.feed(Input::Tick);
    assert_eq!((kind(&twin), twin.raw_text.matches("HttpRequest").count()), ("WaitingNetwork", 0), "the used timer sends nothing offline: {}", twin.raw_text);
    twin.feed(Input::Online(true));
    assert_eq!((kind(&twin), twin.raw_text.matches("HttpRequest").count()), ("InFlight", 1), "the pair goes at once when the network is back: {}", twin.raw_text);
}

#[test]
fn sess_prop_retry_survives_offline() {
    let orders: [&[Input]; 4] = [
        &[Input::Online(false), Input::Online(true)],
        &[Input::Online(false), Input::Tick, Input::Online(true)],
        &[Input::Online(false), Input::Online(false), Input::Tick, Input::Online(true)],
        &[Input::Online(false), Input::Online(true), Input::Tick],
    ];
    for (at, order) in orders.iter().enumerate() {
        let mut twin = calling();
        let input = twin.http_error(true, false);
    twin.feed(input);
        let mut requests = 0;
        for input in *order {
            twin.feed(input.clone());
            requests += twin.raw_text.matches("HttpRequest").count();
        }
        assert_eq!((kind(&twin), requests), ("InFlight", 1), "order {at}: {order:?}");
    }
}
