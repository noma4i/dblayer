//! Owner-level tests of the `session` machine: the server outcome kept by `Closing` in `Landing` (D-346), the write count kept by `EpochEnded` (D-347), one outcome per pair at `Release` in `PendingHeld` (D-348).

use super::*;

fn post(twin: &mut Twin, input: SessionInput) -> (Vec<SessionEffect>, Vec<u64>) {
    let traced = twin.pauses.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len();
    twin.det.post(twin.addr, EpochStamp::Control, input);
    twin.det.run_until_quiet(100).expect("the owner is quiet");
    let pauses = twin.pauses.lock().unwrap_or_else(std::sync::PoisonError::into_inner)[traced..].to_vec();
    (take(&twin.log), pauses)
}

fn written_batch(effects: &[SessionEffect]) -> Option<u64> {
    effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } | SessionEffect::CommitLanding { batch, .. } => Some(*batch), _ => None })
}

fn refuse(twin: &mut Twin, batch: u64) -> (Vec<SessionEffect>, Vec<u64>) {
    post(twin, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage("disk".into()) }))
}

fn last_written_batch(twin: &Twin) -> u64 {
    twin.history.iter().rev().find_map(|effect| match effect { RefEffect::CommitPending { batch, .. } | RefEffect::CommitLanding { batch, .. } => Some(*batch), _ => None }).expect("the owner wrote a batch")
}

fn closed_answer(effects: &[RefEffect]) -> bool {
    effects.iter().any(|effect| matches!(effect, RefEffect::Answer { outcome: Outcome::Error(_, Fail::Closed, _), .. }))
}

/// The server answered, `Closing` comes before the landing is written: the call and the author get the server outcome, not `Closed`.
#[test]
fn sess_landing_closing_keeps_the_server_outcome() {
    let mut mutate_ok = calling_mutate();
    let input = mutate_ok.response(1);
    mutate_ok.feed(input);
    let out = mutate_ok.feed(Input::Closing);
    assert!(out.contains(&answer(7, 11, Outcome::Response("{}".into(), true))) && !closed_answer(&out), "author, answer Ok: {out:?}");
    assert!(out.contains(&RefEffect::Drained), "author, answer Ok: {out:?}");

    let mut mutate_refused = calling_mutate();
    let input = mutate_refused.http_error(false, false);
    mutate_refused.feed(input);
    let out = mutate_refused.feed(Input::Closing);
    assert!(out.contains(&answer(7, 11, retained(Fail::Http))) && !closed_answer(&out), "author, server refusal: {out:?}");

    let mut call_ok = landing(1);
    call_ok.feed(Input::Closing);
    assert_eq!(call_ok.replies, vec![(1, Ok("{}".into()))], "call, answer Ok");

    let mut call_refused = calling();
    let input = Input::Response { request_id: call_refused.rid(), attempt_id: call_refused.aid(), body: "{}".into(), sequence: Some(1), refusal: Some(SessionCode::Unauthenticated) };
    call_refused.feed(input);
    call_refused.feed(Input::Closing);
    assert_eq!(call_refused.replies, vec![(1, Err(Fail::Unauthorized))], "call, server refusal");
}

/// Two `Storage` refusals of one write, `EpochEnded` that does not end the phase, the third refusal: the pause keeps growing and no second batch is in flight.
#[test]
fn sess_epoch_ended_keeps_the_write_count() {
    let cases: [(&str, fn() -> Twin); 2] = [("Persisting", || { let mut twin = Twin::new(); twin.feed(mutate(7, 11)); finish_seal(&mut twin); twin }), ("Landing", || super::session_owner_release_tests::rejected_landing(1))];
    for (phase, start) in cases {
        let mut twin = start();
        let mut batch = last_written_batch(&twin);
        assert_eq!(refuse(&mut twin, batch).1, vec![1], "{phase}: first refusal");
        batch = written_batch(&post(&mut twin, SessionInput::Tick).0).expect("the write repeats");
        assert_eq!(refuse(&mut twin, batch).1, vec![2], "{phase}: second refusal");
        batch = written_batch(&post(&mut twin, SessionInput::Tick).0).expect("the write repeats");
        let during = post(&mut twin, SessionInput::EpochEnded(EpochEnded { epoch: EPOCH, cause: Cause::IdentityChanged })).0;
        let stray = post(&mut twin, SessionInput::Tick).0;
        let third = refuse(&mut twin, batch).1;
        assert!(during.is_empty() && stray.is_empty(), "{phase}: the phase goes on, one batch in flight, a Tick is foreign: {during:?} {stray:?}");
        assert_eq!(third, vec![3], "{phase}: the pause grows over the end of an epoch that does not end the phase");
    }
}

/// The author heard `Dropped` when the pair was held; `Release` of that pair sends it nothing more.
#[test]
fn sess_held_release_no_second_answer() {
    let mut twin = held();
    let out = twin.feed(Input::Release { req_id: 11 });
    assert!(matches!(&out[..], [RefEffect::HttpRequest { .. }]), "the pair is sent again without an author: {out:?}");
    let heard = twin.history.iter().filter(|effect| matches!(effect, RefEffect::Answer { author: 7, req_id: 11, .. })).count();
    assert_eq!(heard, 1, "the author gets the outcome of the pair once: {:?}", twin.history);
}
