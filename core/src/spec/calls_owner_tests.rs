//! Owner-level tests of the `calls` machine: the owner `Calls` on `DetScheduler` against `ref_calls.rs`. Names are the `Test` column.

use super::ref_calls::{Calls as Ref, CallState, Effect as RefEffect, Fail, Input, Out};
use crate::owners::calls::{CallAnswer, CallError, CallPhase, Calls, CallsEffect, CallsInput, CallsState};
use crate::owners::contract::Online;
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

const EPOCH: u64 = 1;

fn fail(error: &Fail) -> CallError {
    match error {
        Fail::NoSession => CallError::NoSession,
        Fail::Invalid => CallError::Invalid,
        Fail::Offline => CallError::Offline,
        Fail::Network => CallError::Network,
        Fail::Graphql(messages) => CallError::Graphql(messages.clone()),
        Fail::Http => CallError::Http,
        Fail::Interrupted => CallError::Interrupted,
    }
}

struct Twin {
    det: DetScheduler,
    addr: Address<Calls>,
    log: Log<CallsEffect>,
    reference: Ref,
    history: Vec<CallsEffect>,
}

impl Twin {
    fn new() -> Self { Self::with(true) }

    fn with(online: bool) -> Self {
        let mut det = DetScheduler::new(5);
        let log = new_log();
        let addr = det.add::<Calls>("calls", CallsState::new(EPOCH, online, &["Op"]), Recorder::<Calls>::new(&log));
        det.start_epoch(EPOCH);
        Self { det, addr, log, reference: Ref::new(EPOCH, online, &["Op"]), history: Vec::new() }
    }

    /// Feeds both sides and compares effects, replies and the state of every call.
    fn feed(&mut self, input: Input) -> Out {
        match input.clone() {
            Input::Run { epoch, operation, reply } => self.det.post(self.addr, EpochStamp::Control, CallsInput::Run { epoch, operation, variables: String::new(), reply }),
            Input::Response { call_id, body, errors } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id, body, errors }),
            Input::TransportError { call_id, error } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id, error: fail(&error) }),
            Input::Dropped { call_id } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Dropped { call_id }),
            Input::Online(online) => self.det.post(self.addr, EpochStamp::Control, CallsInput::Online(Online(online))),
            Input::EpochEnded => self.det.end_epoch(EPOCH, Cause::SessionReset),
            Input::Closing => self.det.close(),
        }
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(input);
        let effects: Vec<RefEffect> = raw.iter().filter_map(|effect| match effect {
            CallsEffect::Request { call_id, request_id, operation, .. } => Some(RefEffect::Request { call_id: *call_id, request_id: *request_id, operation: operation.clone() }),
            CallsEffect::Cancel { request_id } => Some(RefEffect::Cancel { request_id: *request_id }),
            CallsEffect::Drained(_) => Some(RefEffect::Drained),
            CallsEffect::Reply { .. } => None,
        }).collect();
        let replies: Vec<(u64, Result<String, Fail>)> = raw.iter().filter_map(|effect| match effect {
            CallsEffect::Reply { id, answer } => Some((*id, answer.clone().map_err(|error| error_of(&error)))),
            _ => None,
        }).collect();
        assert_eq!(effects, want.effects, "the same effects");
        assert_eq!(replies, want.replies, "the same replies");
        let state = self.det.state(self.addr).expect("state");
        for call in 1..=4 {
            let got = state.phase(call).map(|phase| match phase {
                CallPhase::Sending { request_id } => CallState::Sending { request_id },
                CallPhase::Done => CallState::Done,
            });
            assert_eq!(got, self.reference.state(call), "the same state of call {call}");
        }
        want
    }
}

fn error_of(error: &CallError) -> Fail {
    match error {
        CallError::NoSession => Fail::NoSession,
        CallError::Invalid => Fail::Invalid,
        CallError::Offline => Fail::Offline,
        CallError::Network => Fail::Network,
        CallError::Graphql(messages) => Fail::Graphql(messages.clone()),
        CallError::Http => Fail::Http,
        CallError::Interrupted => Fail::Interrupted,
    }
}

fn run(reply: u64) -> Input { Input::Run { epoch: EPOCH, operation: "Op".into(), reply } }
fn response(call_id: u64) -> Input { Input::Response { call_id, body: "{}".into(), errors: vec![] } }

fn sending() -> Twin {
    let mut twin = Twin::new();
    twin.feed(run(10));
    twin
}

#[test]
fn call_run_stale() { Twin::new().feed(Input::Run { epoch: EPOCH + 1, operation: "Op".into(), reply: 1 }); }

#[test]
fn call_run_unknown() { Twin::new().feed(Input::Run { epoch: EPOCH, operation: "Nope".into(), reply: 1 }); }

#[test]
fn call_run_offline() { Twin::with(false).feed(run(1)); }

#[test]
fn call_run() {
    let out = Twin::new().feed(run(1));
    assert_eq!(out.effects.len(), 1);
}

#[test]
fn call_response() { sending().feed(response(1)); }

#[test]
fn call_response_errors() { sending().feed(Input::Response { call_id: 1, body: String::new(), errors: vec!["boom".into()] }); }

#[test]
fn call_transport_error() {
    sending().feed(Input::TransportError { call_id: 1, error: Fail::Http });
    sending().feed(Input::TransportError { call_id: 1, error: Fail::Network });
}

#[test]
fn call_dropped() { sending().feed(Input::Dropped { call_id: 1 }); }

#[test]
fn call_stray() {
    let mut twin = sending();
    twin.feed(response(9));
    twin.feed(Input::TransportError { call_id: 9, error: Fail::Http });
    twin.feed(Input::Dropped { call_id: 9 });
}

#[test]
fn call_online() {
    let mut twin = sending();
    twin.feed(Input::Online(false));
    twin.feed(Input::Online(true));
    twin.feed(response(1));
}

#[test]
fn call_epoch_ended() { sending().feed(Input::EpochEnded); }

#[test]
fn call_closing() { sending().feed(Input::Closing); }

#[test]
fn call_done_closing() {
    let mut twin = sending();
    twin.feed(response(1));
    twin.feed(Input::Closing);
    twin.feed(Input::Closing);
    assert_eq!(twin.history.iter().filter(|e| matches!(e, CallsEffect::Drained(_))).count(), 1, "Drained goes out once");
    let mut empty = Twin::new();
    empty.feed(Input::Closing);
}

#[test]
fn call_done() {
    let mut twin = sending();
    twin.feed(response(1));
    twin.feed(response(1));
    twin.feed(Input::Dropped { call_id: 1 });
    twin.feed(Input::TransportError { call_id: 1, error: Fail::Http });
    twin.feed(Input::Online(false));
    twin.feed(Input::EpochEnded);
}

#[test]
fn call_wait_run() {
    let mut twin = Twin::new();
    for reply in 1..=4 {
        twin.feed(run(reply));
    }
    twin.feed(response(1));
    twin.feed(Input::Dropped { call_id: 2 });
    twin.feed(Input::EpochEnded);
    let answered: Vec<u64> = twin.history.iter().filter_map(|e| match e { CallsEffect::Reply { id, .. } => Some(*id), _ => None }).collect();
    assert_eq!(answered, vec![1, 2, 3, 4], "each run is answered: success, drop, then interrupted");
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_trace(seed: u64) -> Twin {
    let mut s = seed;
    let mut twin = Twin::new();
    let mut reply = 0;
    for _ in 0..24 {
        let call = 1 + next(&mut s) % 4;
        let input = match next(&mut s) % 9 {
            0..=2 => { reply += 1; run(reply) }
            3 => response(call),
            4 => Input::Dropped { call_id: call },
            5 => Input::TransportError { call_id: call, error: Fail::Http },
            6 => Input::Online(next(&mut s) % 2 == 0),
            7 => Input::EpochEnded,
            _ => Input::Closing,
        };
        let over = matches!(input, Input::EpochEnded | Input::Closing);
        twin.feed(input);
        if over {
            return twin;
        }
    }
    twin.feed(Input::EpochEnded);
    twin
}

#[test]
fn call_prop_reply_once() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut seen = Vec::new();
        for effect in &twin.history {
            if let CallsEffect::Reply { id, .. } = effect {
                assert!(!seen.contains(id), "run {id} is answered twice");
                seen.push(*id);
            }
        }
        let runs = twin.history.iter().filter(|e| matches!(e, CallsEffect::Request { .. })).count();
        assert!(seen.len() >= runs, "every request ends with an answer");
    }
}

#[test]
fn call_prop_epoch() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut ended = false;
        for effect in &twin.history {
            if matches!(effect, CallsEffect::Cancel { .. } | CallsEffect::Drained(_)) { ended = true; }
            if let CallsEffect::Request { .. } = effect {
                assert!(!ended, "a request goes out after the epoch ended");
            }
        }
    }
    let mut twin = sending();
    twin.feed(Input::EpochEnded);
    let out = twin.feed(response(1));
    assert!(out.replies.is_empty(), "no answer after Cancel");
}

#[test]
fn call_prop_no_store_write() {
    let twin = random_trace(7);
    assert!(twin.history.iter().all(|effect| matches!(effect, CallsEffect::Request { .. } | CallsEffect::Cancel { .. } | CallsEffect::Reply { .. } | CallsEffect::Drained(_))), "Calls has no Store effect");
}

#[test]
fn call_prop_single_request() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let requests = twin.history.iter().filter(|e| matches!(e, CallsEffect::Request { .. })).count();
        let mut ids: Vec<u64> = twin.history.iter().filter_map(|e| match e { CallsEffect::Request { request_id, .. } => Some(*request_id), _ => None }).collect();
        ids.dedup();
        assert_eq!(ids.len(), requests, "no repeat of a request");
    }
    let _: CallAnswer = Ok(String::new());
}
