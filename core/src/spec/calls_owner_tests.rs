//! Owner-level tests of the `calls` machine: the owner `Calls` on `DetScheduler` against `ref_calls.rs`. Names are the `Test` column.

use super::ref_calls::{Calls as Ref, CallState, Effect as RefEffect, End, Fail, Input, Kind, Out};
use crate::owners::calls::{CallAnswer, CallError, CallKind, CallPhase, Calls, CallsEffect, CallsInput, CallsState};
use crate::owners::contract::Online;
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

const EPOCH: u64 = 1;

#[test]
fn call_deadline_expired() {
    for (operation, kind) in [("Op", CallKind::Query), ("Change", CallKind::Mutate)] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        twin.det.tick(crate::runtime::Time(29_999));
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).is_empty());
        twin.det.tick(crate::runtime::Time(30_000));
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert!(effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })));
        assert_eq!(effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Query, .. })), kind == CallKind::Query);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Mutate, .. })));
    }
}

#[test]
fn call_caller_cancel() {
    for (operation, kind) in [("Op", CallKind::Query), ("Change", CallKind::Mutate)] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::CallerCancel { reply: 1 });
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { .. })));
        assert_eq!(effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Query, .. })), kind == CallKind::Query);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Mutate, .. })));
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id: 1, body: Some("late".into()), data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Answered { sequence: None } });
        twin.det.tick(crate::runtime::Time(30_000));
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).is_empty());
    }
}

#[test]
fn call_transport_retry() {
    let mut twin = Twin::new();
    twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 2 });
    let first = twin.history.iter().find_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).unwrap();
    let second = twin.history.iter().filter_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).nth(1).unwrap();
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id: 1, error: CallError::Network });
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.tick(crate::runtime::Time(1_000));
    twin.det.run_until_quiet(100).unwrap();
    let effects = take(&twin.log);
    assert!(effects.iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, request_id, .. } if *request_id != first && *request_id != second)));
}

#[test]
fn call_deadline_after_done() {
    let mut twin = Twin::new();
    twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    twin.feed(Input::Response { call_id: 1, body: Some("ok".into()), errors: Vec::new() });
    twin.det.tick(crate::runtime::Time(30_000));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
}

#[test]
fn call_awaiting_snapshot_stray() {
    let mut twin = Twin::with(None);
    twin.feed(run(1));
    for input in [
        CallsInput::Response { call_id: 1, body: Some("late".into()), data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Answered { sequence: None } },
        CallsInput::TransportError { call_id: 1, error: CallError::Network },
        CallsInput::Dropped { call_id: 1 },
        CallsInput::RetryDue { call_id: 1, attempt: 1 },
    ] {
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), input);
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).is_empty());
    }
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

#[test]
fn call_awaiting_snapshot_end() {
    for closing in [false, true] {
        let mut twin = Twin::with(None);
        twin.feed(run(1));
        if closing { twin.det.close(); } else { twin.det.end_epoch(EPOCH, Cause::SessionReset); }
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        let answers: Vec<_> = effects.iter().filter_map(|effect| if let CallsEffect::Reply { id: 1, answer } = effect { Some(format!("{answer:?}")) } else { None }).collect();
        assert_eq!(answers, vec![if closing { "Err(Closed)" } else { "Err(SessionEnded)" }], "calls.md:80 (D-279): the owner answers the end cause 1:1");
        twin.det.tick(crate::runtime::Time(30_000));
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).iter().all(|effect| !matches!(effect, CallsEffect::Reply { id: 1, .. })));
    }
}

#[test]
fn call_awaiting_snapshot_declare() {
    let mut twin = Twin::with(None);
    twin.feed(run(1));
    twin.det.post(twin.addr, EpochStamp::Control, CallsInput::Declare(vec![("Op".into(), CallKind::Query)]));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

#[test]
fn call_awaiting_snapshot_run() {
    let mut twin = Twin::with(None);
    twin.feed(run(1));
    let second = twin.feed(run(2));
    assert!(second.effects.is_empty() && second.replies.is_empty());
    let out = twin.feed(Input::Snapshot(true));
    assert_eq!(out.effects.len(), 2);
    assert!(out.replies.is_empty());
}

#[test]
fn call_response_transient() {
    let mut twin = Twin::new();
    twin.feed(run(1));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id: 1, body: None, data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Transient { reason: "retry".into() } });
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.tick(crate::runtime::Time(1_000));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

#[test]
fn call_transport_offline() {
    let mut twin = Twin::new();
    twin.feed(run(1));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(false)));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id: 1, error: CallError::Network });
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.tick(crate::runtime::Time(1_000));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

#[test]
fn call_waiting_network_offline() {
    let mut twin = Twin::new();
    twin.feed(run(1));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(false)));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Dropped { call_id: 1 });
    twin.det.run_until_quiet(100).unwrap();
    take(&twin.log);
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(false)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

#[test]
fn call_retry_tick() {
    for operation in ["Op", "Change"] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        let original = twin.history.iter().find_map(|effect| if let CallsEffect::Request { request_id, variables, .. } = effect { Some((*request_id, variables.clone())) } else { None }).unwrap();
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id: 1, error: CallError::Network });
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).is_empty());
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::RetryDue { call_id: 1, attempt: 1 });
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        let retried = effects.iter().find_map(|effect| if let CallsEffect::Request { request_id, variables, .. } = effect { Some((*request_id, variables.clone())) } else { None }).unwrap();
        assert_eq!(retried.1, original.1);
        assert_eq!(retried.0 == original.0, operation == "Change");
    }
}

#[test]
fn call_cancel_before_snapshot() {
    let mut twin = Twin::with(None);
    twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::CallerCancel { reply: 1 });
    twin.det.run_until_quiet(100).unwrap();
    twin.det.tick(crate::runtime::Time(30_000));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
}

#[test]
fn call_waiting_network_online() {
    let mut twin = Twin::new();
    twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(false)));
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Dropped { call_id: 1 });
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Online(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
}

fn kind_of(kind: Kind) -> CallKind {
    match kind {
        Kind::Query => CallKind::Query,
        Kind::Mutate => CallKind::Mutate,
    }
}

fn ref_kind(kind: CallKind) -> Kind {
    match kind {
        CallKind::Query => Kind::Query,
        CallKind::Mutate => Kind::Mutate,
    }
}

fn fail(error: &Fail) -> CallError {
    match error {
        Fail::NoSession => CallError::NoSession,
        Fail::Invalid => CallError::Invalid,
        Fail::Offline => CallError::Offline,
        Fail::Network => CallError::Network,
        Fail::Graphql(messages) => CallError::Graphql { messages: messages.clone(), extensions: None },
        Fail::Http => CallError::Http { status: 0, detail: String::new() },
        Fail::Dropped => CallError::Dropped,
        Fail::InvalidResponse => CallError::InvalidResponse,
        Fail::Interrupted => CallError::Interrupted,
        Fail::SessionEnded => CallError::SessionEnded,
        Fail::Closed => CallError::Closed,
    }
}

struct Twin {
    det: DetScheduler,
    addr: Address<Calls>,
    log: Log<CallsEffect>,
    reference: Ref,
    history: Vec<CallsEffect>,
    epoch: u64,
}

impl Twin {
    fn new() -> Self { Self::with(Some(true)) }

    fn with(online: Option<bool>) -> Self {
        let mut det = DetScheduler::new(5);
        let log = new_log();
        let declared = [("Op", Kind::Query), ("Change", Kind::Mutate)];
        let addr = det.add::<Calls>("calls", CallsState::new(EPOCH, None, &declared.map(|(name, kind)| (name, kind_of(kind)))), Recorder::<Calls>::new(&log));
        det.start_epoch(EPOCH);
        if let Some(online) = online { det.post(addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(online))); }
        det.run_until_quiet(100).expect("the owner is quiet");
        take(&log);
        Self { det, addr, log, reference: Ref::new(EPOCH, online, &declared), history: Vec::new(), epoch: EPOCH }
    }

    /// Feeds both sides and compares effects, replies and the state of every call.
    fn feed(&mut self, input: Input) -> Out {
        match input.clone() {
            Input::Run { epoch, operation, reply } => self.det.post(self.addr, EpochStamp::Control, CallsInput::Run { epoch, operation, variables: String::new(), reply }),
            Input::Declare(operations) => self.det.post(self.addr, EpochStamp::Control, CallsInput::Declare(operations.into_iter().map(|(name, kind)| (name, kind_of(kind))).collect())),
            Input::Response { call_id, body, errors } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id, body, data: None, errors: errors.into_iter().map(|message| crate::transport::GraphQLError { message, path: None, extensions: None }).collect(), kind: crate::transport::AnswerKind::Answered { sequence: None } }),
            Input::Transient { call_id, reason } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id, body: None, data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Transient { reason } }),
            Input::TransportError { call_id, error } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id, error: fail(&error) }),
            Input::Dropped { call_id } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Dropped { call_id }),
            Input::RetryDue { call_id, attempt } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::RetryDue { call_id, attempt }),
            Input::Deadline { call_id } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Deadline { call_id }),
            Input::CallerCancel { reply } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::CallerCancel { reply }),
            Input::Online(online) => self.det.post(self.addr, EpochStamp::Control, CallsInput::Online(Online(online))),
            Input::Snapshot(online) => self.det.post(self.addr, EpochStamp::Epoch(self.epoch), CallsInput::Snapshot(Online(online))),
            Input::EpochStarted(epoch) => { self.epoch = epoch; self.det.start_epoch(epoch) }
            Input::EpochEnded(end) => self.det.end_epoch(self.epoch, match end { End::Session => Cause::SessionReset, End::Close => Cause::Close }),
            Input::Closing => self.det.close(),
        }
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(input);
        let effects: Vec<RefEffect> = raw.iter().filter_map(|effect| match effect {
            CallsEffect::Request { call_id, request_id, operation, kind, .. } => Some(RefEffect::Request { call_id: *call_id, request_id: *request_id, operation: operation.clone(), kind: ref_kind(*kind) }),
            CallsEffect::Cancel { request_id, kind } => Some(RefEffect::Cancel { request_id: *request_id, kind: ref_kind(*kind) }),
            CallsEffect::Release { request_id } => Some(RefEffect::Release { request_id: *request_id }),
            CallsEffect::SnapshotRequest { epoch } => Some(RefEffect::SnapshotRequest { epoch: *epoch }),
            CallsEffect::Drained(_) => Some(RefEffect::Drained),
            CallsEffect::Reply { .. } => None,
        }).collect();
        let replies: Vec<(u64, Result<String, Fail>)> = raw.iter().filter_map(|effect| match effect {
            CallsEffect::Reply { id, answer } => Some((*id, answer.clone().map_err(|error| error_of(&error)))),
            _ => None,
        }).collect();
        assert_eq!(effects, want.effects, "the same effects: owner {effects:?} reference {:?}", want.effects);
        assert_eq!(replies, want.replies, "the same replies: owner {replies:?} reference {:?}", want.replies);
        let state = self.det.state(self.addr).expect("state");
        for call in 1..=4 {
            let got = state.phase(call).map(|phase| match phase {
                CallPhase::Sending { request_id, kind, generation } => CallState::Sending { request_id, kind: ref_kind(kind), generation },
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
        CallError::Graphql { messages, .. } => Fail::Graphql(messages.clone()),
        CallError::Http { .. } | CallError::Session { .. } | CallError::Unauthorized => Fail::Http,
        CallError::Dropped => Fail::Dropped,
        CallError::InvalidResponse => Fail::InvalidResponse,
        CallError::Interrupted => Fail::Interrupted,
        CallError::SessionEnded => Fail::SessionEnded,
        CallError::Closed => Fail::Closed,
        CallError::Storage | CallError::Failed(_) => panic!("the Calls table does not read Published"),
    }
}

fn run(reply: u64) -> Input { Input::Run { epoch: EPOCH, operation: "Op".into(), reply } }
fn run_mutate(reply: u64) -> Input { Input::Run { epoch: EPOCH, operation: "Change".into(), reply } }
fn response(call_id: u64) -> Input { Input::Response { call_id, body: Some("{}".into()), errors: vec![] } }

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
fn call_run_offline() { Twin::with(Some(false)).feed(run(1)); }

#[test]
fn call_run() {
    let out = Twin::new().feed(run(1));
    assert_eq!(out.effects.len(), 1);
}

#[test]
fn call_declare() {
    let mut twin = Twin::new();
    twin.feed(Input::Declare(vec![("Other".into(), Kind::Query)]));
    let out = twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    assert_eq!(out.replies, vec![(1, Err(Fail::Invalid))], "the declared set is replaced, not extended");
    let out = twin.feed(Input::Run { epoch: EPOCH, operation: "Other".into(), reply: 2 });
    assert_eq!(out.effects.len(), 1, "the new operation runs");
}

#[test]
fn call_run_mutate() {
    let out = Twin::new().feed(run_mutate(1));
    assert!(matches!(out.effects.as_slice(), [RefEffect::Request { kind: Kind::Mutate, .. }]), "a mutation goes through Session: {:?}", out.effects);
}

#[test]
fn call_response_invalid() {
    let out = sending().feed(Input::Response { call_id: 1, body: None, errors: vec![] });
    assert_eq!(out.replies, vec![(10, Err(Fail::InvalidResponse))]);
}

#[test]
fn call_declare_in_flight() {
    let mut twin = sending();
    twin.feed(Input::Declare(vec![]));
    let out = twin.feed(response(1));
    assert_eq!(out.replies.len(), 1, "an accepted call outlives a new declaration");
    assert_eq!(twin.reference.generation, 1);
    assert_eq!(twin.reference.state(1), Some(CallState::Done));
}

#[test]
fn call_response() { sending().feed(response(1)); }

#[test]
fn call_response_errors() { sending().feed(Input::Response { call_id: 1, body: None, errors: vec!["boom".into()] }); }

#[test]
fn call_transport_error() {
    sending().feed(Input::TransportError { call_id: 1, error: Fail::Http });
    sending().feed(Input::TransportError { call_id: 1, error: Fail::Network });
}

#[test]
fn call_dropped() {
    let mut query = sending();
    let out = query.feed(Input::Dropped { call_id: 1 });
    assert!(out.replies.is_empty() && out.effects.is_empty());
    assert!(matches!(query.feed(Input::RetryDue { call_id: 1, attempt: 1 }).effects.as_slice(), [RefEffect::Request { call_id: 1, kind: Kind::Query, .. }]));
    let mut mutation = Twin::new();
    mutation.feed(run_mutate(11));
    let out = mutation.feed(Input::Dropped { call_id: 1 });
    assert!(out.effects.is_empty() && out.replies.is_empty());
    assert!(matches!(mutation.feed(Input::RetryDue { call_id: 1, attempt: 1 }).effects.as_slice(), [RefEffect::Request { request_id: 1, kind: Kind::Mutate, .. }]));
}

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
fn call_epoch_started_resets_online() {
    let mut twin = Twin::new();
    let out = twin.feed(Input::EpochStarted(EPOCH + 1));
    assert_eq!(out.effects, vec![RefEffect::SnapshotRequest { epoch: EPOCH + 1 }], "the owner asks Network for its snapshot");
    let out = twin.feed(Input::Run { epoch: EPOCH + 1, operation: "Op".into(), reply: 1 });
    assert!(out.replies.is_empty() && out.effects.is_empty(), "the new epoch has no online value until its snapshot: the call waits");
}

#[test]
fn call_run_before_snapshot_waits() {
    let mut armed = Twin::with(None);
    assert!(armed.feed(run(1)).replies.is_empty(), "no answer and no request before the snapshot");
    armed.det.tick(crate::runtime::Time(29_999));
    armed.det.run_until_quiet(100).unwrap();
    assert!(take(&armed.log).is_empty(), "before CALL_DEADLINE the armed timer does not fire");
    armed.det.tick(crate::runtime::Time(30_000));
    armed.det.run_until_quiet(100).unwrap();
    assert!(take(&armed.log).iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })), "accepting the run into AwaitingSnapshot arms Deadline(call) at once (D-227)");

    let mut twin = Twin::with(None);
    assert!(twin.feed(run(1)).replies.is_empty(), "no answer and no request before the snapshot");
    let out = twin.feed(Input::Snapshot(true));
    assert_eq!(out.effects.len(), 1, "the snapshot releases the call as a run with online");
    let mut offline = Twin::with(None);
    offline.feed(run(2));
    let out = offline.feed(Input::Snapshot(false));
    assert_eq!(out.replies, vec![(2, Err(Fail::Offline))], "the snapshot without network answers the waiting call Offline");
    let mut ended = Twin::with(None);
    ended.feed(run(3));
    let out = ended.feed(Input::EpochEnded(End::Session));
    assert_eq!(out.replies, vec![(3, Err(Fail::SessionEnded))], "the end of the epoch answers a waiting call");
}

#[test]
fn call_deadline_before_snapshot() {
    let mut twin = Twin::with(None);
    twin.feed(run(1));
    twin.det.tick(crate::runtime::Time(30_000));
    twin.det.run_until_quiet(100).unwrap();
    let effects = take(&twin.log);
    assert!(effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })), "no snapshot ever arrives: the deadline ends the call Err(Network)");
    assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Request { .. })), "no attempt is answered: the call starts no request");
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().all(|effect| !matches!(effect, CallsEffect::Request { .. })), "a snapshot after the deadline starts no request");
}

#[test]
fn call_deadline_timer_lifecycle() {
    let mut awaiting = Twin::with(None);
    awaiting.feed(run(1));
    awaiting.det.tick(crate::runtime::Time(30_000));
    awaiting.det.run_until_quiet(100).unwrap();
    assert!(take(&awaiting.log).iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })), "the deadline armed at the AwaitingSnapshot acceptance fires");

    let mut sending = Twin::new();
    sending.feed(run(1));
    sending.det.tick(crate::runtime::Time(30_000));
    sending.det.run_until_quiet(100).unwrap();
    assert!(take(&sending.log).iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })), "the deadline armed at the Sending acceptance fires");

    let mut done = Twin::new();
    done.feed(run(1));
    done.feed(response(1));
    done.det.tick(crate::runtime::Time(30_000));
    done.det.run_until_quiet(100).unwrap();
    assert!(take(&done.log).is_empty(), "the deadline is unscheduled once the call reaches Done");
}

#[test]
fn call_online_before_snapshot_ignored() {
    let mut twin = Twin::with(None);
    twin.feed(Input::Online(true));
    let out = twin.feed(run(1));
    assert!(out.replies.is_empty() && out.effects.is_empty(), "the broadcast does not set online before the addressed snapshot");
}

#[test]
fn call_snapshot_sets_online() {
    let mut twin = Twin::with(None);
    twin.feed(Input::Snapshot(true));
    let out = twin.feed(run(1));
    assert_eq!(out.effects.len(), 1, "the snapshot opens the owner to calls");
    twin.feed(Input::Online(false));
    let out = twin.feed(run(2));
    assert_eq!(out.replies, vec![(2, Err(Fail::Offline))], "a broadcast after the snapshot follows the network");
}

fn orders(events: &mut Vec<usize>, rest: &mut Vec<usize>, all: &mut Vec<Vec<usize>>) {
    if rest.is_empty() {
        all.push(events.clone());
        return;
    }
    for index in 0..rest.len() {
        let event = rest.remove(index);
        events.push(event);
        orders(events, rest, all);
        events.pop();
        rest.insert(index, event);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn call_prop_mutation_against_end_any_order() {
    let mut permutations = Vec::new();
    orders(&mut Vec::new(), &mut vec![0, 1, 2, 3, 4], &mut permutations);
    let mut tasks = Vec::new();
    for end in [End::Session, End::Close] {
        for order in &permutations {
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let mut twin = Twin::new();
                let started = twin.feed(run_mutate(1));
                assert!(matches!(started.effects.as_slice(), [RefEffect::Request { kind: Kind::Mutate, .. }]));
                let mut answer = None;
                let mut cancelled = false;
                for event in &order {
                    let input = match event {
                        0 => response(1),
                        1 => Input::Deadline { call_id: 1 },
                        2 => Input::CallerCancel { reply: 1 },
                        3 => Input::EpochEnded(end),
                        _ => Input::Online(false),
                    };
                    let out = twin.feed(input);
                    if answer.is_none() && !cancelled {
                        if let Some((_, result)) = out.replies.first() {
                            let expected = match event {
                                0 => Ok("{}".to_string()),
                                1 => Err(Fail::Network),
                                3 if end == End::Close => Err(Fail::Closed),
                                3 => Err(Fail::SessionEnded),
                                _ => panic!("unexpected reply for order {order:?}"),
                            };
                            assert_eq!(*result, expected, "order {order:?}, end {end:?}");
                            answer = Some(result.clone());
                        } else if *event == 2 {
                            cancelled = true;
                        }
                    } else {
                        assert!(out.replies.is_empty(), "duplicate answer for order {order:?}");
                    }
                    if *event == 3 {
                        assert!(out.effects.iter().all(|effect| !matches!(effect, RefEffect::Request { .. })), "request after end for order {order:?}");
                        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id: 1, body: Some("late".into()), data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Answered { sequence: None } });
                        twin.det.post(twin.addr, EpochStamp::Control, CallsInput::Online(Online(true)));
                        twin.det.run_until_quiet(100).unwrap();
                        assert!(take(&twin.log).iter().all(|effect| !matches!(effect, CallsEffect::Request { .. } | CallsEffect::Reply { .. })), "late input after end for order {order:?}");
                        break;
                    }
                }
                assert!(answer.is_some() || cancelled, "missing outcome for order {order:?}");
            }));
        }
    }
    for task in tasks { task.await.unwrap(); }
}

/// call_prop_second_epoch_orders: every order of EpochStarted (0), its addressed snapshot (1), the broadcast (2) and a run of the new epoch (3). The snapshot follows its request.
#[test]
fn call_prop_second_epoch_orders() {
    let mut all = Vec::new();
    orders(&mut Vec::new(), &mut vec![0, 1, 2, 3], &mut all);
    let before = |order: &[usize], first: usize, second: usize| order.iter().position(|seen| *seen == first) < order.iter().position(|seen| *seen == second);
    let valid: Vec<&Vec<usize>> = all.iter().filter(|order| before(order, 0, 1)).collect();
    assert_eq!(valid.len(), 12);
    for network in [true, false] {
        for order in &valid {
            let mut twin = Twin::new();
            for event in order.iter() {
                twin.feed(match event {
                    0 => Input::EpochStarted(EPOCH + 1),
                    1 => Input::Snapshot(network),
                    2 => Input::Online(network),
                    _ => Input::Run { epoch: EPOCH + 1, operation: "Op".into(), reply: 1 },
                });
            }
            twin.feed(Input::EpochEnded(End::Session));
            let answers = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Reply { id: 1, .. })).count();
            let requests = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Request { .. })).count();
            let cancels = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Cancel { .. })).count();
            assert_eq!(answers, 1, "order {order:?} network {network}: the run has exactly one answer");
            assert!(requests <= 1 && cancels == requests, "order {order:?} network {network}: a request that left is cancelled by the end of the epoch");
        }
    }
}

#[test]
fn call_epoch_ended() { sending().feed(Input::EpochEnded(End::Session)); }

#[test]
fn call_closing() { sending().feed(Input::Closing); }

#[test]
fn call_epoch_ended_close() {
    let out = sending().feed(Input::EpochEnded(End::Close));
    assert_eq!(out.replies, vec![(10, Err(Fail::Closed))], "an epoch ended by close answers the run with Closed");
}

#[test]
fn call_epoch_ended_close_then_closing() {
    let mut twin = sending();
    twin.feed(Input::EpochEnded(End::Close));
    let out = twin.feed(Input::Closing);
    assert!(out.replies.is_empty(), "the run has exactly one answer");
    let answers = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Reply { id: 10, .. })).count();
    let drained = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Drained(_))).count();
    assert_eq!((answers, drained), (1, 1), "one Closed answer, one Drained");
}

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
    twin.feed(Input::EpochEnded(End::Session));
}

#[test]
fn call_wait_run() {
    let mut twin = Twin::new();
    for reply in 1..=4 {
        twin.feed(run(reply));
    }
    twin.feed(response(1));
    twin.feed(Input::Dropped { call_id: 2 });
    twin.feed(Input::EpochEnded(End::Session));
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
            6 => Input::Online(next(&mut s).is_multiple_of(2)),
            7 => Input::EpochEnded(End::Session),
            _ => Input::Closing,
        };
        let over = matches!(input, Input::EpochEnded(_) | Input::Closing);
        twin.feed(input);
        if over {
            return twin;
        }
    }
    twin.feed(Input::EpochEnded(End::Session));
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
        let runs = twin.history.iter().filter_map(|e| if let CallsEffect::Request { call_id, .. } = e { Some(*call_id) } else { None }).collect::<std::collections::BTreeSet<_>>().len();
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
    twin.feed(Input::EpochEnded(End::Session));
    let out = twin.feed(response(1));
    assert!(out.replies.is_empty(), "no answer after Cancel");
}

#[test]
fn call_prop_no_store_write() {
    let twin = random_trace(7);
    assert!(twin.history.iter().all(|effect| matches!(effect, CallsEffect::Request { .. } | CallsEffect::Cancel { .. } | CallsEffect::Reply { .. } | CallsEffect::Drained(_))), "Calls has no Store effect");
}

#[test]
fn call_prop_answer_shape() {
    let root = r#"{"id":"1","tags":["a"]}"#;
    let out = sending().feed(Input::Response { call_id: 1, body: Some(root.into()), errors: vec![] });
    assert_eq!(out.replies, vec![(10, Ok(root.to_string()))], "the answer is the root as the server sent it");
}

#[test]
fn call_prop_single_request() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut requests = std::collections::BTreeMap::<u64, Vec<(u64, CallKind)>>::new();
        for effect in &twin.history {
            if let CallsEffect::Request { call_id, request_id, kind, .. } = effect { requests.entry(*call_id).or_default().push((*request_id, *kind)); }
        }
        for sent in requests.values() {
            if sent[0].1 == CallKind::Mutate { assert!(sent.iter().all(|entry| entry.0 == sent[0].0)); }
            else { assert_eq!(sent.iter().map(|entry| entry.0).collect::<std::collections::BTreeSet<_>>().len(), sent.len()); }
        }
    }
    let _: CallAnswer = Ok(String::new());
}

#[test]
fn matrix_owner_state_input_calls() {
    use crate::test_matrix::{state_input_pairs, Matrix};
    let mut matrix = Matrix::new("S-calls");
    fn offline() -> Twin { Twin::with(Some(false)) }
    fn sending_mutation() -> Twin { let mut twin = Twin::new(); twin.feed(run_mutate(11)); twin }
    fn done() -> Twin { let mut twin = sending(); twin.feed(response(1)); twin }
    fn undeclared_online() -> Twin { Twin::with(None) }
    let alphabet: Vec<(&str, Input)> = vec![
        ("Run query", run(20)), ("Run mutation", run_mutate(21)), ("Run unknown", Input::Run { epoch: EPOCH, operation: "Nope".into(), reply: 22 }), ("Run stale epoch", Input::Run { epoch: EPOCH + 1, operation: "Op".into(), reply: 23 }),
        ("Declare other", Input::Declare(vec![("Other".into(), Kind::Query)])), ("Declare none", Input::Declare(vec![])),
        ("Response body", response(1)), ("Response without body", Input::Response { call_id: 1, body: None, errors: vec![] }), ("Response errors", Input::Response { call_id: 1, body: None, errors: vec!["boom".into()] }), ("Response stray call", response(9)),
        ("TransportError network", Input::TransportError { call_id: 1, error: Fail::Network }), ("TransportError http", Input::TransportError { call_id: 1, error: Fail::Http }), ("TransportError stray", Input::TransportError { call_id: 9, error: Fail::Network }),
        ("Dropped", Input::Dropped { call_id: 1 }), ("Transient", Input::Transient { call_id: 1, reason: "temporary".into() }), ("RetryDue", Input::RetryDue { call_id: 1, attempt: 1 }), ("Deadline", Input::Deadline { call_id: 1 }), ("CallerCancel", Input::CallerCancel { reply: 10 }), ("Online false", Input::Online(false)), ("Online true", Input::Online(true)), ("Snapshot offline", Input::Snapshot(false)),
        ("EpochEnded", Input::EpochEnded(End::Session)), ("Closing", Input::Closing),
    ];
    state_input_pairs(&mut matrix, "reference ref_calls", vec![("online idle", Twin::new), ("offline", offline), ("sending query", sending), ("sending mutation", sending_mutation), ("done", done), ("before the snapshot", undeclared_online)], alphabet, |twin: &mut Twin, input| { twin.feed(input); });
    matrix.finish();
}
