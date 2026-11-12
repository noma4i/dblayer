//! Owner-level tests of the `calls` owner on `DetScheduler`: input -> effects and answers.

use crate::owners::calls::{CallAnswer, CallError, CallKind, CallPhase, Calls, CallsEffect, CallsInput, CallsState};
use crate::owners::contract::Online;
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

const EPOCH: u64 = 1;

#[test]
fn call_deadline_expired() {
    for (operation, kind) in [("Op", CallKind::Query)] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        let request_id = twin.history.iter().find_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).unwrap();
        twin.det.tick(crate::runtime::Time(29_999));
        twin.det.run_until_quiet(100).unwrap();
        assert!(take(&twin.log).is_empty());
        twin.det.tick(crate::runtime::Time(30_000));
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert!(effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { id: 1, answer: Err(CallError::Network) })));
        assert_eq!(effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Query, .. })), kind == CallKind::Query);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Mutate, .. })));
        assert_eq!(effects.iter().filter(|effect| matches!(effect, CallsEffect::Release { request_id: released } if *released == request_id)).count(), usize::from(kind == CallKind::Mutate));
        assert!(effects.iter().all(|effect| !matches!(effect, CallsEffect::Release { request_id: released } if *released != request_id)));
    }
}

#[test]
fn call_caller_cancel() {
    for (operation, kind) in [("Op", CallKind::Query), ("Change", CallKind::Mutate)] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        let request_id = twin.history.iter().find_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).unwrap();
        twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::CallerCancel { reply: 1 });
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { .. })));
        assert_eq!(effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Query, .. })), kind == CallKind::Query);
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { kind: CallKind::Mutate, .. })));
        assert_eq!(effects.iter().filter(|effect| matches!(effect, CallsEffect::Release { request_id: released } if *released == request_id)).count(), usize::from(kind == CallKind::Mutate));
        assert!(effects.iter().all(|effect| !matches!(effect, CallsEffect::Release { request_id: released } if *released != request_id)));
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
    twin.det.post(twin.addr, EpochStamp::Control, CallsInput::Declare(vec![("Op".into(), crate::owners::calls::CallDefinition { kind: CallKind::Query, document: String::new(), root: String::new() })]));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).is_empty());
    twin.det.post(twin.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    twin.det.run_until_quiet(100).unwrap();
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, .. })));
    let mut removed = Twin::with(None);
    removed.feed(run(1));
    removed.det.post(removed.addr, EpochStamp::Control, CallsInput::Declare(vec![]));
    removed.det.post(removed.addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(true)));
    removed.det.run_until_quiet(100).unwrap();
    let effects = take(&removed.log);
    assert!(effects.iter().any(|effect| matches!(effect, CallsEffect::Request { call_id: 1, operation, definition, .. } if operation == "Op" && definition.kind == CallKind::Query)), "accepted declaration must survive replacement: {effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Reply { .. })));
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

type ReplyId = u64;
type CallId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End { Session, Close }

/// One step of a scenario: the owner input without the stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Input {
    Run { epoch: u64, operation: String, reply: ReplyId },
    Declare(Vec<(String, CallKind)>),
    Response { call_id: CallId, body: Option<String>, errors: Vec<String> },
    TransportError { call_id: CallId, error: CallError },
    Dropped { call_id: CallId },
    RetryDue { call_id: CallId, attempt: u32 },
    Deadline { call_id: CallId },
    CallerCancel { reply: ReplyId },
    Online(bool),
    Snapshot(bool),
    EpochStarted(u64),
    EpochEnded(End),
    Closing,
}

/// The effects of one step without the answers, and the answers.
struct Out {
    effects: Vec<CallsEffect>,
    replies: Vec<(u64, CallAnswer)>,
}

fn http() -> CallError { CallError::Http { status: 0, detail: String::new() } }

struct Twin {
    det: DetScheduler,
    addr: Address<Calls>,
    log: Log<CallsEffect>,
    history: Vec<CallsEffect>,
    epoch: u64,
}

impl Twin {
    fn new() -> Self { Self::with(Some(true)) }

    fn with(online: Option<bool>) -> Self {
        let mut det = DetScheduler::new(5);
        let log = new_log();
        let declared = [("Op", CallKind::Query), ("Change", CallKind::Mutate)];
        let addr = det.add::<Calls>("calls", CallsState::new(EPOCH, None, &declared), Recorder::<Calls>::new(&log));
        det.start_epoch(EPOCH);
        if let Some(online) = online { det.post(addr, EpochStamp::Epoch(EPOCH), CallsInput::Snapshot(Online(online))); }
        det.run_until_quiet(100).expect("the owner is quiet");
        take(&log);
        Self { det, addr, log, history: Vec::new(), epoch: EPOCH }
    }

    /// Feeds the owner one input and returns what it did.
    fn feed(&mut self, input: Input) -> Out {
        match input.clone() {
            Input::Run { epoch, operation, reply } => self.det.post(self.addr, EpochStamp::Control, CallsInput::Run { epoch, operation, variables: String::new(), reply }),
            Input::Declare(operations) => self.det.post(self.addr, EpochStamp::Control, CallsInput::Declare(operations.into_iter().map(|(name, kind)| (name, crate::owners::calls::CallDefinition { kind, document: String::new(), root: String::new() })).collect())),
            Input::Response { call_id, body, errors } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::Response { call_id, body, data: None, errors: errors.into_iter().map(|message| crate::transport::GraphQLError { message, path: None, extensions: None }).collect(), kind: crate::transport::AnswerKind::Answered { sequence: None } }),
            Input::TransportError { call_id, error } => self.det.post(self.addr, EpochStamp::Epoch(EPOCH), CallsInput::TransportError { call_id, error }),
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
        let effects: Vec<CallsEffect> = raw.iter().filter(|effect| !matches!(effect, CallsEffect::Reply { .. })).cloned().collect();
        let replies: Vec<(u64, CallAnswer)> = raw.iter().filter_map(|effect| match effect {
            CallsEffect::Reply { id, answer } => Some((*id, answer.clone())),
            _ => None,
        }).collect();
        Out { effects, replies }
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
fn call_run_is_refused_before_it_starts() {
    let out = Twin::new().feed(Input::Run { epoch: EPOCH + 1, operation: "Op".into(), reply: 1 });
    assert_eq!(out.replies, vec![(1, Err(CallError::NoSession))], "a run of another epoch");
    let out = Twin::new().feed(Input::Run { epoch: EPOCH, operation: "Nope".into(), reply: 1 });
    assert_eq!(out.replies, vec![(1, Err(CallError::Invalid))], "an undeclared operation");
    let out = Twin::with(Some(false)).feed(run(1));
    assert_eq!(out.replies, vec![(1, Err(CallError::Offline))], "no network");
    assert!(out.effects.is_empty(), "no request goes out");
}

#[test]
fn call_response_answers_the_run_and_a_stray_input_does_nothing() {
    let out = sending().feed(Input::Response { call_id: 1, body: Some("{}".into()), errors: vec![] });
    assert_eq!(out.replies, vec![(10, Ok("{}".to_string()))]);
    let out = sending().feed(Input::Response { call_id: 1, body: None, errors: vec!["boom".into()] });
    assert_eq!(out.replies, vec![(10, Err(CallError::Graphql { messages: vec!["boom".into()], extensions: None }))]);
    let mut twin = sending();
    for stray in [response(9), Input::TransportError { call_id: 9, error: http() }, Input::Dropped { call_id: 9 }] {
        let out = twin.feed(stray);
        assert!(out.effects.is_empty() && out.replies.is_empty(), "an input of an unknown call changes nothing");
    }
    twin.feed(Input::Online(false));
    twin.feed(Input::Online(true));
    assert_eq!(twin.feed(response(1)).replies, vec![(10, Ok("{}".to_string()))], "a call in flight survives a network change");
}

#[test]
fn call_run() {
    let out = Twin::new().feed(run(1));
    assert_eq!(out.effects.len(), 1);
}

#[test]
fn call_declare() {
    let mut twin = Twin::new();
    twin.feed(Input::Declare(vec![("Other".into(), CallKind::Query)]));
    let out = twin.feed(Input::Run { epoch: EPOCH, operation: "Op".into(), reply: 1 });
    assert_eq!(out.replies, vec![(1, Err(CallError::Invalid))], "the declared set is replaced, not extended");
    let out = twin.feed(Input::Run { epoch: EPOCH, operation: "Other".into(), reply: 2 });
    assert_eq!(out.effects.len(), 1, "the new operation runs");
}

#[test]
fn call_run_mutate() {
    let out = Twin::new().feed(run_mutate(1));
    assert!(matches!(out.effects.as_slice(), [CallsEffect::Request { definition: crate::owners::calls::CallDefinition { kind: CallKind::Mutate, .. }, .. }]), "a mutation goes through Session: {:?}", out.effects);
}

#[test]
fn call_response_invalid() {
    let out = sending().feed(Input::Response { call_id: 1, body: None, errors: vec![] });
    assert_eq!(out.replies, vec![(10, Err(CallError::InvalidResponse))]);
}

#[test]
fn call_declare_in_flight() {
    let mut twin = sending();
    twin.feed(Input::Declare(vec![]));
    let out = twin.feed(response(1));
    assert_eq!(out.replies.len(), 1, "an accepted call outlives a new declaration");
    assert!(matches!(twin.det.state(twin.addr).expect("state").phase(1), Some(CallPhase::Done)));
}

#[test]
fn call_dropped() {
    let mut query = sending();
    let out = query.feed(Input::Dropped { call_id: 1 });
    assert!(out.replies.is_empty() && out.effects.is_empty());
    assert!(matches!(query.feed(Input::RetryDue { call_id: 1, attempt: 1 }).effects.as_slice(), [CallsEffect::Request { call_id: 1, definition: crate::owners::calls::CallDefinition { kind: CallKind::Query, .. }, .. }]));
    let mut mutation = Twin::new();
    mutation.feed(run_mutate(11));
    let out = mutation.feed(Input::Dropped { call_id: 1 });
    assert!(out.effects.is_empty() && out.replies.is_empty());
    assert!(matches!(mutation.feed(Input::RetryDue { call_id: 1, attempt: 1 }).effects.as_slice(), [CallsEffect::Request { request_id: 1, definition: crate::owners::calls::CallDefinition { kind: CallKind::Mutate, .. }, .. }]));
    use crate::runtime::{Effect, StateOwner};
    let mut state = CallsState::new(EPOCH, Some(true), &[("Change", CallKind::Mutate)]);
    Calls::step(&mut state, CallsInput::Run { epoch: EPOCH, operation: "Change".into(), variables: "{}".into(), reply: 1 }, crate::runtime::Time(0));
    for attempt in 1..40 {
        Calls::step(&mut state, CallsInput::Dropped { call_id: 1 }, crate::runtime::Time(0));
        Calls::step(&mut state, CallsInput::RetryDue { call_id: 1, attempt }, crate::runtime::Time(0));
    }
    let exhausted = Calls::step(&mut state, CallsInput::Dropped { call_id: 1 }, crate::runtime::Time(0));
    assert!(exhausted.iter().any(|effect| matches!(effect, Effect::Run(CallsEffect::Release { request_id: 1 }))));
    assert!(exhausted.iter().any(|effect| matches!(effect, Effect::Run(CallsEffect::Reply { id: 1, answer: Err(CallError::Dropped) }))));
}

#[test]
fn call_epoch_started_resets_online() {
    let mut twin = Twin::new();
    let out = twin.feed(Input::EpochStarted(EPOCH + 1));
    assert_eq!(out.effects, vec![CallsEffect::SnapshotRequest { epoch: EPOCH + 1 }], "the owner asks Network for its snapshot");
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
    assert_eq!(out.replies, vec![(2, Err(CallError::Offline))], "the snapshot without network answers the waiting call Offline");
    let mut ended = Twin::with(None);
    ended.feed(run(3));
    let out = ended.feed(Input::EpochEnded(End::Session));
    assert_eq!(out.replies, vec![(3, Err(CallError::SessionEnded))], "the end of the epoch answers a waiting call");
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
    use crate::runtime::{Effect, ScheduleId, StateOwner};
    for online in [None, Some(true)] {
        for terminal in 0..4 {
            let mut state = CallsState::new(EPOCH, online, &[("Op", CallKind::Query)]);
            let scheduled = Calls::step(&mut state, CallsInput::Run { epoch: EPOCH, operation: "Op".into(), variables: "{}".into(), reply: 1 }, crate::runtime::Time(0));
            assert!(scheduled.iter().any(|effect| matches!(effect, Effect::Schedule { id: ScheduleId(2), input: CallsInput::Deadline { call_id: 1 }, .. })));
            let input = match terminal {
                0 => CallsInput::Deadline { call_id: 1 },
                1 => CallsInput::CallerCancel { reply: 1 },
                2 => CallsInput::EpochEnded(Cause::SessionReset),
                _ => CallsInput::Closing,
            };
            let finished = Calls::step(&mut state, input, crate::runtime::Time(0));
            assert!(finished.iter().any(|effect| matches!(effect, Effect::Cancel(ScheduleId(2)))), "deadline must be unscheduled after terminal input {terminal}, online {online:?}");
        }
    }
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
    assert_eq!(out.replies, vec![(2, Err(CallError::Offline))], "a broadcast after the snapshot follows the network");
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
    let mut offline = Twin::with(Some(false));
    assert_eq!(offline.feed(run_mutate(1)).replies, vec![(1, Err(CallError::Offline))]);
    for cancel in [false, true] {
        let mut twin = Twin::with(None);
        twin.feed(run_mutate(1));
        let out = twin.feed(if cancel { Input::CallerCancel { reply: 1 } } else { Input::Deadline { call_id: 1 } });
        assert_eq!(out.replies, if cancel { vec![] } else { vec![(1, Err(CallError::Network))] });
        let late = twin.feed(Input::Snapshot(true));
        assert!(late.effects.is_empty() && late.replies.is_empty());
    }
    for end in [End::Session, End::Close] {
        for snapshot_first in [false, true] {
            let mut twin = Twin::with(None);
            assert!(twin.feed(run_mutate(1)).replies.is_empty());
            if snapshot_first {
                assert!(matches!(twin.feed(Input::Snapshot(true)).effects.as_slice(), [CallsEffect::Request { definition: crate::owners::calls::CallDefinition { kind: CallKind::Mutate, .. }, .. }]));
            }
            let ended = twin.feed(Input::EpochEnded(end));
            let expected = if end == End::Close { CallError::Closed } else { CallError::SessionEnded };
            assert_eq!(ended.replies, vec![(1, Err(expected))]);
            assert!(!ended.effects.iter().any(|effect| matches!(effect, CallsEffect::Request { .. })));
        }
    }
    let mut permutations = Vec::new();
    orders(&mut Vec::new(), &mut vec![0, 1, 2, 3, 4], &mut permutations);
    let mut tasks = Vec::new();
    for end in [End::Session, End::Close] {
        for order in &permutations {
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let mut twin = Twin::new();
                let started = twin.feed(run_mutate(1));
                assert!(matches!(started.effects.as_slice(), [CallsEffect::Request { definition: crate::owners::calls::CallDefinition { kind: CallKind::Mutate, .. }, .. }]));
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
                                1 => Err(CallError::Network),
                                3 if end == End::Close => Err(CallError::Closed),
                                3 => Err(CallError::SessionEnded),
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
                        assert!(out.effects.iter().all(|effect| !matches!(effect, CallsEffect::Request { .. })), "request after end for order {order:?}");
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
fn call_epoch_ended() {
    for operation in ["Op", "Change"] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        let out = twin.feed(Input::EpochEnded(End::Session));
        let kind = if operation == "Op" { CallKind::Query } else { CallKind::Mutate };
        assert!(out.effects.contains(&CallsEffect::Cancel { request_id: 1, kind }));
        assert_eq!(out.replies, vec![(1, Err(CallError::SessionEnded))]);
    }
}

#[test]
fn call_closing() {
    for operation in ["Op", "Change"] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 1 });
        let out = twin.feed(Input::Closing);
        let kind = if operation == "Op" { CallKind::Query } else { CallKind::Mutate };
        assert!(out.effects.contains(&CallsEffect::Cancel { request_id: 1, kind }));
        assert!(out.effects.iter().any(|effect| matches!(effect, CallsEffect::Drained(_))));
        assert_eq!(out.replies, vec![(1, Err(CallError::Closed))]);
    }
}

#[test]
fn call_epoch_ended_close_answers_closed_once() {
    let mut twin = sending();
    let out = twin.feed(Input::EpochEnded(End::Close));
    assert_eq!(out.replies, vec![(10, Err(CallError::Closed))], "an epoch ended by close answers the run with Closed");
    let out = twin.feed(Input::Closing);
    assert!(out.replies.is_empty(), "the run has exactly one answer");
    let drained = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Drained(_))).count();
    assert_eq!(drained, 1, "one Drained");
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
    twin.feed(Input::TransportError { call_id: 1, error: http() });
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
            5 => Input::TransportError { call_id: call, error: http() },
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
            if let CallsEffect::Request { call_id, request_id, definition, .. } = effect { requests.entry(*call_id).or_default().push((*request_id, definition.kind)); }
        }
        for sent in requests.values() {
            if sent[0].1 == CallKind::Mutate {
                assert!(sent.len() <= 40, "mutation retry budget: {sent:?}");
                assert!(sent.iter().all(|entry| entry.0 == sent[0].0));
            } else {
                assert!(sent.len() <= 3, "query retry budget: {sent:?}");
                assert_eq!(sent.iter().map(|entry| entry.0).collect::<std::collections::BTreeSet<_>>().len(), sent.len());
            }
        }
    }
    for (operation, answers) in [("Op", 1), ("Change", 0)] {
        let mut twin = Twin::new();
        twin.feed(Input::Run { epoch: EPOCH, operation: operation.into(), reply: 91 });
        twin.det.tick(crate::runtime::Time(30_000));
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert_eq!(effects.iter().filter(|effect| matches!(effect, CallsEffect::Reply { id: 91, .. })).count(), answers, "deadline for {operation} (a mutation with attempt 0 goes to Releasing, call_deadline_expired_mutate_first): {effects:?}");
    }
    let _: CallAnswer = Ok(String::new());
}

fn change_run() -> CallsState {
    use crate::runtime::StateOwner;
    let mut state = CallsState::new(EPOCH, Some(true), &[("Change", CallKind::Mutate)]);
    Calls::step(&mut state, CallsInput::Run { epoch: EPOCH, operation: "Change".into(), variables: "{}".into(), reply: 1 }, crate::runtime::Time(0));
    state
}

fn step(state: &mut CallsState, input: CallsInput) -> Vec<CallsEffect> {
    use crate::runtime::{Effect, StateOwner};
    Calls::step(state, input, crate::runtime::Time(0)).into_iter().filter_map(|effect| if let Effect::Run(effect) = effect { Some(effect) } else { None }).collect()
}

fn replies(effects: &[CallsEffect]) -> Vec<CallAnswer> { effects.iter().filter_map(|effect| if let CallsEffect::Reply { answer, .. } = effect { Some(answer.clone()) } else { None }).collect() }

#[test]
fn call_deadline_expired_mutate_first() {
    let mut state = change_run();
    let effects = step(&mut state, CallsInput::Deadline { call_id: 1 });
    assert!(replies(&effects).is_empty(), "the answer waits for the verdict of Session: {effects:?}");
    assert_eq!(effects, vec![CallsEffect::Release { request_id: 1 }]);
}

#[test]
fn call_deadline_expired_mutate_sent() {
    for last in [CallsInput::Dropped { call_id: 1 }, CallsInput::TransportError { call_id: 1, error: CallError::Network }] {
        let mut state = change_run();
        step(&mut state, last.clone());
        let effects = step(&mut state, CallsInput::Deadline { call_id: 1 });
        assert_eq!(replies(&effects), vec![Err(CallError::Dropped)], "the pair exists, the outcome is unknown, whatever the last attempt gave ({last:?}): {effects:?}");
        assert!(effects.contains(&CallsEffect::Release { request_id: 1 }) && !effects.iter().any(|effect| matches!(effect, CallsEffect::Cancel { .. })), "{effects:?}");
    }
}

#[test]
fn call_releasing_verdict() {
    let ok = || CallsInput::Response { call_id: 1, body: Some("{}".into()), data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Answered { sequence: None } };
    for (verdict, want) in [(CallsInput::Dropped { call_id: 1 }, Err(CallError::Dropped)), (CallsInput::TransportError { call_id: 1, error: CallError::Http { status: 500, detail: String::new() } }, Err(CallError::Dropped)), (ok(), Ok("{}".to_string()))] {
        let mut state = change_run();
        step(&mut state, CallsInput::Deadline { call_id: 1 });
        let effects = step(&mut state, verdict.clone());
        assert_eq!(replies(&effects), vec![want], "{verdict:?}: {effects:?}");
        assert!(!effects.iter().any(|effect| matches!(effect, CallsEffect::Request { .. })), "no retry after the Release: {effects:?}");
    }
    let mut twin = Twin::new();
    twin.feed(Input::Run { epoch: EPOCH, operation: "Change".into(), reply: 1 });
    twin.feed(Input::Deadline { call_id: 1 });
    let out = twin.feed(Input::TransportError { call_id: 1, error: CallError::NotSent });
    assert_eq!(out.replies, vec![(1, Err(CallError::Network))], "NotSent: no request was made and none will be");
    let requests_before = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Request { .. })).count();
    for later in [Input::RetryDue { call_id: 1, attempt: 1 }, Input::Online(false), Input::Online(true), Input::Deadline { call_id: 1 }] { twin.feed(later); }
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Request { .. })).count(), requests_before, "no request after Err(Network)");
}

/// P-CALL-06: the pair of a mutation leaves with one `request_id`. Calls sees the stage of Session only by its answer: no pair (queue, Sealing, Persisting) = `NotSent`, a pair (Calling, Landing, Held) = `Dropped`.
#[test]
fn call_prop_dropped_pair_single_execution() {
    let preludes: [(&str, Vec<Input>); 3] = [("first attempt", Vec::new()), ("after Dropped", vec![Input::Dropped { call_id: 1 }]), ("after Network", vec![Input::TransportError { call_id: 1, error: CallError::Network }])];
    let stages: [(&str, Input); 6] = [("queue", Input::TransportError { call_id: 1, error: CallError::NotSent }), ("Sealing", Input::TransportError { call_id: 1, error: CallError::NotSent }), ("Persisting", Input::TransportError { call_id: 1, error: CallError::NotSent }), ("Calling", Input::Dropped { call_id: 1 }), ("Landing", Input::Dropped { call_id: 1 }), ("Held", Input::Dropped { call_id: 1 })];
    for (prelude_name, prelude) in &preludes {
        for trigger in [Input::Deadline { call_id: 1 }, Input::CallerCancel { reply: 1 }] {
            for (stage, answer) in &stages {
                let label = format!("{prelude_name} / {trigger:?} / {stage}");
                let mut twin = Twin::new();
                twin.feed(Input::Run { epoch: EPOCH, operation: "Change".into(), reply: 1 });
                for input in prelude { twin.feed(input.clone()); }
                let mut replies: Vec<CallAnswer> = twin.feed(trigger.clone()).replies.into_iter().map(|(_, answer)| answer).collect();
                if *prelude_name == "first attempt" || matches!(trigger, Input::CallerCancel { .. }) { replies.extend(twin.feed(answer.clone()).replies.into_iter().map(|(_, answer)| answer)); }
                let pairs: std::collections::BTreeSet<u64> = twin.history.iter().filter_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).collect();
                assert_eq!(pairs.len(), 1, "{label}: one pair per call: {pairs:?}");
                let requests = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Request { .. })).count();
                match trigger {
                    Input::Deadline { .. } if *prelude_name != "first attempt" => assert_eq!(replies, vec![Err(CallError::Dropped)], "{label}: the pair exists, Dropped always"),
                    Input::Deadline { .. } => assert_eq!(replies, vec![if matches!(answer, Input::Dropped { .. }) { Err(CallError::Dropped) } else { Err(CallError::Network) }], "{label}: Network only when no request was made, Dropped when the pair exists"),
                    _ => assert!(replies.is_empty(), "{label}: the caller is gone, no answer: {replies:?}"),
                }
                let network_answer = replies.contains(&Err(CallError::Network));
                for late in [Input::RetryDue { call_id: 1, attempt: 1 }, Input::RetryDue { call_id: 1, attempt: 2 }, Input::Online(false), Input::Online(true), Input::Deadline { call_id: 1 }, Input::Dropped { call_id: 1 }] { twin.feed(late); }
                let after = twin.history.iter().filter(|effect| matches!(effect, CallsEffect::Request { .. })).count();
                assert_eq!(after, requests, "{label}: no request of this call after the verdict (Err(Network) = {network_answer}) or the Release");
            }
        }
    }
}
