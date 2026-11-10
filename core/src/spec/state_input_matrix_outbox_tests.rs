//! Matrix S for the owner Outbox against `ref_outbox` (the whole-owner reference written from outbox.md only). A cell is a context (what the table reads: online, row busy, slot taken, attempts, policy, origin), a script that brings the target operation to one stage through public inputs, and one more input. Both sides play every step; after each step the ordered list of effects and replies (normalized to the names both sides share), the stage and the cycle of every operation, and the stray rule (an input of an unknown operation changes nothing and emits nothing) must be equal. A mismatch is a red cell; the reference is never changed. A script whose stage the owner or the reference does not reach is a dead cell, not a red one.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::outbox_harness::{OutboxEffect, Policy, Twin};
use super::ref_outbox::{step, Addr, Batch, Effect, Input as RIn, Kind, LandFacts, OpOrigin, OpSpec, Phase, Resp, RestoreItem, SendKind, Sel, Stage as RStage, State, SubmitFlags};
use crate::owners::write::outbox::{OutboxAnswer, OutboxInput};
use crate::test_matrix::Matrix;

#[derive(Clone, Copy, Debug)]
enum Step {
    Submit { row: u8, keep: bool, transfer: bool },
    Commit(u64),
    Response(u64),
    Error { op: u64, temporary: bool },
    Dropped(u64),
    Tick(u64),
    Online(bool),
    Retry,
    Discard,
    Outcome(u64),
    EpochEnd,
    Close,
    Stray,
    SubmitStale,
    SubmitExpiring,
    Age,
    ResponseError { op: u64, temporary: bool },
    ResponseSwap(u64),
    RetryAny,
    DiscardAny,
    ModelsReset,
    SubmitAct { keep: bool, transfer: bool, act: Act },
    ResponseNotFound(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act { Update, Create, Destroy }

impl Act {
    fn name(self) -> &'static str { match self { Act::Update => "update", Act::Create => "create", Act::Destroy => "destroy" } }
}

pub(super) fn row_name(row: u8) -> String { if row == 1 { "r".to_string() } else { format!("r{row}") } }

pub(super) fn spec(row: u8, keep: bool, transfer: bool) -> OpSpec { spec_act(row, keep, transfer, Act::Update) }

fn spec_act(row: u8, keep: bool, transfer: bool, act: Act) -> OpSpec {
    OpSpec { op_id: None, model: "Moment".into(), row_id: row_name(row), action: act.name().into(), expires_after: None, created_at: 0, keep, effects_empty: false, destroy: act == Act::Destroy, response_ignore: false, batch: false, origin: if transfer { OpOrigin::Transfer(1) } else { OpOrigin::Api }, attempts: 0 }
}

fn kind_name(what: &str) -> &'static str {
    if what.contains("ClearMark") { "ClearMark" }
    else if what.contains("MarkFailed") { "ErrorMark" }
    else if what.contains("rows,") || what.contains("delete row") || what.contains("items,") || what.contains("Land") || what.contains("Swap") { "Land" }
    else if what.contains("Rollback") || what.contains("RemoveIntent") { "Rollback" }
    else if what.contains("Requeue") { "Requeue" }
    else if what.contains("attempts+1") { "Attempt" }
    else { "Intent" }
}

/// The outcome token both sides compare, keeping the full `Ended(cause)`.
fn outcome_token(text: &str) -> String {
    if text.starts_with("Ended(") { text.to_string() } else { text.split([' ', '{']).next().unwrap_or("").to_string() }
}

pub(super) fn ref_tokens(effects: &[Effect]) -> Vec<String> {
    effects.iter().filter_map(|effect| match effect {
        Effect::Row(_) => None,
        Effect::Undefined { row, .. } => Some(format!("undefined {row}")),
        Effect::Store { what, .. } if what.starts_with("Swap{") => None,
        Effect::Store { op, what } => Some(format!("store {op} {}", kind_name(what))),
        Effect::Session { op, what } if what.starts_with("Mutate") => Some(format!("send {op}")),
        Effect::Session { what, .. } if what.starts_with("Cancel") => Some("cancel".to_string()),
        Effect::Session { .. } => None,
        Effect::Schedule { .. } => Some("schedule".to_string()),
        Effect::Transfer { what, .. } if what.starts_with("SelectRow") || what.starts_with("SelectConfirmed") => None,
        Effect::Transfer { what, .. } => { let inner = what.strip_prefix("MutationOutcome(").and_then(|rest| rest.strip_suffix(')')).unwrap_or(what); Some(format!("transfer {}", outcome_token(inner))) }
        Effect::Reply { what, .. } => Some(format!("reply {what}")),
        Effect::Drained(which) if *which == "LocalWrites" => None,
        Effect::Drained(_) => Some("drained".to_string()),
        Effect::Lifecycle(_) | Effect::Network(_) => None,
    }).collect()
}

fn own_answer(answer: &OutboxAnswer) -> String {
    match answer {
        OutboxAnswer::Receipt(_) => "Receipt".into(),
        OutboxAnswer::Outcome(outcome) => format!("Outcome:{}", outcome_token(&format!("{outcome:?}"))),
        OutboxAnswer::Ok => "Ok".into(),
        OutboxAnswer::Err(fault) => format!("Err({})", format!("{fault:?}").split(['(', ' ', '{']).next().unwrap_or("")),
        OutboxAnswer::Transfer { .. } => "Transfer".into(),
        other => format!("{other:?}").chars().take(24).collect(),
    }
}

pub(super) fn own_tokens(effects: &[OutboxEffect]) -> Vec<String> {
    effects.iter().filter_map(|effect| match effect {
        OutboxEffect::Store { op_id: Some(op), kind, .. } => Some(format!("store {op} {kind:?}")),
        OutboxEffect::Send { op_id, .. } => Some(format!("send {op_id}")),
        OutboxEffect::CancelRequest => Some("cancel".to_string()),
        OutboxEffect::Reply { answer, .. } => Some(format!("reply {}", own_answer(answer))),
        OutboxEffect::TransferOutcome { outcome } => Some(format!("transfer {}", outcome_token(&format!("{outcome:?}")))),
        OutboxEffect::Schedule => Some("schedule".to_string()),
        OutboxEffect::Drained => Some("drained".to_string()),
        _ => None,
    }).collect()
}

struct Reference { state: State, pairs: std::collections::BTreeMap<u64, u64> }

impl Reference {
    fn new(base: Base) -> Self {
        let mut reference = Self { state: State::new(), pairs: Default::default() };
        let (queue, pending, next_seq) = match base {
            Base::Fresh => (Vec::new(), Vec::new(), 1),
            Base::Restored { attempts, pending } => (vec![RestoreItem { op_id: 1, spec: spec(1, false, false), cycle_id: 1, request_id: 41, attempts, sent: pending, restore_failed: false, has_error_mark: false, answer: None }], if pending { vec![41] } else { Vec::new() }, 2),
        };
        for input in [
            RIn::EpochStarted { epoch: 1, identity: true, generations: Default::default() },
            RIn::RegisterOperations { declarations: vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())] },
            RIn::Restored { epoch: 1, next_seq, queue: Ok(queue), pending, pending_lost: false },
            RIn::RestoreReady { epoch: 1 },
            RIn::Online(true),
        ] { reference.feed(input); }
        assert!(matches!(reference.state.phase, Phase::Ready | Phase::Classified), "the reference is ready");
        reference
    }

    fn feed(&mut self, input: RIn) -> Vec<Effect> {
        let (state, effects) = step(&self.state, &input);
        self.state = state;
        effects
    }

    /// A Retry or Discard without an action asks Transfer which operation of the row is the smallest; Transfer owns none here, so the smallest open operation of the row answers.
    fn select(&mut self, input: RIn) -> Vec<Effect> {
        let mut effects = self.feed(input);
        let asked: Vec<u64> = self.state.selects.keys().copied().collect();
        for reply in asked {
            let version = self.state.selects.get(&reply).map(|entry| entry.4).unwrap_or(0);
            let open = self.state.ops.values().find(|op| op.row_id == "r" && op.stage != RStage::Done).map(|op| op.action.clone());
            effects.extend(self.feed(match open { Some(action) => RIn::SelectedOutbox { reply, action, version }, None => RIn::SelectedNone { reply, version } }));
        }
        effects
    }

    fn stages(&self) -> String { self.state.ops.values().map(|op| format!("{}:{:?}", op.id, op.stage)).collect::<Vec<_>>().join(",") }

    fn apply(&mut self, what: Step) -> Vec<String> {
        let op_cycle = |reference: &Reference, op: u64| reference.state.ops.get(&op).map_or(1, |found| found.cycle_id);
        let kind_of = |reference: &Reference, op: u64| match reference.state.ops.get(&op).map(|found| found.stage) {
            Some(RStage::PersistingAttempt) => Kind::Attempt,
            Some(RStage::Landing) => Kind::Land,
            Some(RStage::RollingBack) => Kind::Rollback,
            Some(RStage::MarkingFailed) => Kind::MarkFailed,
            Some(RStage::MarkingRetry) => Kind::Retry,
            _ => Kind::Submit,
        };
        let effects = match what {
            Step::Submit { row, keep, transfer } => self.feed(RIn::Submit { epoch: 1, op: spec(row, keep, transfer), flags: SubmitFlags { valid: true, send: SendKind::Declared, removes_receiver: false, leaves_scope: false, join_missing_row: false, intent_written: false } }),
            Step::Commit(op) => { let kind = kind_of(self, op); self.feed(RIn::Committed { epoch: 1, batch: Batch::Op(op), kind, revision: 11 }) }
            Step::Response(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Response { op_id: op, cycle_id: cycle, resp: Resp::Data(LandFacts { root_present: true, ..LandFacts::default() }), swap: None, pending_retained: None }) }
            Step::Error { op, temporary } => { let cycle = op_cycle(self, op); self.feed(RIn::TransportError { op_id: op, cycle_id: cycle, temporary, pending_retained: None }) }
            Step::Dropped(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Dropped { op_id: op, cycle_id: cycle, pending_retained: None }) }
            Step::Tick(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Tick { op_id: op, cycle_id: cycle }) }
            Step::Online(on) => self.feed(RIn::Online(on)),
            Step::Retry => self.feed(RIn::Retry { epoch: 1, sel: Sel::Row { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()) }, reply_to: Addr::Call }),
            Step::Discard => self.feed(RIn::Discard { epoch: 1, sel: Sel::Row { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()) }, reply_to: Addr::Call }),
            Step::Outcome(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Outcome { receipt: super::ref_outbox::Receipt { epoch: 1, op_id: op, cycle_id: cycle } }) }
            Step::EpochEnd => self.feed(RIn::EpochEnded(1, super::ref_outbox::EndCause::SessionEnded)),
            Step::Close => self.feed(RIn::Closing),
            Step::Stray => self.feed(RIn::TransportError { op_id: 999, cycle_id: 1, temporary: true, pending_retained: None }),
            Step::SubmitStale => self.feed(RIn::Submit { epoch: 2, op: spec(1, false, false), flags: SubmitFlags { valid: true, send: SendKind::Declared, removes_receiver: false, leaves_scope: false, join_missing_row: false, intent_written: false } }),
            Step::SubmitExpiring => { let mut op = spec(1, false, false); op.expires_after = Some(10_000); self.feed(RIn::Submit { epoch: 1, op, flags: SubmitFlags { valid: true, send: SendKind::Declared, removes_receiver: false, leaves_scope: false, join_missing_row: false, intent_written: false } }) }
            Step::Age => self.feed(RIn::Now(11_000)),
            Step::ResponseError { op, temporary } => { let cycle = op_cycle(self, op); self.feed(RIn::Response { op_id: op, cycle_id: cycle, resp: if temporary { Resp::Temporary { has_sequence: false } } else { Resp::Permanent }, swap: None, pending_retained: None }) }
            Step::RetryAny => self.select(RIn::Retry { epoch: 1, sel: Sel::Row { model: "Moment".into(), row_id: "r".into(), action: None }, reply_to: Addr::Call }),
            Step::DiscardAny => self.select(RIn::Discard { epoch: 1, sel: Sel::Row { model: "Moment".into(), row_id: "r".into(), action: None }, reply_to: Addr::Call }),
            Step::ModelsReset => self.feed(RIn::ModelsReset { models: vec!["Moment".into()], generations: [("Moment".to_string(), 1)].into_iter().collect() }),
            Step::SubmitAct { keep, transfer, act } => self.feed(RIn::Submit { epoch: 1, op: spec_act(1, keep, transfer, act), flags: SubmitFlags { valid: true, send: SendKind::Declared, removes_receiver: false, leaves_scope: false, join_missing_row: false, intent_written: false } }),
            Step::ResponseNotFound(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Response { op_id: op, cycle_id: cycle, resp: Resp::NotFound, swap: None, pending_retained: None }) }
            Step::ResponseSwap(op) => { let cycle = op_cycle(self, op); self.feed(RIn::Response { op_id: op, cycle_id: cycle, resp: Resp::Data(LandFacts { root_present: true, ..LandFacts::default() }), swap: Some(("temp-1".into(), "real-1".into())), pending_retained: None }) }
        };
        let mut tokens = ref_tokens(&effects);
        for token in tokens.iter_mut().filter(|token| token.starts_with("send ")) {
            let op: u64 = token[5..].parse().unwrap_or(0);
            let request = self.state.ops.get(&op).map_or(0, |found| found.request_id);
            let same = self.pairs.insert(op, request) == Some(request);
            token.push_str(if same { " same pair" } else { " new pair" });
        }
        tokens
    }
}

struct Owner { twin: Twin, pairs: std::collections::BTreeMap<u64, (u64, u64)> }

impl Owner {
    fn new(base: Base) -> Self {
        match base {
            Base::Fresh => Self { twin: Twin::new(), pairs: Default::default() },
            Base::Restored { attempts, pending } => {
                let mut twin = Twin::bare();
                twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())] });
                let stored = crate::owners::write::outbox::StoredOp { answered: None, op_id: 1, op: super::outbox_harness::model_op("r", Policy::Rollback, false), error_mark: false, pending: pending.then_some((41, 1)), attempts, cycle_id: 1, cause: None, restore_failed: None, restore_element: None };
                twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
                twin.post(OutboxInput::RestoreReady { epoch: super::outbox_harness::EPOCH });
                Self { twin, pairs: Default::default() }
            }
        }
    }

    fn stages(&self) -> String {
        let mut ids: Vec<u64> = self.twin.state().ops.keys().copied().collect();
        ids.sort_unstable();
        ids.iter().map(|id| format!("{id}:{}", self.twin.stage(*id).map_or("Done".to_string(), |stage| format!("{stage:?}").split('(').next().unwrap_or("").to_string()))).collect::<Vec<_>>().join(",")
    }

    fn apply(&mut self, what: Step) -> Vec<String> {
        use crate::owners::write::outbox::OutboxInput as I;
        let effects = match what {
            Step::Submit { row, keep, transfer } => { self.twin.submit(&row_name(row), if keep { Policy::Keep } else { Policy::Rollback }, transfer); self.twin.last.clone() }
            Step::Commit(op) => { if self.twin.batches.contains_key(&op) { self.twin.committed(op) } else { Vec::new() } }
            Step::Response(op) => self.twin.post(I::Response { op_id: op, rows: super::outbox_harness::server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }),
            Step::Error { op, temporary } => self.twin.post(I::TransportError { op_id: op, temporary }),
            Step::Dropped(op) => self.twin.post(I::Dropped { op_id: op }),
            Step::Tick(op) => self.twin.post(I::RetryDue { op_id: op, cycle_id: 1 }),
            Step::Online(on) => self.twin.post(I::Online(crate::owners::contract::Online(on))),
            Step::Retry => self.twin.post(I::Retry { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: 900 }),
            Step::Discard => self.twin.post(I::Discard { model: "Moment".into(), row_id: "r".into(), action: Some("update".into()), reply: 901 }),
            Step::Outcome(op) => { let mut receipt = self.twin.receipt(op, super::outbox_harness::EPOCH); if let Some(found) = self.twin.state().ops.get(&op) { receipt.cycle_id = found.cycle_id; } self.twin.post(I::Outcome { receipt, reply: 902, discard_on_failure: false }) }
            Step::EpochEnd => { super::outbox_harness::take_fx(&self.twin.log, None); self.twin.det.end_epoch(super::outbox_harness::EPOCH, crate::runtime::Cause::SessionReset); self.twin.run(); super::outbox_harness::take_fx(&self.twin.log, None) }
            Step::Close => { super::outbox_harness::take_fx(&self.twin.log, None); self.twin.det.close(); self.twin.run(); super::outbox_harness::take_fx(&self.twin.log, None) }
            Step::Stray => self.twin.post(I::TransportError { op_id: 999, temporary: true }),
            Step::SubmitStale => self.twin.post(I::Submit { epoch: super::outbox_harness::EPOCH + 1, op: super::outbox_harness::model_op("r", Policy::Rollback, false), reply: 950 }),
            Step::SubmitExpiring => { let op = crate::owners::write::outbox::NewOp { expires_after: Some(std::time::Duration::from_secs(10)), ..super::outbox_harness::model_op("r", Policy::Rollback, false) }; self.twin.post(I::Submit { epoch: super::outbox_harness::EPOCH, op, reply: 951 }) }
            Step::Age => self.twin.post(I::Tick(crate::runtime::Time(11_000))),
            Step::ResponseError { op, temporary } => self.twin.post(I::Response { op_id: op, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(temporary) }),
            Step::RetryAny => self.twin.post(I::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 903 }),
            Step::DiscardAny => self.twin.post(I::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 904 }),
            Step::ModelsReset => self.twin.post(I::ModelsReset(crate::owners::contract::ModelsReset { epoch: super::outbox_harness::EPOCH, generation: 1, models: vec!["Moment".into()] })),
            Step::SubmitAct { keep, transfer, act } => { self.twin.submit_as("r", if keep { Policy::Keep } else { Policy::Rollback }, transfer, act.name()); self.twin.last.clone() }
            Step::ResponseNotFound(op) => { let destroy = self.twin.state().ops.get(&op).is_some_and(|found| found.op.action == "destroy"); self.twin.post(I::Response { op_id: op, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: if destroy { None } else { Some(false) } }) }
            Step::ResponseSwap(op) => self.twin.post(I::Response { op_id: op, rows: super::outbox_harness::server_rows(), swaps: vec![("temp-1".to_string(), "real-1".to_string())], stale: Vec::new(), result: None, missing: Vec::new(), errors: None }),
        };
        let mut tokens = own_tokens(&effects);
        let sends: Vec<(u64, (u64, u64))> = effects.iter().filter_map(|effect| match effect { OutboxEffect::Send { op_id, request_id, sequence } => Some((*op_id, (*request_id, *sequence))), _ => None }).collect();
        let mut sends = sends.into_iter();
        for token in tokens.iter_mut().filter(|token| token.starts_with("send ")) {
            if let Some((op, pair)) = sends.next() {
                let same = self.pairs.insert(op, pair) == Some(pair);
                token.push_str(if same { " same pair" } else { " new pair" });
            }
        }
        tokens
    }
}

#[derive(Clone, Copy)]
enum Base { Fresh, Restored { attempts: u32, pending: bool } }

const R1: Step = Step::Submit { row: 1, keep: false, transfer: false };

struct Context {
    act: Act,
    name: &'static str,
    base: Base,
    prefix: Vec<Step>,
    target: u64,
    restored: bool,
}

fn contexts() -> Vec<Context> {
    let fresh = |name, prefix, target| Context { act: Act::Update, name, base: Base::Fresh, prefix, target, restored: false };
    vec![
        fresh("online", vec![], 1),
        fresh("offline", vec![Step::Online(false)], 1),
        fresh("row busy", vec![Step::Submit { row: 1, keep: false, transfer: false }, Step::Commit(1)], 2),
        fresh("slot taken", vec![Step::Submit { row: 2, keep: false, transfer: false }, Step::Commit(1)], 2),
        Context { act: Act::Update, name: "attempts exhausted", base: Base::Restored { attempts: 39, pending: false }, prefix: vec![], target: 1, restored: true },
        Context { act: Act::Update, name: "restored with pending pair", base: Base::Restored { attempts: 1, pending: true }, prefix: vec![], target: 1, restored: true },
        Context { act: Act::Destroy, name: "destroy action", base: Base::Fresh, prefix: vec![], target: 1, restored: false },
        Context { act: Act::Create, name: "create action", base: Base::Fresh, prefix: vec![], target: 1, restored: false },
        Context { act: Act::Destroy, name: "destroy of an unsent create", base: Base::Fresh, prefix: vec![Step::Online(false), Step::SubmitAct { keep: false, transfer: false, act: Act::Create }, Step::Commit(1)], target: 2, restored: false },
    ]
}

/// The stage a script must bring the target operation to (`""` = no operation).
fn goal(name: &str) -> &'static str {
    match name {
        "none" => "",
        "submitting" => "Submitting",
        "queued or sending" => "Queued|Sending",
        "landing" => "Landing",
        "waiting retry" | "transfer origin sending" => "Sending",
        "persisting attempt" => "PersistingAttempt",
        "marking failed" => "MarkingFailed",
        "failed" => "Failed",
        "marking retry" => "MarkingRetry",
        "rolling back" | "discarding" => "RollingBack",
        "expired queued" => "Queued",
        _ => "",
    }
}

fn scripts(context: &Context) -> Vec<(&'static str, Vec<Step>)> {
    use Step::*;
    let t = context.target;
    if context.restored {
        let mut list = vec![
            ("none", vec![]),
            ("queued or sending", vec![Commit(t)]),
            ("landing", vec![Commit(t), Response(t)]),
            ("marking failed", vec![Commit(t), Error { op: t, temporary: false }]),
        ];
        if context.name != "attempts exhausted" {
            list.push(("waiting retry", vec![Commit(t), Error { op: t, temporary: true }]));
            list.push(("persisting attempt", vec![Commit(t), Error { op: t, temporary: true }, Tick(t)]));
        }
        return list;
    }
    let mut list = vec![
        ("none", vec![]),
        ("submitting", vec![R1]),
        ("queued or sending", vec![R1, Commit(t)]),
        ("landing", vec![R1, Commit(t), Response(t)]),
        ("waiting retry", vec![R1, Commit(t), Error { op: t, temporary: true }]),
        ("persisting attempt", vec![R1, Commit(t), Error { op: t, temporary: true }, Tick(t)]),
        ("marking failed", vec![Submit { row: 1, keep: true, transfer: false }, Commit(t), Error { op: t, temporary: false }]),
        ("failed", vec![Submit { row: 1, keep: true, transfer: false }, Commit(t), Error { op: t, temporary: false }, Commit(t)]),
        ("marking retry", vec![Submit { row: 1, keep: true, transfer: false }, Commit(t), Error { op: t, temporary: false }, Commit(t), Retry]),
        ("rolling back", vec![R1, Commit(t), Error { op: t, temporary: false }]),
        ("discarding", vec![R1, Commit(t), Discard]),
        ("transfer origin sending", vec![Submit { row: 1, keep: false, transfer: true }, Commit(t)]),
    ];
    if context.name == "offline" { list.push(("expired queued", vec![SubmitExpiring, Commit(t), Age])); }
    list
}

fn seconds(target: u64) -> Vec<(&'static str, Option<Step>)> {
    use Step::*;
    vec![
        ("-", None), ("Commit", Some(Commit(target))), ("Response", Some(Response(target))),
        ("TransportError temporary", Some(Error { op: target, temporary: true })), ("TransportError permanent", Some(Error { op: target, temporary: false })),
        ("Dropped", Some(Dropped(target))), ("Tick", Some(Tick(target))), ("Offline", Some(Online(false))), ("Online", Some(Online(true))),
        ("Retry", Some(Retry)), ("Discard", Some(Discard)), ("Outcome", Some(Outcome(target))), ("EpochEnded", Some(EpochEnd)), ("Closing", Some(Close)), ("Stray", Some(Stray)), ("Response NotFound", Some(ResponseNotFound(target))), ("Response with temporary errors", Some(ResponseError { op: target, temporary: true })), ("Response with permanent errors", Some(ResponseError { op: target, temporary: false })), ("Response with a swap", Some(ResponseSwap(target))), ("Retry without an action", Some(RetryAny)), ("Discard without an action", Some(DiscardAny)), ("ModelsReset", Some(ModelsReset)), ("Submit with another epoch", Some(SubmitStale)),
    ]
}

pub(super) fn split_line(stages: &str, tokens: &[String]) -> String {
    let replies: Vec<&str> = tokens.iter().filter(|token| token.starts_with("reply ")).map(String::as_str).collect();
    let effects: Vec<&str> = tokens.iter().filter(|token| !token.starts_with("reply ")).map(String::as_str).collect();
    format!("{stages} effects [{}] replies [{}]", effects.join("; "), replies.join("; "))
}

pub(super) fn first_line(payload: &(dyn std::any::Any + Send)) -> String {
    payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default().lines().next().unwrap_or("").chars().take(110).collect()
}

fn retarget(step: Step, act: Act) -> Step {
    match step {
        Step::Submit { row: 1, keep, transfer } if act != Act::Update => Step::SubmitAct { keep, transfer, act },
        other => other,
    }
}

fn run(index: usize) {
    let _ = OutboxInput::Online;
    let mut matrix = Matrix::new(Box::leak(format!("S-outbox-v2-{index}").into_boxed_str()));
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for context in contexts().into_iter().skip(index).take(1) {
        let target = context.target;
        for (name, script) in scripts(&context) {
            let path = format!("{} / {name}", context.name);
            let reach = catch_unwind(AssertUnwindSafe(|| {
                let (mut reference, mut owner) = (Reference::new(context.base), Owner::new(context.base));
                for what in context.prefix.iter().copied().chain(script.iter().copied().map(|step| retarget(step, context.act))) { reference.apply(what); owner.apply(what); }
                (reference.state.ops.get(&target).map_or(String::new(), |op| format!("{:?}", op.stage)), owner.twin.stage(target).map_or(String::new(), |stage| format!("{stage:?}").split('(').next().unwrap_or("").to_string()))
            }));
            let wanted = goal(name);
            let reached = |stage: &str| wanted.split('|').any(|option| option == stage);
            match &reach {
                Ok((left, right)) => {
                    if reached(left) && reached(right) {
                        matrix.check(&path, "reach", "target stage", wanted, wanted);
                    } else if left == right {
                        let dead = format!("dead cell: reference and owner both {left:?}");
                        matrix.check(&path, "reach", "target stage", &dead, &dead);
                    } else {
                        matrix.check(&path, "reach", "target stage", wanted, &format!("not reached: reference {left:?}, owner {right:?}"));
                    }
                }
                Err(_) => { matrix.check(&path, "reach", "target stage", wanted, "not reached: the script panics"); }
            }
            for (second_name, second) in seconds(target) {
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    let (mut reference, mut owner) = (Reference::new(context.base), Owner::new(context.base));
                    let mut seen: Vec<(String, String, String)> = Vec::new();
                    for what in context.prefix.iter().copied().chain(script.iter().copied().map(|step| retarget(step, context.act))).chain(second).chain(second.map(|_| Step::Commit(target))) {
                        let (left, right) = (reference.apply(what), owner.apply(what));
                        seen.push((format!("{what:?}"), split_line(&reference.stages(), &left), split_line(&owner.stages(), &right)));
                    }
                    seen
                }));
                let (expected, observed) = match outcome {
                    Ok(seen) => match seen.iter().find(|row| row.1 != row.2) {
                        Some(row) => (format!("{} -> {}", row.0, row.1), format!("{} -> {}", row.0, row.2)),
                        None => ("agree".to_string(), "agree".to_string()),
                    },
                    Err(payload) => ("the script runs".to_string(), format!("panic: {}", first_line(payload.as_ref()))),
                };
                matrix.check(&path, "ref_outbox", second_name, &expected, &observed);
            }
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}

#[test]
fn matrix_owner_state_input_outbox_online() { run(0) }

#[test]
fn matrix_owner_state_input_outbox_offline() { run(1) }

#[test]
fn matrix_owner_state_input_outbox_row_busy() { run(2) }

#[test]
fn matrix_owner_state_input_outbox_slot_taken() { run(3) }

#[test]
fn matrix_owner_state_input_outbox_attempts_exhausted() { run(4) }

#[test]
fn matrix_owner_state_input_outbox_restored_pending() { run(5) }

#[test]
fn matrix_owner_state_input_outbox_destroy() { run(6) }

#[test]
fn matrix_owner_state_input_outbox_create() { run(7) }

#[test]
fn matrix_owner_state_input_outbox_destroy_unsent_create() { run(8) }
