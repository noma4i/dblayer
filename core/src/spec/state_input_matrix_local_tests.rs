//! Matrix S for LocalWrites against `ref_outbox` (v2 `local.rs`): a set_ephemeral write, with and without expiry, is played on the reference and on the owner through scripts (none, writing, writing with expiry, committed, committed with expiry) and every second input (second write, commit, reject by reason, epoch end, closing, expire). After every step the ordered effects (store, schedule, drained) and the ordered replies (Ok, Err(X)) must be equal. Kinds SetLocal, Attach, Detach, AttachBytes, ModelWrite are not driven: they need rows of the stand and the flags of the table (listed in the report).
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::outbox_harness::{OutboxEffect, Twin, EPOCH};
use super::ref_outbox::{step, Addr, Batch, Effect, Input as RIn, Kind, LFlags, LKind, Reason, State};
use crate::owners::contract::{Committed, Rejected};
use crate::owners::write::outbox::{LocalWriteKind, OutboxAnswer, OutboxInput};
use crate::runtime::Time;
use crate::test_matrix::Matrix;

#[derive(Clone, Copy, Debug)]
enum Step {
    Write { expiry: bool },
    Commit,
    Reject(&'static str),
    EpochEnd,
    Close,
    Expire,
}

fn ref_tokens(effects: &[Effect]) -> (Vec<String>, Vec<String>) {
    let mut own = Vec::new();
    let mut replies = Vec::new();
    for effect in effects {
        match effect {
            Effect::Store { what, .. } if what.contains("EphemeralField") || what.contains("rows local") => own.push("store write".to_string()),
            Effect::Store { what, .. } if what.contains("ExpireRow") => own.push("store remove row".to_string()),
            Effect::Store { what, .. } if what.contains("basis=generations") => own.push("store write".to_string()),
            Effect::Store { what, .. } => own.push(format!("store {what}")),
            Effect::Schedule { .. } => own.push("schedule".to_string()),
            Effect::Drained("LocalWrites") => { if own.last().map(String::as_str) != Some("drained") { own.push("drained".to_string()); } }
            Effect::Reply { what, to: Addr::Call, .. } => replies.push(format!("reply {what}")),
            _ => {}
        }
    }
    (own, replies)
}

fn own_tokens(effects: &[OutboxEffect]) -> (Vec<String>, Vec<String>) {
    let mut own = Vec::new();
    let mut replies = Vec::new();
    for effect in effects {
        match effect {
            OutboxEffect::Store { kind, .. } => own.push(match format!("{kind:?}").as_str() { "LocalWrite" => "store write".to_string(), "RemoveRow" => "store remove row".to_string(), other => format!("store {other}") }),
            OutboxEffect::Schedule => own.push("schedule".to_string()),
            OutboxEffect::Drained => own.push("drained".to_string()),
            OutboxEffect::Reply { answer: OutboxAnswer::Written { .. }, .. } => replies.push("reply Ok".to_string()),
            OutboxEffect::Reply { answer, .. } => replies.push(format!("reply {}", match answer { OutboxAnswer::Err(crate::owners::write::outbox::Fault::Rejected(reason)) => format!("Err({})", format!("{reason:?}").split(['(', ' ', '{']).next().unwrap_or("")),
            OutboxAnswer::Err(fault) => format!("Err({})", format!("{fault:?}").split(['(', ' ', '{']).next().unwrap_or("")), other => format!("{other:?}").chars().take(20).collect() })),
            _ => {}
        }
    }
    (own, replies)
}

struct Reference { state: State }

impl Reference {
    fn new() -> Self {
        let mut reference = Self { state: State::new() };
        for input in [
            RIn::EpochStarted { epoch: 1, identity: true, generations: Default::default() },
            RIn::RegisterOperations { declarations: Vec::new() },
            RIn::Restored { epoch: 1, next_seq: 1, queue: Ok(Vec::new()), pending: Vec::new(), pending_lost: false },
            RIn::RestoreReady { epoch: 1 },
            RIn::Online(true),
        ] { reference.feed(input); }
        reference
    }

    fn feed(&mut self, input: RIn) -> Vec<Effect> {
        let (state, effects) = step(&self.state, &input);
        self.state = state;
        effects
    }

    fn apply(&mut self, what: Step, id: &mut u64) -> (Vec<String>, Vec<String>) {
        let effects = match what {
            Step::Write { expiry } => { *id += 1; self.feed(RIn::LocalWrite { epoch: 1, id: *id, kind: LKind::SetEphemeral, flags: LFlags { row_exists: true, expiry, ..LFlags::default() } }) }
            Step::Commit => self.feed(RIn::Committed { epoch: 1, batch: Batch::Local(*id), kind: Kind::Submit, revision: 11 }),
            Step::Reject(reason) => self.feed(RIn::Rejected { epoch: 1, batch: Batch::Local(*id), kind: Kind::Submit, reason: match reason { "NoSession" => Reason::NoSession, "Closed" => Reason::Closed, "Storage" => Reason::Storage, _ => Reason::StaleGeneration(Default::default()) } }),
            Step::EpochEnd => self.feed(RIn::EpochEnded(1)),
            Step::Close => self.feed(RIn::Closing),
            Step::Expire => self.feed(RIn::ExpireRow { model: "Moment".into(), row_id: "w".into(), write_revision: 11 }),
        };
        ref_tokens(&effects)
    }
}

struct Owner { twin: Twin, batch: u64 }

impl Owner {
    fn new() -> Self { Self { twin: Twin::new(), batch: 0 } }

    fn apply(&mut self, what: Step, id: &mut u64) -> (Vec<String>, Vec<String>) {
        let effects = match what {
            Step::Write { expiry } => { *id += 1; self.twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: expiry.then_some(10) }, row: "w".into(), values: "{}".into(), reply: *id, now: Time(100) }) }
            Step::Commit => self.twin.post(OutboxInput::Committed(Committed { batch: self.batch, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 })),
            Step::Reject(reason) => self.twin.post(OutboxInput::Rejected(Rejected { batch: self.batch, reason: match reason { "NoSession" => crate::owners::contract::RejectReason::NoSession, "Closed" => crate::owners::contract::RejectReason::Closed, "Storage" => crate::owners::contract::RejectReason::Storage("disk".into()), _ => crate::owners::contract::RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 9 } } })),
            Step::EpochEnd => { super::outbox_harness::take_fx(&self.twin.log, None); self.twin.det.end_epoch(EPOCH, crate::runtime::Cause::SessionReset); self.twin.run(); super::outbox_harness::take_fx(&self.twin.log, None) }
            Step::Close => { super::outbox_harness::take_fx(&self.twin.log, None); self.twin.det.close(); self.twin.run(); super::outbox_harness::take_fx(&self.twin.log, None) }
            Step::Expire => self.twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(110) }),
        };
        for effect in &effects {
            if let OutboxEffect::Store { batch, kind, .. } = effect { if format!("{kind:?}") == "LocalWrite" { self.batch = *batch; } }
        }
        own_tokens(&effects)
    }
}

fn scripts() -> Vec<(&'static str, Vec<Step>)> {
    use Step::*;
    vec![
        ("none", vec![]),
        ("writing", vec![Write { expiry: false }]),
        ("writing with expiry", vec![Write { expiry: true }]),
        ("committed", vec![Write { expiry: false }, Commit]),
        ("committed with expiry", vec![Write { expiry: true }, Commit]),
    ]
}

fn seconds() -> Vec<(&'static str, Option<Step>)> {
    use Step::*;
    vec![("-", None), ("second write", Some(Write { expiry: false })), ("Commit", Some(Commit)), ("Rejected NoSession", Some(Reject("NoSession"))), ("Rejected Closed", Some(Reject("Closed"))), ("Rejected Storage", Some(Reject("Storage"))), ("Rejected StaleGeneration", Some(Reject("Stale"))), ("EpochEnded", Some(EpochEnd)), ("Closing", Some(Close)), ("Expire", Some(Expire))]
}

fn line(tokens: &(Vec<String>, Vec<String>)) -> String { format!("effects [{}] replies [{}]", tokens.0.join("; "), tokens.1.join("; ")) }

#[test]
fn matrix_owner_state_input_local_writes() {
    let mut matrix = Matrix::new("S-local-writes");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (name, script) in scripts() {
        for (second_name, second) in seconds() {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let (mut reference, mut owner) = (Reference::new(), Owner::new());
                let (mut left_id, mut right_id) = (0u64, 0u64);
                let mut seen = Vec::new();
                for what in script.iter().copied().chain(second) {
                    let (left, right) = (reference.apply(what, &mut left_id), owner.apply(what, &mut right_id));
                    seen.push((format!("{what:?}"), line(&left), line(&right)));
                }
                seen
            }));
            let (expected, observed) = match outcome {
                Ok(seen) => match seen.iter().find(|row| row.1 != row.2) {
                    Some(row) => (format!("{} -> {}", row.0, row.1), format!("{} -> {}", row.0, row.2)),
                    None => ("agree".to_string(), "agree".to_string()),
                },
                Err(payload) => ("the script runs".to_string(), format!("panic: {}", payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default().lines().next().unwrap_or("").chars().take(110).collect::<String>())),
            };
            matrix.check(name, "ref_outbox local", second_name, &expected, &observed);
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
