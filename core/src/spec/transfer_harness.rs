//! Harness of the owner-level tests of the `transfer` machine: the owner `Transfers` on `DetScheduler`, the twin of the reference and the book of what it sent.


use crate::schema::OnFailure;
use super::ref_transfer::{step_of, Ctx, Eff, In, Kind, Noise, Obs, Shown, St, Step};
use crate::owners::contract::{Online};
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::files::Location;
use crate::owners::support::{new_log, text_after, Discard, Emitted, Log, Silent, Sink, Spy, SpyState};
use crate::owners::store::StoreWriter;
use crate::owners::write::outbox::{NewOp, Origin, Outbox};
use crate::owners::write::transfer::{self as t, Source, StoreKind, StoredTransfer, Steps, TransferAnswer, TransferEffect as Run, TransferInput, TransferPeers, TransferStage, TransferState, TransferTick, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use crate::transport::NetworkPurpose;

/// The slots of the peers and of the owner in every test: the peers first.
pub(super) const STORE_AT: usize = 0;
pub(super) const OUTBOX_AT: usize = 1;
pub(super) const TRANSFER_AT: usize = 2;

/// What the owner gave out in a step, in the terms the rows of the table use: the executor effects (`Run`) and the inputs for the Store and
/// Outbox (`Send`), read from the `Debug` form the `Spy` keeps. `Schedule` is a timer of the owner.
#[derive(Clone, Debug)]
pub(super) enum TransferEffect {
    Copy { key: String },
    CopyCancel { key: String },
    Store { key: String, kind: StoreKind },
    Step { key: String, purpose: NetworkPurpose },
    Complete { key: String },
    Abort { key: String },
    CancelStep { key: String },
    DeleteFile { key: String },
    SubmitMutation { key: String },
    DiscardMutation { key: String },
    Reply { reply: u64, answer: TransferAnswer },
    Shows { key: String, shown: t::Shown },
    Refused { key: String },
    ScheduleJob { epoch: u64 },
    EndJob { round_id: u32 },
    Schedule,
    Drained,
}

/// The kind of an `Upload` batch: the op names it.
pub(super) fn upload_kind(input: &str) -> StoreKind {
    let kind = input.rsplit("kind: ").next().unwrap_or_default();
    [("SyncUpload", StoreKind::SyncUpload), ("ConfirmedParts", StoreKind::ConfirmedParts), ("Completed", StoreKind::Completed), ("Failed", StoreKind::Failed), ("Retrying", StoreKind::Retrying)]
        .into_iter()
        .find(|(name, _)| kind.starts_with(name))
        .map(|(_, kind)| kind)
        .unwrap_or_else(|| panic!("an Upload without a kind: {input}"))
}

pub(super) fn lower(emitted: Emitted<Run>) -> Option<TransferEffect> {
    Some(match emitted {
        Emitted::Run(Run::Copy { key, .. }) => TransferEffect::Copy { key },
        Emitted::Run(Run::CopyCancel { key }) => TransferEffect::CopyCancel { key },
        Emitted::Run(Run::Step { key, purpose, .. }) => TransferEffect::Step { key, purpose },
        Emitted::Run(Run::Complete { key, .. }) => TransferEffect::Complete { key },
        Emitted::Run(Run::Abort { key, .. }) => TransferEffect::Abort { key },
        Emitted::Run(Run::CancelStep { key }) => TransferEffect::CancelStep { key },
        Emitted::Run(Run::DeleteFile { path }) => TransferEffect::DeleteFile { key: path },
        Emitted::Run(Run::Reply { reply, answer }) => TransferEffect::Reply { reply, answer },
        Emitted::Run(Run::Shows { key, shown }) => TransferEffect::Shows { key, shown },
        Emitted::Run(Run::Refused { key }) => TransferEffect::Refused { key },
        Emitted::Run(Run::ScheduleJob { epoch }) => TransferEffect::ScheduleJob { epoch },
        Emitted::Run(Run::EndJob { round_id }) => TransferEffect::EndJob { round_id },
        Emitted::Run(Run::Drained(_)) => TransferEffect::Drained,
        Emitted::Send { to: STORE_AT, input } => {
            let key = text_after(&input, "key: ")?;
            let kind = if input.contains("RemoveUpload {") { StoreKind::Removed } else { upload_kind(&input) };
            TransferEffect::Store { key, kind }
        }
        Emitted::Send { to: OUTBOX_AT, input } if input.starts_with("Submit") => TransferEffect::SubmitMutation { key: text_after(&input, "origin: Transfer(")? },
        Emitted::Send { to: OUTBOX_AT, input } => TransferEffect::DiscardMutation { key: text_after(&input, "row_id: ")? },
        Emitted::Send { to, .. } => panic!("a send to the slot {to}"),
        Emitted::Schedule { .. } => TransferEffect::Schedule,
        Emitted::Cancel(_) | Emitted::Broadcast(_) => return None,
    })
}

pub(super) fn take_fx(log: &Log<Emitted<Run>>) -> Vec<TransferEffect> { crate::owners::support::take(log).into_iter().filter_map(lower).collect() }

pub(super) fn state_of(det: &DetScheduler) -> Result<&TransferState, String> { det.state(Address::<Spy<Transfers>>::new(TRANSFER_AT)).map(|spy: &SpyState<Transfers>| &spy.inner).ok_or_else(|| "no state".to_string()) }

/// The owner with its two peers: the Store and Outbox keep what they receive.
pub(super) fn world(seed: u64, slots: u32) -> (DetScheduler, Log<Emitted<Run>>) {
    let mut det = DetScheduler::new(seed);
    let store = det.add::<Sink<StoreWriter>>("store", new_log(), Silent).index();
    let outbox = det.add::<Sink<Outbox>>("outbox", new_log(), Silent).index();
    assert_eq!((store, outbox), (STORE_AT, OUTBOX_AT));
    let log = new_log();
    let peers = TransferPeers { store: Address::new(store), outbox: Address::new(outbox) };
    let addr = det.add::<Spy<Transfers>>("transfer", SpyState::new(TransferState::new(EPOCH, slots, peers), &log), Discard);
    assert_eq!(addr.index(), TRANSFER_AT);
    (det, log)
}

pub(super) fn mutation() -> NewOp {
    NewOp { model: "Moment".into(), row_id: KEY.into(), action: "upload".into(), on_failure: OnFailure::Keep, origin: Origin::Api, path: "/moments".into(), body: String::new(), effects: Default::default(), durable: true }
}

pub(super) fn steps() -> Steps { Steps { path: "/upload".into(), body: String::new() } }

pub(super) const EPOCH: u64 = 1;
pub(super) const KEY: &str = "k1";
pub(super) const PARTS: u32 = 3;
pub(super) const DAY: u64 = 86_400_000;

pub(super) fn stage_of(stage: TransferStage) -> St {
    match stage {
        TransferStage::Copying => St::Copying,
        TransferStage::Ready => St::Ready,
        TransferStage::Uploading => St::Uploading,
        TransferStage::Completing => St::Completing,
        TransferStage::Mutating => St::Mutating,
        TransferStage::Failed => St::Failed,
        TransferStage::Done => St::Done,
    }
}

pub(super) fn source() -> Source { Source { path: "a.mp4".into(), parts: PARTS } }

pub(super) struct Twin {
    pub(super) det: DetScheduler,
    pub(super) addr: Address<Transfers>,
    pub(super) log: Log<Emitted<Run>>,
    pub(super) reply: u64,
}

impl Twin {
    pub(super) fn new() -> Self {
        let (mut det, log) = world(7, 1);
        det.start_epoch(EPOCH);
        Self { det, addr: Address::new(TRANSFER_AT), log, reply: 100 }
    }

    pub(super) fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    pub(super) fn post(&mut self, input: TransferInput) -> Vec<TransferEffect> {
        take_fx(&self.log);
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
        take_fx(&self.log)
    }

    pub(super) fn control(&mut self, input: TransferInput) -> Vec<TransferEffect> {
        take_fx(&self.log);
        self.det.post(self.addr, EpochStamp::Control, input);
        self.run();
        take_fx(&self.log)
    }

    pub(super) fn state(&self) -> &TransferState { state_of(&self.det).expect("the owner state") }

    pub(super) fn stage(&self, key: &str) -> St { self.state().stage_of(key).map_or(St::Done, stage_of) }

    pub(super) fn reply(&mut self) -> u64 {
        self.reply += 1;
        self.reply
    }

    pub(super) fn accept(&mut self, key: &str, batch: bool) -> Vec<TransferEffect> {
        let reply = self.reply();
        self.post(TransferInput::Accept { epoch: EPOCH, key: key.into(), source: source(), batch, reply, now: Time(0), mutation: mutation(), steps: steps() })
    }
}

pub(super) fn stored(key: &str, ctx: &Ctx, failed: bool, completed: bool, confirmed: u32) -> StoredTransfer {
    StoredTransfer {
        key: key.into(),
        seq: 1,
        source: source(),
        batch: ctx.kind == Kind::Batch,
        upload_id: ctx.has_upload_id.then(|| "u1".to_string()),
        confirmed_parts: confirmed,
        completed,
        failed: failed.then(|| "e".to_string()),
        file_present: true,
        created_at: Time(0),
        place: Location::Directory,
        size: 1,
        sha256: "h".into(),
        mutation: mutation(),
        steps: steps(),
    }
}

/// Brings the owner to `state` of the transfer `k1` under the conditions of `ctx`.
pub(super) fn reach(state: St, ctx: &Ctx) -> Twin {
    let mut twin = Twin::new();
    let batch = ctx.kind == Kind::Batch;
    let last_part = if ctx.parts_all_confirmed { PARTS - 1 } else { 0 };
    match state {
        St::None => {}
        St::Copying => { twin.accept(KEY, batch); }
        St::Ready => {
            twin.post(TransferInput::Online(Online(false)));
            if !ctx.first_to_start {
                twin.accept("k0", false);
                twin.post(TransferInput::CopyDone { key: "k0".into(), size: 1, sha256: "h".into(), place: Location::Directory });
            }
            twin.accept(KEY, batch);
            twin.post(TransferInput::CopyDone { key: KEY.into(), size: 1, sha256: "h".into(), place: Location::Directory });
        }
        St::Uploading => {
            twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, false, last_part)]));
            if !ctx.attempts_left {
                for _ in 0..49 {
                    twin.post(TransferInput::StepError { key: KEY.into(), part: Some(1), temporary: true });
                    twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key: KEY.into() } });
                }
            }
        }
        St::Completing => {
            twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, false, PARTS)]));
            twin.post(TransferInput::CompleteError { key: KEY.into(), temporary: true });
        }
        St::Mutating => { twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, true, PARTS)])); }
        St::Failed => {
            let confirmed = if ctx.parts_all_confirmed { PARTS } else { 0 };
            twin.post(TransferInput::Restored(vec![stored(KEY, ctx, true, false, confirmed)]));
        }
        St::Done => {
            twin.accept(KEY, batch);
            let reply = twin.reply();
            twin.post(TransferInput::Discard { key: KEY.into(), reply });
        }
    }
    if !ctx.online && state != St::Ready {
        twin.post(TransferInput::Online(Online(false)));
    }
    if state != St::None {
        assert_eq!(twin.stage(KEY), if state == St::Done { St::Done } else { state }, "the owner reached {state:?}");
    }
    twin
}

pub(super) fn input_of(twin: &mut Twin, ctx: &Ctx, input: In) -> Vec<TransferEffect> {
    let key: String = KEY.into();
    let part = if ctx.stray_step { Some(99) } else { Some(if ctx.parts_all_confirmed { PARTS } else { 1 }) };
    let reply = twin.reply();
    match input {
        In::Accept => {
            let epoch = if ctx.epoch_current { EPOCH } else { EPOCH + 1 };
            twin.post(TransferInput::Accept { epoch, key, source: source(), batch: ctx.kind == Kind::Batch, reply, now: Time(0), mutation: mutation(), steps: steps() })
        }
        In::CopyDone => twin.post(TransferInput::CopyDone { key, size: 1, sha256: "h".into(), place: Location::Directory }),
        In::CopyFailed => twin.post(TransferInput::CopyFailed { key, error: "e".into() }),
        In::Cancel => twin.post(TransferInput::Cancel { key, reply }),
        In::Start => twin.post(TransferInput::Online(Online(true))),
        In::StepOk { parts_left } => twin.post(TransferInput::StepOk { key, part: if ctx.stray_step { Some(99) } else if parts_left { Some(1) } else { Some(PARTS) }, upload_id: None }),
        In::StepError { temporary } => twin.post(TransferInput::StepError { key, part, temporary }),
        In::Online(on) => twin.post(TransferInput::Online(Online(on))),
        In::Completed => twin.post(TransferInput::Completed { key }),
        In::CompleteError => twin.post(TransferInput::CompleteError { key, temporary: false }),
        In::CompleteTemp => twin.post(TransferInput::CompleteError { key, temporary: true }),
        In::MutationOutcome { confirmed } => twin.post(TransferInput::MutationOutcome { key, outcome: if confirmed { OutboxOutcome::Confirmed { revision: 1 } } else { OutboxOutcome::Rejected { error: "e".into() } } }),
        In::MutationDiscarded => twin.post(TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Discarded }),
        In::MutationInterrupted => twin.post(TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Interrupted }),
        In::Retry => twin.post(TransferInput::Retry { key, reply }),
        In::Discard => twin.post(TransferInput::Discard { key, reply }),
        In::Expire => twin.post(TransferInput::Tick { now: Time(ctx.age_days * DAY), kind: TransferTick::Expire }),
        In::TickRetry => twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key } }),
        In::SlotFreed => twin.post(TransferInput::SlotFreed),
        In::EpochEnded => {
            take_fx(&twin.log);
            twin.det.end_epoch(EPOCH, Cause::SessionReset);
            twin.run();
            take_fx(&twin.log)
        }
        In::Closing => {
            take_fx(&twin.log);
            twin.det.close();
            twin.run();
            take_fx(&twin.log)
        }
        In::Noise(noise) => twin.post(match noise {
            Noise::StepOk => TransferInput::StepOk { key, part: Some(1), upload_id: None },
            Noise::StepError => TransferInput::StepError { key, part: Some(1), temporary: true },
            Noise::Completed => TransferInput::Completed { key },
            Noise::CompleteError => TransferInput::CompleteError { key, temporary: false },
            Noise::MutationOutcome => TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Confirmed { revision: 1 } },
            Noise::TickRetry => TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key } },
            Noise::OnlineTrue => TransferInput::Online(Online(true)),
            Noise::OnlineFalse => TransferInput::Online(Online(false)),
            Noise::SlotFreed => TransferInput::SlotFreed,
        }),
    }
}

pub(super) fn effect_key(effect: &TransferEffect) -> Option<&str> {
    match effect {
        TransferEffect::Copy { key } | TransferEffect::CopyCancel { key } | TransferEffect::Store { key, .. } | TransferEffect::Step { key, .. } | TransferEffect::Complete { key } | TransferEffect::Abort { key } | TransferEffect::CancelStep { key } | TransferEffect::DeleteFile { key } | TransferEffect::SubmitMutation { key } | TransferEffect::DiscardMutation { key } | TransferEffect::Shows { key, .. } | TransferEffect::Refused { key } => Some(key),
        TransferEffect::Reply { .. } | TransferEffect::ScheduleJob { .. } | TransferEffect::EndJob { .. } | TransferEffect::Schedule | TransferEffect::Drained => None,
    }
}

pub(super) fn shown_of(shown: &t::Shown) -> Shown {
    match shown {
        t::Shown::Copying => Shown::Copying,
        t::Shown::Queued => Shown::Queued,
        t::Shown::Uploading { .. } => Shown::Uploading,
        t::Shown::Failed { .. } => Shown::Failed,
        t::Shown::Gone => Shown::Gone,
    }
}

pub(super) fn translate(input: In, effects: Vec<TransferEffect>, scheduled: bool) -> (Vec<Obs>, Vec<Eff>) {
    let mut obs = Vec::new();
    let mut eff: Vec<Eff> = Vec::new();
    for effect in effects {
        if effect_key(&effect).is_some_and(|key| key != KEY) {
            continue;
        }
        match effect {
            TransferEffect::Copy { .. } => eff.push(Eff::Copy),
            TransferEffect::CopyCancel { .. } => eff.push(Eff::CopyCancelled),
            TransferEffect::Store { kind, .. } => {
                if kind == StoreKind::Removed && eff.last() == Some(&Eff::DeleteFile) {
                    eff.pop();
                }
                eff.push(match kind {
                    StoreKind::SyncUpload => Eff::StoreSyncUpload,
                    StoreKind::ConfirmedParts => Eff::StoreConfirmedParts,
                    StoreKind::Completed => Eff::StoreCompleted,
                    StoreKind::Failed => Eff::StoreFailed,
                    StoreKind::Retrying => Eff::StoreRetrying,
                    StoreKind::Removed => Eff::DeleteFileAndOperation,
                });
            }
            TransferEffect::Step { purpose, .. } => {
                assert_eq!(purpose, NetworkPurpose::Transfer, "P-XFER-03: a step carries the purpose Transfer");
                eff.push(if matches!(input, In::StepOk { .. }) { Eff::NextStep } else { Eff::TransportStep });
            }
            TransferEffect::Complete { .. } => eff.push(Eff::Complete),
            TransferEffect::Schedule => {}
            TransferEffect::Abort { .. } => eff.push(Eff::Abort),
            TransferEffect::CancelStep { .. } => eff.push(if matches!(input, In::EpochEnded | In::Closing) { Eff::TransportCancel } else { Eff::StepCancelled }),
            TransferEffect::DeleteFile { .. } => eff.push(if matches!(input, In::MutationOutcome { confirmed: true }) { Eff::DeleteLocalCopy } else { Eff::DeleteFile }),
            TransferEffect::SubmitMutation { .. } => eff.push(Eff::SubmitFinalMutation),
            TransferEffect::DiscardMutation { .. } => eff.push(Eff::DiscardMutation),
            TransferEffect::Reply { answer, .. } => obs.push(match answer {
                TransferAnswer::Receipt(_) => Obs::Receipt,
                TransferAnswer::Ok => Obs::DiscardOk,
                TransferAnswer::Cancelled(true) => Obs::CancelTrue,
                TransferAnswer::Cancelled(false) => Obs::CancelFalse,
                TransferAnswer::Err(Fault::InFlight) => Obs::CancelInFlight,
                TransferAnswer::Err(Fault::NotFound) => Obs::NotFound,
                TransferAnswer::Err(Fault::NoSession) => Obs::NoSession,
                TransferAnswer::Err(other) => panic!("a fault outside the table: {other:?}"),
            }),
            TransferEffect::Shows { shown: t::Shown::Uploading { progress }, .. } if matches!(input, In::StepOk { .. }) => obs.push(if progress == 100 { Obs::Progress100 } else { Obs::Progress }),
            TransferEffect::Shows { shown, .. } => obs.push(Obs::Shows(shown_of(&shown))),
            TransferEffect::Refused { .. } | TransferEffect::EndJob { .. } => {}
            TransferEffect::ScheduleJob { .. } => eff.push(Eff::RoundNeeded),
            TransferEffect::Drained => eff.push(Eff::Drained),
        }
    }
    if scheduled {
        eff.push(if matches!(input, In::Online(true)) { Eff::ScheduleExpire } else { Eff::ScheduleBackoff });
    }
    (obs, eff)
}

/// The part of the reference that the owner shows: `SlotFreed` and `StartInOrder` are inside the owner, progress is the `Shows` value.
pub(super) fn visible(step: &Step) -> (Vec<Obs>, Vec<Eff>) {
    let obs = step.obs.iter().copied().filter(|obs| !matches!(obs, Obs::Progress | Obs::Progress100 | Obs::Interrupted | Obs::RowWithServerFile)).collect();
    let eff = step.eff.iter().copied().filter(|eff| !matches!(eff, Eff::SlotFreed | Eff::StartInOrder)).collect();
    (obs, eff)
}

pub(super) fn sorted(mut obs: Vec<Obs>) -> Vec<String> {
    let mut named: Vec<String> = obs.drain(..).map(|obs| format!("{obs:?}")).collect();
    named.sort();
    named
}

pub(super) fn row_check(state: St, ctx: Ctx, input: In) {
    let want = step_of(state, &ctx, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}"));
    let mut twin = reach(state, &ctx);
    let before = twin.det.trace().len();
    let effects = input_of(&mut twin, &ctx, input);
    let scheduled = twin.det.trace()[before..].iter().any(|entry| entry.effects.contains("Schedule {") || entry.effects.contains("ScheduleRetry"));
    let (mut obs, mut eff) = translate(input, effects, scheduled);
    obs.retain(|obs| !matches!(obs, Obs::Progress | Obs::Progress100));
    let (want_obs, want_eff) = visible(&want);
    let mut next = twin.state().stage_of(KEY).map_or(want.next, stage_of);
    // `Self SlotFreed` runs inside the owner: a Ready transfer that starts at once shows `Uploading` and a step.
    let internal_start = want.eff.contains(&Eff::SlotFreed) || want.eff.contains(&Eff::StartInOrder);
    if want.next == St::Ready && next == St::Uploading && internal_start {
        next = St::Ready;
        if let Some(at) = obs.iter().position(|obs| *obs == Obs::Shows(Shown::Uploading)) { obs.remove(at); }
        if let Some(at) = eff.iter().position(|eff| *eff == Eff::TransportStep) { eff.remove(at); }
    }
    assert_eq!(next, want.next, "{state:?} x {input:?}: the next state");
    assert_eq!(sorted(obs), sorted(want_obs), "{state:?} x {input:?}: the replies and the values");
    assert_eq!(eff, want_eff, "{state:?} x {input:?}: the effects");
}

pub(super) fn ready() -> Ctx { Ctx::ready() }

pub(super) fn multipart() -> Ctx { Ctx { has_upload_id: true, ..Ctx::ready() } }

pub(super) fn batch() -> Ctx { Ctx { kind: Kind::Batch, ..Ctx::ready() } }

pub(super) fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

pub(super) fn queued_behind() -> Ctx { Ctx { first_to_start: false, ..Ctx::ready() } }

pub(super) fn all_parts() -> Ctx { Ctx { parts_all_confirmed: true, ..multipart() } }

pub(super) fn stray() -> Ctx { Ctx { stray_step: true, ..Ctx::ready() } }
