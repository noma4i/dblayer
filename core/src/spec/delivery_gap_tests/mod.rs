//! Owner-level tests of the `delivery` machine: the owner `Deliveries` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_delivery.rs`) and to the owner through its address, then compares the effects.
//! The values a reader gets are the `Answer` effects of `Pull`. Names are the `Test` column.

use std::cell::RefCell;
use std::rc::Rc;

use super::ref_delivery::{self as r, rows, Cause, Delivery, Effect, Fetch, Input, Kind, Phase, Pulled, Val, Ws, insert, remove, Ed};
use crate::owners::contract::{Committed, EpochEnded, Models, ModelsReset, WindowPhase, WindowState};
use crate::owners::read::deliveries::{self as d, DeliveryEffect, DeliveryPeers, Deliveries, PullAnswer, StreamEffect};
use crate::owners::read::feed::Feed;
use crate::owners::read::windows::{WindowKey, Windows};
use crate::spec::support::{as_owner, new_log, snapshot, take, Discard, Emitted, Ender, EnderState, Log, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState};
use crate::runtime::{explore, Address, Cause as Ended, DetScheduler, EpochStamp, Limits, Outcomes};

mod delivery_changes_tests;
mod delivery_operations_tests;
mod delivery_more_tests;
mod delivery_read_tests;
mod delivery_revision_cells;

const STREAM: d::StreamId = 1;
const EPOCH: u64 = 1;
const WINDOWS: usize = 0;
const FEED: usize = 1;

type Spied = Log<Emitted<DeliveryEffect>>;

fn peers() -> DeliveryPeers { DeliveryPeers { windows: Address::<Windows>::new(WINDOWS), feed: Address::<Feed>::new(FEED) } }

fn window() -> WindowKey { WindowKey { model: "Moment".into(), name: "list".into() } }

fn add_deliveries(det: &mut DetScheduler, log: &Spied) -> Address<Spy<Deliveries>> {
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Sink<Feed>>("feed", new_log(), Discard);
    det.add::<Spy<Deliveries>>("deliveries", SpyState::new(d::DeliveriesState::new(EPOCH, peers()), log), Discard)
}

fn inner(det: &DetScheduler, address: Address<Spy<Deliveries>>) -> &d::DeliveriesState { &det.state(address).expect("the owner state").inner }

fn view(kind: Kind) -> d::View {
    let (kind, model) = match kind {
        Kind::List => (d::Kind::List, "Moment"),
        Kind::Count => (d::Kind::Count, "Moment"),
        Kind::Entity(fetch) => (d::Kind::Entity { fetch: match fetch { Fetch::Never => d::Fetch::Never, Fetch::Stale => d::Fetch::Stale, Fetch::Force => d::Fetch::Force, Fetch::IfMissing => d::Fetch::IfMissing } }, "Moment"),
        Kind::Events => (d::Kind::Events, "Chat"),
        Kind::Changes => return d::View { kind: d::Kind::Changes, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None },
        Kind::Blob => (d::Kind::Blob, "Moment"),
        Kind::Retry => (d::Kind::RetryStatus, "Moment"),
    };
    d::View { kind, model: model.into(), tables: vec!["moments".into()], local: false, remote: true, live: false, scoped: Default::default(), reload: false, window: window(), subject: "t1".into(), query: None }
}

fn rows_of(rows: &[r::Row]) -> Vec<d::StreamRow> { rows.iter().map(|(id, stamp)| d::StreamRow::new(id, *stamp)).collect() }

fn window_state(state: &Ws) -> WindowState {
    let phase = match state.phase { Phase::Idle => WindowPhase::Idle, Phase::Loading => WindowPhase::Loading, Phase::Loaded => WindowPhase::Loaded, Phase::Failed => WindowPhase::Failed };
    WindowState { phase, error: state.error.then(|| crate::ffi::DbError::Storage { detail: "e".into() }), loaded: state.loaded, has_more: false, settled: false, total: None }
}

fn ws_of(state: &WindowState) -> Ws {
    let phase = match state.phase { WindowPhase::Idle => Phase::Idle, WindowPhase::Loading => Phase::Loading, WindowPhase::Loaded => Phase::Loaded, WindowPhase::Failed => Phase::Failed };
    Ws { phase, error: state.error.is_some(), loaded: state.loaded }
}

thread_local! {
    static MODES: std::cell::RefCell<Vec<d::ReadMode>> = const { std::cell::RefCell::new(Vec::new()) };
}

thread_local! {
    /// The `total` of each rows value the owner gave, in order.
    static TOTALS: std::cell::RefCell<Vec<Option<u64>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn ed_of(edit: &d::Edit) -> Ed {
    match edit {
        d::Edit::Insert { index, row } => Ed::Insert { index: *index, id: row.id.clone() },
        d::Edit::Rewrite { index, row, .. } => Ed::Rewrite { index: *index, id: row.id.clone() },
        d::Edit::Remove { index, id } => Ed::Remove { index: *index, id: id.clone() },
        d::Edit::Move { from, to } => Ed::Move { from: *from, to: *to },
    }
}

fn val_of(value: &d::Value) -> Val {
    if let d::Value::Snapshot { state, .. } | d::Value::Count { state, .. } | d::Value::State { state, .. } | d::Value::Delta(d::Delta { state, .. }) = value {
        TOTALS.with(|totals| totals.borrow_mut().push(state.total));
    }
    let ids = |rows: &[d::StreamRow]| rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    match value {
        d::Value::Snapshot { revision, rows, state, .. } => Val::Snapshot { revision: *revision, rows: ids(rows), state: ws_of(state) },
        d::Value::Delta(delta) => Val::Delta { base: delta.base, revision: delta.revision, size: delta.size as usize, edits: delta.edits.iter().map(ed_of).collect(), state: ws_of(&delta.state) },
        d::Value::Count { revision, count, state } => Val::Count { revision: *revision, count: *count, state: ws_of(state) },
        d::Value::Operations { .. } => unreachable!("the reference models no queue stream"),
        d::Value::State { revision, state } => Val::State { revision: *revision, state: ws_of(state) },
        d::Value::Ready => Val::Ready,
        d::Value::Changes { revision, rows } => Val::Changes { revision: *revision, rows: rows.clone() },
        d::Value::Event { position, payload } => Val::Event { position: *position, payload: payload.clone() },
        d::Value::CableClose { .. } => unreachable!("the reference models no cable close stream"),
        d::Value::Lost { first, last } => Val::Lost { first: *first, last: *last },
        d::Value::Gap => Val::Gap,
        d::Value::Status { revision, blob } => Val::Status { revision: *revision, state: blob.state.clone() },
        d::Value::Retry(retry) => Val::Retry { attempts: retry.attempts },
        d::Value::Ended(cause) => Val::Ended(match cause {
            d::EndCause::SessionEnded => Cause::SessionEnded,
            d::EndCause::Closed => Cause::Closed,
            d::EndCause::Cancelled => Cause::Cancelled,
            d::EndCause::Invalid { .. } => Cause::Invalid,
            d::EndCause::Discarded | d::EndCause::Internal { .. } => panic!("unexpected delivery cause"),
            d::EndCause::Storage { .. } => Cause::Storage,
        }),
    }
}

fn pulled_of(answer: &PullAnswer) -> Pulled {
    match answer {
        PullAnswer::Value(value) => Pulled::Value(val_of(value)),
        PullAnswer::Done => Pulled::Done,
        PullAnswer::Interrupted => Pulled::Interrupted,
    }
}

struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Deliveries>>,
    log: Spied,
    answers: Outcomes<PullAnswer>,
    reference: Delivery,
    /// The total the reads of this twin give with their rows.
    total: Option<u64>,
    pull: Option<crate::runtime::OutcomeId>,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = add_deliveries(&mut det, &log);
        det.start_epoch(EPOCH);
        Self { det, addr, log, answers: Outcomes::new(), reference: Delivery::new(), total: None, pull: None }
    }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: d::DeliveryInput) {
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
    }

    fn call(&mut self, input: d::DeliveryInput) {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.run();
    }

    fn state(&self) -> &d::DeliveriesState { inner(&self.det, self.addr) }

    fn read_id(&self) -> u64 { self.state().read_id(STREAM) }

    fn modelled(entries: Vec<Emitted<DeliveryEffect>>) -> Vec<Effect> {
        let mut modelled = Vec::new();
        for entry in entries {
            match entry {
                Emitted::Run(DeliveryEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(DeliveryEffect::Stream { stream, effect }) => {
                    assert_eq!(stream, STREAM);
                    modelled.push(match effect {
                        StreamEffect::Read { revision, mode, .. } => {
                            MODES.with(|modes| modes.borrow_mut().push(mode));
                            Effect::Read(revision)
                        }
                        StreamEffect::Answer { answer, .. } => Effect::Answer(pulled_of(&answer)),
                        StreamEffect::Available => Effect::Available,
                        StreamEffect::CloseSink => Effect::CloseSink,
                        StreamEffect::Cancelled { reply } => Effect::Cancelled(reply),
                        StreamEffect::ReaderClaimed { .. } => unreachable!("the twin posts no ClaimReader"),
                        StreamEffect::StatusInterest { plus, .. } => Effect::StatusInterest(plus),
                    });
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPlus") => modelled.push(Effect::InterestPlus),
                Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPassive") => modelled.push(Effect::InterestPassive),
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Load") => modelled.push(Effect::Load { force: input.contains("force: true") }),
                Emitted::Send { to: WINDOWS, .. } => modelled.push(Effect::InterestMinus),
                Emitted::Run(DeliveryEffect::WantPlus { .. }) => modelled.push(Effect::WantPlus),
                Emitted::Run(DeliveryEffect::WantMinus { .. }) => modelled.push(Effect::WantMinus),
                _ => {}
            }
        }
        modelled
    }

    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        let stream = STREAM;
        match &input {
            Input::Open { kind, epoch_current } => {
                let epoch = if *epoch_current { EPOCH } else { EPOCH + 1 };
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(*kind), epoch, identity: true, valid: true, now: 1_000_000, revision: 0 });
                self.run();
            }
            Input::OpenNoSession => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::List), epoch: EPOCH, identity: false, valid: true, now: 1_000_000, revision: 0 });
                self.run();
            }
            Input::OpenInvalid => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::List), epoch: EPOCH, identity: true, valid: false, now: 1_000_000, revision: 0 });
                self.run();
            }
            Input::Committed { revision, touches } => {
                let table = if *touches { "moments" } else { "other" };
                self.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables: vec![table.into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
            }
            Input::Changed { revision, rows, reset } => {
                let tables = rows.iter().map(|(table, _)| table.clone()).collect();
                let changed = rows.iter().cloned().collect::<std::collections::BTreeSet<_>>();
                self.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables, rows: (!*reset).then_some(changed), models: if *reset { Models::All } else { Default::default() }, stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
            }
            Input::ReadDone { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id(), revision: *revision, data: d::ReadData::Rows { rows: rows_of(rows), replaced: Vec::new(), total: self.total } }),
            Input::ReadOverflow { revision } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id(), revision: *revision, data: d::ReadData::Overflow }),
            Input::CountDone { revision, count } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id(), revision: *revision, data: d::ReadData::Count(*count) }),
            Input::ReadStale { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id() + 100, revision: *revision, data: d::ReadData::Rows { rows: rows_of(rows), replaced: Vec::new(), total: None } }),
            Input::ReadFailed { revision } => self.post(d::DeliveryInput::ReadFailed { stream, read_id: self.read_id(), revision: *revision, error: "e".into() }),
            Input::ReadFailedStale => self.post(d::DeliveryInput::ReadFailed { stream, read_id: self.read_id() + 100, revision: 2, error: "e".into() }),
            Input::WindowState(state) => self.post(d::DeliveryInput::WindowState { stream, state: window_state(state) }),
            Input::Joined { position } => self.post(d::DeliveryInput::Joined { stream, position: *position }),
            Input::ModelReady => self.post(d::DeliveryInput::ModelReady { stream }),
            Input::ModelEvent { position, payload } => self.post(d::DeliveryInput::ModelEvent { stream, position: *position, payload: payload.clone() }),
            Input::ModelLost { first, last } => self.post(d::DeliveryInput::ModelLost { stream, first: *first, last: *last }),
            Input::ModelGap => self.post(d::DeliveryInput::ModelGap { stream }),
            Input::Resync => self.call(d::DeliveryInput::Resync { stream }),
            Input::Transfer { state } => self.post(d::DeliveryInput::TransferStatus { stream, blob: d::BlobStatus { state: state.clone(), wait_reason: None, progress: 0, error: None, local_url: None, processing: None } }),
            Input::Retry { attempts } => self.post(d::DeliveryInput::RetryState { stream, retry: d::RetryStatus { attempts: *attempts, phase: "idle".into() } }),
            Input::Pull => {
                let outcome = self.answers.register(EPOCH).id();
                if !self.reference.waiting() { self.pull = Some(outcome); }
                self.call(d::DeliveryInput::Pull { stream, outcome, reader: 1 });
            }
            Input::PullCancelled { matched } => {
                let outcome = self.pull.filter(|_| *matched).unwrap_or_else(|| crate::runtime::OutcomeId::from_raw(u64::MAX));
                self.call(d::DeliveryInput::PullCancelled { stream, outcome });
            }
            Input::Cancel { reply } => self.call(d::DeliveryInput::Cancel { stream, reply: *reply }),
            Input::Release => self.call(d::DeliveryInput::Release { stream, reader: None, now: crate::runtime::Time(0) }),
            Input::SinkClosed => self.call(d::DeliveryInput::SinkClosed { stream }),
            Input::EpochEnded { own } => {
                self.det.end_epoch(if *own { EPOCH } else { EPOCH + 1 }, Ended::SessionReset);
                self.run();
            }
            Input::EpochClosed => {
                self.det.end_epoch(EPOCH, Ended::Close);
                self.run();
            }
            Input::ModelsReset { own_model } => {
                let model = if *own_model { if self.reference_events() { "Chat" } else { "Moment" } } else { "Other" };
                self.post(d::DeliveryInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec![model.into()] }));
            }
            Input::Closing => {
                self.det.close();
                self.run();
            }
        }
        let mut entries = take(&self.log);
        loop {
            let mut asked = Vec::new();
            entries.retain(|entry| match entry {
                Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { read_id, view, .. }, .. }) if view.kind == d::Kind::Blob => { asked.push(*read_id); false }
                _ => true,
            });
            if asked.is_empty() { break; }
            for read_id in asked {
                self.post(d::DeliveryInput::ReadDone { stream, read_id, revision: 0, data: d::ReadData::Upload { wait_reason: None, local_url: None, processing: None } });
            }
            entries.extend(take(&self.log));
        }
        let got = Self::modelled(entries);
        self.reference.next_total(self.total);
        let want = self.reference.step(input);
        assert_eq!(got, want, "the owner and the reference give the same effects");
        if !self.reference.gone {
            assert_eq!(self.state().journal_len(STREAM), self.reference.journal.len(), "the same journal length");
        }
        want
    }

    fn reference_events(&self) -> bool { self.reference.is_events() }

    fn read_all(&mut self) -> Vec<Val> {
        let mut values = Vec::new();
        for _ in 0..2_000 {
            if self.reference.journal.is_empty() {
                break;
            }
            for effect in self.step(Input::Pull) {
                if let Effect::Answer(Pulled::Value(value)) = effect {
                    values.push(value);
                }
            }
        }
        assert!(self.reference.journal.is_empty(), "the journal is read");
        values
    }

    fn step_read(&mut self, input: Input) -> (Vec<Effect>, Vec<Val>) {
        let effects = self.step(input);
        (effects, self.read_all())
    }
}

fn ids(list: &[&str]) -> Vec<String> { list.iter().map(|id| id.to_string()).collect() }

fn snap(revision: i64, list: &[&str], state: Ws) -> Val { Val::Snapshot { revision, rows: ids(list), state } }

fn open_as(kind: Kind) -> Twin {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind, epoch_current: true });
    if matches!(kind, Kind::List | Kind::Count | Kind::Entity(Fetch::Stale | Fetch::IfMissing | Fetch::Force)) { twin.step(Input::WindowState(Ws::idle())); }
    twin
}

fn open_rows() -> Twin { open_as(Kind::List) }

fn open_events() -> Twin {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    twin.step(Input::ModelReady);
    twin.read_all();
    twin
}

fn live_rows() -> Twin {
    let mut twin = open_rows();
    twin.step(Input::WindowState(Ws::loaded_at(1)));
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.read_all();
    twin
}

/// The streams that read a window and the streams that ended without a `Release`, asked of the owner.
fn census_of(twin: &mut Twin) -> (usize, usize) {
    take(&twin.log);
    twin.call(d::DeliveryInput::Census { reply: 7 });
    take(&twin.log).into_iter().find_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Census { reply: 7, observed, retained }) => Some((observed, retained)),
        _ => None,
    }).expect("the owner answers the census")
}

#[test]
fn dlv_wait_subscription_census() {
    let mut twin = open_rows();
    assert_eq!(census_of(&mut twin), (1, 0), "an open stream of a window is observed");
    twin.step(Input::EpochEnded { own: true });
    assert_eq!(census_of(&mut twin), (0, 1), "an ended stream stays retained until its Release");
    twin.call(d::DeliveryInput::Release { stream: STREAM, reader: None, now: crate::runtime::Time(0) });
    assert_eq!(census_of(&mut twin), (0, 0), "a released stream leaves the census");
}

fn ended_by_epoch() -> Twin {
    let mut twin = live_rows();
    twin.step(Input::EpochEnded { own: true });
    twin
}

#[test]
fn dlv_open_rows() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::Open { kind: Kind::List, epoch_current: true }), [Effect::Read(0), Effect::InterestPlus]);
    assert!(twin.read_all().is_empty(), "no value before the first read");
    for (kind, effects) in [
        (Kind::Count, vec![Effect::Read(0), Effect::InterestPlus]),
        (Kind::Entity(Fetch::Never), vec![Effect::Read(0)]),
        (Kind::Entity(Fetch::Stale), vec![Effect::Read(0), Effect::InterestPlus]),
        (Kind::Entity(Fetch::Force), vec![Effect::Read(0), Effect::InterestPlus, Effect::Load { force: true }]),
        (Kind::Entity(Fetch::IfMissing), vec![Effect::Read(0), Effect::InterestPassive]),
    ] {
        assert_eq!(Twin::new().step(Input::Open { kind, epoch_current: true }), effects, "{kind:?}");
    }
}

/// P-DLV-26: an Entity `if_missing` opens with a passive interest and asks the window to load only after a read without a row; `stale` and `force` hold an active interest.
#[test]
fn dlv_open_if_missing_passive() {
    for (fetch, active) in [(Fetch::IfMissing, false), (Fetch::Stale, true), (Fetch::Force, true)] {
        let effects = Twin::new().step(Input::Open { kind: Kind::Entity(fetch), epoch_current: true });
        assert_eq!(effects.contains(&Effect::InterestPlus), active, "{fetch:?}: {effects:?}");
        assert_eq!(effects.contains(&Effect::InterestPassive), !active, "{fetch:?}: {effects:?}");
    }
    let mut twin = open_as(Kind::Entity(Fetch::IfMissing));
    let (effects, _) = twin.step_read(Input::ReadDone { revision: 1, rows: Vec::new() });
    assert_eq!(effects.iter().filter(|effect| matches!(effect, Effect::Load { .. })).count(), 1, "one load after the empty read: {effects:?}");
}

#[test]
fn dlv_open_events() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::Open { kind: Kind::Events, epoch_current: true }), [Effect::WantPlus]);
    assert!(twin.read_all().is_empty(), "no value before the first event");
}

#[test]
fn dlv_open_status() {
    for kind in [Kind::Blob, Kind::Retry] {
        let mut twin = Twin::new();
        assert_eq!(twin.step(Input::Open { kind, epoch_current: true }), [Effect::StatusInterest(true)]);
        assert!(twin.read_all().is_empty(), "no value before the first status");
    }
}

#[test]
fn dlv_opening_status() {
    let mut blob = open_as(Kind::Blob);
    assert_eq!(blob.step(Input::Transfer { state: "uploading".into() }), [Effect::Available]);
    assert_eq!(blob.read_all(), [Val::Status { revision: 1, state: "uploading".into() }]);
    let mut retry = open_as(Kind::Retry);
    assert_eq!(retry.step(Input::Retry { attempts: 2 }), [Effect::Available]);
    assert_eq!(retry.read_all(), [Val::Retry { attempts: 2 }]);
}

#[test]
fn dlv_live_status() {
    let mut twin = open_as(Kind::Blob);
    twin.step(Input::Transfer { state: "uploading".into() });
    twin.read_all();
    assert_eq!(twin.step(Input::Transfer { state: "done".into() }), [Effect::Available]);
    assert_eq!(twin.read_all(), [Val::Status { revision: 2, state: "done".into() }]);
    twin.step(Input::Transfer { state: "failed".into() });
    twin.step(Input::Transfer { state: "retrying".into() });
    assert_eq!(twin.read_all(), [Val::Status { revision: 4, state: "retrying".into() }], "an unread status gives way to the last one");
}

#[test]
fn dlv_live_status_same() {
    let mut twin = open_as(Kind::Blob);
    twin.step(Input::Transfer { state: "uploading".into() });
    twin.read_all();
    assert!(twin.step(Input::Transfer { state: "uploading".into() }).is_empty());
    let mut other = open_as(Kind::Retry);
    other.step(Input::Retry { attempts: 1 });
    assert!(other.step(Input::Transfer { state: "x".into() }).is_empty(), "a status of another kind changes nothing");
    assert_eq!(other.read_all(), [Val::Retry { attempts: 1 }]);
}

#[test]
fn dlv_open_no_session() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::OpenNoSession), [Effect::Available]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_open_stale() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::Open { kind: Kind::List, epoch_current: false }), [Effect::Available]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_open_invalid() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::OpenInvalid), [Effect::Available]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::Invalid)]);
}

#[test]
fn dlv_opening_read_holds() {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind: Kind::List, epoch_current: true });
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!((effects, values), (Vec::new(), Vec::new()), "the read of a watched window waits for the first state of the window");
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(1)));
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [snap(1, &["r1"], Ws::loaded_at(1))], "the first value holds the loaded phase");
}

#[test]
fn dlv_snapshot_total() {
    let mut list = open_rows();
    list.total = Some(53);
    list.step(Input::WindowState(Ws::loaded_at(1)));
    TOTALS.with(|totals| totals.borrow_mut().clear());
    list.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1", "r2"]) });
    list.total = Some(54);
    list.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2", "r3"]) });
    assert_eq!(TOTALS.with(|totals| totals.borrow().clone()), [Some(53), Some(54)], "a snapshot and a delta carry the total of the window read with their rows");
    let mut entity = open_as(Kind::Entity(Fetch::Never));
    entity.total = Some(7);
    TOTALS.with(|totals| totals.borrow_mut().clear());
    entity.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(TOTALS.with(|totals| totals.borrow().clone()), [None], "a row by id has no total");
}

#[test]
fn dlv_opening_read_done() {
    let mut twin = open_rows();
    twin.step(Input::WindowState(Ws::loaded_at(1)));
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [snap(1, &["r1"], Ws::loaded_at(1))]);
    let mut count = open_as(Kind::Count);
    count.step(Input::WindowState(Ws::loaded_at(1)));
    let (_, values) = count.step_read(Input::CountDone { revision: 1, count: 3 });
    assert_eq!(values, [Val::Count { revision: 1, count: 3, state: Ws::loaded_at(1) }]);
}

#[test]
fn dlv_opening_read_done_if_missing() {
    let mut twin = open_as(Kind::Entity(Fetch::IfMissing));
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 1, rows: Vec::new() });
    assert_eq!(effects, [Effect::Available, Effect::Load { force: false }], "no row after the first read: ask the window once");
    assert_eq!(values, [snap(1, &[], Ws::idle())]);
    let mut present = open_as(Kind::Entity(Fetch::IfMissing));
    let (effects, _) = present.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(effects, [Effect::Available], "a row is there: no load");
}

#[test]
fn dlv_opening_read_stale() {
    for kind in [Kind::List, Kind::Entity(Fetch::Stale)] {
        let mut twin = open_as(kind);
        let (effects, values) = twin.step_read(Input::ReadStale { revision: 1, rows: rows(&["rX"]) });
        assert!(effects.is_empty() && values.is_empty(), "a read of another read_id is dropped");
        let (_, values) = twin.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
        assert_eq!(values, [snap(1, &["r1"], Ws::idle())]);
    }
}

#[test]
fn dlv_opening_read_failed() {
    let mut twin = open_rows();
    let (_, values) = twin.step_read(Input::ReadFailed { revision: 1 });
    assert_eq!(values, [snap(1, &[], Ws { phase: Phase::Failed, error: true, loaded: None })], "a failed first read has no rows and Failed");
    let mut count = open_as(Kind::Count);
    let (_, values) = count.step_read(Input::ReadFailed { revision: 1 });
    assert_eq!(values, [Val::Count { revision: 1, count: 0, state: Ws { phase: Phase::Failed, error: true, loaded: None } }]);
}

#[test]
fn dlv_opening_committed() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_read(Input::Committed { revision: 2, touches: true });
    assert_eq!(effects, [Effect::Read(2)]);
    assert!(values.is_empty());
    twin.step(Input::WindowState(Ws::loaded_at(3)));
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: rows(&["r1", "r2"]) });
    assert_eq!(values, [snap(3, &["r1", "r2"], Ws::loaded_at(3))], "the first value shows the newer rows");
}

#[test]
fn dlv_opening_unrelated() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_read(Input::Committed { revision: 2, touches: false });
    assert!(effects.is_empty() && values.is_empty());
}

/// dlv_open_rows: a stream that reads no window (its relation is not remote) reads the rows and asks the window for nothing.
#[test]
fn dlv_open_rows_without_a_window_asks_for_nothing() {
    let mut twin = Twin::new();
    take(&twin.log);
    twin.call(d::DeliveryInput::Open { stream: STREAM, view: d::View { remote: false, ..view(Kind::List) }, epoch: EPOCH, identity: true, valid: true, now: 1_000_000, revision: 0 });
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Read(0)]);
}

/// dlv_open_live: a read stream of a model that takes server events wants the subscription of the Feed and gives it back when it ends.
#[test]
fn dlv_open_live() {
    let mut twin = Twin::new();
    take(&twin.log);
    twin.call(d::DeliveryInput::Open { stream: STREAM, view: d::View { live: true, ..view(Kind::List) }, epoch: EPOCH, identity: true, valid: true, now: 1_000_000, revision: 0 });
    assert!(Twin::modelled(take(&twin.log)).contains(&Effect::WantPlus));
}

/// D-254: a reset commit of `All` models reads no row stream; `ModelsReset` re-reads them.
#[test]
fn dlv_committed_reset_reads_no_row_stream() {
    let mut twin = open_rows();
    take(&twin.log);
    twin.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: 2, tables: vec!["other".into()], rows: None, models: Models::All, stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(Twin::modelled(take(&twin.log)), []);
}

#[test]
fn dlv_opening_window() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(1)));
    assert!(effects.is_empty() && values.is_empty(), "the window state waits for the first read");
    twin.step(Input::WindowState(Ws::loaded_at(5)));
    let (_, values) = twin.step_read(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(values, [snap(1, &["r1"], Ws::idle())], "a state visible only after a newer revision is held back");
    twin.step(Input::Committed { revision: 5, touches: true });
    let (_, values) = twin.step_read(Input::ReadDone { revision: 5, rows: rows(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { base: 1, revision: 5, size: 2, edits: vec![insert(1, "r2")], state: Ws::loaded_at(5) }]);
}

mod delivery_cells_tests;
mod delivery_properties_tests;

#[test]
fn matrix_owner_state_input_delivery() {
    use crate::test_matrix::{state_input_pairs, Matrix};
    let mut matrix = Matrix::new("S-delivery");
    fn open_count() -> Twin { open_as(Kind::Count) }
    fn open_entity() -> Twin { open_as(Kind::Entity(Fetch::Stale)) }
    let alphabet: Vec<(&str, Input)> = vec![
        ("Open list", Input::Open { kind: Kind::List, epoch_current: true }), ("Open stale epoch", Input::Open { kind: Kind::List, epoch_current: false }), ("OpenNoSession", Input::OpenNoSession), ("OpenInvalid", Input::OpenInvalid),
        ("Committed touching", Input::Committed { revision: 2, touches: true }), ("Committed not touching", Input::Committed { revision: 3, touches: false }),
        ("Changed", Input::Changed { revision: 2, rows: vec![("Moment".into(), "r1".into())], reset: false }), ("Changed reset", Input::Changed { revision: 3, rows: vec![], reset: true }),
        ("ReadDone", Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }), ("ReadOverflow", Input::ReadOverflow { revision: 2 }), ("CountDone", Input::CountDone { revision: 2, count: 3 }),
        ("ReadStale", Input::ReadStale { revision: 1, rows: rows(&["r1"]) }), ("ReadFailed", Input::ReadFailed { revision: 2 }), ("ReadFailedStale", Input::ReadFailedStale),
        ("WindowState idle", Input::WindowState(Ws::idle())), ("WindowState loaded", Input::WindowState(Ws::loaded_at(2))),
        ("Joined", Input::Joined { position: 0 }), ("ModelReady", Input::ModelReady), ("ModelEvent", Input::ModelEvent { position: 1, payload: "p".into() }), ("ModelLost", Input::ModelLost { first: 2, last: 3 }), ("ModelGap", Input::ModelGap),
        ("Resync", Input::Resync), ("Transfer", Input::Transfer { state: "sending".into() }), ("Retry", Input::Retry { attempts: 1 }),
        ("Pull", Input::Pull), ("PullCancelled matched", Input::PullCancelled { matched: true }), ("Cancel", Input::Cancel { reply: 5 }), ("Release", Input::Release), ("SinkClosed", Input::SinkClosed),
        ("EpochEnded own", Input::EpochEnded { own: true }), ("EpochEnded other", Input::EpochEnded { own: false }), ("EpochClosed", Input::EpochClosed), ("ModelsReset own", Input::ModelsReset { own_model: true }), ("Closing", Input::Closing),
    ];
    state_input_pairs(&mut matrix, "reference ref_delivery", vec![("new", Twin::new), ("open rows", open_rows), ("live rows", live_rows), ("open events", open_events), ("open count", open_count), ("open entity", open_entity), ("ended by epoch", ended_by_epoch)], alphabet, |twin: &mut Twin, input| { twin.step(input); });
    matrix.finish();
}
