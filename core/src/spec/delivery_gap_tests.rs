//! Owner-level tests of `specs/machines/delivery.md`: the owner `Deliveries` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_delivery.rs`) and to the owner through its address, then compares the effects
//! and, after a drain with `Pull`, the values. Names are the `Test` column, `_owner` marks the owner twin of a test that also runs on the API.

use super::harness::{loaded, Cause, Phase, Val, WState};
use super::ref_delivery::{self as r, Delivery, Effect, Input, Kind};
use crate::owners::contract::{Committed, EpochEnded, ModelsReset, WindowPhase, WindowState};
use crate::owners::read::deliveries::{self as d, DeliveryEffect, DeliveryPeers, Deliveries, PullAnswer, StreamEffect};
use crate::owners::read::feed::Feed;
use crate::owners::read::windows::{WindowKey, Windows};
use crate::owners::support::{as_owner, new_log, snapshot, take, Discard, Emitted, Ender, EnderState, Log, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState};
use crate::runtime::{explore, Address, Cause as Ended, DetScheduler, EpochStamp, Limits, OutcomeId, Outcomes};

const STREAM: d::StreamId = 1;
const EPOCH: u64 = 1;
/// Slots of the peers and of the owner: the Windows, the Feed, then the Deliveries.
const WINDOWS: usize = 0;
const FEED: usize = 1;

type Spied = Log<Emitted<DeliveryEffect>>;

fn peers() -> DeliveryPeers { DeliveryPeers { windows: Address::<Windows>::new(WINDOWS), feed: Address::<Feed>::new(FEED) } }

fn window() -> WindowKey { WindowKey { model: "Moment".into(), name: "list".into() } }

/// Adds the Windows and the Feed as sinks, then the Deliveries watched by `log`.
fn add_deliveries(det: &mut DetScheduler, log: &Spied) -> Address<Spy<Deliveries>> {
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Sink<Feed>>("feed", new_log(), Discard);
    det.add::<Spy<Deliveries>>("deliveries", SpyState::new(d::DeliveriesState::new(EPOCH, peers()), log), Discard)
}

fn inner(det: &DetScheduler, address: Address<Spy<Deliveries>>) -> &d::DeliveriesState { &det.state(address).expect("the owner state").inner }


fn ids(rows: &[&str]) -> Vec<String> { rows.iter().map(|id| id.to_string()).collect() }

fn rows_of(rows: &[String]) -> Vec<d::StreamRow> { rows.iter().map(|id| d::StreamRow { id: id.clone(), stamp: 0 }).collect() }

fn view(kind: Kind) -> d::View {
    match kind {
        Kind::Rows => d::View { kind: d::Kind::List, model: "Moment".into(), tables: vec!["moments".into()], local: false, window: window() },
        Kind::Count => d::View { kind: d::Kind::Count, model: "Moment".into(), tables: vec!["moments".into()], local: false, window: window() },
        Kind::Entity(fetch) => {
            let fetch = match fetch {
                r::Fetch::Never => d::Fetch::Never,
                r::Fetch::Stale => d::Fetch::Stale,
                r::Fetch::Force => d::Fetch::Force,
                r::Fetch::IfMissing => d::Fetch::IfMissing,
            };
            d::View { kind: d::Kind::Entity { fetch }, model: "Moment".into(), tables: vec!["moments".into()], local: false, window: window() }
        }
        Kind::Events => d::View { kind: d::Kind::Events, model: "Chat".into(), tables: Vec::new(), local: false, window: window() },
    }
}

fn load_state(state: WState) -> WindowState {
    let phase = match state.phase {
        Phase::Idle => WindowPhase::Idle,
        Phase::Loading => WindowPhase::Loading,
        Phase::Loaded => WindowPhase::Loaded,
        Phase::Failed => WindowPhase::Failed,
    };
    WindowState { phase, error: state.error.then(|| "e".to_string()), loaded: state.loaded.then_some(1) }
}

fn val_of(value: &d::Value) -> Val {
    let state_of = |state: &WindowState| WState {
        phase: match state.phase {
            WindowPhase::Idle => Phase::Idle,
            WindowPhase::Loading => Phase::Loading,
            WindowPhase::Loaded => Phase::Loaded,
            WindowPhase::Failed => Phase::Failed,
        },
        error: state.error.is_some(),
        loaded: state.loaded.is_some(),
    };
    match value {
        d::Value::Snapshot { rows, state, .. } => Val::Snapshot { rows: rows.iter().map(|row| row.id.clone()).collect(), state: state_of(state) },
        d::Value::Delta { upserted, removed, .. } => Val::Delta { upserted: upserted.iter().map(|row| row.id.clone()).collect(), removed: removed.clone() },
        d::Value::State { state, .. } => Val::StateOnly(state_of(state)),
        d::Value::Count { count, state, .. } => Val::Count { count: *count, state: state_of(state) },
        d::Value::Ready => Val::Ready,
        d::Value::Event { position, payload } => Val::Event { position: *position, id: payload.clone() },
        d::Value::Lost { first, last } => Val::Lost { first: *first, last: *last },
        d::Value::Ended(cause) => Val::Ended(match cause {
            d::EndCause::SessionEnded => Cause::SessionEnded,
            d::EndCause::Closed => Cause::Closed,
            d::EndCause::Cancelled => Cause::Cancelled,
            d::EndCause::Invalid => Cause::Invalid,
        }),
    }
}

/// The owner and the reference side by side.
struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Deliveries>>,
    log: Spied,
    answers: Outcomes<PullAnswer>,
    reference: Delivery,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = add_deliveries(&mut det, &log);
        det.start_epoch(EPOCH);
        Self { det, addr, log, answers: Outcomes::new(), reference: Delivery::new() }
    }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: d::DeliveryInput) {
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
    }

    fn read_id(&self) -> u64 { self.state().read_id(STREAM) }

    fn state(&self) -> &d::DeliveriesState { inner(&self.det, self.addr) }

    fn pull_input(&self) -> d::DeliveryInput { d::DeliveryInput::Pull { stream: STREAM, outcome: self.answers.register(EPOCH).id() } }

    /// The effects the reference models, in the order the owner produced them. Drained is keyless.
    fn modelled(&self, emitted: Vec<Emitted<DeliveryEffect>>) -> Vec<Effect> {
        emitted
            .into_iter()
            .filter_map(|entry| match entry {
                Emitted::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Read { revision, .. } }) => {
                    assert_eq!(stream, STREAM);
                    Some(Effect::Read(revision))
                }
                Emitted::Run(DeliveryEffect::Drained(_)) => Some(Effect::Drained),
                Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPlus") => Some(Effect::InterestPlus { force: input.contains("force: true") }),
                Emitted::Send { to: WINDOWS, .. } => Some(Effect::InterestMinus),
                Emitted::Send { to: FEED, input } if input.starts_with("WantPlus") => Some(Effect::WantPlus),
                Emitted::Send { to: FEED, .. } => Some(Effect::WantMinus),
                _ => None,
            })
            .collect()
    }

    /// Feeds one input of the table to both sides, checks that the effects are equal and returns them.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        match &input {
            Input::Open { kind, epoch_current } => {
                let epoch = if *epoch_current { EPOCH } else { EPOCH + 1 };
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(*kind), epoch, revision: 0, identity: true, valid: true });
                self.run();
            }
            Input::OpenNoSession => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::Rows), epoch: EPOCH, revision: 0, identity: false, valid: true });
                self.run();
            }
            Input::OpenInvalid => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::Rows), epoch: EPOCH, revision: 0, identity: true, valid: false });
                self.run();
            }
            Input::Joined { position } => self.post(d::DeliveryInput::Joined { stream: STREAM, position: *position }),
            Input::ModelReady => self.post(d::DeliveryInput::ModelReady { stream: STREAM }),
            Input::Committed { revision, touches } => {
                let table = if *touches { "moments" } else { "other" };
                self.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables: vec![table.into()], models: Vec::new() }));
            }
            Input::ReadDone { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: self.read_id(), revision: *revision, data: d::ReadData::Rows(rows_of(rows)) }),
            Input::CountDone { revision, count } => self.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: self.read_id(), revision: *revision, data: d::ReadData::Count(*count) }),
            Input::ReadStale { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: self.read_id() + 100, revision: *revision, data: d::ReadData::Rows(rows_of(rows)) }),
            Input::ReadFailed { revision } => self.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id: self.read_id(), revision: *revision, error: "e".into() }),
            Input::WindowState(state) => self.post(d::DeliveryInput::WindowState { stream: STREAM, state: load_state(*state) }),
            Input::ModelEvent { position, id } => self.post(d::DeliveryInput::ModelEvent { stream: STREAM, position: *position, payload: id.clone() }),
            Input::ModelLost { first, last } => self.post(d::DeliveryInput::ModelLost { stream: STREAM, first: *first, last: *last }),
            Input::Cancel => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Cancel { stream: STREAM });
                self.run();
            }
            Input::EpochEnded { own } => {
                self.det.end_epoch(if *own { EPOCH } else { EPOCH + 1 }, Ended::SessionReset);
                self.run();
            }
            Input::ModelsReset { own_model } => {
                let model = if *own_model { "Moment" } else { "Chat" };
                let models = if *own_model && self.reference_kind_events() { "Chat" } else { model };
                self.post(d::DeliveryInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec![models.into()] }));
            }
            Input::Closing => {
                self.det.close();
                self.run();
            }
        }
        let got = self.modelled(take(&self.log));
        let want = self.reference.step(input);
        assert_eq!(got, want, "the owner and the reference give the same effects");
        want
    }

    fn reference_kind_events(&self) -> bool { matches!(self.reference.state, super::ref_delivery::State::Live { kind: Kind::Events, .. }) }

    /// Reads the stream with `Pull` until the reader waits or the stream is over, and compares with the reference.
    fn drain(&mut self) -> Vec<Val> {
        let values = self.pull_all();
        assert_eq!(values, self.reference.drain(), "the owner and the reference give the same values");
        values
    }

    fn pull_all(&mut self) -> Vec<Val> {
        let mut values = Vec::new();
        for _ in 0..2_000 {
            if self.state().journal_len(STREAM) == 0 {
                break;
            }
            let input = self.pull_input();
            let d::DeliveryInput::Pull { outcome, .. } = input else { unreachable!() };
            self.det.post(self.addr, EpochStamp::Control, input);
            self.run();
            for entry in take(&self.log) {
                if let Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { outcome: got, answer: PullAnswer::Value(value) }, .. }) = entry {
                    assert_eq!(got, outcome);
                    values.push(val_of(&value));
                }
            }
        }
        values
    }

    fn step_drain(&mut self, input: Input) -> (Vec<Effect>, Vec<Val>) {
        let effects = self.step(input);
        (effects, self.drain())
    }
}

fn open_rows() -> Twin {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    twin
}

fn open_events() -> Twin {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind: Kind::Events, epoch_current: true });
    twin.step(Input::Joined { position: 0 });
    twin
}

/// A rows stream in `Live`, revision 1, rows `r1`, window `Loaded`, its first value read.
fn live_rows() -> Twin {
    let mut twin = open_rows();
    twin.step(Input::WindowState(loaded()));
    twin.step(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    twin.drain();
    twin
}

fn snapshot_val(rows: &[&str], state: WState) -> Val { Val::Snapshot { rows: ids(rows), state } }

fn failed() -> WState { WState { phase: Phase::Failed, error: true, loaded: false } }

fn loading() -> WState { WState { phase: Phase::Loading, error: false, loaded: true } }

#[test]
fn dlv_open_rows_owner() {
    let mut twin = Twin::new();
    let effects = twin.step(Input::Open { kind: Kind::Rows, epoch_current: true });
    assert_eq!(effects, [Effect::Read(0), Effect::InterestPlus { force: false }]);
    assert!(twin.drain().is_empty(), "no value before the first read");
}

#[test]
fn dlv_open_events_owner() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::Open { kind: Kind::Events, epoch_current: true }), [Effect::WantPlus]);
    assert!(twin.drain().is_empty(), "no value before the first event");
}

#[test]
fn dlv_open_stale_owner() {
    let mut twin = Twin::new();
    assert!(twin.step(Input::Open { kind: Kind::Rows, epoch_current: false }).is_empty());
    assert_eq!(twin.drain(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_opening_read_done_owner() {
    let mut twin = open_rows();
    twin.step(Input::WindowState(loaded()));
    let (effects, values) = twin.step_drain(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    assert!(effects.is_empty());
    assert_eq!(values, [snapshot_val(&["r1"], loaded())]);
}

#[test]
fn dlv_opening_read_failed() {
    let mut twin = open_rows();
    let (_, values) = twin.step_drain(Input::ReadFailed { revision: 1 });
    assert_eq!(values, [snapshot_val(&[], failed())], "the first value of a failed read has no rows and Failed");
}

#[test]
fn dlv_opening_committed() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_drain(Input::Committed { revision: 2, touches: true });
    assert_eq!(effects, [Effect::Read(2)]);
    assert!(values.is_empty());
    twin.step(Input::WindowState(loaded()));
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 3, rows: ids(&["r1", "r2"]) });
    assert_eq!(values, [snapshot_val(&["r1", "r2"], loaded())], "the first value shows the newer rows");
}

#[test]
fn dlv_opening_unrelated() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_drain(Input::Committed { revision: 2, touches: false });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_opening_window() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_drain(Input::WindowState(loaded()));
    assert!(effects.is_empty() && values.is_empty(), "the window state waits for the first read");
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    assert_eq!(values, [snapshot_val(&["r1"], loaded())], "the first value carries the window state received while opening");
}

#[test]
fn dlv_opening_event() {
    let mut twin = open_rows();
    for input in [Input::ModelEvent { position: 1, id: "c1".into() }, Input::ModelLost { first: 2, last: 3 }] {
        let (effects, values) = twin.step_drain(input);
        assert!(effects.is_empty() && values.is_empty());
    }
}

#[test]
fn dlv_opening_pull() {
    let mut twin = open_rows();
    assert!(twin.drain().is_empty(), "the reader waits while the first read is pending");
    let waits = snapshot(&twin.log).is_empty();
    assert!(waits, "a waiting reader gives no value");
}

#[test]
fn dlv_opening_models_reset() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_drain(Input::ModelsReset { own_model: true });
    assert_eq!(effects, [Effect::Read(0)]);
    assert!(values.is_empty());
}

#[test]
fn dlv_opening_models_other() {
    let mut twin = open_rows();
    let (effects, values) = twin.step_drain(Input::ModelsReset { own_model: false });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_cancel_owner() {
    for (mut twin, leave) in [(live_rows(), Effect::InterestMinus), (open_events(), Effect::WantMinus)] {
        let (effects, values) = twin.step_drain(Input::Cancel);
        assert_eq!(effects, [leave]);
        assert_eq!(values, [Val::Ended(Cause::Cancelled)]);
    }
    let mut opening = open_rows();
    assert_eq!(opening.step_drain(Input::Cancel).1, [Val::Ended(Cause::Cancelled)], "Opening is cancelled too");
}

#[test]
fn dlv_epoch_ended() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::EpochEnded { own: true });
    assert_eq!(effects, [Effect::InterestMinus]);
    assert_eq!(values, [Val::Ended(Cause::SessionEnded)]);
    let mut opening = open_rows();
    assert_eq!(opening.step_drain(Input::EpochEnded { own: true }).1, [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_epoch_other() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::EpochEnded { own: false });
    assert!(effects.is_empty() && values.is_empty(), "an end of another epoch changes nothing");
    assert_eq!(twin.state().stage(STREAM), Some(d::DeliveryStage::Live));
}

#[test]
fn dlv_closing_owner() {
    for mut twin in [live_rows(), open_rows()] {
        let (effects, values) = twin.step_drain(Input::Closing);
        assert_eq!(effects, [Effect::Drained]);
        assert_eq!(values, [Val::Ended(Cause::Closed)]);
    }
}

#[test]
fn dlv_live_committed_owner() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::Committed { revision: 2, touches: true });
    assert_eq!(effects, [Effect::Read(2)]);
    assert!(values.is_empty(), "no value before the read is done");
}

#[test]
fn dlv_live_unrelated_owner() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::Committed { revision: 2, touches: false });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_read_changed_owner() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 2, rows: ids(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { upserted: ids(&["r2"]), removed: Vec::new() }]);
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 3, rows: ids(&["r2"]) });
    assert_eq!(values, [Val::Delta { upserted: Vec::new(), removed: ids(&["r1"]) }]);
}

#[test]
fn dlv_live_read_changed_big_shift() {
    let mut twin = Twin::new();
    twin.det.post(twin.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::Rows), epoch: EPOCH, revision: 0, identity: true, valid: true });
    twin.run();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision: 1, data: d::ReadData::Rows(Vec::new()) });
    let pull = twin.pull_input();
    twin.post(pull);
    take(&twin.log);
    let many: Vec<d::StreamRow> = (0..300).map(|n| d::StreamRow { id: format!("r{n}"), stamp: 0 }).collect();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision: 2, data: d::ReadData::Rows(many) });
    let pull = twin.pull_input();
    twin.post(pull);
    let delivered: Vec<d::Value> = take(&twin.log).into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) => Some(value),
        _ => None,
    }).collect();
    assert!(matches!(delivered.as_slice(), [d::Value::Snapshot { revision: 2, rows, .. }] if rows.len() == 300), "a shift over the limit arrives as a Snapshot");
}

#[test]
fn dlv_live_read_same_owner() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::ReadDone { revision: 2, rows: ids(&["r1"]) });
    assert!(effects.is_empty() && values.is_empty(), "the same rows give no value");
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 3, rows: ids(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { upserted: ids(&["r2"]), removed: Vec::new() }]);
    assert!(twin.state().journal(STREAM).is_empty(), "the journal is empty after the drain");
}

#[test]
fn dlv_live_read_old() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 0, rows: ids(&["rX"]) });
    assert!(values.is_empty(), "an older read gives no value");
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 1, rows: ids(&["rY"]) });
    assert!(values.is_empty(), "a read of the same revision gives no value");
}

#[test]
fn dlv_live_read_failed() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::ReadFailed { revision: 2 });
    assert_eq!(values, [Val::StateOnly(WState { phase: Phase::Loaded, error: true, loaded: true })], "Loaded with an error at the same rows, as a State value");
}

#[test]
fn dlv_live_window_owner() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::WindowState(loading()));
    assert_eq!(values, [Val::StateOnly(loading())]);
}

#[test]
fn dlv_live_window_same_owner() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::WindowState(loaded()));
    assert!(values.is_empty(), "the same window state gives no value");
}

#[test]
fn dlv_live_event_owner() {
    let mut twin = open_events();
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 1, id: "c1".into() });
    assert_eq!(values, [Val::Event { position: 1, id: "c1".into() }]);
}

#[test]
fn dlv_live_event_gap() {
    let mut twin = open_events();
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 3, id: "c3".into() });
    assert_eq!(values, [Val::Lost { first: 1, last: 2 }, Val::Event { position: 3, id: "c3".into() }]);
}

#[test]
fn dlv_live_event_old() {
    let mut twin = open_events();
    twin.step_drain(Input::ModelEvent { position: 2, id: "c2".into() });
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 1, id: "c1".into() });
    assert!(values.is_empty(), "an event with a position behind the next gives nothing");
}

#[test]
fn dlv_live_lost() {
    let mut twin = open_events();
    let (_, values) = twin.step_drain(Input::ModelLost { first: 1, last: 4 });
    assert_eq!(values, [Val::Lost { first: 1, last: 4 }]);
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 5, id: "c5".into() });
    assert_eq!(values, [Val::Event { position: 5, id: "c5".into() }], "the next position follows the lost range");
}

#[test]
fn dlv_live_event_rows_kind() {
    let mut twin = live_rows();
    for input in [Input::ModelEvent { position: 1, id: "c1".into() }, Input::ModelLost { first: 2, last: 3 }] {
        let (effects, values) = twin.step_drain(input);
        assert!(effects.is_empty() && values.is_empty(), "a rows stream takes no model events");
    }
}

#[test]
fn dlv_live_models_reset() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::ModelsReset { own_model: true });
    assert_eq!(effects, [Effect::Read(1)]);
    assert!(values.is_empty(), "no value before the read");
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 2, rows: Vec::new() });
    assert_eq!(values, [Val::Delta { upserted: Vec::new(), removed: ids(&["r1"]) }], "the stream shows the emptied model");
}

#[test]
fn dlv_live_models_other() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::ModelsReset { own_model: false });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_pull_owner() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(loading()));
    twin.step(Input::ReadDone { revision: 2, rows: ids(&["r1", "r2"]) });
    assert_eq!(twin.state().journal(STREAM).len(), 2, "the journal holds the Snapshot and the Delta");
    let values = twin.drain();
    assert_eq!(values.len(), 2, "Pull gives the journal one value at a time, oldest first");
}

#[test]
fn dlv_live_pull_empty_owner() {
    let mut twin = live_rows();
    assert!(twin.drain().is_empty(), "the reader waits for the next value");
    let (_, values) = twin.step_drain(Input::WindowState(loading()));
    assert_eq!(values.len(), 1, "the waiting reader gets the next value");
}

#[test]
fn dlv_ended_owner() {
    let mut twin = live_rows();
    twin.step(Input::Cancel);
    assert_eq!(twin.drain(), [Val::Ended(Cause::Cancelled)]);
    for input in [Input::ReadDone { revision: 5, rows: ids(&["r9"]) }, Input::WindowState(loading()), Input::Committed { revision: 6, touches: true }, Input::Cancel, Input::EpochEnded { own: true }] {
        let (_, values) = twin.step_drain(input);
        assert!(values.is_empty(), "nothing follows Ended");
    }
    take(&twin.log);
    twin.det.close();
    twin.run();
    assert!(take(&twin.log).iter().any(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))), "Closing is answered with Drained also when every stream has ended");
    let pull = twin.pull_input();
    twin.det.post(twin.addr, EpochStamp::Control, pull);
    twin.run();
    assert!(take(&twin.log).iter().any(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Done, .. }, .. }))), "Pull after Ended gives None");
}

// Properties. The stubs are owners on the same scheduler: the explorer orders their inputs to the owner.

fn delivered(log: &Spied) -> Vec<d::Value> {
    snapshot(log).into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) => Some(value),
        _ => None,
    }).collect()
}

fn reads(log: &Spied) -> usize { snapshot(log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { .. }, .. }))).count() }

struct Rig {
    det: DetScheduler,
    log: Spied,
    deliveries: Address<Spy<Deliveries>>,
}

fn rig(kind: Kind) -> Rig {
    let mut det = DetScheduler::new(1);
    let log = new_log();
    let deliveries = add_deliveries(&mut det, &log);
    det.start_epoch(EPOCH);
    det.post(deliveries, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(kind), epoch: EPOCH, revision: 0, identity: true, valid: true });
    if kind == Kind::Events {
        det.post(deliveries, EpochStamp::Control, d::DeliveryInput::Joined { stream: STREAM, position: 0 });
    }
    det.run_until_quiet(100).expect("open");
    take(&log);
    Rig { det, log, deliveries }
}

impl Rig {
    fn source(&mut self, name: &'static str, script: Vec<d::DeliveryInput>) -> Address<Source<Deliveries>> {
        let source = self.det.add::<Source<Deliveries>>(name, SourceState { to: as_owner(self.deliveries), script }, Silent);
        self.det.post(source, EpochStamp::Control, SourceInput::Go);
        source
    }
}

fn pulls(count: usize) -> Vec<d::DeliveryInput> {
    let answers = Outcomes::<PullAnswer>::new();
    (0..count).map(|_| d::DeliveryInput::Pull { stream: STREAM, outcome: answers.register(EPOCH).id() }).collect()
}

fn row_sources(rig: &mut Rig) {
    rig.source("store", vec![
        d::DeliveryInput::ReadDone { stream: STREAM, read_id: 1, revision: 1, data: d::ReadData::Rows(rows_of(&ids(&["r1"]))) },
        d::DeliveryInput::Committed(Committed { batch: 0, revision: 2, tables: vec!["moments".into()], models: Vec::new() }),
        d::DeliveryInput::ReadDone { stream: STREAM, read_id: 2, revision: 2, data: d::ReadData::Rows(rows_of(&ids(&["r1", "r2"]))) },
    ]);
    rig.source("window", vec![d::DeliveryInput::WindowState { stream: STREAM, state: load_state(loading()) }, d::DeliveryInput::WindowState { stream: STREAM, state: load_state(loaded()) }]);
    rig.source("reader", pulls(4));
}

fn state_rows_revision(value: &d::Value) -> Option<i64> {
    match value {
        d::Value::Snapshot { revision, .. } | d::Value::Delta { revision, .. } => Some(*revision),
        _ => None,
    }
}

const LIMITS: Limits = Limits { depth: 4, max_traces: 20_000 };

/// P-DLV-01, P-DLV-05, P-DLV-06.
#[test]
fn dlv_prop_first_and_base_owner() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Rows);
            row_sources(&mut rig);
            let key = Rc::new(rig.log.clone());
            LOG.with(|slot| *slot.borrow_mut() = Some(key));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            if let Some(first) = values.first() {
                if !matches!(first, d::Value::Snapshot { .. }) {
                    return Err(format!("the first value is {first:?}"));
                }
            }
            let mut previous: Option<i64> = None;
            for value in &values {
                if let d::Value::Delta { base, .. } = value {
                    if previous != Some(*base) {
                        return Err(format!("a Delta with base {base} follows revision {previous:?}"));
                    }
                }
                if let d::Value::Snapshot { rows, state, .. } = value {
                    if state.phase == WindowPhase::Failed && !rows.is_empty() {
                        return Err("Failed with rows".into());
                    }
                }
                previous = state_rows_revision(value).or(previous);
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 10, "the explorer walked {} traces", report.traces);
}

use std::cell::RefCell;
use std::rc::Rc;

thread_local! {
    static LOG: RefCell<Option<Rc<Spied>>> = const { RefCell::new(None) };
}

/// P-DLV-04: rows that are in the base at revision R are in the last value (read or unread) at revision >= R once the reads are in.
#[test]
fn dlv_prop_rows_visible_owner() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Rows);
            row_sources(&mut rig);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |det| {
            if !det.ready().is_empty() {
                return Ok(());
            }
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            let count = values.iter().filter(|value| matches!(value, d::Value::Snapshot { .. } | d::Value::Delta { .. })).count();
            if count == 0 {
                return Ok(());
            }
            let ids_after: Vec<String> = {
                let mut rows: Vec<String> = Vec::new();
                for value in &values {
                    match value {
                        d::Value::Snapshot { rows: now, .. } => rows = now.iter().map(|row| row.id.clone()).collect(),
                        d::Value::Delta { upserted, removed, .. } => {
                            rows.retain(|id| !removed.contains(id));
                            rows.extend(upserted.iter().map(|row| row.id.clone()));
                        }
                        _ => {}
                    }
                }
                rows
            };
            let last_revision = values.iter().filter_map(state_rows_revision).last().unwrap_or(0);
            if last_revision >= 2 && !(ids_after.contains(&"r1".to_string()) && ids_after.contains(&"r2".to_string())) {
                return Err(format!("revision {last_revision} shows {ids_after:?}"));
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-DLV-02: positions of `Event` and ranges of `Lost` cover 1..N without gaps and repeats.
#[test]
fn dlv_prop_events_cover_owner() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Events);
            rig.source("feed", vec![
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 1, payload: "c1".into() },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 2, payload: "c2".into() },
                d::DeliveryInput::ModelLost { stream: STREAM, first: 3, last: 4 },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 6, payload: "c6".into() },
            ]);
            rig.source("reader", pulls(5));
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let mut next = 1;
            for value in delivered(&log) {
                match value {
                    d::Value::Event { position, .. } if position == next => next += 1,
                    d::Value::Lost { first, last } if first == next => next = last + 1,
                    other => return Err(format!("{other:?} at position {next}")),
                }
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-DLV-03 and P-DLV-08: `Ended` once and last, whatever the order of the end of the epoch and the other inputs.
#[test]
fn dlv_prop_ended_once_owner() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Rows);
            row_sources(&mut rig);
            let ender = rig.det.add::<Ender>("lifecycle", EnderState { epoch: EPOCH, cause: Ended::SessionReset }, Silent);
            rig.det.post(ender, EpochStamp::Control, SourceInput::Go);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            let ended = values.iter().filter(|value| matches!(value, d::Value::Ended(_))).count();
            if ended > 1 {
                return Err("Ended twice".into());
            }
            if let Some(at) = values.iter().position(|value| matches!(value, d::Value::Ended(_))) {
                if at + 1 != values.len() {
                    return Err(format!("{:?} follows Ended", &values[at + 1..]));
                }
                if values[at] != d::Value::Ended(d::EndCause::SessionEnded) {
                    return Err(format!("{:?}", values[at]));
                }
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-DLV-08: a stream of the ended epoch gives only `Ended(SessionEnded)`.
#[test]
fn dlv_prop_epoch() {
    let mut twin = live_rows();
    twin.step(Input::EpochEnded { own: true });
    for input in [Input::ReadDone { revision: 9, rows: ids(&["r9"]) }, Input::WindowState(loading()), Input::Committed { revision: 10, touches: true }] {
        twin.step(input);
    }
    assert_eq!(twin.drain(), [Val::Ended(Cause::SessionEnded)]);
}

/// P-DLV-05: the rows and the window state of a value are from one revision: the unread journal holds each pair as it was.
#[test]
fn dlv_prop_same_revision_owner() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(loading()));
    twin.step(Input::ReadDone { revision: 2, rows: ids(&["r1", "r2"]) });
    let journal = twin.state().journal(STREAM);
    assert!(matches!(&journal[0], d::Value::State { revision: 1, state } if state.phase == WindowPhase::Loading), "{journal:?}");
    assert!(matches!(&journal[1], d::Value::Delta { base: 1, revision: 2, .. }), "{journal:?}");
    twin.drain();
}

/// P-DLV-06: `Failed` only without rows; an error with rows is `Loaded` with the error.
#[test]
fn dlv_prop_failed_without_rows_owner() {
    let mut empty = open_rows();
    assert_eq!(empty.step_drain(Input::ReadFailed { revision: 1 }).1, [snapshot_val(&[], failed())]);
    let mut with_rows = live_rows();
    let (_, values) = with_rows.step_drain(Input::ReadFailed { revision: 2 });
    assert!(matches!(values.as_slice(), [Val::StateOnly(WState { phase: Phase::Loaded, error: true, .. })]));
}

/// P-DLV-07: no commit touches the stream: no read and no value.
#[test]
fn dlv_prop_idle_owner() {
    let mut twin = live_rows();
    let before = snapshot(&twin.log).len();
    for revision in 2..8 {
        let (effects, values) = twin.step_drain(Input::Committed { revision, touches: false });
        assert!(effects.is_empty() && values.is_empty());
    }
    assert_eq!(snapshot(&twin.log).len(), before);
    assert_eq!(reads(&twin.log), 0);
    assert_eq!(twin.det.ready().len(), 0, "the owner has nothing to do");
}

/// P-DLV-09: the journal never holds more than the limit and a slow reader gets `Lost` for the evicted events.
#[test]
fn dlv_prop_bounded_owner() {
    let mut twin = open_events();
    for position in 1..=600u64 {
        twin.post(d::DeliveryInput::ModelEvent { stream: STREAM, position, payload: format!("c{position}") });
        assert!(twin.state().journal_len(STREAM) <= d::JOURNAL_LIMIT);
    }
    let values = twin.pull_all();
    let mut next = 1;
    for value in &values {
        match value {
            Val::Event { position, .. } if *position == next => next += 1,
            Val::Lost { first, last } if *first == next => next = last + 1,
            other => panic!("{other:?} at {next}"),
        }
    }
    assert_eq!(next, 601, "events and lost ranges cover 1..=600");
    assert!(values.iter().any(|value| matches!(value, Val::Lost { .. })));
}

/// The dictionary: streams are independent, `Closing` gives one `Drained` for all of them.
#[test]
fn dlv_dictionary_owner() {
    let mut rig = rig(Kind::Rows);
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::Open { stream: 2, view: view(Kind::Events), epoch: EPOCH, revision: 0, identity: true, valid: true });
    rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::Cancel { stream: 2 });
    rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::ReadDone { stream: STREAM, read_id: 1, revision: 1, data: d::ReadData::Rows(rows_of(&ids(&["r1"]))) });
    rig.det.close();
    rig.det.run_until_quiet(100).expect("quiet");
    let drained = snapshot(&rig.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 1, "one Drained for every stream");
    let state = inner(&rig.det, rig.deliveries);
    assert_eq!(state.stage(STREAM), Some(d::DeliveryStage::Ended));
    assert_eq!(state.stage(2), Some(d::DeliveryStage::Ended));
}

#[test]
fn dlv_epoch_started_unknown_cause() {
    let mut rig = rig(Kind::Rows);
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochStarted(crate::owners::contract::EpochStarted { epoch: 2, identity: None, generation: 0 }));
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochEnded(EpochEnded { epoch: 2, cause: Ended::SessionReset }));
    rig.det.run_until_quiet(100).expect("quiet");
    assert_eq!(inner(&rig.det, rig.deliveries).stage(STREAM), Some(d::DeliveryStage::Opening), "the stream of epoch 1 is not touched by the end of epoch 2");
}

// Cells of the second revision of `delivery.md`: `Joined`, `ModelReady`, `State`, the refused `open`, the waits.

#[test]
fn dlv_open_no_session_owner() {
    let mut twin = Twin::new();
    assert!(twin.step(Input::OpenNoSession).is_empty());
    assert_eq!(twin.state().stage(STREAM), Some(d::DeliveryStage::Ended));
    assert_eq!(twin.drain(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_open_invalid_owner() {
    let mut twin = Twin::new();
    assert!(twin.step(Input::OpenInvalid).is_empty());
    assert_eq!(twin.drain(), [Val::Ended(Cause::Invalid)]);
}

#[test]
fn dlv_live_joined_owner() {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind: Kind::Events, epoch_current: true });
    assert!(twin.step(Input::Joined { position: 4 }).is_empty());
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 5, id: "c5".into() });
    assert_eq!(values, [Val::Event { position: 1, id: "c5".into() }], "the first event after the join is position 1 of the stream");
    assert!(twin.step(Input::Joined { position: 9 }).is_empty(), "a second Joined changes nothing");
    let (_, values) = twin.step_drain(Input::ModelEvent { position: 6, id: "c6".into() });
    assert_eq!(values, [Val::Event { position: 2, id: "c6".into() }]);
}

#[test]
fn dlv_live_event_before_join_owner() {
    let mut twin = Twin::new();
    twin.step(Input::Open { kind: Kind::Events, epoch_current: true });
    let (effects, values) = twin.step_drain(Input::ModelEvent { position: 1, id: "c1".into() });
    assert!(effects.is_empty() && values.is_empty(), "an event before Joined gives nothing");
    let (_, values) = twin.step_drain(Input::ModelLost { first: 1, last: 2 });
    assert!(values.is_empty(), "a loss before Joined gives nothing");
}

#[test]
fn dlv_live_lost_old_owner() {
    let mut twin = open_events();
    twin.step_drain(Input::ModelEvent { position: 3, id: "c3".into() });
    let (_, values) = twin.step_drain(Input::ModelLost { first: 1, last: 2 });
    assert!(values.is_empty(), "a loss behind the next position gives nothing");
    let (_, values) = twin.step_drain(Input::ModelLost { first: 3, last: 6 });
    assert_eq!(values, [Val::Lost { first: 4, last: 6 }], "only the positions from next_feed are lost, counted in stream positions");
}

#[test]
fn dlv_live_ready_owner() {
    let mut twin = open_events();
    let (_, values) = twin.step_drain(Input::ModelReady);
    assert_eq!(values, [Val::Ready]);
}

#[test]
fn dlv_live_ready_again_owner() {
    let mut twin = open_events();
    twin.step_drain(Input::ModelReady);
    let (effects, values) = twin.step_drain(Input::ModelReady);
    assert!(effects.is_empty() && values.is_empty(), "Ready comes once per stream");
}

#[test]
fn dlv_ended_closing_owner() {
    let mut twin = live_rows();
    twin.step(Input::EpochEnded { own: true });
    take(&twin.log);
    twin.det.close();
    twin.run();
    let drained = take(&twin.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 1, "an ended stream still lets the owner answer Closing once");
}

/// Waits: `pull` gives the first value or none at once, the sink is woken once by the first value of an empty journal, `cancel` ends at once.
#[test]
fn dlv_wait_pull_owner() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(loading()));
    twin.step(Input::EpochEnded { own: true });
    assert_eq!(twin.drain(), [Val::StateOnly(loading()), Val::Ended(Cause::SessionEnded)], "the journal is read in order, `Ended` last");
    assert!(twin.pull_all().is_empty(), "after Ended the stream gives nothing");
}

#[test]
fn dlv_wait_available_owner() {
    let mut twin = live_rows();
    let pull = twin.pull_input();
    let d::DeliveryInput::Pull { outcome, .. } = pull else { unreachable!() };
    twin.det.post(twin.addr, EpochStamp::Control, pull);
    twin.run();
    assert!(take(&twin.log).iter().all(|entry| !matches!(entry, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { .. }, .. }))), "a Pull on an empty journal waits");
    twin.post(d::DeliveryInput::WindowState { stream: STREAM, state: load_state(loading()) });
    let answers: Vec<OutcomeId> = take(&twin.log).into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { outcome, answer: PullAnswer::Value(d::Value::State { .. }) }, .. }) => Some(outcome),
        _ => None,
    }).collect();
    assert_eq!(answers, [outcome], "the first value of an empty journal answers the waiting Pull once");
    twin.post(d::DeliveryInput::WindowState { stream: STREAM, state: load_state(loaded()) });
    assert!(take(&twin.log).iter().all(|entry| !matches!(entry, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { .. }, .. }))), "a value added with no waiting Pull answers nobody");
    assert_eq!(twin.state().journal_len(STREAM), 1);
}

#[test]
fn dlv_wait_cancel_owner() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_drain(Input::Cancel);
    assert_eq!(effects, [Effect::InterestMinus]);
    assert_eq!(values, [Val::Ended(Cause::Cancelled)]);
}

/// P-DLV-10: `Ready` is one value per `Events` stream, however often `ModelReady` comes.
#[test]
fn dlv_prop_ready_once_owner() {
    let mut twin = open_events();
    for _ in 0..3 {
        twin.step(Input::ModelReady);
        twin.step(Input::ModelEvent { position: 1, id: "c1".into() });
    }
    let values = twin.drain();
    assert_eq!(values.iter().filter(|value| matches!(value, Val::Ready)).count(), 1);
}

/// P-DLV-11: the rows of a `Delta` are the changed rows; the whole result only goes in the first value and over 256 changes.
#[test]
fn dlv_prop_delta_size_owner() {
    let mut twin = live_rows();
    let (_, values) = twin.step_drain(Input::ReadDone { revision: 2, rows: ids(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { upserted: ids(&["r2"]), removed: Vec::new() }], "one changed row, one row in the value");
    let many: Vec<String> = (0..300).map(|n| format!("n{n}")).collect();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision: 3, data: d::ReadData::Rows(rows_of(&many)) });
    let values = twin.pull_all();
    assert!(matches!(values.as_slice(), [Val::Snapshot { rows, .. }] if rows.len() == 300), "over 256 changes the whole result goes");
}

// Kinds `Entity` and `Count`, `read_id` and the `force` flag of `Interest+` (`delivery.md`, kinds of streams).

fn open_kind(kind: Kind) -> (Twin, Vec<Effect>) {
    let mut twin = Twin::new();
    let effects = twin.step(Input::Open { kind, epoch_current: true });
    (twin, effects)
}

#[test]
fn dlv_open_entity_fetch() {
    let cases = [(r::Fetch::Never, None), (r::Fetch::Stale, Some(false)), (r::Fetch::Force, Some(true)), (r::Fetch::IfMissing, None)];
    for (fetch, force) in cases {
        let (_, effects) = open_kind(Kind::Entity(fetch));
        let want: Vec<Effect> = std::iter::once(Effect::Read(0)).chain(force.map(|force| Effect::InterestPlus { force })).collect();
        assert_eq!(effects, want, "{fetch:?}: Interest+ at open carries force only for stale and force");
    }
}

#[test]
fn dlv_opening_read_done_if_missing() {
    let (mut twin, _) = open_kind(Kind::Entity(r::Fetch::IfMissing));
    let (effects, values) = twin.step_drain(Input::ReadDone { revision: 1, rows: Vec::new() });
    assert_eq!(effects, [Effect::InterestPlus { force: false }], "no row after the first read: ask the window");
    assert_eq!(values, [snapshot_val(&[], WState::IDLE)]);
    let (mut present, _) = open_kind(Kind::Entity(r::Fetch::IfMissing));
    let (effects, values) = present.step_drain(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
    assert!(effects.is_empty(), "a row is there: no Interest+");
    assert_eq!(values, [snapshot_val(&["r1"], WState::IDLE)]);
}

#[test]
fn dlv_opening_read_stale() {
    for kind in [Kind::Rows, Kind::Entity(r::Fetch::Stale)] {
        let (mut twin, _) = open_kind(kind);
        let (effects, values) = twin.step_drain(Input::ReadStale { revision: 1, rows: ids(&["rX"]) });
        assert!(effects.is_empty() && values.is_empty(), "a read of another read_id is dropped");
        let (_, values) = twin.step_drain(Input::ReadDone { revision: 1, rows: ids(&["r1"]) });
        assert_eq!(values, [snapshot_val(&["r1"], WState::IDLE)]);
        let (_, values) = twin.step_drain(Input::ReadStale { revision: 2, rows: ids(&["rY"]) });
        assert!(values.is_empty(), "Live drops the read of another read_id too");
    }
}

#[test]
fn dlv_count_values() {
    let (mut twin, effects) = open_kind(Kind::Count);
    assert_eq!(effects, [Effect::Read(0), Effect::InterestPlus { force: false }]);
    let (_, values) = twin.step_drain(Input::CountDone { revision: 1, count: 3 });
    assert_eq!(values, [Val::Count { count: 3, state: WState::IDLE }]);
    twin.step(Input::Committed { revision: 2, touches: true });
    let (_, values) = twin.step_drain(Input::CountDone { revision: 2, count: 3 });
    assert!(values.is_empty(), "the same count: no value");
    twin.step(Input::Committed { revision: 3, touches: true });
    let (_, values) = twin.step_drain(Input::CountDone { revision: 3, count: 4 });
    assert_eq!(values, [Val::Count { count: 4, state: WState::IDLE }]);
}
