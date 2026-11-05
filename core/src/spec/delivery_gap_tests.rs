//! Owner-level tests of the `delivery` machine: the owner `Deliveries` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_delivery.rs`) and to the owner through its address, then compares the effects.
//! The values a reader gets are the `Answer` effects of `Pull`. Names are the `Test` column.

use std::cell::RefCell;
use std::rc::Rc;

use super::ref_delivery::{self as r, row, rows, Cause, Delivery, Effect, Fetch, Input, Kind, Phase, Pulled, Val, Ws};
use crate::owners::contract::{Committed, EpochEnded, Models, ModelsReset, WindowPhase, WindowState};
use crate::owners::read::deliveries::{self as d, DeliveryEffect, DeliveryPeers, Deliveries, PullAnswer, StreamEffect};
use crate::owners::read::feed::Feed;
use crate::owners::read::windows::{WindowKey, Windows};
use crate::owners::support::{as_owner, new_log, snapshot, take, Discard, Emitted, Ender, EnderState, Log, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState};
use crate::runtime::{explore, Address, Cause as Ended, DetScheduler, EpochStamp, Limits, OutcomeId, Outcomes};

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
        Kind::Blob => (d::Kind::Blob, "Moment"),
        Kind::Retry => (d::Kind::RetryStatus, "Moment"),
    };
    d::View { kind, model: model.into(), tables: vec!["moments".into()], local: false, remote: true, window: window(), subject: "t1".into() }
}

fn rows_of(rows: &[r::Row]) -> Vec<d::StreamRow> { rows.iter().map(|(id, stamp)| d::StreamRow::new(id, *stamp)).collect() }

fn window_state(state: &Ws) -> WindowState {
    let phase = match state.phase { Phase::Idle => WindowPhase::Idle, Phase::Loading => WindowPhase::Loading, Phase::Loaded => WindowPhase::Loaded, Phase::Failed => WindowPhase::Failed };
    WindowState { phase, error: state.error.then(|| "e".to_string()), loaded: state.loaded }
}

fn ws_of(state: &WindowState) -> Ws {
    let phase = match state.phase { WindowPhase::Idle => Phase::Idle, WindowPhase::Loading => Phase::Loading, WindowPhase::Loaded => Phase::Loaded, WindowPhase::Failed => Phase::Failed };
    Ws { phase, error: state.error.is_some(), loaded: state.loaded }
}

fn val_of(value: &d::Value) -> Val {
    let ids = |rows: &[d::StreamRow]| rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    match value {
        d::Value::Snapshot { revision, rows, state } => Val::Snapshot { revision: *revision, rows: ids(rows), state: ws_of(state) },
        d::Value::Delta(delta) => Val::Delta { base: delta.base, revision: delta.revision, size: delta.size as usize, upserted: ids(&delta.rows), removed: delta.removed.clone(), state: ws_of(&delta.state) },
        d::Value::Count { revision, count, state } => Val::Count { revision: *revision, count: *count, state: ws_of(state) },
        d::Value::State { revision, state } => Val::State { revision: *revision, state: ws_of(state) },
        d::Value::Ready => Val::Ready,
        d::Value::Event { position, payload } => Val::Event { position: *position, payload: payload.clone() },
        d::Value::Lost { first, last } => Val::Lost { first: *first, last: *last },
        d::Value::Gap => Val::Gap,
        d::Value::Status { revision, blob } => Val::Status { revision: *revision, state: blob.state.clone() },
        d::Value::Retry(retry) => Val::Retry { attempts: retry.attempts },
        d::Value::Ended(cause) => Val::Ended(match cause {
            d::EndCause::SessionEnded => Cause::SessionEnded,
            d::EndCause::Closed => Cause::Closed,
            d::EndCause::Cancelled => Cause::Cancelled,
            d::EndCause::Invalid => Cause::Invalid,
        }),
    }
}

fn pulled_of(answer: &PullAnswer) -> Pulled {
    match answer {
        PullAnswer::Value(value) => Pulled::Value(val_of(value)),
        PullAnswer::Done => Pulled::Done,
        PullAnswer::Invalid => Pulled::Invalid,
    }
}

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
                        StreamEffect::Read { revision, .. } => Effect::Read(revision),
                        StreamEffect::Answer { answer, .. } => Effect::Answer(pulled_of(&answer)),
                        StreamEffect::Available => Effect::Available,
                        StreamEffect::CloseSink => Effect::CloseSink,
                        StreamEffect::Cancelled { reply } => Effect::Cancelled(reply),
                        StreamEffect::StatusInterest { plus } => Effect::StatusInterest(plus),
                    });
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPlus") => modelled.push(Effect::InterestPlus),
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Load") => modelled.push(Effect::Load { force: input.contains("force: true") }),
                Emitted::Send { to: WINDOWS, .. } => modelled.push(Effect::InterestMinus),
                Emitted::Send { to: FEED, input } if input.starts_with("WantPlus") => modelled.push(Effect::WantPlus),
                Emitted::Send { to: FEED, .. } => modelled.push(Effect::WantMinus),
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
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(*kind), epoch, revision: 0, identity: true, valid: true });
                self.run();
            }
            Input::OpenNoSession => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::List), epoch: EPOCH, revision: 0, identity: false, valid: true });
                self.run();
            }
            Input::OpenInvalid => {
                self.det.post(self.addr, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::List), epoch: EPOCH, revision: 0, identity: true, valid: false });
                self.run();
            }
            Input::Committed { revision, touches } => {
                let table = if *touches { "moments" } else { "other" };
                self.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables: vec![table.into()], models: Default::default() }));
            }
            Input::ReadDone { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id(), revision: *revision, data: d::ReadData::Rows { rows: rows_of(rows), replaced: Vec::new() } }),
            Input::CountDone { revision, count } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id(), revision: *revision, data: d::ReadData::Count(*count) }),
            Input::ReadStale { revision, rows } => self.post(d::DeliveryInput::ReadDone { stream, read_id: self.read_id() + 100, revision: *revision, data: d::ReadData::Rows { rows: rows_of(rows), replaced: Vec::new() } }),
            Input::ReadFailed { revision } => self.post(d::DeliveryInput::ReadFailed { stream, read_id: self.read_id(), revision: *revision, error: "e".into() }),
            Input::WindowState(state) => self.post(d::DeliveryInput::WindowState { stream, state: window_state(state) }),
            Input::Joined { position } => self.post(d::DeliveryInput::Joined { stream, position: *position }),
            Input::ModelReady => self.post(d::DeliveryInput::ModelReady { stream }),
            Input::ModelEvent { position, payload } => self.post(d::DeliveryInput::ModelEvent { stream, position: *position, payload: payload.clone() }),
            Input::ModelLost { first, last } => self.post(d::DeliveryInput::ModelLost { stream, first: *first, last: *last }),
            Input::ModelGap => self.post(d::DeliveryInput::ModelGap { stream }),
            Input::Transfer { state } => self.post(d::DeliveryInput::TransferStatus { stream, blob: d::BlobStatus { state: state.clone(), progress: 0, error: None } }),
            Input::Retry { attempts } => self.post(d::DeliveryInput::RetryState { stream, retry: d::RetryStatus { attempts: *attempts, phase: "idle".into() } }),
            Input::Pull => {
                let outcome = self.answers.register(EPOCH).id();
                self.call(d::DeliveryInput::Pull { stream, outcome });
            }
            Input::Cancel { reply } => self.call(d::DeliveryInput::Cancel { stream, reply: *reply }),
            Input::Release => self.call(d::DeliveryInput::Release { stream }),
            Input::SinkClosed => self.call(d::DeliveryInput::SinkClosed { stream }),
            Input::EpochEnded { own } => {
                self.det.end_epoch(if *own { EPOCH } else { EPOCH + 1 }, Ended::SessionReset);
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
        let got = Self::modelled(take(&self.log));
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
        (Kind::Entity(Fetch::IfMissing), vec![Effect::Read(0), Effect::InterestPlus]),
    ] {
        assert_eq!(Twin::new().step(Input::Open { kind, epoch_current: true }), effects, "{kind:?}");
    }
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
    twin.call(d::DeliveryInput::Open { stream: STREAM, view: d::View { remote: false, ..view(Kind::List) }, epoch: EPOCH, revision: 0, identity: true, valid: true });
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Read(0)]);
}

/// dlv_opening_committed (D-34): a commit of `All` models touches a stream whose tables it does not list; the empty set touches none.
#[test]
fn dlv_committed_all_models_touch_every_stream() {
    let mut twin = open_rows();
    take(&twin.log);
    twin.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: 2, tables: vec!["other".into()], models: Models::All }));
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Read(2)]);
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
    assert_eq!(values, [Val::Delta { base: 1, revision: 5, size: 2, upserted: ids(&["r2"]), removed: Vec::new(), state: Ws::loaded_at(5) }]);
}

#[test]
fn dlv_opening_event() {
    let mut twin = open_rows();
    for input in [Input::Joined { position: 3 }, Input::ModelReady, Input::ModelEvent { position: 1, payload: "c1".into() }, Input::ModelLost { first: 2, last: 3 }, Input::ModelGap] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_opening_pull() {
    let mut twin = open_rows();
    assert!(twin.step(Input::Pull).is_empty(), "nothing to give yet");
    let effects = twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(effects, [Effect::Answer(Pulled::Value(snap(1, &["r1"], Ws::idle())))], "the first value goes to the waiting Pull, no Available");
}

#[test]
fn dlv_pull_twice() {
    let mut opening = open_rows();
    opening.step(Input::Pull);
    assert_eq!(opening.step(Input::Pull), [Effect::Answer(Pulled::Invalid)]);
    let mut live = live_rows();
    live.step(Input::Pull);
    assert_eq!(live.step(Input::Pull), [Effect::Answer(Pulled::Invalid)], "the first Pull keeps waiting");
    assert_eq!(live.step(Input::Committed { revision: 2, touches: true }), [Effect::Read(2)]);
    assert_eq!(live.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).len(), 1, "the waiting Pull is answered");
}

#[test]
fn dlv_opening_models_reset() {
    let mut twin = open_rows();
    twin.step(Input::Committed { revision: 2, touches: true });
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Read(2)]);
}

#[test]
fn dlv_opening_models_other() {
    let mut twin = open_rows();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
}

#[test]
fn dlv_cancel() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::Cancel { reply: 7 }), [Effect::InterestMinus, Effect::CloseSink], "the reply waits for SinkClosed");
    }
    let mut events = open_events();
    assert_eq!(events.step(Input::Cancel { reply: 7 }), [Effect::WantMinus, Effect::CloseSink]);
    let mut blob = open_as(Kind::Blob);
    assert_eq!(blob.step(Input::Cancel { reply: 7 }), [Effect::StatusInterest(false), Effect::CloseSink]);
}

#[test]
fn dlv_cancel_sink_closed() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))], "the waiting Pull gets Ended");
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Done)]);
    let mut first = live_rows();
    first.step(Input::Cancel { reply: 7 });
    assert_eq!(first.step(Input::SinkClosed), [Effect::Cancelled(7)]);
    assert_eq!(first.read_all(), [Val::Ended(Cause::Cancelled)], "the first Pull reads Ended");
}

#[test]
fn dlv_cancel_sink_closed_released() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Release);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7)]);
    assert!(twin.reference.gone);
    assert_eq!(twin.state().streams(), 0, "the machine is out of the dictionary");
}

#[test]
fn dlv_cancelling_other() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    for input in [Input::Committed { revision: 2, touches: true }, Input::ReadDone { revision: 2, rows: rows(&["r2"]) }, Input::ReadFailed { revision: 2 }, Input::WindowState(Ws::loaded_at(2)), Input::Joined { position: 1 }, Input::ModelReady, Input::ModelGap, Input::Transfer { state: "x".into() }, Input::ModelsReset { own_model: true }] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.step(Input::Pull).is_empty(), "a Pull waits for Ended");
    assert_eq!(twin.step(Input::Cancel { reply: 2 }), [], "a second Cancel waits for the same SinkClosed");
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Cancelled(2), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))]);
}

#[test]
fn dlv_cancelling_end() {
    let mut epoch = live_rows();
    epoch.step(Input::Cancel { reply: 1 });
    assert!(epoch.step(Input::EpochEnded { own: true }).is_empty());
    epoch.step(Input::SinkClosed);
    assert_eq!(epoch.read_all(), [Val::Ended(Cause::SessionEnded)]);
    let mut closing = live_rows();
    closing.step(Input::Cancel { reply: 1 });
    assert!(closing.step(Input::Closing).is_empty(), "Drained waits for SinkClosed");
    assert_eq!(closing.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Drained]);
    assert_eq!(closing.read_all(), [Val::Ended(Cause::Closed)]);
}

#[test]
fn dlv_release_live() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::Release), [Effect::InterestMinus, Effect::CloseSink]);
        assert!(twin.step(Input::SinkClosed).is_empty(), "no Ended for a reader that is gone");
        assert_eq!(twin.state().streams(), 0);
    }
}

#[test]
fn dlv_release_cancelling() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 3 });
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(3)]);
    assert_eq!(twin.state().streams(), 0);
}

#[test]
fn dlv_release_ended() {
    let mut twin = ended_by_epoch();
    twin.step(Input::SinkClosed);
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.state().streams(), 0, "the machine goes at once: the sink is closed");
}

#[test]
fn dlv_release_ended_sink_open() {
    let mut twin = ended_by_epoch();
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.state().streams(), 1);
    assert!(twin.step(Input::SinkClosed).is_empty());
    assert_eq!(twin.state().streams(), 0, "the machine goes after SinkClosed");
}

#[test]
fn dlv_ended_sink_closed() {
    let mut twin = ended_by_epoch();
    twin.step(Input::Cancel { reply: 9 });
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(9)]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_epoch_ended() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::EpochEnded { own: true }), [Effect::InterestMinus, Effect::Available, Effect::CloseSink]);
        assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
    }
    let mut waiting = live_rows();
    waiting.step(Input::Pull);
    assert_eq!(waiting.step(Input::EpochEnded { own: true }), [Effect::InterestMinus, Effect::Answer(Pulled::Value(Val::Ended(Cause::SessionEnded))), Effect::CloseSink]);
}

#[test]
fn dlv_epoch_other() {
    let mut twin = live_rows();
    assert!(twin.step(Input::EpochEnded { own: false }).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_closing() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::Closing), [Effect::InterestMinus, Effect::Available, Effect::CloseSink]);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Drained], "Drained once every sink is closed");
    assert_eq!(twin.read_all(), [Val::Ended(Cause::Closed)]);
}

#[test]
fn dlv_live_committed() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::Committed { revision: 2, touches: true }), [Effect::Read(2)]);
}

#[test]
fn dlv_live_unrelated() {
    let mut twin = live_rows();
    assert!(twin.step(Input::Committed { revision: 2, touches: false }).is_empty());
    let mut events = open_events();
    assert!(events.step(Input::Committed { revision: 2, touches: true }).is_empty());
    let mut blob = open_as(Kind::Blob);
    assert!(blob.step(Input::Committed { revision: 2, touches: true }).is_empty());
}

#[test]
fn dlv_live_read_unacked_first() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    twin.step(Input::Committed { revision: 2, touches: true });
    assert!(twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).is_empty(), "the journal held a value: no new Available");
    assert_eq!(twin.read_all(), [snap(2, &["r1", "r2"], Ws::loaded_at(2))], "the unread first value is replaced by the newer one");
}

#[test]
fn dlv_live_read_unread_snapshot() {
    let mut twin = live_rows();
    let many: Vec<r::Row> = (0..300).map(|n| (format!("n{n}"), 0)).collect();
    twin.step(Input::ReadDone { revision: 2, rows: many });
    twin.step(Input::ReadDone { revision: 3, rows: rows(&["n1", "r1"]) });
    assert_eq!(twin.read_all(), [snap(3, &["n1", "r1"], Ws::loaded_at(1))], "an unread recovery Snapshot gives way to a Snapshot");
}

#[test]
fn dlv_live_read_changed() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [Val::Delta { base: 1, revision: 2, size: 2, upserted: ids(&["r2"]), removed: Vec::new(), state: Ws::loaded_at(2) }]);
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: rows(&["r2"]) });
    assert_eq!(values, [Val::Delta { base: 2, revision: 3, size: 1, upserted: Vec::new(), removed: ids(&["r1"]), state: Ws::loaded_at(2) }]);
    let mut counted = open_as(Kind::Count);
    counted.step(Input::CountDone { revision: 1, count: 1 });
    counted.read_all();
    let (_, values) = counted.step_read(Input::CountDone { revision: 2, count: 2 });
    assert_eq!(values, [Val::Count { revision: 2, count: 2, state: Ws::idle() }]);
}

#[test]
fn dlv_live_read_unacknowledged() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    twin.step(Input::ReadDone { revision: 3, rows: rows(&["r1", "r2", "r3"]) });
    assert_eq!(twin.read_all(), [Val::Delta { base: 1, revision: 3, size: 3, upserted: ids(&["r2", "r3"]), removed: Vec::new(), state: Ws::loaded_at(1) }], "one Delta from the last read revision");
    assert_eq!(twin.state().acknowledged(STREAM), Some(3));
}

#[test]
fn dlv_live_read_same() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1"]) });
    assert!(effects.is_empty() && values.is_empty(), "the same rows: nothing");
    twin.step(Input::WindowState(Ws::loaded_at(3)));
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: rows(&["r1"]) });
    assert_eq!(values, [Val::State { revision: 1, state: Ws::loaded_at(3) }], "a held state that became visible goes alone");
}

#[test]
fn dlv_live_read_old() {
    let mut twin = live_rows();
    for input in [Input::ReadStale { revision: 2, rows: rows(&["rX"]) }, Input::ReadDone { revision: 1, rows: rows(&["rY"]) }, Input::ReadDone { revision: 0, rows: rows(&["rZ"]) }] {
        let (effects, values) = twin.step_read(input);
        assert!(effects.is_empty() && values.is_empty());
    }
}

#[test]
fn dlv_live_read_failed() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::ReadFailed { revision: 2 });
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Loaded, error: true, loaded: Some(1) } }], "an error with rows keeps Loaded");
    let mut empty = open_rows();
    empty.step(Input::ReadDone { revision: 1, rows: Vec::new() });
    empty.read_all();
    let (_, values) = empty.step_read(Input::ReadFailed { revision: 2 });
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Failed, error: true, loaded: None } }]);
    let (effects, values) = twin.step_read(Input::ReadFailed { revision: 1 });
    assert!(effects.is_empty() && values.is_empty(), "an old failure is dropped");
}

#[test]
fn dlv_live_window() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::WindowState(Ws::loading_after(1)));
    assert_eq!(values, [Val::State { revision: 1, state: Ws::loading_after(1) }]);
}

#[test]
fn dlv_live_window_held() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(2)));
    assert!(values.is_empty(), "the state is visible only with revision 2");
    assert_eq!(effects, [Effect::Read(2)], "a state past the last read starts a read");
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { base: 1, revision: 2, size: 2, upserted: ids(&["r2"]), removed: Vec::new(), state: Ws::loaded_at(2) }]);
}

#[test]
fn dlv_live_window_same() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(1)));
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_joined() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::ModelEvent { position: 5, payload: "c5".into() });
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready], "an event before Joined waits");
    twin.step(Input::Joined { position: 4 });
    assert_eq!(twin.read_all(), [Val::Event { position: 1, payload: "c5".into() }], "the held event follows Joined");
    assert!(twin.step(Input::Joined { position: 9 }).is_empty(), "a second Joined changes nothing");
}

#[test]
fn dlv_live_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    twin.step(Input::ModelEvent { position: 1, payload: "c1".into() });
    assert!(twin.read_all().is_empty(), "no event before Ready");
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Event { position: 1, payload: "c1".into() }]);
}

#[test]
fn dlv_live_ready_again() {
    let mut twin = open_events();
    assert!(twin.step(Input::ModelReady).is_empty());
}

#[test]
fn dlv_live_event_before_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    assert!(twin.step(Input::ModelEvent { position: 1, payload: "c1".into() }).is_empty());
    assert!(twin.step(Input::ModelLost { first: 2, last: 3 }).is_empty());
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Event { position: 1, payload: "c1".into() }, Val::Lost { first: 2, last: 3 }], "held inputs replay in order");
}

#[test]
fn dlv_live_event() {
    let mut twin = open_events();
    let (effects, values) = twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [Val::Event { position: 1, payload: "c1".into() }]);
}

#[test]
fn dlv_live_event_gap() {
    let mut twin = open_events();
    let (_, values) = twin.step_read(Input::ModelEvent { position: 3, payload: "c3".into() });
    assert_eq!(values, [Val::Lost { first: 1, last: 2 }, Val::Event { position: 3, payload: "c3".into() }]);
}

#[test]
fn dlv_live_event_old() {
    let mut twin = open_events();
    twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    let (effects, values) = twin.step_read(Input::ModelEvent { position: 1, payload: "again".into() });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_lost() {
    let mut twin = open_events();
    let (_, values) = twin.step_read(Input::ModelLost { first: 1, last: 3 });
    assert_eq!(values, [Val::Lost { first: 1, last: 3 }]);
    let (_, values) = twin.step_read(Input::ModelLost { first: 3, last: 5 });
    assert_eq!(values, [Val::Lost { first: 4, last: 5 }], "a range that starts below the next position is cut");
}

#[test]
fn dlv_live_lost_old() {
    let mut twin = open_events();
    twin.step_read(Input::ModelLost { first: 1, last: 3 });
    let (effects, values) = twin.step_read(Input::ModelLost { first: 1, last: 2 });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_gap() {
    let mut twin = open_events();
    twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    let (_, values) = twin.step_read(Input::ModelGap);
    assert_eq!(values, [Val::Gap]);
    let (_, values) = twin.step_read(Input::ModelEvent { position: 2, payload: "c2".into() });
    assert_eq!(values, [Val::Event { position: 2, payload: "c2".into() }], "a Gap takes no position");
}

#[test]
fn dlv_live_gap_before_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    assert!(twin.step(Input::ModelGap).is_empty());
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Gap]);
}

#[test]
fn dlv_live_event_rows_kind() {
    let mut twin = live_rows();
    for input in [Input::Joined { position: 1 }, Input::ModelReady, Input::ModelEvent { position: 1, payload: "c".into() }, Input::ModelLost { first: 1, last: 2 }, Input::ModelGap] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_live_models_reset() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Read(1)]);
}

#[test]
fn dlv_live_models_other() {
    let mut rows = live_rows();
    assert!(rows.step(Input::ModelsReset { own_model: false }).is_empty());
    let mut events = open_events();
    assert!(events.step(Input::ModelsReset { own_model: true }).is_empty());
}

#[test]
fn dlv_live_pull() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Delta { base: 1, revision: 2, size: 2, upserted: ids(&["r2"]), removed: Vec::new(), state: Ws::loaded_at(1) }))]);
    assert_eq!(twin.state().acknowledged(STREAM), Some(2), "the read of a rows value moves the acknowledged revision");
}

#[test]
fn dlv_live_pull_empty() {
    let mut twin = live_rows();
    assert!(twin.step(Input::Pull).is_empty());
    assert_eq!(twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).len(), 1, "the next value goes to the waiting Pull");
}

#[test]
fn dlv_ended_pull() {
    let mut twin = ended_by_epoch();
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Ended(Cause::SessionEnded)))]);
}

#[test]
fn dlv_ended_pull_none() {
    let mut twin = ended_by_epoch();
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Done)]);
}

#[test]
fn dlv_ended_closing() {
    let mut open = ended_by_epoch();
    open.step(Input::SinkClosed);
    assert_eq!(open.step(Input::Closing), [Effect::Drained], "all streams are settled");
    let mut slow = ended_by_epoch();
    assert!(slow.step(Input::Closing).is_empty());
    assert_eq!(slow.step(Input::SinkClosed), [Effect::Drained], "after SinkClosed");
    let mut none = Twin::new();
    assert_eq!(none.step(Input::Closing), [Effect::Drained], "no streams");
}

#[test]
fn dlv_ended_cancel() {
    let mut twin = ended_by_epoch();
    twin.step(Input::SinkClosed);
    assert_eq!(twin.step(Input::Cancel { reply: 5 }), [Effect::Cancelled(5)]);
}

#[test]
fn dlv_ended_cancel_wait() {
    let mut twin = ended_by_epoch();
    assert!(twin.step(Input::Cancel { reply: 5 }).is_empty());
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(5)]);
}

#[test]
fn dlv_ended() {
    let mut twin = ended_by_epoch();
    for input in [Input::Committed { revision: 9, touches: true }, Input::ReadDone { revision: 9, rows: rows(&["r9"]) }, Input::WindowState(Ws::loading_after(9)), Input::ModelsReset { own_model: true }, Input::ModelReady] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_wait_pull() {
    let mut twin = live_rows();
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::Closing), [Effect::InterestMinus, Effect::Answer(Pulled::Value(Val::Ended(Cause::Closed))), Effect::CloseSink]);
    let mut by_epoch = live_rows();
    by_epoch.step(Input::Pull);
    assert_eq!(by_epoch.step(Input::EpochEnded { own: true }).len(), 3, "a waiting Pull is answered with Ended(SessionEnded)");
}

#[test]
fn dlv_wait_cancel() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    let effects = twin.step(Input::SinkClosed);
    assert_eq!(effects, [Effect::Cancelled(1)], "the reply comes with SinkClosed, not before");
    let mut by_epoch = live_rows();
    by_epoch.step(Input::Cancel { reply: 1 });
    by_epoch.step(Input::EpochEnded { own: true });
    assert_eq!(by_epoch.step(Input::SinkClosed), [Effect::Cancelled(1)]);
    assert_eq!(by_epoch.read_all(), [Val::Ended(Cause::SessionEnded)]);
}


fn delivered(log: &Spied) -> Vec<d::Value> {
    snapshot(log).into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) => Some(value),
        _ => None,
    }).collect()
}

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
        det.post(deliveries, EpochStamp::Control, d::DeliveryInput::ModelReady { stream: STREAM });
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

fn rows_script(rev: &[(i64, &[&str])]) -> Vec<d::DeliveryInput> {
    let mut script = Vec::new();
    for (index, (revision, list)) in rev.iter().enumerate() {
        if index > 0 {
            script.push(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables: vec!["moments".into()], models: Default::default() }));
        }
        let rows = list.iter().map(|id| d::StreamRow::new(id, 0)).collect();
        script.push(d::DeliveryInput::ReadDone { stream: STREAM, read_id: index as u64 + 1, revision: *revision, data: d::ReadData::Rows { rows, replaced: Vec::new() } });
    }
    script
}

fn row_sources(rig: &mut Rig) {
    rig.source("store", rows_script(&[(1, &["r1"]), (2, &["r1", "r2"])]));
    rig.source("window", vec![d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loading_after(0)) }, d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loaded_at(2)) }]);
    rig.source("reader", pulls(4));
}

fn rows_revision(value: &d::Value) -> Option<i64> {
    match value {
        d::Value::Snapshot { revision, .. } => Some(*revision),
        d::Value::Delta(delta) => Some(delta.revision),
        _ => None,
    }
}

thread_local! {
    static LOG: RefCell<Option<Rc<Spied>>> = const { RefCell::new(None) };
}

const LIMITS: Limits = Limits { depth: 4, max_traces: 20_000 };

#[test]
fn dlv_prop_first_and_base() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
            row_sources(&mut rig);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
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
                if let d::Value::Delta(delta) = value {
                    if previous != Some(delta.base) {
                        return Err(format!("a Delta with base {} follows revision {previous:?}", delta.base));
                    }
                }
                if let d::Value::Snapshot { rows, state, .. } | d::Value::Delta(d::Delta { rows, state, .. }) = value {
                    if state.phase == WindowPhase::Failed && !rows.is_empty() {
                        return Err("Failed with rows".into());
                    }
                }
                previous = rows_revision(value).or(previous);
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 10, "the explorer walked {} traces", report.traces);
}

#[test]
fn dlv_prop_rows_visible() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
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
            let mut list: Vec<String> = Vec::new();
            for value in &values {
                match value {
                    d::Value::Snapshot { rows, .. } => list = rows.iter().map(|row| row.id.clone()).collect(),
                    d::Value::Delta(delta) => {
                        list.retain(|id| !delta.removed.contains(id));
                        for row in &delta.rows {
                            if !list.contains(&row.id) {
                                list.push(row.id.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            let last = values.iter().filter_map(rows_revision).last().unwrap_or(0);
            if last >= 2 && !(list.contains(&"r1".to_string()) && list.contains(&"r2".to_string())) {
                return Err(format!("revision {last} shows {list:?}"));
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

#[test]
fn dlv_prop_events_cover() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Events);
            rig.source("feed", vec![
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 1, payload: "c1".into() },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 2, payload: "c2".into() },
                d::DeliveryInput::ModelGap { stream: STREAM },
                d::DeliveryInput::ModelLost { stream: STREAM, first: 3, last: 4 },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 6, payload: "c6".into() },
            ]);
            rig.source("reader", pulls(7));
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
                    d::Value::Gap | d::Value::Ready => {}
                    other => return Err(format!("{other:?} at position {next}")),
                }
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

#[test]
fn dlv_prop_ended_once() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
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
            if values.iter().filter(|value| matches!(value, d::Value::Ended(_))).count() > 1 {
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

#[test]
fn dlv_prop_epoch() {
    let mut twin = ended_by_epoch();
    for input in [Input::ReadDone { revision: 9, rows: rows(&["r9"]) }, Input::WindowState(Ws::loading_after(9)), Input::Committed { revision: 10, touches: true }] {
        twin.step(input);
    }
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_prop_same_revision() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(Ws::loading_after(1)));
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    let journal = twin.state().journal(STREAM);
    assert!(matches!(&journal[0], d::Value::State { revision: 1, state } if state.phase == WindowPhase::Loading), "{journal:?}");
    assert!(matches!(&journal[1], d::Value::Delta(delta) if delta.base == 1 && delta.revision == 2 && delta.state.loaded == Some(2)), "{journal:?}");
    twin.read_all();
}

#[test]
fn dlv_prop_failed_without_rows() {
    let mut empty = open_rows();
    let (_, values) = empty.step_read(Input::ReadFailed { revision: 1 });
    assert!(matches!(values.as_slice(), [Val::Snapshot { rows, state: Ws { phase: Phase::Failed, .. }, .. }] if rows.is_empty()));
    let mut with_rows = live_rows();
    let (_, values) = with_rows.step_read(Input::ReadFailed { revision: 2 });
    assert!(matches!(values.as_slice(), [Val::State { state: Ws { phase: Phase::Loaded, error: true, .. }, .. }]));
}

#[test]
fn dlv_prop_idle() {
    let mut twin = live_rows();
    for revision in 2..8 {
        let (effects, values) = twin.step_read(Input::Committed { revision, touches: false });
        assert!(effects.is_empty() && values.is_empty());
    }
    assert_eq!(twin.det.ready().len(), 0, "the owner has nothing to do");
}

#[test]
fn dlv_prop_bounded() {
    let mut twin = open_events();
    for position in 1..=600u64 {
        twin.post(d::DeliveryInput::ModelEvent { stream: STREAM, position, payload: format!("c{position}") });
        twin.reference.step(Input::ModelEvent { position, payload: format!("c{position}") });
        assert!(twin.state().journal_len(STREAM) <= d::JOURNAL_LIMIT);
    }
    let values = twin.read_all();
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

#[test]
fn dlv_prop_ready_once() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::ModelEvent { position: 1, payload: "c1".into() });
    twin.step(Input::Joined { position: 0 });
    for _ in 0..3 {
        twin.step(Input::ModelReady);
        twin.step(Input::ModelEvent { position: 2, payload: "c2".into() });
    }
    let values = twin.read_all();
    assert_eq!(values.iter().filter(|value| matches!(value, Val::Ready)).count(), 1);
    assert_eq!(values.first(), Some(&Val::Ready), "Ready is before the first Event");
}

#[test]
fn dlv_prop_delta_size() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert!(matches!(values.as_slice(), [Val::Delta { upserted, .. }] if *upserted == ids(&["r2"])), "one changed row, one row in the value");
    let many: Vec<r::Row> = (0..300).map(|n| (format!("n{n}"), 0)).collect();
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: many });
    assert!(matches!(values.as_slice(), [Val::Snapshot { rows, .. }] if rows.len() == 300), "over 256 changes the whole result goes");
}

#[test]
fn dlv_prop_dictionary_bounded() {
    let mut rig = rig(Kind::List);
    for stream in 2..=40u64 {
        rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::Events), epoch: EPOCH, revision: 0, identity: true, valid: true });
        rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::Release { stream });
        rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::SinkClosed { stream });
    }
    rig.det.run_until_quiet(10_000).expect("quiet");
    assert_eq!(inner(&rig.det, rig.deliveries).streams(), 1, "only the stream with a handle is left");
    rig.det.close();
    rig.det.run_until_quiet(100).expect("quiet");
    let drained = snapshot(&rig.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 0, "the stream with a handle still waits for SinkClosed");
    rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::SinkClosed { stream: STREAM });
    rig.det.run_until_quiet(100).expect("quiet");
    let drained = snapshot(&rig.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 1, "one Drained for every stream");
}

#[test]
fn dlv_prop_no_available_after_cancel() {
    let paths: [(&str, Vec<Input>); 4] = [
        ("cancel", vec![Input::Cancel { reply: 1 }]),
        ("release", vec![Input::Release]),
        ("epoch", vec![Input::EpochEnded { own: true }, Input::Cancel { reply: 1 }]),
        ("closing", vec![Input::Closing, Input::Cancel { reply: 1 }]),
    ];
    for (name, inputs) in paths {
        let mut twin = live_rows();
        let mut effects = Vec::new();
        for input in inputs {
            effects.extend(twin.step(input));
        }
        assert_eq!(effects.iter().filter(|effect| **effect == Effect::CloseSink).count(), 1, "{name}: the sink closes once");
        assert!(effects.contains(&Effect::InterestMinus), "{name}: the interest goes");
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Cancelled(_))), "{name}: the reply waits for SinkClosed");
        twin.step(Input::SinkClosed);
        for input in [Input::Committed { revision: 5, touches: true }, Input::ReadDone { revision: 5, rows: rows(&["r5"]) }, Input::WindowState(Ws::loading_after(5))] {
            assert!(!twin.step(input).contains(&Effect::Available), "{name}: no Available after SinkClosed");
        }
    }
}

#[test]
fn dlv_prop_single_unread_delta() {
    let mut twin = live_rows();
    let mut previous = 1;
    for round in 2..40i64 {
        let list: Vec<String> = (0..=round % 5).map(|n| format!("r{n}")).collect();
        let list: Vec<r::Row> = list.into_iter().map(|id| (id, round)).collect();
        twin.step(Input::ReadDone { revision: round, rows: list });
        let unread = twin.state().journal(STREAM).iter().filter(|value| matches!(value, d::Value::Delta(_) | d::Value::Snapshot { .. })).count();
        assert!(unread <= 1, "at most one unread rows value");
        if round % 3 == 0 {
            for value in twin.read_all() {
                match value {
                    Val::Delta { base, revision, .. } => {
                        assert_eq!(base, previous);
                        previous = revision;
                    }
                    Val::Snapshot { revision, .. } => previous = revision,
                    _ => {}
                }
            }
        }
    }
}

#[test]
fn dlv_prop_delta_order() {
    fn apply(mut list: Vec<String>, delta: &d::Delta) -> Vec<String> {
        for (from, to) in &delta.replaced {
            for id in list.iter_mut().filter(|id| *id == from) {
                *id = to.clone();
            }
        }
        list.retain(|id| !delta.removed.contains(id));
        list.retain(|id| !delta.placed.iter().any(|(placed, _)| placed == id));
        let mut placed = delta.placed.clone();
        placed.sort_by_key(|(_, index)| *index);
        for (id, index) in placed {
            let at = (index as usize).min(list.len());
            list.insert(at, id);
        }
        list
    }
    let mut det = DetScheduler::new(3);
    let log: Spied = new_log();
    let addr = add_deliveries(&mut det, &log);
    det.start_epoch(EPOCH);
    det.post(addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::List), epoch: EPOCH, revision: 0, identity: true, valid: true });
    let mut seed = 12_345u64;
    let mut next = move |bound: u64| {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let pool: Vec<String> = (0..12).map(|n| format!("p{n}")).collect();
    let mut platform: Vec<String> = Vec::new();
    let mut lists: std::collections::BTreeMap<i64, Vec<String>> = std::collections::BTreeMap::new();
    let answers = Outcomes::<PullAnswer>::new();
    let mut checked = 0;
    for revision in 1..120i64 {
        let mut list: Vec<String> = pool.iter().filter(|_| next(3) != 0).cloned().collect();
        for index in (1..list.len()).rev() {
            if next(4) == 0 {
                list.swap(index, next(index as u64 + 1) as usize);
            }
        }
        lists.insert(revision, list.clone());
        let read_id = inner(&det, addr).read_id(STREAM);
        let rows: Vec<d::StreamRow> = list.iter().map(|id| d::StreamRow::new(id, if next(5) == 0 { revision } else { 0 })).collect();
        det.post(addr, EpochStamp::Epoch(EPOCH), d::DeliveryInput::ReadDone { stream: STREAM, read_id, revision, data: d::ReadData::Rows { rows, replaced: Vec::new() } });
        det.post(addr, EpochStamp::Epoch(EPOCH), d::DeliveryInput::Committed(Committed { batch: 0, revision, tables: vec!["moments".into()], models: Default::default() }));
        if next(2) == 0 {
            det.post(addr, EpochStamp::Control, d::DeliveryInput::Pull { stream: STREAM, outcome: answers.register(EPOCH).id() });
        }
        det.run_until_quiet(1_000).expect("quiet");
        for entry in take(&log) {
            let Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) = entry else { continue };
            match value {
                d::Value::Snapshot { rows, .. } => platform = rows.iter().map(|row| row.id.clone()).collect(),
                d::Value::Delta(delta) => {
                    platform = apply(platform.clone(), &delta);
                    assert_eq!(platform.len(), delta.size as usize, "the size of the result at {}", delta.revision);
                    let core: Vec<String> = lists.get(&delta.revision).cloned().expect("a list of the revision");
                    assert_eq!(platform, core, "revision {}", delta.revision);
                    checked += 1;
                }
                _ => {}
            }
        }
    }
    assert!(checked > 10, "{checked} deltas were applied");
}

#[test]
fn dlv_epoch_started_unknown_cause() {
    let mut rig = rig(Kind::List);
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochStarted(crate::owners::contract::EpochStarted { epoch: 2, identity: None, generation: 0 }));
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochEnded(EpochEnded { epoch: 2, cause: Ended::SessionReset }));
    rig.det.run_until_quiet(100).expect("quiet");
    assert_eq!(inner(&rig.det, rig.deliveries).stage(STREAM), Some(d::DeliveryStage::Opening), "the stream of epoch 1 is not touched by the end of epoch 2");
}

/// dlv_cancelling_other: a `Pull` after `Cancel` waits for `Ended` and does not hand out the value that was unread when the reader cancelled.
#[test]
fn dlv_cancelling_drops_the_unread_values() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.step(Input::Cancel { reply: 1 });
    assert!(twin.step(Input::Pull).is_empty(), "the unread snapshot is gone: the pull waits");
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))]);
}

/// P-DLV-06 (win_loading_error): the first load of a window fails while the snapshot holds cached rows: the stream shows `Loaded` with the error, `Failed` only without rows.
#[test]
fn dlv_failed_window_with_cached_rows_shows_loaded_with_the_error() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.read_all();
    let failed = Ws { phase: Phase::Failed, error: true, loaded: None };
    let (_, values) = twin.step_read(Input::WindowState(failed.clone()));
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Loaded, error: true, loaded: None } }]);
    let mut empty = open_rows();
    empty.step(Input::ReadDone { revision: 1, rows: Vec::new() });
    empty.read_all();
    let (_, values) = empty.step_read(Input::WindowState(failed.clone()));
    assert_eq!(values, [Val::State { revision: 1, state: failed }], "no rows: the failure stays");
}
