//! `WindowIo` on `DetScheduler` with the mock transport. Requirement -> observed value -> assert:
//! win_io_request_lands: a `Request` is asked through `Transport`, the answer is an input, the landed page reaches the Store as `Commit`.
//! win_io_error_classes: a transport error is an input of its class (network -> a `Schedule` of `RetryDue`, permanent -> `Failed`).
//! win_io_cancel_no_answer: a cancelled request gives the owner no `Transport` input.

use std::sync::{Arc, Mutex};
use std::time::Duration;

const NOW: i64 = 1_000_000;

use serde_json::json;

use super::rig::{admit, open_gate, collector, pump, pump_for, runtime, saw_input, BatchPeer, Batches};
use super::{WindowCodec, WindowIo};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{self as w, AnswerPage, WindowEffect, WindowKey, WindowOutcome, Windows, WindowsEffect, WindowsInput};
use crate::owners::store::{RowValue, StoreInput, StoreOp, StoreWriter};
use crate::owners::support::Sink;
use crate::runtime::{Address, Cause, DetScheduler, Effect, Effects, EpochStamp, Outbound, StateOwner, Time};
use crate::transport::{GraphQLRequest, MockTransport, Reply, Response, TransportError};

const EPOCH: u64 = 1;

struct Pages;

impl WindowCodec for Pages {
    fn request(&self, _key: &WindowKey, page: u32, cursor: Option<&str>, _kind: w::RequestKind) -> Result<GraphQLRequest, String> { Ok(GraphQLRequest::new("query Page", "Page", json!({ "page": page, "cursor": cursor }))) }

    fn answer(&self, _key: &WindowKey, page: u32, _kind: w::RequestKind, response: Response) -> Result<AnswerPage, String> {
        let data = response.data.ok_or("no data")?;
        let rows = data["rows"].as_array().ok_or("no rows")?.iter().filter_map(|row| row.as_str().map(str::to_string)).collect();
        let mut answer = AnswerPage::new(page, rows, data["cursor"].as_str().map(str::to_string));
        answer.payload.roots = answer.rows.iter().map(|id| RowValue { row_id: id.clone(), values: "{}".into() }).collect();
        Ok(answer)
    }
}

fn key() -> WindowKey { WindowKey { model: "Moment".into(), name: "list".into() } }

fn interest(id: u64) -> WindowsInput { WindowsInput::Load { key: key(), id, goal: w::Goal::First, force: false, store_revision: 0, now: NOW } }

struct WindowStore;

impl StateOwner for WindowStore {
    const EPOCHED: bool = false;
    type State = Batches;
    type Input = StoreInput;
    type Effect = crate::owners::support::NoEffect;

    fn step(state: &mut Batches, input: StoreInput) -> Effects<Self> {
        if let StoreInput::Pin { request_id, .. } = input {
            return vec![Effect::Send(Outbound::new(Address::<Windows>::new(2), WindowsInput::Pinned { request_id, base_revision: 0 }))];
        }
        BatchPeer::step(state, input).into_iter().map(|effect| match effect {
            Effect::Send(outbound) => Effect::Send(outbound),
            Effect::Run(effect) => match effect {},
            _ => unreachable!(),
        }).collect()
    }

    fn tick(_: Time) -> StoreInput { StoreInput::Idle }
    fn epoch_ended(epoch: u64, cause: Cause) -> StoreInput { StoreWriter::epoch_ended(epoch, cause) }
    fn closing() -> StoreInput { StoreInput::Idle }
}

struct Rig {
    det: DetScheduler,
    windows: Address<Windows>,
    mock: MockTransport,
    batches: Batches,
    rest: Arc<Mutex<Vec<WindowsEffect>>>,
    _runtime: tokio::runtime::Runtime,
}

fn rig() -> Rig {
    let runtime = runtime();
    let mock = MockTransport::new();
    let batches = Batches::default();
    let (rest, take_rest) = collector();
    let io = WindowIo::new(Some(Arc::new(mock.clone())), Arc::new(Pages), (admit(), open_gate(EPOCH)), (runtime.handle().clone(), Arc::new(|| NOW)), take_rest);
    let mut det = DetScheduler::new(1);
    let store = det.add::<WindowStore>("store", batches.clone(), crate::owners::support::Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), crate::owners::support::Discard);
    let peers = w::WindowsPeers { store: Address::<StoreWriter>::new(store.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let windows = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, Some(true), peers), io);
    det.start_epoch(EPOCH);
    Rig { det, windows, mock, batches, rest, _runtime: runtime }
}

impl Rig {
    /// A stream opens the key; the saved window of a key that never landed is none.
    fn watch(&mut self, id: u64) {
        self.det.post(self.windows, EpochStamp::Epoch(EPOCH), interest(id));
        self.det.post(self.windows, EpochStamp::Epoch(EPOCH), WindowsInput::WindowRead { key: key(), read_id: 1, result: Ok(None) });
    }
}

#[test]
fn win_io_request_lands() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1", "r2"], "cursor": null })));
    rig.watch(1);
    let batches = rig.batches.clone();
    pump(&mut rig.det, rig.windows, |_| !batches.all().is_empty());
    let landed = rig.batches.all();
    assert_eq!(landed.len(), 1);
    let batch = &landed[0];
    assert_eq!((batch.epoch, batch.intents), (EPOCH, 0));
    let landed_rows: Vec<&str> = batch.ops.iter().flat_map(|op| match op { StoreOp::Window { roots, .. } => roots.iter().map(|row| row.row_id.as_str()).collect(), _ => Vec::new() }).collect();
    assert_eq!(landed_rows, ["r1", "r2"]);
    assert_eq!(batch.basis.get("Moment"), Some(&0));
    assert_eq!(saw_input(&rig.det, "Response"), 1, "the answer came back as one input");
}

#[test]
fn win_io_error_classes() {
    type Holds = fn(&[WindowsEffect], &DetScheduler) -> bool;
    let cases: [(TransportError, Holds); 2] = [
        (TransportError::Network("down".into()), |_, det| det.trace().iter().any(|entry| entry.effects.contains("Schedule {") && entry.effects.contains("RetryDue { request: 1 }"))),
        (TransportError::Decode("broken".into()), |seen, _| seen.iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { outcome: WindowOutcome::Failed { .. }, .. }, .. }))),
    ];
    for (error, holds) in cases {
        let mut rig = rig();
        rig.mock.enqueue("Page", Reply::fail(error.clone()));
        rig.watch(1);
        let rest = rig.rest.clone();
        pump(&mut rig.det, rig.windows, |det| holds(&rest.lock().unwrap(), det));
        assert!(rig.batches.all().is_empty(), "{error:?} lands no page");
    }
}

#[test]
fn win_io_cancel_no_answer() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1"], "cursor": null })).after(Duration::from_millis(300)));
    rig.watch(1);
    pump(&mut rig.det, rig.windows, |det| det.trace().iter().any(|entry| entry.effects.contains("Request")));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::InterestMinus { key: key(), id: 1 });
    pump(&mut rig.det, rig.windows, |det| det.trace().iter().any(|entry| entry.effects.contains("Cancel")));
    pump_for(&mut rig.det, rig.windows, Duration::from_millis(900));
    assert_eq!(saw_input(&rig.det, "Response"), 0, "a cancelled request gives no answer");
    assert!(rig.batches.all().is_empty());
}

/// P-SESS-14: the same raw outcome gets one class from `RetryClass`, and Windows and Transfer take their verdict from that class.
#[test]
fn sess_prop_transport_classify() {
    use crate::owners::contract::{ErrorClass, TransportOutcome};
    use crate::transport::RetryClass;
    let graphql = |code: &str| TransportError::Graphql { messages: vec![code.into()], extensions: Some(crate::transport::GraphqlExtensions { code: Some(code.into()), transaction_disposition: None, fields: Default::default() }) };
    let table = [
        (TransportError::Network("down".into()), RetryClass::Network, ErrorClass::Network, true),
        (TransportError::Http { status: 503, body: String::new() }, RetryClass::Server, ErrorClass::Server, true),
        (TransportError::Http { status: 429, body: String::new() }, RetryClass::Retriable, ErrorClass::Server, true),
        (TransportError::Http { status: 400, body: String::new() }, RetryClass::Fatal, ErrorClass::Permanent, false),
        (graphql("TIMEOUT"), RetryClass::Retriable, ErrorClass::Server, true),
        (graphql("FORBIDDEN"), RetryClass::Fatal, ErrorClass::Permanent, false),
    ];
    for (error, class, window, transfer) in table {
        assert_eq!(RetryClass::of_error(&error), class, "{error}");
        assert!(matches!(super::window_io::failure::<()>(&error), TransportOutcome::Error(found) if found.class == window), "{error}: Windows");
        assert_eq!(crate::owners::write::transfer_executor::temporary(&error), transfer, "{error}: Transfer");
    }
    assert!(matches!(super::window_io::failure::<()>(&TransportError::Interrupted("cancelled".into())), TransportOutcome::Dropped), "only the core sets Interrupted, and it is Dropped everywhere");
}
