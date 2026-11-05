//! `WindowIo` on `DetScheduler` with the mock transport. Requirement -> observed value -> assert:
//! win_io_request_lands: a `Request` is asked through `Transport`, the answer is an input, the landed page reaches the Store as `Commit`.
//! win_io_error_classes: a transport error is an input of its class (network -> a `Schedule` of `RetryDue`, permanent -> `Failed`).
//! win_io_cancel_no_answer: a cancelled request gives the owner no `Transport` input.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::rig::{admit, collector, pump, pump_for, runtime, saw_input, BatchPeer, Batches};
use super::{WindowCodec, WindowIo};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{self as w, AnswerPage, WindowEffect, WindowKey, WindowOutcome, Windows, WindowsEffect, WindowsInput};
use crate::owners::store::{RowValue, StoreWriter};
use crate::owners::support::Sink;
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::transport::{GraphQLRequest, MockTransport, Reply, Response, TransportError};

const EPOCH: u64 = 1;

struct Pages;

impl WindowCodec for Pages {
    fn request(&self, _key: &WindowKey, page: u32, cursor: Option<&str>) -> GraphQLRequest { GraphQLRequest::new("query Page", "Page", json!({ "page": page, "cursor": cursor })) }

    fn answer(&self, _key: &WindowKey, page: u32, response: Response) -> Result<AnswerPage, String> {
        let data = response.data.ok_or("no data")?;
        let rows = data["rows"].as_array().ok_or("no rows")?.iter().filter_map(|row| row.as_str().map(str::to_string)).collect();
        let mut answer = AnswerPage::new(page, rows, data["cursor"].as_str().map(str::to_string));
        answer.payload.roots = answer.rows.iter().map(|id| RowValue { row_id: id.clone(), values: "{}".into() }).collect();
        Ok(answer)
    }
}

fn key() -> WindowKey { WindowKey { model: "Moment".into(), name: "list".into() } }

fn interest(id: u64) -> WindowsInput { WindowsInput::Load { key: key(), id, goal: w::Goal::First, force: false, store_revision: 0 } }

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
    let io = WindowIo::new(Arc::new(mock.clone()), Arc::new(Pages), admit(), runtime.handle().clone(), take_rest);
    let mut det = DetScheduler::new(1);
    let store = det.add::<BatchPeer>("store", batches.clone(), crate::owners::support::Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), crate::owners::support::Discard);
    let peers = w::WindowsPeers { store: Address::<StoreWriter>::new(store.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let windows = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, Some(true), peers), io);
    det.start_epoch(EPOCH);
    Rig { det, windows, mock, batches, rest, _runtime: runtime }
}

#[test]
fn win_io_request_lands() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1", "r2"], "cursor": null })));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), interest(1));
    let batches = rig.batches.clone();
    pump(&mut rig.det, rig.windows, |_| !batches.all().is_empty());
    let landed = rig.batches.all();
    assert_eq!(landed.len(), 1);
    let batch = &landed[0];
    assert_eq!((batch.epoch, batch.intents), (EPOCH, 0));
    assert_eq!(batch.rows, [("Moment".to_string(), "r1".to_string()), ("Moment".to_string(), "r2".to_string())]);
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
        rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), interest(1));
        let rest = rig.rest.clone();
        pump(&mut rig.det, rig.windows, |det| holds(&rest.lock().unwrap(), det));
        assert!(rig.batches.all().is_empty(), "{error:?} lands no page");
    }
}

#[test]
fn win_io_cancel_no_answer() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1"], "cursor": null })).after(Duration::from_millis(300)));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), interest(1));
    pump(&mut rig.det, rig.windows, |det| det.trace().iter().any(|entry| entry.effects.contains("Request")));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::InterestMinus { key: key(), id: 1 });
    pump(&mut rig.det, rig.windows, |det| det.trace().iter().any(|entry| entry.effects.contains("Cancel")));
    pump_for(&mut rig.det, rig.windows, Duration::from_millis(900));
    assert_eq!(saw_input(&rig.det, "Response"), 0, "a cancelled request gives no answer");
    assert!(rig.batches.all().is_empty());
}
