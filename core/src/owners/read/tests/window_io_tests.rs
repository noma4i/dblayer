//! `WindowIo` on `DetScheduler` with the mock transport. Requirement -> observed value -> assert:
//! win_io_request_lands: a `Request` is asked through `Upstream`, the answer is an input, the landed page reaches the Store as `Commit`.
//! win_io_error_classes: a transport error is an input of its class (network -> a `Schedule` of `RetryDue`, permanent -> `Failed`).
//! win_io_cancel_no_answer: a cancelled request gives the owner no `Upstream` input.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::rig::{admit, open_gate_over, collector, pump, runtime, saw_input, BatchPeer, Batches};
use super::{WindowCodec, WindowIo, WindowReader};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{self as w, AnswerPage, WindowEffect, WindowKey, WindowOutcome, Windows, WindowsEffect, WindowsInput};
use crate::owners::store::{RowValue, StoreInput, StoreOp, StoreWriter};
use crate::spec::support::Sink;
use crate::runtime::{Address, Cause, DetScheduler, Effect, Effects, EpochStamp, Outbound, StateOwner};
use crate::transport::{GraphQLRequest, MockTransport, PutRequest, PutReply, Reply, Response, Upstream, TransportError};

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

fn interest(id: u64) -> WindowsInput { WindowsInput::Load { key: key(), id, goal: w::Goal::First, force: false, store_revision: 0 } }

struct WindowStore;

impl StateOwner for WindowStore {
    const EPOCHED: bool = false;
    type State = Batches;
    type Input = StoreInput;
    type Effect = crate::spec::support::NoEffect;

    fn step(state: &mut Batches, input: StoreInput, now: crate::runtime::Time) -> Effects<Self> {
        if let StoreInput::Pin { request_id, .. } = input {
            return vec![Effect::Send(Outbound::new(Address::<Windows>::new(2), WindowsInput::Pinned { request_id, base_revision: 0 }))];
        }
        BatchPeer::step(state, input, now).into_iter().map(|effect| match effect {
            Effect::Send(outbound) => Effect::Send(outbound),
            Effect::Run(effect) => match effect {},
            _ => unreachable!(),
        }).collect()
    }

    fn epoch_ended(epoch: u64, cause: Cause) -> StoreInput { StoreWriter::epoch_ended(epoch, cause) }
    fn closing() -> StoreInput { StoreInput::Idle }
}

struct Rig {
    det: DetScheduler,
    windows: Address<Windows>,
    mock: MockTransport,
    gate: Arc<crate::runtime::EpochGate>,
    flights: super::ports::RequestsOnTheWay,
    batches: Batches,
    rest: Arc<Mutex<Vec<WindowsEffect>>>,
    _runtime: tokio::runtime::Runtime,
}

fn rig() -> Rig {
    let mock = MockTransport::new();
    rig_with(mock.clone(), Arc::new(mock), None)
}

fn rig_with(mock: MockTransport, transport: Arc<dyn Upstream>, reader: Option<WindowReader>) -> Rig {
    let runtime = runtime();
    let batches = Batches::default();
    let (rest, take_rest) = collector();
    let gate = open_gate_over(EPOCH, transport);
    let io = WindowIo::new(Arc::new(Pages), (admit(), gate.clone()), runtime.handle().clone(), take_rest);
    let flights = super::ports::RequestsOnTheWay::default();
    let io = io.flights_in(flights.clone());
    let io = match reader { Some(reader) => io.reading(reader), None => io };
    let mut det = DetScheduler::new(1);
    let store = det.add::<WindowStore>("store", batches.clone(), crate::spec::support::Discard);
    let deliveries = det.add::<Sink<Deliveries>>("deliveries", Default::default(), crate::spec::support::Discard);
    let peers = w::WindowsPeers { store: Address::<StoreWriter>::new(store.index()), deliveries: Address::<Deliveries>::new(deliveries.index()) };
    let windows = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, Some(true), peers).with_local_request_ids(), io);
    det.start_epoch(EPOCH);
    Rig { det, windows, mock, gate, flights, batches, rest, _runtime: runtime }
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
    pump(&mut rig.det, |_| !batches.all().is_empty());
    let landed = rig.batches.all();
    assert_eq!(landed.len(), 1);
    let batch = &landed[0];
    assert_eq!((batch.stamp, batch.intents), (crate::runtime::EpochStamp::Epoch(EPOCH), 0));
    let landed_rows: Vec<&str> = batch.ops.iter().flat_map(|op| match op { StoreOp::Window { roots, .. } => roots.iter().map(|row| row.row_id.as_str()).collect(), _ => Vec::new() }).collect();
    assert_eq!(landed_rows, ["r1", "r2"]);
    assert_eq!(batch.basis.get("Moment"), Some(&0));
    assert_eq!(saw_input(&rig.det, "Response"), 1, "the answer came back as one input");
    assert_eq!(rig.mock.calls().iter().map(|call| call.wire).collect::<Vec<_>>(), [Some(crate::transport::WireIds::new(crate::transport::Author::Window, 1, 1))], "the request goes out with the request and attempt of the Window");
}

#[test]
fn win_io_error_classes() {
    type Holds = fn(&[WindowsEffect], &DetScheduler) -> bool;
    let cases: [(TransportError, Holds); 2] = [
        (TransportError::Network("down".into()), |_, det| det.trace().iter().any(|entry| entry.effects.contains("Backoff") && entry.effects.contains("RetryDue { request: 1 }"))),
        (TransportError::Decode("broken".into()), |seen, _| seen.iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { outcome: WindowOutcome::Failed { .. }, .. }, .. }))),
    ];
    for (error, holds) in cases {
        let mut rig = rig();
        rig.mock.enqueue("Page", Reply::fail(error.clone()));
        rig.watch(1);
        let rest = rig.rest.clone();
        pump(&mut rig.det, |det| holds(&rest.lock().unwrap(), det));
        assert!(rig.batches.all().is_empty(), "{error:?} lands no page");
    }
}

#[test]
fn win_io_cancel_no_answer() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1"], "cursor": null })).after(Duration::MAX));
    rig.watch(1);
    pump(&mut rig.det, |det| det.trace().iter().any(|entry| entry.effects.contains("Request")));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::InterestMinus { key: key(), id: 1 });
    pump(&mut rig.det, |det| det.trace().iter().any(|entry| entry.effects.contains("Cancel")));
    let flights = rig.flights.clone();
    pump(&mut rig.det, |_| flights.load(std::sync::atomic::Ordering::SeqCst) == 0);
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

type Sent<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, TransportError>> + Send + 'a>>;

struct Panicking;

impl Upstream for Panicking {
    fn query<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { panic!("the transport failed") }
    fn mutate<'a, 'b>(&'a self, _: GraphQLRequest) -> Sent<'b, Response> where 'a: 'b, Self: 'b { panic!("the transport failed") }
    fn put<'a, 'b>(&'a self, _: PutRequest) -> Sent<'b, PutReply> where 'a: 'b, Self: 'b { panic!("the transport failed") }
}

fn task_failed(det: &DetScheduler) -> Vec<String> { det.trace().iter().filter(|entry| entry.input.starts_with("TaskFailed")).map(|entry| entry.input.clone()).collect() }

pub(in super::super) fn panicked_request() -> Vec<String> {
    let mut rig = rig_with(MockTransport::new(), Arc::new(Panicking), None);
    rig.watch(1);
    let rest = rig.rest.clone();
    pump(&mut rig.det, |_| rest.lock().unwrap().iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { outcome: WindowOutcome::Failed { .. }, .. }, .. })));
    task_failed(&rig.det)
}

pub(in super::super) fn panicked_read() -> Vec<String> {
    let reader: WindowReader = Arc::new(|_| panic!("the read failed"));
    let mut rig = rig_with(MockTransport::new(), Arc::new(MockTransport::new()), Some(reader));
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), interest(1));
    pump(&mut rig.det, |det| saw_input(det, "Pin") > 0 && saw_input(det, "TaskFailed") > 0);
    task_failed(&rig.det)
}

pub(in super::super) fn request_id(request: u64, attempt: u32) -> String { format!("TaskFailed {{ id: {} }}", w::request_task(request, attempt)) }

pub(in super::super) fn read_id(read: u64) -> String { format!("TaskFailed {{ id: {} }}", w::read_task(read)) }

/// P-RT-14 for Window: a close while a page request is inside the transport leaves that request alone and no later request of the Window reaches the old transport.
#[test]
fn rt_prop_close_against_query_no_old_transport_window() {
    let mut rig = rig();
    rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1"], "cursor": null })).after(Duration::MAX));
    rig.watch(1);
    let mock = rig.mock.clone();
    pump(&mut rig.det, |_| mock.calls().len() == 1);
    rig.gate.close(EPOCH);
    let other = WindowKey { model: "Moment".into(), name: "other".into() };
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::Load { key: other.clone(), id: 2, goal: w::Goal::First, force: false, store_revision: 0 });
    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::WindowRead { key: other, read_id: 2, result: Ok(None) });
    pump(&mut rig.det, |det| saw_input(det, "Dropped") == 1);
    let late: Vec<_> = rig.mock.calls().iter().skip(1).map(|call| (call.operation_name.clone(), call.wire)).collect();
    assert!(late.is_empty(), "late calls reached the old transport after the close: {late:?}");
}

enum Turn {
    Stale,
    Load,
    LoadRefused,
    Commit,
    Reject(crate::owners::contract::RejectReason),
}

/// P-WIN-34: the sources of the landing batches that the owner of the windows hands to the Store, in the order of the inputs.
#[test]
fn win_change_after_commit_marks_the_next_load() {
    use crate::schema::RowSource::{NodeReread, Page};
    use crate::owners::contract::RejectReason::{Invalid, StaleGeneration, Storage};
    use Turn::{Commit, Load, LoadRefused, Reject, Stale};
    let moved = || StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".to_string(), 7)].into_iter().collect() };
    let node = WindowKey::relation("Moment", "node", &[(crate::schema::NODE_PARAM.to_string(), json!("n1"))].into_iter().collect());
    let cases: Vec<(&str, WindowKey, Vec<Turn>, Vec<crate::schema::RowSource>)> = vec![
        ("a: a load without Stale(Change)", node.clone(), vec![Load], vec![Page]),
        ("b: Stale(Change), a load, the next load", node.clone(), vec![Stale, Load, Commit, Load], vec![NodeReread, Page]),
        ("c: Stale(Change) after the landing batch, before Committed", node.clone(), vec![Load, Stale, Commit, Load], vec![Page, NodeReread]),
        ("d: Stale(Change), a refused load, a new load", node.clone(), vec![Stale, LoadRefused, Load], vec![NodeReread]),
        ("e: Stale(Change) of a list window", key(), vec![Stale, Load], vec![Page]),
        ("f: the marked batch meets a moved generation, the window reads again", node.clone(), vec![Stale, Load, Reject(moved())], vec![NodeReread, NodeReread]),
        ("g: the Store fails the marked batch, a new load", node.clone(), vec![Stale, Load, Reject(Storage("disk".into())), Load], vec![NodeReread, NodeReread]),
        ("h: the Store refuses the data of the marked batch, a new load", node.clone(), vec![Stale, Load, Reject(Invalid), Load], vec![NodeReread, NodeReread]),
        ("i: a batch without the mark meets a moved generation", node.clone(), vec![Load, Reject(moved())], vec![Page, Page]),
    ];
    let mut wrong = Vec::new();
    for (case, window, turns, want) in cases {
        let mut rig = rig();
        rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::InterestPassive { key: window.clone(), id: 9 });
        rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::WindowRead { key: window.clone(), read_id: 1, result: Ok(None) });
        rig.det.run_until_quiet(10_000).expect("the scheduler goes quiet");
        for turn in turns {
            let landed = rig.batches.all().len();
            match turn {
                Stale => rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::Stale { model: "Moment".into(), keys: w::StaleKeys::All, cause: w::StaleCause::Change }),
                Load | LoadRefused => {
                    let reply = if matches!(turn, Load) { Reply::data(json!({ "rows": ["r1"], "cursor": null })) } else { Reply::fail(TransportError::Decode("broken".into())) };
                    rig.mock.enqueue("Page", reply);
                    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::Load { key: window.clone(), id: 1, goal: w::Goal::First, force: true, store_revision: 0 });
                    let (batches, rest) = (rig.batches.clone(), rig.rest.clone());
                    let refused = matches!(turn, LoadRefused);
                    pump(&mut rig.det, |_| if refused { rest.lock().unwrap().iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { outcome: WindowOutcome::Failed { .. }, .. }, .. })) } else { batches.all().len() > landed });
                }
                Reject(reason) => {
                    let batch = rig.batches.all().last().expect("a landing batch").id;
                    let reads_again = matches!(reason, StaleGeneration { .. });
                    if reads_again {
                        rig.mock.enqueue("Page", Reply::data(json!({ "rows": ["r1"], "cursor": null })));
                    }
                    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::Rejected(crate::owners::contract::Rejected { batch, reason }));
                    let batches = rig.batches.clone();
                    pump(&mut rig.det, |_| !reads_again || batches.all().len() > landed);
                }
                Commit => {
                    let batch = rig.batches.all().last().expect("a landing batch").id;
                    let committed = crate::owners::contract::Committed { batch, revision: 5, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() };
                    rig.det.post(rig.windows, EpochStamp::Epoch(EPOCH), WindowsInput::Committed(committed));
                }
            }
            rig.det.run_until_quiet(10_000).expect("the scheduler goes quiet");
        }
        let got: Vec<crate::schema::RowSource> = rig.batches.all().iter().flat_map(|batch| batch.ops.iter().filter_map(|op| match op { StoreOp::Window { source, .. } => Some(*source), _ => None })).collect();
        if got != want {
            wrong.push(format!("{case}: the landing batches carry {got:?}, the spec asks {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong cases:\n{}", wrong.len(), wrong.join("\n"));
}
