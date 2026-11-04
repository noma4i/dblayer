use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::ffi::wire::WireRow;
use crate::ffi::{Database, DbError};
use super::{LivePhase, Placement, Replacement, StreamDelivery, StreamEnd, StreamRequest, StreamTable, StreamValue, ValueSink};

/// A test reader of a stream: it hears every value the way a platform reads them, through `read_stream`.
pub(crate) trait Reader: Send + Sync {
    fn delivered(&self, delivery: StreamDelivery);
}

/// Reads the journal of a stream each time the core says values wait, and keeps what `loaded()` of a platform would return: the first settled state, its error when it failed without rows, `Interrupted` after an end that is not `Invalid`.
pub(crate) struct Pull {
    table: Weak<StreamTable>,
    id: OnceLock<u64>,
    reader: Arc<dyn Reader>,
    again: AtomicBool,
    reading: Mutex<()>,
    outcome: tokio::sync::watch::Sender<Option<Result<u64, DbError>>>,
    failure: Mutex<Option<DbError>>,
}

type Pulls = Mutex<HashMap<(usize, u64), tokio::sync::watch::Receiver<Option<Result<u64, DbError>>>>>;

fn pulls() -> &'static Pulls {
    static PULLS: OnceLock<Pulls> = OnceLock::new();
    PULLS.get_or_init(Pulls::default)
}

fn key(table: &Arc<StreamTable>, id: u64) -> (usize, u64) { (Arc::as_ptr(table) as usize, id) }

impl ValueSink for Pull {
    fn available(&self) { self.drain(); }
}

impl Pull {
    fn drain(&self) {
        self.again.store(true, Ordering::SeqCst);
        let Some(id) = self.id.get().copied() else { return };
        while self.again.load(Ordering::SeqCst) {
            let Ok(_reading) = self.reading.try_lock() else { return };
            self.again.store(false, Ordering::SeqCst);
            while let Some(delivery) = self.table.upgrade().and_then(|table| table.find(id)).and_then(|stream| stream.read()) {
                self.observe(&delivery);
                self.reader.delivered(delivery);
            }
        }
    }

    fn observe(&self, delivery: &StreamDelivery) {
        let state = match &delivery.value {
            StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::Count { state, .. } | StreamValue::State { state } => state,
            StreamValue::Ended { reason } => {
                let failure = self.failure.lock().unwrap().clone();
                let outcome = match reason {
                    StreamEnd::Invalid => Err(failure.unwrap_or(DbError::Invalid { detail: "the stream could not open".into() })),
                    _ => Err(DbError::Interrupted { detail: "the stream ended before its rows were shown".into() }),
                };
                self.outcome.send_replace(Some(outcome));
                return;
            }
            _ => return,
        };
        if state.phase == LivePhase::Failed { self.failure.lock().unwrap().clone_from(&state.error); }
        let outcome = state.settled.then(|| match (&state.phase, &state.error, state.error_kind.as_deref()) {
            (LivePhase::Failed, Some(error), _) => Err(error.clone()),
            (_, None | Some(DbError::Network { .. }), _) | (_, _, Some("subscription")) => Ok(delivery.seq),
            (_, Some(error), _) => Err(error.clone()),
        });
        self.outcome.send_replace(outcome);
    }
}

/// One delivery as a test reads it: the rows part and the state side by side.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) full: bool,
    pub(crate) rows: Vec<WireRow>,
    pub(crate) row_indexes: Vec<u32>,
    pub(crate) size: u32,
    pub(crate) revision: i64,
    pub(crate) seq: u64,
    pub(crate) removed: Vec<String>,
    pub(crate) placed: Vec<Placement>,
    pub(crate) replaced: Vec<Replacement>,
    pub(crate) count: Option<i64>,
    pub(crate) phase: Option<LivePhase>,
    pub(crate) initial: bool,
    pub(crate) refreshing: bool,
    pub(crate) fetching_more: bool,
    pub(crate) has_more: bool,
    pub(crate) complete: bool,
    pub(crate) total: Option<i64>,
    pub(crate) loaded_at: Option<i64>,
    pub(crate) error: Option<DbError>,
    pub(crate) error_kind: Option<String>,
}

#[allow(clippy::result_large_err)]
fn seen_of(delivery: StreamDelivery, size: &mut u32) -> Result<Seen, StreamValue> {
    let (revision, state, full, rows, row_indexes, removed, placed, replaced, count) = match delivery.value {
        StreamValue::Snapshot { revision, rows, state } => { *size = rows.len() as u32; (revision, state, true, rows, Vec::new(), Vec::new(), Vec::new(), Vec::new(), None) }
        StreamValue::Delta { revision, size: now, rows, row_indexes, removed, placed, replaced, state, .. } => { *size = now; (revision, state, false, rows, row_indexes, removed, placed, replaced, None) }
        StreamValue::Count { revision, count, state } => (revision, state, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Some(count)),
        StreamValue::State { state } => (0, state, false, Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), None),
        other => return Err(other),
    };
    Ok(Seen {
        full, rows, row_indexes, size: *size, revision, seq: delivery.seq, removed, placed, replaced, count, phase: Some(state.phase), initial: state.initial, refreshing: state.refreshing,
        fetching_more: state.fetching_more, has_more: state.has_more, complete: state.complete, total: state.total, loaded_at: state.loaded_at, error: state.error, error_kind: state.error_kind,
    })
}

/// Applies one value to a list the way a projection does: rename, drop, take the placed ones out, insert them in ascending index, write the carried rows.
fn apply(rows: &mut Vec<WireRow>, value: &StreamValue) {
    match value {
        StreamValue::Snapshot { rows: whole, .. } => rows.clone_from(whole),
        StreamValue::Delta { rows: carried, row_indexes, removed, placed, replaced, size, .. } => {
            for pair in replaced {
                if let Some(row) = rows.iter_mut().find(|row| row.id == pair.from) { row.id.clone_from(&pair.to); }
            }
            rows.retain(|row| !removed.contains(&row.id));
            let mut taken: Vec<WireRow> = Vec::new();
            for place in placed {
                if let Some(at) = rows.iter().position(|row| row.id == place.id) { taken.push(rows.remove(at)); }
            }
            let mut order: Vec<_> = placed.iter().collect();
            order.sort_by_key(|place| place.index);
            for place in order {
                let row = taken.iter().find(|row| row.id == place.id).cloned().or_else(|| carried.iter().find(|row| row.id == place.id).cloned());
                if let Some(row) = row { rows.insert((place.index as usize).min(rows.len()), row); }
            }
            for (row, at) in carried.iter().zip(row_indexes) {
                if let Some(slot) = rows.get_mut(*at as usize) { *slot = row.clone(); }
            }
            assert_eq!(rows.len(), *size as usize, "a delta leaves the list at its size");
        }
        _ => {}
    }
}

/// Test reader of a stream: the rows it holds after applying every value in protocol order (`.0`), every delivery as `Seen` (`.1`), and the outcomes (`.2`).
#[derive(Default)]
pub(crate) struct Rows(pub(crate) Mutex<Vec<WireRow>>, pub(crate) Mutex<Vec<Seen>>, pub(crate) Mutex<Vec<StreamEnd>>, Mutex<(u32, Vec<StreamValue>)>);

impl Reader for Rows {
    fn delivered(&self, delivery: StreamDelivery) {
        apply(&mut self.0.lock().unwrap(), &delivery.value);
        let mut kept = self.3.lock().unwrap();
        kept.1.push(delivery.value.clone());
        match seen_of(delivery, &mut kept.0) {
            Ok(seen) => self.1.lock().unwrap().push(seen),
            Err(StreamValue::Ended { reason }) => self.2.lock().unwrap().push(reason),
            Err(_) => {}
        }
    }
}

impl Rows {
    pub(crate) fn values(&self) -> Vec<StreamValue> { self.3.lock().unwrap().1.clone() }

    pub(crate) fn last(&self) -> Option<Seen> { self.1.lock().unwrap().last().cloned() }

    pub(crate) fn ended(&self) -> Vec<StreamEnd> { self.2.lock().unwrap().clone() }

    pub(crate) fn len(&self) -> usize { self.0.lock().unwrap().len() }

    pub(crate) fn state(&self) -> Option<Seen> { self.last() }

    /// Reads every value `stream` holds, the way a platform reads after `available`.
    pub(in crate::ffi) fn read_all(&self, stream: &super::Stream) {
        while let Some(delivery) = stream.read() { self.delivered(delivery); }
    }
}

/// A sink that only hears that values wait: the test reads the stream itself.
pub(crate) struct Silent;

impl ValueSink for Silent {
    fn available(&self) {}
}

/// Like `Rows`, for tests that only read the deliveries.
#[derive(Default)]
pub(crate) struct Tape(Rows);

impl Reader for Tape {
    fn delivered(&self, delivery: StreamDelivery) { self.0.delivered(delivery); }
}

impl Tape {
    pub(crate) fn heard(&self) -> Vec<Seen> { self.0.1.lock().unwrap().clone() }

    pub(crate) fn last(&self) -> Option<Seen> { self.0.last() }
}

/// The phase, the ids and the size after each delivery.
pub(crate) fn folded(changes: &[Seen]) -> Vec<(Option<LivePhase>, Vec<String>, u32)> {
    let mut ids: Vec<String> = Vec::new();
    changes
        .iter()
        .map(|change| {
            if change.full { ids.clear(); }
            ids.retain(|id| !change.removed.contains(id));
            for row in &change.rows {
                if !ids.contains(&row.id) { ids.push(row.id.clone()); }
            }
            (change.phase, ids.clone(), change.size)
        })
        .collect()
}

/// A stream opened for a test in the epoch of the session now.
pub(crate) struct Watched {
    database: Database,
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Watched {
    pub(crate) async fn wait_for_initial_load(&self) -> Result<(), DbError> { self.database.loaded(self.id).await.map(drop) }

    pub(crate) fn cancel(&self) { self.database.cancel_stream(self.id); }

    pub(crate) fn report_desync(&self) { self.database.resync_stream(self.id); }
}

impl Database {
    /// Opens a stream whose values reach `reader` as a platform reads them.
    pub(crate) fn subscribe_reading(&self, epoch: u64, request: StreamRequest, reader: Arc<dyn Reader>) -> u64 {
        let pull = Arc::new(Pull { table: Arc::downgrade(&self.stream_table), id: OnceLock::new(), reader, again: AtomicBool::new(false), reading: Mutex::new(()), outcome: tokio::sync::watch::channel(None).0, failure: Mutex::new(None) });
        let id = self.stream_table.next.load(Ordering::SeqCst) + 1;
        let _ = pull.id.set(id);
        pulls().lock().unwrap().insert(key(&self.stream_table, id), pull.outcome.subscribe());
        assert_eq!(self.subscribe(epoch, request, pull), id, "a test opened streams of one database from two threads at once");
        id
    }

    /// What `loaded()` of a platform returns for the stream: the position of the first settled value, or the error it fails with.
    pub(crate) async fn loaded(&self, stream: u64) -> Result<u64, DbError> {
        let outcome = pulls().lock().unwrap().get(&key(&self.stream_table, stream)).cloned();
        let Some(mut outcome) = outcome else { return Err(DbError::Interrupted { detail: "the stream is not read".into() }) };
        let seen = outcome.wait_for(Option::is_some).await.map_err(|_| DbError::Interrupted { detail: "the stream is gone".into() })?.clone();
        seen.unwrap_or_else(|| Err(DbError::Interrupted { detail: "the stream is gone".into() }))
    }

    pub(crate) fn watch(&self, request: StreamRequest, reader: Arc<dyn Reader>) -> Arc<Watched> {
        let epoch = self.session_epoch();
        let id = self.subscribe_reading(epoch, request, reader);
        Arc::new(Watched { database: self.clone(), id, epoch })
    }

    pub(crate) fn watch_query(&self, query_json: &str, reader: Arc<dyn Reader>) -> Arc<Watched> { self.watch(StreamRequest::Query { query_json: query_json.into() }, reader) }
}

impl Database {
    /// What `load()` of a platform does: begin the load, wait for its result, give up the interest when the wait is dropped.
    pub(crate) async fn load(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.begin_load(epoch, model, relation, params_json)?.result().await
    }

    pub(crate) async fn create_durable(&self, model: &str, values_json: &str) -> super::super::WireOperationReceipt {
        let request = crate::ffi::CommandRequest { epoch: self.session_epoch(), model: model.into(), values_json: values_json.into(), args_json: "{}".into(), upload: None };
        self.send(request).await.unwrap()
    }

    pub(crate) async fn refusal_of(&self, receipt: super::super::WireOperationReceipt) -> super::super::WireFailure {
        match self.outcome(receipt).await.unwrap() {
            super::super::WireOperationOutcome::Rejected { failure } => failure,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}
