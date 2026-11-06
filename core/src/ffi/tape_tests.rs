//! A test reader of the streams of `subscribe_values`: it pulls the way a platform does and keeps what it read.

use std::sync::{Arc, Mutex};

use super::read::{ReadEnd, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, RowPlace, RowRename};
use super::wire::WireRow;
use super::{Database, DbError};

/// Hears every value of a stream in the order it was pulled.
pub(crate) trait Reader: Send + Sync {
    fn delivered(&self, delivery: StreamDelivery);
}

/// One pulled value with its position.
#[derive(Clone, Debug)]
pub(crate) struct StreamDelivery {
    pub(crate) stream: u64,
    pub(crate) epoch: u64,
    pub(crate) seq: u64,
    pub(crate) value: ReadValue,
}

/// One value as a test reads it: the rows part and the state side by side.
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
    pub(crate) placed: Vec<RowPlace>,
    pub(crate) replaced: Vec<RowRename>,
    pub(crate) count: Option<u64>,
    pub(crate) phase: Option<ReadPhase>,
    pub(crate) error: Option<DbError>,
    pub(crate) loaded: Option<i64>,
}

fn seen_of(seq: u64, value: ReadValue, size: &mut u32) -> Result<Seen, ReadValue> {
    let blank = |revision: i64, state: ReadState, size: u32| Seen { full: false, rows: Vec::new(), row_indexes: Vec::new(), size, revision, seq, removed: Vec::new(), placed: Vec::new(), replaced: Vec::new(), count: None, phase: Some(state.phase), error: state.error, loaded: state.loaded };
    Ok(match value {
        ReadValue::Snapshot { revision, rows, state } => {
            *size = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            Seen { full: true, rows, ..blank(revision, state, *size) }
        }
        ReadValue::Delta { revision, size: now, rows, indexes, removed, placed, replaced, state, .. } => {
            *size = now;
            Seen { rows, row_indexes: indexes, removed, placed, replaced, ..blank(revision, state, now) }
        }
        ReadValue::Count { revision, count, state } => Seen { count: Some(count), ..blank(revision, state, *size) },
        ReadValue::State { revision, state } => blank(revision, state, *size),
        other => return Err(other),
    })
}

/// Applies one value to a list the way a projection does: rename, drop, take the placed ones out, insert them in ascending index, write the carried rows.
fn apply(rows: &mut Vec<WireRow>, value: &ReadValue) {
    match value {
        ReadValue::Snapshot { rows: whole, .. } => rows.clone_from(whole),
        ReadValue::Delta { rows: carried, indexes, removed, placed, replaced, size, .. } => {
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
            for (row, at) in carried.iter().zip(indexes) {
                if let Some(slot) = rows.get_mut(*at as usize) { *slot = row.clone(); }
            }
            assert_eq!(rows.len(), *size as usize, "a delta leaves the list at its size");
        }
        _ => {}
    }
}

/// The rows a stream holds after every value in protocol order (`.0`), every value as `Seen` (`.1`), and how it ended (`.2`).
#[derive(Default)]
pub(crate) struct Rows(pub(crate) Mutex<Vec<WireRow>>, pub(crate) Mutex<Vec<Seen>>, pub(crate) Mutex<Vec<ReadEnd>>, Mutex<(u32, Vec<ReadValue>)>);

impl Reader for Rows {
    fn delivered(&self, delivery: StreamDelivery) {
        let StreamDelivery { seq, value, .. } = delivery;
        apply(&mut self.0.lock().unwrap(), &value);
        let mut kept = self.3.lock().unwrap();
        kept.1.push(value.clone());
        match seen_of(seq, value, &mut kept.0) {
            Ok(seen) => self.1.lock().unwrap().push(seen),
            Err(ReadValue::Ended { cause }) => self.2.lock().unwrap().push(cause),
            Err(_) => {}
        }
    }
}

impl Rows {

    pub(crate) fn last(&self) -> Option<Seen> { self.1.lock().unwrap().last().cloned() }

    pub(crate) fn ended(&self) -> Vec<ReadEnd> { self.2.lock().unwrap().clone() }

    pub(crate) fn len(&self) -> usize { self.0.lock().unwrap().len() }

}

/// Like `Rows`, for tests that only read the values.
#[derive(Default)]
pub(crate) struct Tape(Rows);

impl Reader for Tape {
    fn delivered(&self, delivery: StreamDelivery) { self.0.delivered(delivery); }
}

impl Tape {
    pub(crate) fn heard(&self) -> Vec<Seen> { self.0.1.lock().unwrap().clone() }

    pub(crate) fn last(&self) -> Option<Seen> { self.0.last() }
}


impl ReadState {
    /// The first state a reader may show: the load ended with rows or with an error.
    pub(crate) fn settled(&self) -> bool { matches!(self.phase, ReadPhase::Loaded | ReadPhase::Failed) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamCensus {
    pub(crate) open: u64,
}

type Registry = Mutex<std::collections::HashMap<(usize, u64), Arc<Watched>>>;

fn registry() -> &'static Registry {
    static REGISTRY: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

type Settled = tokio::sync::watch::Receiver<Option<Result<u64, DbError>>>;

/// A stream opened for a test in the epoch of the session now, pulled by a task until it ends.
pub(crate) struct Watched {
    stream: Arc<ReadStream>,
    pub(crate) epoch: u64,
    settled: Settled,
    pump: tokio::task::JoinHandle<()>,
    request: ReadRequest,
    ended: Arc<std::sync::atomic::AtomicBool>,
    stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl Watched {
    /// What `loaded()` of a platform returns: the position of the first settled state, or the error it fails with.
    pub(crate) async fn wait_for_initial_load(&self) -> Result<u64, DbError> {
        let mut settled = self.settled.clone();
        let seen = settled.wait_for(Option::is_some).await.map_err(|_| DbError::Interrupted { detail: "the stream is gone".into() })?.clone();
        if self.ended.load(std::sync::atomic::Ordering::SeqCst) && !matches!(seen, Some(Err(DbError::Invalid { .. }))) {
            return Err(DbError::Interrupted { detail: "the stream ended".into() });
        }
        seen.unwrap_or_else(|| Err(DbError::Interrupted { detail: "the stream is gone".into() }))
    }

    /// What `cancel()` of a platform does: it returns once the stream is cancelled.
    pub(crate) fn cancel(&self) {
        let stream = self.stream.clone();
        let handle = tokio::runtime::Handle::current();
        if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
            self.stopped.store(true, std::sync::atomic::Ordering::SeqCst);
            self.pump.abort();
            self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
            tokio::task::block_in_place(|| handle.block_on(async move { stream.cancel().await }));
        } else {
            drop(tokio::spawn(async move { stream.cancel().await }));
        }
    }

}

impl Drop for Watched {
    fn drop(&mut self) { self.pump.abort(); }
}

fn settled_by(value: &ReadValue, seq: u64) -> Option<Result<u64, DbError>> {
    match value {
        ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::Count { state, .. } | ReadValue::State { state, .. } => match state.phase {
            ReadPhase::Loaded => Some(Ok(seq)),
            ReadPhase::Failed => Some(Err(state.error.clone().unwrap_or(DbError::Database { detail: String::new() }))),
            ReadPhase::Idle | ReadPhase::Loading => None,
        },
        ReadValue::Ended { cause: ReadEnd::Invalid } => Some(Err(DbError::Invalid { detail: "the stream could not open".into() })),
        ReadValue::Ended { .. } => Some(Err(DbError::Interrupted { detail: "the stream ended before its rows were shown".into() })),
        _ => None,
    }
}

impl Database {
    /// Opens a stream whose values reach `reader` as a platform pulls them.
    pub(crate) fn watch(&self, request: ReadRequest, reader: Arc<dyn Reader>) -> Arc<Watched> { self.watch_at(self.session_epoch(), request, reader) }

    pub(crate) fn watch_at(&self, epoch: u64, request: ReadRequest, reader: Arc<dyn Reader>) -> Arc<Watched> {
        let stream = self.subscribe_values(epoch, request.clone());
        let (outcome, settled) = tokio::sync::watch::channel(None);
        let pulled = stream.clone();
        let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ended.clone();
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let halt = stopped.clone();
        let pump = tokio::spawn(async move {
            let mut seq = 0;
            loop {
                match pulled.pull().await {
                    Ok(Some(value)) => {
                        if halt.load(std::sync::atomic::Ordering::SeqCst) { return; }
                        seq += 1;
                        if matches!(value, ReadValue::Ended { .. }) { flag.store(true, std::sync::atomic::Ordering::SeqCst); }
                        let done = settled_by(&value, seq);
                        reader.delivered(StreamDelivery { stream: pulled.id(), epoch, seq, value });
                        if let Some(done) = done {
                            outcome.send_if_modified(|current| current.is_none().then(|| *current = Some(done)).is_some());
                        }
                    }
                    Ok(None) => return,
                    Err(error) => {
                        outcome.send_if_modified(|current| current.is_none().then(|| *current = Some(Err(error))).is_some());
                        return;
                    }
                }
            }
        });
        let watched = Arc::new(Watched { stream, epoch, settled, pump, request, ended, stopped });
        registry().lock().unwrap().insert((Arc::as_ptr(&self.life) as usize, watched.stream.id()), watched.clone());
        watched
    }

    fn watched_by(&self, id: u64) -> Option<Arc<Watched>> { registry().lock().unwrap().get(&(Arc::as_ptr(&self.life) as usize, id)).cloned() }

    /// Opens a stream and returns its number, as the platform of the old stream API did.
    pub(crate) fn subscribe_reading(&self, epoch: u64, request: ReadRequest, reader: Arc<dyn Reader>) -> u64 { self.watch_at(epoch, request, reader).stream.id() }

    /// What `loaded()` of a platform returns for the stream.
    pub(crate) async fn loaded(&self, id: u64) -> Result<u64, DbError> {
        let Some(watched) = self.watched_by(id) else { return Err(DbError::Interrupted { detail: "the stream is not read".into() }) };
        watched.wait_for_initial_load().await
    }

    pub(crate) fn cancel_stream(&self, id: u64) {
        if let Some(watched) = self.watched_by(id) { watched.cancel(); }
    }

    /// The streams of this database that did not end.
    pub(crate) fn stream_census(&self) -> StreamCensus {
        let mine = Arc::as_ptr(&self.life) as usize;
        let open = registry().lock().unwrap().iter().filter(|((life, _), watched)| *life == mine && !watched.pump.is_finished()).count();
        StreamCensus { open: open as u64 }
    }

    /// Loads the relation of the stream again from the server.
    pub(crate) async fn refresh_stream(&self, id: u64) -> Result<(), DbError> {
        let Some(watched) = self.watched_by(id).filter(|watched| !watched.ended.load(std::sync::atomic::Ordering::SeqCst)) else { return Err(DbError::Interrupted { detail: "the stream is gone".into() }) };
        if let ReadRequest::Entity { model, id, .. } = &watched.request {
            let spec = self.db.spec(model)?;
            let relation = spec.relations.iter().find(|relation| relation.node == Some(crate::schema::NodeRead::One)).map(|relation| relation.name.clone()).ok_or_else(|| DbError::Invalid { detail: format!("{model} declares no node read") })?;
            return self.node(watched.epoch, model.clone(), relation, id.clone(), true).await.map(drop);
        }
        let ReadRequest::List { query_json } = &watched.request else { return Err(DbError::Invalid { detail: "the stream has nothing to refresh".into() }) };
        let query: serde_json::Value = serde_json::from_str(query_json)?;
        let (model, relation) = (query["model"].as_str().unwrap_or_default().to_string(), query["relation"].as_str().unwrap_or_default().to_string());
        self.refresh(watched.epoch, model, relation, query.get("params").cloned().unwrap_or_else(|| serde_json::json!({})).to_string()).await
    }

    pub(crate) fn watch_query(&self, query_json: &str, reader: Arc<dyn Reader>) -> Arc<Watched> { self.watch(ReadRequest::List { query_json: query_json.into() }, reader) }

    /// The data of the account goes and its epoch ends, as at a sign-out.
    pub(crate) async fn reset_data(&self) { let _ = self.await_life(self.life.session_reset()).await; }

    /// The cache of these models goes; the epoch stays.
    pub(crate) async fn reset_models_of(&self, models: &[&str]) { let _ = self.await_life(self.life.reset_models(models.iter().map(|model| model.to_string()).collect())).await; }

    /// What `load()` of a platform does: begin the load, wait for its result, give up the interest when the wait is dropped.
    pub(crate) async fn load(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.begin_load(epoch, model, relation, params_json)?.result().await
    }

    pub(crate) async fn create_durable(&self, model: &str, values_json: &str) -> super::WireOperationReceipt {
        let request = crate::ffi::CommandRequest { epoch: self.session_epoch(), model: model.into(), values_json: values_json.into(), args_json: "{}".into(), upload: None };
        self.send(request).await.unwrap()
    }

    pub(crate) async fn refusal_of(&self, receipt: super::WireOperationReceipt) -> super::WireFailure {
        match self.outcome(receipt).await.unwrap() {
            super::WireOperationOutcome::Rejected { failure } => failure,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}
