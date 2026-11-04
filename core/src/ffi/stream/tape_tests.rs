use std::sync::{Arc, Mutex};

use crate::ffi::wire::WireRow;
use crate::ffi::{Database, DbError};
use super::{LivePhase, Placement, Replacement, StreamDelivery, StreamEnd, StreamRequest, StreamValue, ValueSink};

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

impl ValueSink for Rows {
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
}

/// Like `Rows`, for tests that only read the deliveries.
#[derive(Default)]
pub(crate) struct Tape(Rows);

impl ValueSink for Tape {
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
    pub(crate) async fn wait_for_initial_load(&self) -> Result<(), DbError> { self.database.stream_loaded(self.id).await.map(drop) }

    pub(crate) fn cancel(&self) { self.database.cancel_stream(self.id); }

    pub(crate) fn report_desync(&self) { self.database.resync_stream(self.id); }
}

impl Database {
    pub(crate) fn watch(&self, request: StreamRequest, sink: Arc<dyn ValueSink>) -> Arc<Watched> {
        let epoch = self.session_epoch();
        let id = self.subscribe(epoch, request, sink);
        Arc::new(Watched { database: self.clone(), id, epoch })
    }

    pub(crate) fn watch_query(&self, query_json: &str, sink: Arc<dyn ValueSink>) -> Arc<Watched> { self.watch(StreamRequest::Query { query_json: query_json.into() }, sink) }
}

impl Database {
    pub(crate) async fn create_durable(&self, model: &str, values_json: &str) -> super::super::wire::WireRow {
        let request = crate::ffi::CommandRequest { epoch: self.session_epoch(), model: model.into(), values_json: values_json.into(), args_json: "{}".into(), upload: None };
        self.send(request).await.unwrap().optimistic_value.unwrap()
    }
}
