use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::identity::RowId;
use crate::query::Query;

use super::error::DbError;
use super::observe::ObserverHandle;
use super::open::{locked, Db};
use super::row::Row;
use super::watch::Subject;

const WARM_QUERIES: usize = 16;
const FIRST_DELIVERY_CEILING: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq)]
pub struct Peeked {
    pub revision: i64,
    pub rows: Vec<Row>,
}

#[derive(Default)]
struct Shelf {
    peek: Mutex<Peek>,
    delivered: Condvar,
}

#[derive(Default)]
struct Peek {
    ready: bool,
    revision: i64,
    rows: Vec<Row>,
}

struct Warmed {
    _handle: ObserverHandle,
    peek: Arc<Shelf>,
}

#[derive(Default)]
pub(crate) struct Warm {
    entries: HashMap<String, (Arc<Warmed>, u64)>,
    clock: u64,
}

impl Warm {
    fn touch(&mut self, key: &str) -> Option<Arc<Warmed>> {
        self.clock += 1;
        let clock = self.clock;
        self.entries.get_mut(key).map(|(warmed, used)| {
            *used = clock;
            warmed.clone()
        })
    }

    fn insert(&mut self, key: String, warmed: Arc<Warmed>) -> Vec<(Arc<Warmed>, u64)> {
        self.clock += 1;
        let mut dropped: Vec<_> = self.entries.insert(key, (warmed, self.clock)).into_iter().collect();
        while self.entries.len() > WARM_QUERIES {
            let oldest = self.entries.iter().min_by_key(|(_, (_, used))| *used).map(|(key, _)| key.clone());
            dropped.extend(oldest.and_then(|key| self.entries.remove(&key)));
        }
        dropped
    }
}

impl Db {
    /// Reads the query once, waits for its first delivery, and keeps its rows in memory, current with every commit, so `peek_query` answers without SQLite. The newest `WARM_QUERIES` queries stay warm.
    pub fn warm(&self, query: &Query) -> Result<(), DbError> {
        let warmed = self.warm_shelf(query)?;
        let shelf = &warmed.peek;
        let (peek, _) = shelf.delivered.wait_timeout_while(locked(&shelf.peek), FIRST_DELIVERY_CEILING, |peek| !peek.ready)
            .unwrap_or_else(|error| error.into_inner());
        if !peek.ready {
            return Err(DbError::Internal("query warming did not deliver its first value".into()));
        }
        Ok(())
    }

    pub(crate) fn retain_query(&self, query: &Query) -> Result<(), DbError> {
        self.warm_shelf(query).map(drop)
    }

    fn warm_shelf(&self, query: &Query) -> Result<Arc<Warmed>, DbError> {
        let key = Subject::Rows(query.clone()).key();
        if let Some(shelf) = locked(&self.shared.warm).touch(&key) {
            return Ok(shelf);
        }
        let shelf = Arc::new(Shelf::default());
        let sink = shelf.clone();
        let handle = self.observe(query.clone(), move |change| {
            let mut peek = locked(&sink.peek);
            change.apply_to(&mut peek.rows);
            peek.revision = change.revision;
            peek.ready = true;
            sink.delivered.notify_all();
        })?;
        self.shared.observers().warm_ids.insert(handle.id());
        let warmed = Arc::new(Warmed { _handle: handle, peek: shelf });
        let dropped = locked(&self.shared.warm).insert(key, warmed.clone());
        drop(dropped);
        Ok(warmed)
    }

    /// The rows and revision of a warm query as last delivered, or `None` when the query is not warm or has no delivery yet. Takes no SQLite and no store lock.
    pub fn peek_query(&self, query: &Query) -> Option<Peeked> {
        let key = Subject::Rows(query.clone()).key();
        let found = locked(&self.shared.warm).touch(&key).and_then(|warmed| {
            let peek = locked(&warmed.peek.peek);
            peek.ready.then(|| Peeked { revision: peek.revision, rows: peek.rows.clone() })
        });
        self.count_peek(found.is_some());
        found
    }

    /// The row from the row cache, or `None` when it is not cached (which does not mean that it does not exist).
    pub fn peek_row(&self, model: &str, id: &RowId) -> Option<Row> {
        let found = locked(&self.shared.cache).get(model, id.as_str());
        self.count_peek(found.is_some());
        found
    }

    fn count_peek(&self, hit: bool) {
        let mut counters = locked(&self.shared.counters);
        if hit {
            counters.peek_hits += 1;
        } else {
            counters.peek_misses += 1;
        }
    }
}
