use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::identity::RowId;
use crate::query::{compile_ranked, fetch_ranked, windowable, Query, QueryError, RankedRow};
use crate::store::Reader;

use super::diff::{Entry, Stamp};
use super::error::DbError;
use super::observe::ObserverHandle;
use super::open::{locked, Db, Shared};
use super::ranked::{OrderKey, Ranked};
use super::row::Row;
use super::watch::{Body, Evaluated, Listing, Loaded, Related, RowsBody, Shown, Subject, Version};
use super::Change;
use crate::identity::touches::touch;

const MAX_STEP_IDS: usize = 500;

static NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub enum WindowSpec {
    Tail { size: usize },
    Around { id: RowId, before: usize, after: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowEdge {
    pub has_before: bool,
    pub has_after: bool,
    pub total: Option<usize>,
}

#[derive(Clone)]
enum Span {
    Tail { size: usize },
    Around { id: RowId, key: Arc<OrderKey>, before: usize, after: usize },
}

impl Span {
    fn start(spec: &WindowSpec, index: &Ranked, model: &str) -> Result<Self, DbError> {
        match spec {
            WindowSpec::Tail { size } => Ok(Span::Tail { size: *size }),
            WindowSpec::Around { id, before, after } => {
                let (key, _) = index.get(id.as_str()).ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
                Ok(Span::Around { id: id.clone(), key: key.clone(), before: *before, after: *after })
            }
        }
    }

    fn bounds(&mut self, index: &Ranked) -> (usize, usize) {
        let len = index.len();
        match self {
            Span::Tail { size } => (len.saturating_sub(*size), len),
            Span::Around { id, key, before, after } => {
                let present = index.get(id.as_str()).map(|(found, _)| found.clone());
                let anchored = present.is_some();
                if let Some(found) = present {
                    *key = found;
                }
                let at = index.position(key);
                (at.saturating_sub(*before), (at + *after + usize::from(anchored)).min(len))
            }
        }
    }

    fn grown(&self, index: &Ranked, more_before: usize, more_after: usize) -> Self {
        let mut next = self.clone();
        let (start, end) = next.bounds(index);
        match &mut next {
            Span::Tail { size } => *size += more_before.min(start),
            Span::Around { before, after, .. } => {
                *before += more_before.min(start);
                *after += more_after.min(index.len() - end);
            }
        }
        next
    }
}

pub(crate) struct WindowSeed {
    pub at: (i64, u64),
    index: Ranked,
}

impl WindowSeed {
    pub(crate) fn of(query: &Query, subject: &Subject, version: &Version) -> Option<Self> {
        let (Subject::Window(held, _, _), Body::Rows(body)) = (subject, &version.body) else { return None };
        if held != query {
            return None;
        }
        Some(Self { at: version.position(), index: body.window.as_ref()?.index.clone() })
    }
}

pub(crate) struct WindowPart {
    index: Ranked,
    span: Span,
    pub edge: WindowEdge,
    pub shape: u64,
}

impl RowsBody {
    pub(crate) fn reshaped_from(&self, old: &Self) -> bool {
        matches!((&old.window, &self.window), (Some(before), Some(after)) if after.shape > before.shape)
    }

    pub(crate) fn edge_differs(&self, old: &Self) -> bool {
        matches!((&old.window, &self.window), (Some(before), Some(after)) if before.edge != after.edge)
    }
}

impl Version {
    pub(crate) fn announces(&self, previous: &Version) -> bool {
        let Body::Rows(body) = &self.body else { return true };
        if body.window.is_none() {
            return true;
        }
        body.step.as_ref().is_some_and(|step| !step.diff.is_empty()) || matches!(&previous.body, Body::Rows(old) if body.edge_differs(old))
    }
}

impl Subject {
    pub(crate) fn window(query: Query, spec: WindowSpec) -> Subject {
        Subject::Window(query, spec, NONCE.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone)]
pub enum Slice {
    Offset { offset: usize, limit: usize },
    After { id: RowId, limit: usize },
    Before { id: RowId, limit: usize },
}

#[derive(Debug, Clone)]
pub struct Page {
    pub rows: Vec<Row>,
    pub has_before: bool,
    pub has_after: bool,
    pub total: usize,
}

pub struct WindowHandle {
    observer: ObserverHandle,
}

impl WindowHandle {
    pub fn expand_before(&self, rows: usize) {
        self.observer.grow(rows, 0);
    }

    pub fn expand_after(&self, rows: usize) {
        self.observer.grow(0, rows);
    }
}

fn lean(record: RankedRow, descending: &[bool]) -> Result<(Arc<OrderKey>, Arc<Entry>), DbError> {
    let id = RowId::new(record.id.as_str())?;
    let entry = Entry { id, stamp: Stamp { revision: record.revision, ..Stamp::default() } };
    Ok((Arc::new(OrderKey::new(record.cells, descending, record.id)), Arc::new(entry)))
}

fn edge_of(start: usize, end: usize, len: usize) -> WindowEdge {
    WindowEdge { has_before: start > 0, has_after: end < len, total: Some(len) }
}

impl Shared {
    fn window_index(&self, reader: &Reader<'_>, query: &Query) -> Result<Ranked, DbError> {
        let statement = compile_ranked(query, self.spec(query.model.as_str())?, None)?;
        let found = fetch_ranked(reader.connection(), &statement.statement)?;
        touch(found.len());
        let items = found.into_iter().map(|record| lean(record, &statement.descending)).collect::<Result<Vec<_>, _>>()?;
        Ok(Ranked::from_sorted(items))
    }

    pub(crate) fn window_body(&self, reader: &Reader<'_>, query: &Query, spec: &WindowSpec, seed: Option<&WindowSeed>) -> Result<(Body, Loaded), DbError> {
        if !windowable(query) {
            return Err(QueryError::NotWindowable.into());
        }
        let index = match seed {
            Some(seed) => seed.index.clone(),
            None => self.window_index(reader, query)?,
        };
        let span = Span::start(spec, &index, query.model.as_str())?;
        self.windowed(reader, query, index, span, None, 0)
    }

    fn windowed(&self, reader: &Reader<'_>, query: &Query, index: Ranked, mut span: Span, kept: Option<(&RowsBody, &HashSet<String>)>, shape: u64) -> Result<(Body, Loaded), DbError> {
        let model = query.model.as_str();
        let (start, end) = span.bounds(&index);
        let slots = index.slice(start, end);
        let mut wanted = Vec::new();
        let reused: Vec<Option<Arc<Entry>>> = slots
            .iter()
            .map(|slot| {
                let held = kept.and_then(|(body, touched)| body.entry(slot.id.as_str()).filter(|_| !touched.contains(slot.id.as_str())));
                if held.is_none() {
                    wanted.push((slot.id.to_string(), slot.stamp.revision));
                }
                held
            })
            .collect();
        let rows = self.rows_for_keys(reader, model, &wanted)?;
        locked(&self.counters).rows_loaded += rows.len();
        let loaded: HashMap<String, Row> = rows.into_iter().map(|row| (row.id.to_string(), row)).collect();
        let entries = slots
            .iter()
            .zip(reused)
            .filter_map(|(slot, held)| held.or_else(|| loaded.get(slot.id.as_str()).map(|row| Arc::new(Entry { id: row.id.clone(), stamp: Stamp::of(row, Vec::new()) }))))
            .collect();
        let edge = edge_of(start, end, index.len());
        let body = RowsBody { list: Listing::Whole(Shown::of(entries)), changed: None, step: None, window: Some(WindowPart { index, span, edge, shape }) };
        Ok((Body::Rows(body), Loaded { rows: loaded, related: Related::new() }))
    }

    pub(crate) fn window_step(&self, query: &Query, previous: &Version, touched: &HashSet<String>, horizon: (i64, u64), grow: (usize, usize)) -> Result<Option<Evaluated>, DbError> {
        let Body::Rows(old) = &previous.body else { return Ok(None) };
        let (Some(part), Listing::Whole(shown)) = (&old.window, &old.list) else { return Ok(None) };
        let model = query.model.as_str();
        let spec = self.spec(model)?;
        let epoch = self.ephemeral.lock().epoch;
        self.read(&[model], |reader| {
            let revision = reader.revision_seq()?;
            if (revision, epoch) != horizon {
                return Ok(None);
            }
            let mut changed = touched.clone();
            changed.extend(self.dispatch.replaced_between(previous.revision, revision).iter().flat_map(|(from, to)| [from.to_string(), to.to_string()]));
            let wanted: Vec<String> = changed.iter().cloned().collect();
            let mut index = part.index.clone();
            let mut present: HashSet<String> = HashSet::new();
            for chunk in wanted.chunks(MAX_STEP_IDS) {
                let statement = compile_ranked(query, spec, Some(chunk))?;
                for record in fetch_ranked(reader.connection(), &statement.statement)? {
                    present.insert(record.id.clone());
                    let (key, entry) = lean(record, &statement.descending)?;
                    index = index.with(key, entry);
                }
            }
            for id in changed.iter().filter(|id| !present.contains(*id)) {
                index = index.without(id);
            }
            let (mut before_span, mut span) = (part.span.clone(), part.span.clone());
            let ((old_start, old_end), (start, end)) = (before_span.bounds(&part.index), span.bounds(&index));
            let within = |at: Option<usize>, from: usize, to: usize| at.is_some_and(|at| (from..to).contains(&at));
            let position = |list: &Ranked, id: &str| list.get(id).map(|(key, _)| list.position(key));
            let hit = changed.iter().any(|id| within(position(&part.index, id), old_start, old_end) || within(position(&index, id), start, end));
            if grow == (0, 0) && !hit {
                let edge = edge_of(start, end, index.len());
                let part = WindowPart { index, span, edge, shape: part.shape };
                let body = RowsBody { list: Listing::Whole(shown.clone()), changed: None, step: None, window: Some(part) };
                return Ok(Some(Evaluated { version: Version { revision, epoch, body: Body::Rows(body) }, loaded: Loaded::default() }));
            }
            let (span, shape) = if grow == (0, 0) { (span, part.shape) } else { (span.grown(&index, grow.0, grow.1), part.shape + 1) };
            let (body, loaded) = self.windowed(reader, query, index, span, Some((old, &changed)), shape)?;
            Ok(Some(Evaluated { version: Version { revision, epoch, body }, loaded }))
        })
    }

    pub(crate) fn window_next(&self, query: &Query, previous: &Version, touched: Option<&HashSet<String>>, horizon: (i64, u64), grow: (usize, usize)) -> Result<Evaluated, DbError> {
        if let Some(touched) = touched {
            if let Some(stepped) = self.window_step(query, previous, touched, horizon, grow)? {
                return Ok(stepped);
            }
        }
        let Body::Rows(old) = &previous.body else { return Err(QueryError::NotWindowable.into()) };
        let Some(part) = &old.window else { return Err(QueryError::NotWindowable.into()) };
        let epoch = self.ephemeral.lock().epoch;
        self.read(&[query.model.as_str()], |reader| {
            let revision = reader.revision_seq()?;
            let index = self.window_index(reader, query)?;
            let (span, shape) = if grow == (0, 0) { (part.span.clone(), part.shape) } else { (part.span.grown(&index, grow.0, grow.1), part.shape + 1) };
            let (body, loaded) = self.windowed(reader, query, index, span, None, shape)?;
            Ok(Evaluated { version: Version { revision, epoch, body }, loaded })
        })
    }
}

impl Db {
    pub fn observe_window(&self, query: Query, spec: WindowSpec, listener: impl Fn(&Change) + Send + Sync + 'static) -> Result<WindowHandle, DbError> {
        if !windowable(&query) {
            return Err(QueryError::NotWindowable.into());
        }
        self.observe_subject(Subject::window(query, spec), listener).map(|observer| WindowHandle { observer })
    }
}

impl Db {
    pub(crate) fn observe_window_feed(&self, query: Query, spec: WindowSpec, listener: impl Fn(super::listeners::Feed) + Send + Sync + 'static) -> Result<WindowHandle, DbError> {
        if !windowable(&query) {
            return Err(QueryError::NotWindowable.into());
        }
        self.observe_feed(Subject::window(query, spec), listener).map(|observer| WindowHandle { observer })
    }
}

impl Db {
    pub fn read_slice(&self, query: &Query, slice: Slice) -> Result<Page, DbError> {
        if !windowable(query) {
            return Err(QueryError::NotWindowable.into());
        }
        let model = query.model.as_str();
        self.shared.read(&[model], |reader| {
            let index = self.shared.window_index(reader, query)?;
            let anchored = |id: &RowId| index.get(id.as_str()).map(|(key, _)| key.clone()).ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() });
            let (first, entries) = match &slice {
                Slice::Offset { offset, limit } => (*offset, index.slice(*offset, offset.saturating_add(*limit))),
                Slice::After { id, limit } => {
                    let key = anchored(id)?;
                    (index.position(&key) + 1, index.after(&key, *limit))
                }
                Slice::Before { id, limit } => {
                    let key = anchored(id)?;
                    let entries = index.before(&key, *limit);
                    (index.position(&key) - entries.len(), entries)
                }
            };
            let keys: Vec<(String, i64)> = entries.iter().map(|entry| (entry.id.to_string(), entry.stamp.revision)).collect();
            let rows = self.shared.rows_for_keys(reader, model, &keys)?;
            locked(&self.shared.counters).rows_loaded += rows.len();
            Ok(Page { rows, has_before: first > 0, has_after: first + entries.len() < index.len(), total: index.len() })
        })
    }
}
