use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::commit::ScopeRef;
use crate::identity::ModelKey;
use crate::pacing::{selected_policy, FetchVerdict};
use crate::read::{LoadMoreVerdict, PageChain};
use crate::schema::{Coverage, FieldSpec, PageDirection, RemoteShape, RemoteSpec};
use crate::store::window_key;
use crate::transport::{FetchAttempt, FetchReport, GraphQLRequest, Response, ResponseOutcome};
use crate::transport::retry::INVALID_CURSOR;
use crate::value::ScalarKind;

use super::error::SyncError;
use super::extract::extract;
use super::freshness::stale_window;
use super::land::{Landed, Landing};
use super::normalize::normalize_nodes;
use super::registry::OperationDef;
use super::variables::load_variables;
use super::load_machine::{LoadError, Stored};
use super::load_owner::FlightTicket;
use super::{locked, FlightEnd, FlightResult, Inner, Sync};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadOptions {
    pub force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardReason {
    StaleGeneration,
    Superseded,
    ChainMoved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStatus {
    Loaded,
    SkippedFresh,
    KeptStale,
    NoMorePages(LoadMoreVerdict),
    Discarded(DiscardReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadOutcome {
    pub status: LoadStatus,
    pub rows: usize,
    pub dropped: usize,
    pub has_next: bool,
    pub attempts: u32,
    pub kept_error: Option<SyncError>,
}

impl LoadOutcome {
    pub(super) fn of(status: LoadStatus) -> Self {
        Self { status, rows: 0, dropped: 0, has_next: false, attempts: 0, kept_error: None }
    }
}

pub(crate) struct Resolved<'a> {
    pub origin: crate::lifecycle::Origin,
    pub media: Option<&'a crate::schema::MediaSpec>,
    pub members_complete: bool,
    pub node_reread: bool,
    pub model: String,
    pub relation: String,
    pub remote: &'a RemoteSpec,
    pub operation: Arc<OperationDef>,
    pub scope: ScopeRef,
    pub window_key: String,
    pub params: Map<String, Value>,
    pub(crate) scope_values: Map<String, Value>,
    pub edge_fields: &'a [FieldSpec],
}

impl Resolved<'_> {
    pub(crate) fn page_size(&self) -> u32 {
        self.remote.page_size.unwrap_or(self.operation.page_size)
    }

    fn is_connection(&self) -> bool {
        matches!(self.remote.shape, RemoteShape::Connection { .. })
    }
}

/// What a flight task needs to read its scope again.
struct Flight {
    key: String,
    token: u64,
    origin: crate::lifecycle::Origin,
    model: String,
    relation: String,
    params: Map<String, Value>,
    node_reread: bool,
    cursor: Option<String>,
    force: bool,
}

pub(super) enum Executed {
    Landed(LoadOutcome, i64),
    Kept(SyncError),
    Failed(SyncError),
}

/// One caller that waits for the flight of a key. The flight stops when the last interest in its key is gone, and a caller that leaves does not stop the flight of the others.
pub(crate) struct Interest {
    inner: std::sync::Weak<Inner>,
    key: String,
}

impl Interest {
    pub(crate) fn new(inner: &Arc<Inner>, key: &str) -> Self {
        *locked(&inner.flights).interest.entry(key.to_string()).or_default() += 1;
        Self { inner: Arc::downgrade(inner), key: key.to_string() }
    }
}

impl Drop for Interest {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else { return };
        let task = {
            let mut flights = locked(&inner.flights);
            let left = flights.interest.get_mut(&self.key).map(|count| { *count -= 1; *count });
            if left == Some(0) {
                flights.interest.remove(&self.key);
                flights.tasks.remove(&self.key)
            } else {
                None
            }
        };
        if let Some((_, task)) = task { crate::db::lock::effect(|| task.abort()); }
        drop(inner);
    }
}

struct FlightGuard<'a> {
    inner: &'a Inner,
    key: String,
    sender: Option<watch::Sender<Option<FlightEnd>>>,
    token: u64,
}

impl FlightGuard<'_> {
    fn finish(&mut self, result: FlightResult) {
        self.release(FlightEnd::Settled(result));
    }

    fn release(&mut self, end: FlightEnd) {
        let Some(sender) = self.sender.take() else { return };
        let waiting = {
            let mut flights = locked(&self.inner.flights);
            if flights.tasks.get(&self.key).is_some_and(|(token, _)| *token == self.token) { flights.tasks.remove(&self.key); }
            if flights.owners.get(&self.key).is_some_and(|(token, _)| *token == self.token) {
                flights.owners.remove(&self.key);
                flights.coalescing.finish(&self.key);
                flights.node_rereads.remove(&self.key);
                flights.waiting.remove(&self.key)
            } else {
                None
            }
        };
        crate::db::lock::effect(|| sender.send_replace(Some(end)));
        drop(waiting);
    }
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        self.release(FlightEnd::Abandoned);
    }
}

impl Inner {
    pub(crate) async fn load_scope(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>, force: bool) -> FlightResult {
        if resolved.members_complete || resolved.media.is_some() {
            self.load_members(admitted, resolved, force).await?;
            return Ok(LoadOutcome::of(LoadStatus::Loaded));
        }
        self.fetch(admitted, resolved, None, force).await
    }

    pub(crate) fn resolve(&self, origin: crate::lifecycle::Origin, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Resolved<'_>, SyncError> {
        let spec = self.db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
        let declared = spec
            .relations
            .iter()
            .find(|candidate| candidate.name == relation)
            .ok_or_else(|| SyncError::UnknownRelation { model: model.to_string(), relation: relation.to_string() })?;
        let remote = declared
            .remote
            .as_ref()
            .ok_or_else(|| SyncError::NotRemote { model: model.to_string(), relation: relation.to_string() })?;
        let operation = locked(&self.operations)
            .get(&remote.operation)
            .cloned()
            .ok_or_else(|| SyncError::UnknownOperation(remote.operation.clone()))?;
        let scope_key = spec.selection_key(relation, params).map_err(|error| SyncError::Params(error.to_string()))?;
        let scope_values: Map<String, Value> = declared.by_columns.iter().filter_map(|column| params.get(column).map(|value| (column.clone(), value.clone()))).collect();
        for param in &declared.text_params {
            let kind = declared.scalar_types.get(param).copied().unwrap_or(ScalarKind::Str);
            let sent_as_declared = match kind {
                ScalarKind::Int => params[param].is_null() || params[param].is_i64() || params[param].is_u64(),
                ScalarKind::Bool => params[param].is_null() || params[param].is_boolean(),
                _ => true,
            };
            if !sent_as_declared {
                return Err(SyncError::Params(format!("param {param} must be a JSON {}", if kind == ScalarKind::Int { "integer" } else { "boolean" })));
            }
        }
        let direction = match remote.shape {
            RemoteShape::Connection { direction, .. } => direction,
            _ => PageDirection::Forward,
        };
        let window_key = window_key(scope_key.as_str(), direction);
        let scope = ScopeRef::new(ModelKey::new(model), scope_key);
        Ok(Resolved { origin, media: declared.media.as_ref(), members_complete: declared.members_complete, node_reread: false, model: model.to_string(), relation: relation.to_string(), remote, operation, scope, window_key, params: params.clone(), scope_values, edge_fields: &declared.edge_fields })
    }

    pub(crate) fn begin_attempt(&self, key: &str) -> Result<FetchAttempt, SyncError> {
        let (generation, base_revision) = crate::blocking::blocking_unit(|| -> Result<_, SyncError> { Ok((self.db.generation(), self.db.revision_seq()?)) })?;
        Ok(FetchAttempt::new(key, self.next_issued()?, generation, base_revision))
    }

    pub(crate) fn next_issued(&self) -> Result<i64, SyncError> {
        let floor = self.issued_floor.get_or_init(|| self.db.shared.store.max_window_sequence().map_err(|error| error.to_string()));
        let floor = *floor.as_ref().map_err(|error| SyncError::Store(error.clone()))?;
        self.issued.fetch_max(floor, Ordering::SeqCst);
        self.issued.try_update(Ordering::SeqCst, Ordering::SeqCst, |previous| previous.checked_add(1))
            .map(|previous| previous + 1)
            .map_err(|_| SyncError::Store("window sequence exhausted".into()))
    }

    fn window_is_fresh(&self, resolved: &Resolved<'_>) -> Result<bool, SyncError> {
        let store = &self.db.shared.store;
        #[cfg(test)]
        {
            let probe = crate::db::locked(&store.before_freshness).take();
            if let Some(probe) = probe { probe(); }
        }
        let Some(freshness) = store.window_freshness(&resolved.window_key)? else { return Ok(false) };
        let is_empty = store.read(&[&resolved.model], |reader| reader.scope_last_id(&resolved.model, resolved.scope.key.as_str()))?.is_none();
        let policy = selected_policy(is_empty, &resolved.remote.stale_time, resolved.remote.empty_stale_time.as_deref());
        let window = stale_window(policy)?;
        Ok(freshness.is_fresh(self.now_ms(), i64::try_from(window.as_millis()).unwrap_or(i64::MAX)))
    }

    pub(super) fn is_loading(&self, resolved: &Resolved<'_>) -> bool {
        let prefix = format!("{}|", resolved.window_key);
        locked(&self.flights).coalescing.in_flight().iter().any(|key| key.starts_with(&prefix))
    }

    pub(super) async fn flights_settled(&self, resolved: &Resolved<'_>) {
        let paged = format!("{}|", resolved.window_key);
        loop {
            let pending = locked(&self.flights).waiting.iter().find(|(key, _)| **key == resolved.window_key || key.starts_with(&paged)).map(|(_, sender)| sender.subscribe());
            let Some(mut receiver) = pending else { break };
            let _ = receiver.wait_for(Option::is_some).await;
        }
    }

    pub(crate) async fn fetch_after_flights(&self, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>) -> FlightResult {
        self.flights_settled(resolved).await;
        self.fetch(admitted, resolved, None, true).await
    }

    pub(crate) async fn fetch(&self, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>, cursor: Option<String>, force: bool) -> FlightResult {
        match self.fetch_in(resolved, cursor, force, admitted).await {
            Ok(outcome) if resolved.origin == crate::lifecycle::Origin::Explicit && outcome.status == LoadStatus::Discarded(DiscardReason::StaleGeneration) => Err(crate::lifecycle::Ended.into()),
            result => result,
        }
    }

    /// Joins the flight of the key or starts it. The flight runs as its own task with its own lease and ends with one result for every caller that waits; it stops when the last caller left.
    async fn fetch_in(&self, resolved: &Resolved<'_>, cursor: Option<String>, force: bool, lease: &crate::lifecycle::Admitted) -> FlightResult {
        let Some(this) = self.this.get().and_then(std::sync::Weak::upgrade) else { return Err(SyncError::Cancelled) };
        let generation = self.db.generation();
        let flight_key = match &cursor {
            Some(cursor) => format!("{}|{cursor}", resolved.window_key),
            None => resolved.window_key.clone(),
        };
        let forced = force || cursor.is_some();
        let _interest = Interest::new(&this, &flight_key);
        loop {
            if !self.connectivity.is_online() {
                FlightTicket::new(&self.owner, &resolved.window_key, resolved.target(), cursor.is_some(), self.stored(resolved)?).failed(LoadError::of(&SyncError::Offline));
                return Err(SyncError::Offline);
            }
            let started = locked(&self.flights).started;
            let freshness = if forced { Ok(false) } else { self.window_is_fresh(resolved) };
            let verdict = {
                let mut flights = locked(&self.flights);
                let current = self.db.generation();
                if flights.owners.get(&flight_key).is_some_and(|(_, started)| *started != current) {
                    flights.owners.remove(&flight_key);
                    flights.coalescing.finish(&flight_key);
                    flights.node_rereads.remove(&flight_key);
                    flights.waiting.remove(&flight_key);
                }
                if !forced && !flights.coalescing.in_flight().contains(&flight_key) && flights.started != started {
                    continue;
                }
                let mut freshness_error = None;
                let verdict = flights.coalescing.request(&flight_key, forced, || match freshness {
                    Ok(fresh) => fresh,
                    Err(error) => { freshness_error = Some(error); false }
                });
                if let Some(error) = freshness_error {
                    flights.coalescing.finish(&flight_key);
                    drop(flights);
                    self.owner.submit(super::load_machine::Event::ReadFailed { window: resolved.window_key.clone(), error: LoadError::of(&error) });
                    return Err(error);
                }
                if resolved.node_reread && matches!(verdict, FetchVerdict::StartFetch | FetchVerdict::AwaitInFlight) {
                    flights.node_rereads.insert(flight_key.clone());
                }
                match verdict {
                    FetchVerdict::StartFetch => {
                        flights.started = flights.started.wrapping_add(1);
                        let sender = watch::channel(None).0;
                        flights.waiting.insert(flight_key.clone(), sender.clone());
                        let token = flights.started;
                        flights.owners.insert(flight_key.clone(), (token, current));
                        Ok((sender, token))
                    }
                    FetchVerdict::AwaitInFlight => Err(flights.waiting.get(&flight_key).map(watch::Sender::subscribe)),
                    FetchVerdict::SkipFresh => Err(None),
                }
            };
            let mut receiver = match verdict {
                Ok((sender, token)) => {
                    let receiver = sender.subscribe();
                    let flight = Flight { key: flight_key.clone(), token, origin: resolved.origin, model: resolved.model.clone(), relation: resolved.relation.clone(), params: resolved.params.clone(), node_reread: resolved.node_reread, cursor: cursor.clone(), force };
                    let task = crate::task::spawn(lease.clone().into(), this.clone().fly(flight, sender, lease.clone()));
                    locked(&self.flights).tasks.insert(flight_key.clone(), (token, task.abort_handle()));
                    receiver
                }
                Err(Some(receiver)) => {
                    #[cfg(test)]
                    {
                        let joined = locked(&self.flights).joined.0.clone();
                        crate::db::lock::effect(|| joined.send_modify(|joined| *joined += 1));
                    }
                    receiver
                }
                Err(None) => return Ok(LoadOutcome::of(LoadStatus::SkippedFresh)),
            };
            let end = lease.run(async { receiver.wait_for(Option::is_some).await.ok().and_then(|end| end.clone()) }).await?;
            if let Some(FlightEnd::Settled(result)) = end {
                if generation == self.db.generation() && matches!(&result, Ok(outcome) if outcome.status == LoadStatus::Discarded(DiscardReason::StaleGeneration)) { continue; }
                return result;
            }
        }
    }

    /// The one run of a flight. Its guard ends the flight with the result, or as abandoned when the task stops before.
    async fn fly(self: Arc<Self>, flight: Flight, sender: watch::Sender<Option<FlightEnd>>, lease: crate::lifecycle::Admitted) {
        let mut guard = FlightGuard { inner: &self, key: flight.key.clone(), sender: Some(sender), token: flight.token };
        let resolved = match self.resolve(flight.origin, &flight.model, &flight.relation, &flight.params) {
            Ok(mut resolved) => { resolved.node_reread = flight.node_reread; resolved }
            Err(error) => return guard.finish(Err(error)),
        };
        let seed = match self.stored(&resolved) { Ok(seed) => seed, Err(error) => return guard.finish(Err(error)) };
        let ticket = FlightTicket::new(&self.owner, &resolved.window_key, resolved.target(), flight.cursor.is_some(), seed);
        let ticket = if resolved.media.is_some() { ticket.cancel_failed() } else { ticket };
        let executed = Box::pin(self.execute_bound(&resolved, flight.cursor, flight.force, &lease)).await;
        let result = match executed {
            Executed::Landed(outcome, revision) => {
                match outcome.status {
                    LoadStatus::Discarded(_) => ticket.discarded(),
                    _ => match self.stored(&resolved) {
                        Ok(seed) => ticket.landed(seed.map(|stored| Stored { revision, ..stored })),
                        Err(error) => { ticket.failed(LoadError::of(&error)); return guard.finish(Err(error)); }
                    },
                }
                Ok(outcome)
            }
            Executed::Kept(error) => {
                ticket.failed(LoadError::of(&error));
                Ok(LoadOutcome { kept_error: Some(error), ..LoadOutcome::of(LoadStatus::KeptStale) })
            }
            Executed::Failed(error) => {
                ticket.failed(LoadError::of(&error));
                Err(error)
            }
        };
        guard.finish(result);
    }

    async fn execute_bound(&self, resolved: &Resolved<'_>, cursor: Option<String>, force: bool, lease: &crate::lifecycle::Admitted) -> Executed {
        match lease.run(self.execute(lease, resolved, cursor, force)).await {
            Ok(executed) => executed,
            Err(ended) => Executed::Failed(ended.into()),
        }
    }

    async fn execute(&self, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>, cursor: Option<String>, force: bool) -> Executed {
        let loaded = if let Some(media) = resolved.media { self.media_load(admitted, resolved, media).await } else { self.run(admitted, resolved, cursor, force).await };
        match loaded {
            Ok(executed) => executed,
            Err(error) => Executed::Failed(error),
        }
    }

    async fn run(&self, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>, mut cursor: Option<String>, force: bool) -> Result<Executed, SyncError> {
        let attempt = self.begin_attempt(&resolved.window_key)?;
        let has_window = crate::blocking::blocking_unit(|| self.db.shared.store.window(&resolved.window_key))?.is_some();
        let variables = load_variables(&resolved.params, &resolved.remote.shape, resolved.page_size(), cursor.as_deref(), &resolved.remote.variables);
        let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, variables);
        let mut replace_window = false;
        let response = match self.query_with_cursor_error(admitted, request, cursor.is_some()).await {
            Ok((response, attempts)) if response.errors.iter().any(|error| error.code() == Some(INVALID_CURSOR)) => {
                replace_window = true;
                cursor = None;
                let variables = load_variables(&resolved.params, &resolved.remote.shape, resolved.page_size(), None, &resolved.remote.variables);
                let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, variables);
                self.query_with_retry(admitted, request).await.map(|(response, retried)| (response, attempts + retried))
            }
            result => result,
        };
        let fetched = response.and_then(|(response, attempts)| {
            let data = response.data.ok_or_else(|| SyncError::Malformed("response has no data".to_string()))?;
            Ok((extract(&data, resolved.remote.root.as_deref().unwrap_or(&resolved.operation.root_path), &resolved.remote.shape, resolved.remote.total_from.as_deref())?, attempts))
        });
        let (extracted, attempts) = match fetched {
            Ok(fetched) => fetched,
            Err(error) => return Ok(Self::failed(error, force, has_window)),
        };
        let stamped = stamp_scope_values(extracted.nodes, &resolved.scope_values);
        let normalized = normalize_nodes(&self.db, &resolved.model, &stamped)?;
        let dropped = normalized.dropped;
        let is_first_page = cursor.is_none();
        let plans_as_first = is_first_page && (resolved.is_connection() || resolved.remote.coverage() == Coverage::Complete);
        let has_next = extracted.page_info.as_ref().is_some_and(|info| info.has_next_page);
        let landing = Landing {
            marker: None,
            leave_stale: false,
            node_reread: locked(&self.flights).node_rereads.contains(&resolved.window_key),
            model: resolved.model.clone(),
            scope: resolved.scope.clone(),
            window_key: resolved.window_key.clone(),
            normalized,
            is_first_page,
            replace_window,
            plans_as_first,
            coverage: resolved.remote.coverage(),
            edge_fields: resolved.edge_fields.to_vec(),
            requested_cursor: cursor,
            page_info: extracted.page_info,
            keeps_total: resolved.remote.total_from.is_some(),
            attempt,
        };
        Ok(match self.land(admitted, landing)? {
            Landed::Applied { rows, invalid, revision } => {
                self.preload_on_source(&resolved.model, &resolved.relation);
                Executed::Landed(LoadOutcome { status: LoadStatus::Loaded, rows, dropped: dropped + invalid, has_next, attempts, kept_error: None }, revision)
            }
            Landed::Discarded(reason) => Executed::Landed(LoadOutcome { attempts, ..LoadOutcome::of(LoadStatus::Discarded(reason)) }, 0),
        })
    }

    fn failed(error: SyncError, force: bool, has_window: bool) -> Executed {
        if error == SyncError::Offline { return Executed::Failed(error); }
        match FetchReport::of(force, has_window, Some(&error.to_string())) {
            FetchReport::KeptStale => Executed::Kept(error),
            _ => Executed::Failed(error),
        }
    }

    pub(crate) async fn query_with_retry(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<(Response, u32), SyncError> {
        self.query_with_cursor_error(admitted, request, false).await
    }

    async fn query_with_cursor_error(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest, return_invalid_cursor: bool) -> Result<(Response, u32), SyncError> {
        let (response, attempts) = tokio::select! {
            biased;
            () = self.connectivity.offline() => return Err(SyncError::Offline),
            result = crate::transport::retry::query(admitted, self.transport.as_ref(), request, self.clock.as_ref(), &self.connectivity, || locked(&self.counters).record_transport_call()) => result.map_err(SyncError::Transport)?,
        };
        if response.shape().outcome() == ResponseOutcome::Commit || return_invalid_cursor && response.errors.iter().any(|error| error.code() == Some(INVALID_CURSOR)) {
            return Ok((response, attempts));
        }
        let mut messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
        if messages.is_empty() { messages.push("empty response".to_string()); }
        Err(SyncError::Graphql { messages, extensions: response.extensions() })
    }

    pub(crate) async fn load_more(&self, admitted: &crate::lifecycle::Admitted, resolved: &Resolved<'_>) -> FlightResult {
        let RemoteShape::Connection { max_pages, .. } = resolved.remote.shape else {
            return Err(SyncError::NotPaged { model: resolved.model.clone(), relation: resolved.scope.key.to_string() });
        };
        let Some(window) = self.db.shared.store.window(&resolved.window_key)? else {
            return Ok(LoadOutcome::of(LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor)));
        };
        let chain = PageChain {
            cursor: window.cursor.clone(),
            has_next: window.has_next,
            loaded_pages: usize::try_from(window.pages).map_err(|_| SyncError::Store("invalid stored window page count".into()))?,
            is_loading: self.is_loading(resolved),
        };
        let verdict = LoadMoreVerdict::of(&chain, max_pages.map(|pages| pages as usize));
        if !verdict.calls_transport() {
            return Ok(LoadOutcome::of(LoadStatus::NoMorePages(verdict)));
        }
        self.fetch(admitted, resolved, window.cursor, true).await
    }
}

impl Sync {
    #[cfg(test)]
    pub fn window_key(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<String, SyncError> {
        Ok(self.inner.resolve(crate::lifecycle::Origin::Session, model, relation, params)?.window_key)
    }

    pub fn edge_values(&self, model: &str, relation: &str, params: &Map<String, Value>, ids: &[String]) -> Result<std::collections::BTreeMap<String, Map<String, Value>>, SyncError> {
        let resolved = self.inner.resolve(crate::lifecycle::Origin::Session, model, relation, params)?;
        let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
        let read = self.inner.db.shared.store.read(&[model], |reader| reader.scope_edges(model, resolved.scope.key.as_str(), &wanted))?;
        {
            let mut counters = locked(&self.inner.counters);
            counters.edge_read_steps += read.steps;
            counters.edge_read_scans += read.scans;
        }
        Ok(read.members.into_iter().map(|(id, stored)| {
            let values = stored.into_iter().filter_map(|(name, value)| {
                let field = resolved.edge_fields.iter().find(|field| field.name == name)?;
                Some((name, edge_json(&field.kind, value)))
            }).collect();
            (id, values)
        }).collect())
    }

    #[cfg(test)]
    pub(crate) fn joined_loads(&self) -> usize { *locked(&self.inner.flights).joined.0.borrow() }

    #[cfg(test)]
    pub(crate) async fn wait_joined(&self, at_least: usize) {
        let mut joined = locked(&self.inner.flights).joined.0.subscribe();
        let _ = joined.wait_for(|joined| *joined >= at_least).await;
    }

    /// The interest of one caller in the load of a scope, taken before the load starts.
    pub(crate) fn interest(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Interest, SyncError> {
        let key = self.inner.resolve(crate::lifecycle::Origin::Explicit, model, relation, params)?.window_key;
        Ok(Interest::new(&self.inner, &key))
    }

    pub async fn load(&self, admitted: &crate::lifecycle::Admitted, model: &str, relation: &str, params: &Map<String, Value>, options: LoadOptions) -> Result<LoadOutcome, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        let _app = self.app_load();
        let resolved = self.inner.resolve(crate::lifecycle::Origin::Explicit, model, relation, params)?;
        self.inner.load_scope(admitted, &resolved, options.force).await
    }

    pub async fn load_more(&self, admitted: &crate::lifecycle::Admitted, model: &str, relation: &str, params: &Map<String, Value>) -> Result<LoadOutcome, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        let _app = self.app_load();
        let resolved = self.inner.resolve(crate::lifecycle::Origin::Explicit, model, relation, params)?;
        self.inner.load_more(admitted, &resolved).await
    }
}

pub(super) fn stamp_scope_values(mut nodes: Vec<Map<String, Value>>, values: &Map<String, Value>) -> Vec<Map<String, Value>> {
    for node in &mut nodes {
        for (column, value) in values {
            node.entry(column.clone()).or_insert_with(|| value.clone());
        }
    }
    nodes
}

fn edge_json(kind: &crate::schema::FieldKind, value: crate::commit::EdgeValue) -> Value {
    use crate::commit::EdgeValue;
    match (kind, value) {
        (crate::schema::FieldKind::Scalar { kind: ScalarKind::Bool }, EdgeValue::Whole(whole)) => Value::Bool(whole != 0),
        (_, EdgeValue::Whole(whole)) => Value::from(whole),
        (_, EdgeValue::Fraction(fraction)) => Value::from(fraction),
        (_, EdgeValue::Text(text)) => Value::String(text),
    }
}
