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
use crate::value::{normalize_relation_params, relation_scope_key, ScalarKind};

use super::error::SyncError;
use super::extract::extract;
use super::freshness::stale_window;
use super::land::{Landed, Landing};
use super::normalize::normalize_nodes;
use super::registry::OperationDef;
use super::variables::load_variables;
use super::state::LoadKind;
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
}

impl LoadOutcome {
    fn of(status: LoadStatus) -> Self {
        Self { status, rows: 0, dropped: 0, has_next: false, attempts: 0 }
    }
}

pub(crate) struct Resolved<'a> {
    pub model: String,
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

struct FlightGuard<'a> {
    inner: &'a Inner,
    key: String,
    sender: Option<watch::Sender<Option<FlightEnd>>>,
    loading: Option<(String, LoadKind)>,
}

impl FlightGuard<'_> {
    fn settle(&mut self, result: FlightResult) {
        if let Some((window_key, kind)) = self.loading.take() {
            self.inner.end_load(&window_key, kind, &result);
        }
        self.release(FlightEnd::Settled(result));
    }

    fn release(&mut self, end: FlightEnd) {
        let Some(sender) = self.sender.take() else { return };
        let mut flights = locked(&self.inner.flights);
        flights.coalescing.finish(&self.key);
        flights.waiting.remove(&self.key);
        sender.send_replace(Some(end));
    }
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        if let Some((window_key, kind)) = self.loading.take() {
            self.inner.abandon_load(&window_key, kind);
        }
        self.release(FlightEnd::Abandoned);
    }
}

impl Inner {
    pub(crate) fn resolve(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Resolved<'_>, SyncError> {
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
        let mut scope_params = Map::new();
        let mut kinds = std::collections::BTreeMap::new();
        for column in &declared.by_columns {
            let kind = match spec.field(column).map(|field| &field.kind) {
                Some(crate::schema::FieldKind::Scalar { kind }) => *kind,
                Some(crate::schema::FieldKind::Enum { .. }) => ScalarKind::Str,
                _ => return Err(SyncError::Params(format!("{column} is not a scalar or enum field"))),
            };
            let value = params.get(column).ok_or_else(|| SyncError::Params(format!("missing param {column}")))?;
            scope_params.insert(column.clone(), value.clone());
            kinds.insert(column.clone(), kind as ScalarKind);
        }
        let scope_values = scope_params.clone();
        for param in &declared.text_params {
            let value = params.get(param).ok_or_else(|| SyncError::Params(format!("missing param {param}")))?;
            let kind = declared.scalar_types.get(param).copied().unwrap_or(ScalarKind::Str);
            let sent_as_declared = match kind {
                ScalarKind::Int => value.is_null() || value.is_i64() || value.is_u64(),
                ScalarKind::Bool => value.is_null() || value.is_boolean(),
                _ => true,
            };
            if !sent_as_declared {
                return Err(SyncError::Params(format!("param {param} must be a JSON {}", if kind == ScalarKind::Int { "integer" } else { "boolean" })));
            }
            scope_params.insert(param.clone(), value.clone());
            kinds.insert(param.clone(), kind);
        }
        let normalized = normalize_relation_params(&scope_params, &kinds).map_err(|error| SyncError::Params(error.to_string()))?;
        let scope_key = relation_scope_key(model, relation, &normalized).map_err(|error| SyncError::Params(error.to_string()))?;
        let direction = match remote.shape {
            RemoteShape::Connection { direction, .. } => direction,
            _ => PageDirection::Forward,
        };
        let window_key = window_key(scope_key.as_str(), direction);
        let scope = ScopeRef::new(ModelKey::new(model), scope_key);
        Ok(Resolved { model: model.to_string(), remote, operation, scope, window_key, params: params.clone(), scope_values, edge_fields: &declared.edge_fields })
    }

    pub(crate) fn begin_attempt(&self, key: &str) -> Result<FetchAttempt, SyncError> {
        let generation = self.db.generation();
        let base_revision = self.db.revision_seq()?;
        Ok(FetchAttempt::new(key, self.next_issued(), generation, base_revision))
    }

    fn next_issued(&self) -> i64 {
        let now = self.now_ms();
        let update = |previous: i64| Some(previous.max(now - 1) + 1);
        self.issued.try_update(Ordering::SeqCst, Ordering::SeqCst, update).unwrap_or(now)
    }

    fn window_is_fresh(&self, resolved: &Resolved<'_>) -> Result<bool, SyncError> {
        let store = &self.db.shared.store;
        let Some(freshness) = store.window_freshness(&resolved.window_key)? else { return Ok(false) };
        let is_empty = store.read(&[&resolved.model], |reader| reader.scope_last_id(&resolved.model, resolved.scope.key.as_str()))?.is_none();
        let policy = selected_policy(is_empty, &resolved.remote.stale_time, resolved.remote.empty_stale_time.as_deref());
        let window = stale_window(policy)?;
        Ok(freshness.is_fresh(self.now_ms(), i64::try_from(window.as_millis()).unwrap_or(i64::MAX)))
    }

    fn is_loading(&self, resolved: &Resolved<'_>) -> bool {
        let prefix = format!("{}|", resolved.window_key);
        locked(&self.flights).coalescing.in_flight().iter().any(|key| key.starts_with(&prefix))
    }

    pub(crate) async fn fetch_after_flights(&self, resolved: &Resolved<'_>) -> FlightResult {
        let paged = format!("{}|", resolved.window_key);
        loop {
            let pending = locked(&self.flights).waiting.iter().find(|(key, _)| **key == resolved.window_key || key.starts_with(&paged)).map(|(_, receiver)| receiver.clone());
            let Some(mut receiver) = pending else { break };
            let _ = receiver.wait_for(Option::is_some).await;
        }
        self.fetch(resolved, None, true).await
    }

    pub(crate) async fn fetch(&self, resolved: &Resolved<'_>, cursor: Option<String>, force: bool) -> FlightResult {
        let flight_key = match &cursor {
            Some(cursor) => format!("{}|{cursor}", resolved.window_key),
            None => resolved.window_key.clone(),
        };
        let forced = force || cursor.is_some();
        loop {
            if !self.connectivity.is_online() { return Err(SyncError::Offline); }
            let verdict = {
                let mut flights = locked(&self.flights);
                let verdict = flights.coalescing.request(&flight_key, forced, || self.window_is_fresh(resolved).unwrap_or(false));
                match verdict {
                    FetchVerdict::StartFetch => {
                        let (sender, receiver) = watch::channel(None);
                        flights.waiting.insert(flight_key.clone(), receiver);
                        Ok(Some(sender))
                    }
                    FetchVerdict::AwaitInFlight => match flights.waiting.get(&flight_key) {
                        Some(receiver) => Err(Some(receiver.clone())),
                        None => Err(None),
                    },
                    FetchVerdict::SkipFresh => Err(None),
                }
            };
            match verdict {
                Ok(sender) => {
                    let mut guard = FlightGuard { inner: self, key: flight_key, sender, loading: None };
                    let kind = self.load_kind(&resolved.scope, cursor.as_deref());
                    self.begin_load(&resolved.window_key, kind);
                    guard.loading = Some((resolved.window_key.clone(), kind));
                    let result = self.execute(resolved, cursor, force).await;
                    guard.settle(result.clone());
                    return result;
                }
                Err(Some(mut receiver)) => {
                    let end = receiver.wait_for(Option::is_some).await.ok().and_then(|end| end.clone());
                    if let Some(FlightEnd::Settled(result)) = end { return result; }
                }
                Err(None) => return Ok(LoadOutcome::of(LoadStatus::SkippedFresh)),
            }
        }
    }

    async fn execute(&self, resolved: &Resolved<'_>, mut cursor: Option<String>, force: bool) -> FlightResult {
        let attempt = self.begin_attempt(&resolved.window_key)?;
        let has_window = self.db.shared.store.window(&resolved.window_key)?.is_some();
        let variables = load_variables(&resolved.params, &resolved.remote.shape, resolved.page_size(), cursor.as_deref(), &resolved.remote.variables);
        let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, variables);
        let mut replace_window = false;
        let response = match self.query_with_cursor_error(request, cursor.is_some()).await {
            Ok((response, attempts)) if response.errors.iter().any(|error| error.code() == Some(INVALID_CURSOR)) => {
                replace_window = true;
                cursor = None;
                let variables = load_variables(&resolved.params, &resolved.remote.shape, resolved.page_size(), None, &resolved.remote.variables);
                let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, variables);
                self.query_with_retry(request).await.map(|(response, retried)| (response, attempts + retried))
            }
            result => result,
        };
        let fetched = response.and_then(|(response, attempts)| {
            let data = response.data.ok_or_else(|| SyncError::Malformed("response has no data".to_string()))?;
            Ok((extract(&data, resolved.remote.root.as_deref().unwrap_or(&resolved.operation.root_path), &resolved.remote.shape, resolved.remote.total_from.as_deref())?, attempts))
        });
        let (extracted, attempts) = match fetched {
            Ok(fetched) => fetched,
            Err(error) => return Self::failed(error, force, has_window),
        };
        let stamped = stamp_scope_values(extracted.nodes, &resolved.scope_values);
        let normalized = normalize_nodes(&self.db, &resolved.model, &stamped)?;
        let dropped = normalized.dropped;
        let is_first_page = cursor.is_none();
        let plans_as_first = is_first_page && (resolved.is_connection() || resolved.remote.coverage() == Coverage::Complete);
        let has_next = extracted.page_info.as_ref().is_some_and(|info| info.has_next_page);
        let landing = Landing {
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
        Ok(match self.land(landing)? {
            Landed::Applied { rows, invalid } => LoadOutcome { status: LoadStatus::Loaded, rows, dropped: dropped + invalid, has_next, attempts },
            Landed::Discarded(reason) => LoadOutcome { attempts, ..LoadOutcome::of(LoadStatus::Discarded(reason)) },
        })
    }

    fn failed(error: SyncError, force: bool, has_window: bool) -> FlightResult {
        if error == SyncError::Offline { return Err(error); }
        match FetchReport::of(force, has_window, Some(&error.to_string())) {
            FetchReport::KeptStale => Ok(LoadOutcome::of(LoadStatus::KeptStale)),
            _ => Err(error),
        }
    }

    pub(crate) async fn query_with_retry(&self, request: GraphQLRequest) -> Result<(Response, u32), SyncError> {
        self.query_with_cursor_error(request, false).await
    }

    async fn query_with_cursor_error(&self, request: GraphQLRequest, return_invalid_cursor: bool) -> Result<(Response, u32), SyncError> {
        let (response, attempts) = tokio::select! {
            biased;
            () = self.connectivity.offline() => return Err(SyncError::Offline),
            result = crate::transport::retry::query(self.transport.as_ref(), request, self.clock.as_ref(), &self.connectivity, || locked(&self.counters).record_transport_call()) => result.map_err(SyncError::Transport)?,
        };
        if response.shape().outcome() == ResponseOutcome::Commit || return_invalid_cursor && response.errors.iter().any(|error| error.code() == Some(INVALID_CURSOR)) {
            return Ok((response, attempts));
        }
        let mut messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
        if messages.is_empty() { messages.push("empty response".to_string()); }
        Err(SyncError::Graphql { messages, extensions: response.extensions() })
    }

    pub(crate) async fn load_more(&self, resolved: &Resolved<'_>) -> FlightResult {
        let RemoteShape::Connection { max_pages, .. } = resolved.remote.shape else {
            return Err(SyncError::NotPaged { model: resolved.model.clone(), relation: resolved.scope.key.to_string() });
        };
        let Some(window) = self.db.shared.store.window(&resolved.window_key)? else {
            return Ok(LoadOutcome::of(LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor)));
        };
        let chain = PageChain {
            cursor: window.cursor.clone(),
            has_next: window.has_next,
            loaded_pages: usize::try_from(window.pages).unwrap_or_default(),
            is_loading: self.is_loading(resolved),
        };
        let verdict = LoadMoreVerdict::of(&chain, max_pages.map(|pages| pages as usize));
        if !verdict.calls_transport() {
            return Ok(LoadOutcome::of(LoadStatus::NoMorePages(verdict)));
        }
        self.fetch(resolved, window.cursor, true).await
    }
}

impl Sync {
    #[cfg(test)]
    pub fn window_key(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<String, SyncError> {
        Ok(self.inner.resolve(model, relation, params)?.window_key)
    }

    pub fn edge_values(&self, model: &str, relation: &str, params: &Map<String, Value>, ids: &[String]) -> Result<std::collections::BTreeMap<String, Map<String, Value>>, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
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

    pub async fn load(&self, model: &str, relation: &str, params: &Map<String, Value>, options: LoadOptions) -> Result<LoadOutcome, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        self.inner.fetch(&resolved, None, options.force).await
    }

    pub async fn load_more(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<LoadOutcome, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        self.inner.load_more(&resolved).await
    }
}

fn stamp_scope_values(mut nodes: Vec<Map<String, Value>>, values: &Map<String, Value>) -> Vec<Map<String, Value>> {
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
