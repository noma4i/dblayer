use std::sync::atomic::Ordering;

use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::commit::ScopeRef;
use crate::identity::ModelKey;
use crate::pacing::{selected_policy, FetchVerdict};
use crate::read::{LoadMoreVerdict, PageChain};
use crate::schema::{Coverage, PageDirection, RemoteShape, RemoteSpec};
use crate::store::window_key;
use crate::transport::{FetchAttempt, FetchReport, GraphQLRequest, Response, ResponseOutcome, RetryClass, RETRY_BACKOFF};
use crate::value::{normalize_relation_params, relation_scope_key, ScalarKind};

use super::error::SyncError;
use super::extract::extract;
use super::freshness::stale_window;
use super::land::{Landed, Landing};
use super::normalize::normalize_nodes;
use super::registry::OperationDef;
use super::variables::load_variables;
use super::{locked, FlightResult, Inner, Sync};

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

pub(crate) struct Resolved {
    pub model: String,
    pub remote: RemoteSpec,
    pub operation: OperationDef,
    pub scope: ScopeRef,
    pub window_key: String,
    pub params: Map<String, Value>,
}

impl Resolved {
    fn is_connection(&self) -> bool {
        matches!(self.remote.shape, RemoteShape::Connection { .. })
    }
}

struct FlightGuard<'a> {
    inner: &'a Inner,
    key: String,
    sender: Option<watch::Sender<Option<FlightResult>>>,
}

impl FlightGuard<'_> {
    fn release(&mut self, result: FlightResult) {
        let Some(sender) = self.sender.take() else { return };
        let mut flights = locked(&self.inner.flights);
        flights.coalescing.finish(&self.key);
        flights.waiting.remove(&self.key);
        sender.send_replace(Some(result));
    }
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        self.release(Err(SyncError::Cancelled));
    }
}

impl Inner {
    pub(crate) fn resolve(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Resolved, SyncError> {
        let spec = self.db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
        let declared = spec
            .relations
            .iter()
            .find(|candidate| candidate.name == relation)
            .ok_or_else(|| SyncError::UnknownRelation { model: model.to_string(), relation: relation.to_string() })?;
        let remote = declared
            .remote
            .clone()
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
                _ => return Err(SyncError::Params(format!("{column} is not a scalar field"))),
            };
            let value = params.get(column).ok_or_else(|| SyncError::Params(format!("missing param {column}")))?;
            scope_params.insert(column.clone(), value.clone());
            kinds.insert(column.clone(), kind as ScalarKind);
        }
        let normalized = normalize_relation_params(&scope_params, &kinds).map_err(|error| SyncError::Params(error.to_string()))?;
        let scope_key = relation_scope_key(model, relation, &normalized).map_err(|error| SyncError::Params(error.to_string()))?;
        let direction = match remote.shape {
            RemoteShape::Connection { direction, .. } => direction,
            _ => PageDirection::Forward,
        };
        let window_key = window_key(scope_key.as_str(), direction);
        let scope = ScopeRef::new(ModelKey::new(model), scope_key);
        Ok(Resolved { model: model.to_string(), remote, operation, scope, window_key, params: params.clone() })
    }

    fn next_issued(&self) -> i64 {
        let now = self.now_ms();
        let update = |previous: i64| Some(previous.max(now - 1) + 1);
        self.issued.try_update(Ordering::SeqCst, Ordering::SeqCst, update).unwrap_or(now)
    }

    fn window_is_fresh(&self, resolved: &Resolved) -> Result<bool, SyncError> {
        let store = &self.db.shared.store;
        let Some(freshness) = store.window_freshness(&resolved.window_key)? else { return Ok(false) };
        let is_empty = store.window(&resolved.window_key)?.is_none_or(|window| window.ids.is_empty());
        let policy = selected_policy(is_empty, &resolved.remote.stale_time, resolved.remote.empty_stale_time.as_deref());
        let window = stale_window(policy)?;
        Ok(freshness.is_fresh(self.now_ms(), i64::try_from(window.as_millis()).unwrap_or(i64::MAX)))
    }

    fn is_loading(&self, resolved: &Resolved) -> bool {
        let prefix = format!("{}|", resolved.window_key);
        locked(&self.flights).coalescing.in_flight().iter().any(|key| key.starts_with(&prefix))
    }

    pub(crate) async fn fetch(&self, resolved: &Resolved, cursor: Option<String>, force: bool) -> FlightResult {
        let flight_key = match &cursor {
            Some(cursor) => format!("{}|{cursor}", resolved.window_key),
            None => resolved.window_key.clone(),
        };
        let is_stale = force || cursor.is_some() || !self.window_is_fresh(resolved)?;
        let verdict = {
            let mut flights = locked(&self.flights);
            let verdict = flights.coalescing.request(&flight_key, is_stale);
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
                let mut guard = FlightGuard { inner: self, key: flight_key, sender };
                let result = self.execute(resolved, cursor, force).await;
                guard.release(result.clone());
                result
            }
            Err(Some(mut receiver)) => {
                let finished = receiver.wait_for(Option::is_some).await.map_err(|_| SyncError::Cancelled)?;
                finished.clone().ok_or(SyncError::Cancelled)?
            }
            Err(None) => Ok(LoadOutcome::of(LoadStatus::SkippedFresh)),
        }
    }

    async fn execute(&self, resolved: &Resolved, cursor: Option<String>, force: bool) -> FlightResult {
        let generation = self.db.generation();
        let base_revision = self.db.revision_seq()?;
        let issued = self.next_issued();
        let has_window = self.db.shared.store.window(&resolved.window_key)?.is_some();
        let variables = load_variables(&resolved.params, &resolved.remote.shape, resolved.operation.page_size, cursor.as_deref());
        let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, variables);
        let fetched = self.query_with_retry(request).await.and_then(|(response, attempts)| {
            let data = response.data.ok_or_else(|| SyncError::Malformed("response has no data".to_string()))?;
            Ok((extract(&data, &resolved.operation.root_path, &resolved.remote.shape)?, attempts))
        });
        let (extracted, attempts) = match fetched {
            Ok(fetched) => fetched,
            Err(error) => return Self::failed(error, force, has_window),
        };
        let normalized = normalize_nodes(&self.db, &resolved.model, &extracted.nodes)?;
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
            plans_as_first,
            coverage: resolved.remote.coverage(),
            requested_cursor: cursor,
            page_info: extracted.page_info,
            attempt: FetchAttempt::new(&resolved.window_key, issued, generation, base_revision),
        };
        Ok(match self.land(landing)? {
            Landed::Applied { rows } => LoadOutcome { status: LoadStatus::Loaded, rows, dropped, has_next, attempts },
            Landed::Discarded(reason) => LoadOutcome { attempts, ..LoadOutcome::of(LoadStatus::Discarded(reason)) },
        })
    }

    fn failed(error: SyncError, force: bool, has_window: bool) -> FlightResult {
        match FetchReport::of(force, has_window, Some(&error.to_string())) {
            FetchReport::KeptStale => Ok(LoadOutcome::of(LoadStatus::KeptStale)),
            _ => Err(error),
        }
    }

    async fn query_with_retry(&self, request: GraphQLRequest) -> Result<(Response, u32), SyncError> {
        let mut attempt = 0;
        loop {
            if !self.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            locked(&self.counters).record_transport_call();
            let (class, error) = match self.transport.query(request.clone()).await {
                Ok(response) if response.shape().outcome() == ResponseOutcome::Commit => return Ok((response, attempt + 1)),
                Ok(response) => {
                    let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
                    let message = response.errors.first().map_or("empty response".to_string(), |error| error.message.clone());
                    (RetryClass::classify(false, None, &codes), SyncError::Graphql(message))
                }
                Err(error) => (RetryClass::of_error(&error), SyncError::Transport(error.to_string())),
            };
            if !class.allows_retry(attempt) {
                return Err(error);
            }
            tokio::time::sleep(RETRY_BACKOFF.delay(attempt)).await;
            attempt += 1;
        }
    }

    pub(crate) async fn load_more(&self, resolved: &Resolved) -> FlightResult {
        let RemoteShape::Connection { max_pages, .. } = resolved.remote.shape else {
            return Err(SyncError::NotPaged { model: resolved.model.clone(), relation: resolved.scope.key.to_string() });
        };
        let Some(window) = self.db.shared.store.window(&resolved.window_key)? else {
            return Ok(LoadOutcome::of(LoadStatus::NoMorePages(LoadMoreVerdict::NoCursor)));
        };
        let chain = PageChain {
            cursor: window.cursor.clone(),
            has_next: window.has_next,
            loaded_pages: window.ids.len().div_ceil(resolved.operation.page_size.max(1) as usize),
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
    pub fn window_key(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<String, SyncError> {
        Ok(self.inner.resolve(model, relation, params)?.window_key)
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
