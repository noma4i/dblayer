use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, FileOp, OperationTransition, ScopeOp, WriteOrigin};
use crate::db::{read_outbox, Prior};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::pacing::Debounce;
use crate::store::PlanCommitError;
use crate::syncstate::operation_keys;
use crate::transport::{
    EventSpec, GraphQLRequest, Response, Subscription, SubscriptionEvent, SubscriptionFailureKind, SubscriptionFault,
    SubscriptionRetry, SubscriptionFailure, TransportError, RETRY_BACKOFF,
};
use crate::value::{fractional_between, read_id};

use super::error::SyncError;
use super::extract::root_value;
use super::normalize::{merge_stored, normalize_nodes};
use super::{locked, Inner, Sync};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventEffect {
    Upsert,
    Patch,
    Destroy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionDef {
    pub event: EventSpec,
    pub operation: String,
    pub model: String,
    pub effect: EventEffect,
    pub variables: Value,
    pub deleted_by: Option<String>,
    pub set: Map<String, Value>,
    pub root: Option<String>,
    pub id: Option<String>,
}

impl SubscriptionDef {
    pub fn new(event: EventSpec, operation: &str, model: &str, effect: EventEffect) -> Self {
        Self { event, operation: operation.to_string(), model: model.to_string(), effect, variables: json!({}), deleted_by: None, set: Map::new(), root: None, id: None }
    }

    #[cfg(test)]
    pub fn setting(self, set: Map<String, Value>) -> Self {
        Self { set, ..self }
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionStatus {
    pub connects: u32,
    pub applied: usize,
    pub rejected: usize,
    pub degraded: bool,
    pub stopped: bool,
    pub failure: Option<SubscriptionFailure>,
}

type SharedStatus = Arc<Mutex<SubscriptionStatus>>;

pub struct EventSubscription {
    abort: AbortHandle,
    status: SharedStatus,
    pub(crate) definition: SubscriptionDef,
}

impl EventSubscription {
    pub fn status(&self) -> SubscriptionStatus {
        locked(&self.status).clone()
    }

    pub fn cancel(&self) {
        self.abort.abort();
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

struct Pending {
    bucket: String,
    id: RowId,
    node: Map<String, Value>,
    generation: Generation,
}

struct Feed {
    inner: Arc<Inner>,
    def: SubscriptionDef,
    root_path: String,
    status: SharedStatus,
}

impl Feed {
    fn notify_failure(&self, failure: &SubscriptionFailure) {
        locked(&self.status).failure = Some(failure.clone());
        if matches!(failure.error, TransportError::Session { .. }) {
            let _ = self.inner.subscription_failures.send(Some((format!("{}|{}", self.def.model, self.def.operation), failure.clone())));
        }
    }

    async fn invalidation(&self, response: &Response) -> bool {
        if self.inner.db.spec(&self.def.model).is_ok_and(|spec| spec.is_memory()) { return false; }
        let Some(data) = response.data.as_ref() else { return false };
        if let Some(activity) = data.get("chatActivity") {
            if activity["kind"] == "TYPING" {
                if let (Some(id), Some(active)) = (activity.get("userId").and_then(read_id), activity["active"].as_bool()) {
                    match self.inner.db.set_ephemeral(&self.def.model, &id, Map::from_iter([("typing".into(), json!(active))])) {
                        Ok(_) => locked(&self.status).applied += 1,
                        Err(_) => locked(&self.status).rejected += 1,
                    }
                }
            }
            return true;
        }
        if data.get("viewerNotice").is_some() { return true; }
        let changes = if let Some(changes) = data["viewerChanges"]["changes"].as_array() {
            changes.clone()
        } else if let Some(notice) = data.get("interactionReceived") {
            vec![json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":notice["messageId"]}), json!({"area":"USER", "kind":"UPDATED", "entityId":notice["actorUserId"]})]
        } else { return false };
        for change in changes {
            let (Some(area), Some(id), Some(kind)) = (change["area"].as_str(), change["entityId"].as_str(), change["kind"].as_str()) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            if !["CREATED", "UPDATED", "DELETED", "ACCESS_REVOKED"].contains(&kind) {
                locked(&self.status).rejected += 1;
                continue;
            }
            let targets: Vec<_> = self.inner.db.shared.specs.values().flat_map(|spec| spec.changes.iter().filter(|read| read.area == area).map(|read| (spec.key.to_string(), read.clone()))).collect();
            if targets.is_empty() { locked(&self.status).rejected += 1; }
            for (model, read) in targets {
                if read.removes_on.iter().any(|removal| removal == kind) {
                    let Ok(id) = RowId::new(id) else {
                        locked(&self.status).rejected += 1;
                        continue;
                    };
                    self.apply(vec![Pending { bucket: String::new(), id, node: Map::new(), generation: self.inner.db.generation() }], &model, EventEffect::Destroy).await;
                    continue;
                }
                let spec = match self.inner.db.spec(&model) { Ok(spec) => spec, Err(_) => continue };
                let Some(relation) = spec.relations.iter().find(|relation| relation.name == read.relation) else { continue };
                let parameter = if relation.node == Some(crate::schema::NodeRead::One) { Some(crate::schema::NODE_PARAM) } else { read.parameter.as_deref() };
                let params = parameter.map(|name| Map::from_iter([(name.to_string(), json!(id))])).unwrap_or_default();
                let result = match self.inner.resolve(&model, &read.relation, &params) {
                    Ok(resolved) => self.inner.fetch(&resolved, None, true).await.map(|_| ()),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => locked(&self.status).applied += 1,
                    Err(_) => locked(&self.status).rejected += 1,
                }
            }
        }
        true
    }

    fn parse(&self, response: &Response) -> Option<Pending> {
        let node = root_value(response.data.as_ref()?, &self.root_path)?.as_object()?;
        let id = match node.get(self.def.id.as_deref().unwrap_or("id")) {
            None | Some(Value::Null) if self.inner.db.spec(&self.def.model).is_ok_and(|spec| spec.is_memory()) => RowId::new(&uuid::Uuid::new_v4().to_string()).ok()?,
            Some(value) => read_id(value)?,
            None => return None,
        };
        let mut node = node.clone();
        node.insert("id".to_string(), Value::String(id.to_string()));
        Some(Pending { bucket: self.def.event.bucket_key(id.as_str()), id, node, generation: self.inner.db.generation() })
    }

    async fn apply(&self, mut items: Vec<Pending>, model: &str, effect: EventEffect) {
        let current = self.inner.db.generation();
        items.retain(|item| item.generation == current);
        if items.is_empty() {
            return;
        }
        match self.try_apply(items, current, model, effect) {
            Ok(stale) => {
                for model in stale {
                    let _ = self.inner.reload_lists_of_absent_parent(&model).await;
                }
            }
            Err(_) => locked(&self.status).rejected += 1,
        }
    }

    fn try_apply(&self, items: Vec<Pending>, generation: Generation, model: &str, effect: EventEffect) -> Result<Vec<String>, SyncError> {
        let db = &self.inner.db;
        let spec = db.spec(model)?;
        let items = items.into_iter().map(|item| {
            let deleted = self.def.deleted_by.as_ref().and_then(|field| item.node.get(field)).is_some_and(is_truthy);
            let normalized = if deleted || effect == EventEffect::Destroy {
                None
            } else {
                Some(normalize_nodes(db, model, std::slice::from_ref(&item.node))?)
            };
            Ok::<_, SyncError>((item, normalized))
        }).collect::<Result<Vec<_>, _>>()?;
        let mut count = 0;
        let mut announced: Vec<RowId> = Vec::new();
        let mut held_files: Vec<String> = Vec::new();
        let committed = db.commit_following_report(generation, |reader| {
            let mut operations: BTreeMap<(String, String), EntityOp> = BTreeMap::new();
            let mut touches: Vec<EntityOp> = Vec::new();
            let mut joins: Vec<ScopeOp> = Vec::new();
            let mut destroyed: BTreeSet<(String, String)> = BTreeSet::new();
            for (item, normalized) in items {
                let key = (model.to_string(), item.id.as_str().to_string());
                if normalized.is_some() && !self.def.set.is_empty() {
                    announced.push(item.id.clone());
                }
                if let Some(normalized) = normalized {
                    let stored = reader.point(model, item.id.as_str())?;
                    if stored.is_none() && effect == EventEffect::Patch {
                        continue;
                    }
                    let Some(patch) = normalized.roots.first() else { continue };
                    let Some(row) = db.merged(model, stored.as_ref(), patch)? else { continue };
                    let own = RowRef::new(ModelKey::new(model), item.id.clone());
                    let seen: Vec<&Map<String, Value>> = std::iter::once(&row).chain(stored.as_ref()).collect();
                    touches.extend(db.touch_ops(reader, spec, &seen, &own)?);
                    for (related_model, patch) in normalized.related {
                        let Some(row) = merge_stored(db, reader, related_model.as_str(), &patch)? else { continue };
                        let Some(id) = row.get("id").and_then(read_id) else { continue };
                        operations.insert((related_model.to_string(), id.to_string()), EntityOp::Upsert { model: related_model, id, row });
                    }
                    if stored.is_none() {
                        joins.extend(self.inner.joins_of(reader, model, &item.id, &row)?);
                    }
                    operations.insert(key, EntityOp::Upsert { model: ModelKey::new(model), id: item.id, row });
                } else {
                    let own = RowRef::new(ModelKey::new(model), item.id);
                    for target in db.destroy_targets(reader, &own, WriteOrigin::Event)? {
                        destroyed.insert((target.model.to_string(), target.id.to_string()));
                        operations.insert((target.model.to_string(), target.id.to_string()), EntityOp::Destroy { model: target.model, id: target.id, reason: DestroyReason::Server });
                    }
                }
                count += 1;
            }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
            envelope.entity_ops = operations.into_values().chain(touches).collect();
            envelope.scope_ops = joins;
            if !destroyed.is_empty() {
                for entry in read_outbox(reader)? {
                    let keys = operation_keys(&entry.operation.action_key, &entry.operation.row_ids, entry.operation.temp_id.as_ref());
                    if !keys.iter().any(|key| destroyed.contains(key)) {
                        continue;
                    }
                    envelope.operation_transitions.push(OperationTransition::Remove { seq: entry.operation.id.0, expected: None });
                    for file in Prior::of(&entry).map(|prior| prior.held_files()).unwrap_or_default() {
                        let Ok(row_id) = RowId::new(&file.row_id) else { continue };
                        envelope.file_ops.push(FileOp::Remove { model: ModelKey::new(&file.model), row_id, field: file.field });
                        held_files.push(file.id);
                    }
                }
            }
            Ok::<_, SyncError>(Some(envelope))
        });
        let followed = match committed {
            Ok((_, followed)) => followed,
            Err(PlanCommitError::Plan(error)) => return Err(error),
            Err(PlanCommitError::Store(error)) => return Err(error.into()),
        };
        db.shared.files.discard(&held_files);
        for id in &announced {
            db.set_ephemeral(model, id, self.def.set.clone())?;
        }
        locked(&self.status).applied += count;
        let stale: Vec<String> = followed.absent.iter().map(ToString::to_string).collect();
        Ok(stale)
    }

    async fn pump(&self, connection: &mut Subscription, retry: &mut SubscriptionRetry) -> Option<SubscriptionFailureKind> {
        let debounce = self.def.event.debounce_interval.map(|interval| Debounce::new(interval, |item: &Pending| item.bucket.clone()));
        let mut pending: Vec<Pending> = Vec::new();
        let mut deadline: Option<Instant> = None;
        let mut ready = false;
        loop {
            let wake = deadline.unwrap_or_else(Instant::now);
            tokio::select! {
                event = connection.next() => match event {
                    None => {
                        self.apply(std::mem::take(&mut pending), &self.def.model, self.def.effect).await;
                        return None;
                    }
                    Some(SubscriptionEvent::Ready) => {
                        if ready {
                            let models: Vec<_> = self.inner.db.shared.specs.values().filter(|spec| !spec.changes.is_empty() || spec.key.as_str() == self.def.model).map(|spec| spec.key.to_string()).collect();
                            for model in models { self.inner.reload_watched(&model, None, false).await; }
                        }
                        ready = true;
                        *retry = retry.confirmed();
                        locked(&self.status).degraded = false;
                    }
                    Some(SubscriptionEvent::Next(response)) => {
                        if self.invalidation(&response).await { continue; }
                        let Some(item) = self.parse(&response) else { continue };
                        match &debounce {
                            Some(debounce) => {
                                pending = debounce.coalesce(std::mem::take(&mut pending), item);
                                deadline.get_or_insert_with(|| Instant::now() + debounce.interval);
                            }
                            None => self.apply(vec![item], &self.def.model, self.def.effect).await,
                        }
                    }
                    Some(SubscriptionEvent::Failed(failure)) => {
                        self.notify_failure(&failure);
                        self.apply(std::mem::take(&mut pending), &self.def.model, self.def.effect).await;
                        return Some(failure.kind);
                    }
                },
                _ = tokio::time::sleep_until(wake), if deadline.is_some() => {
                    self.apply(std::mem::take(&mut pending), &self.def.model, self.def.effect).await;
                    deadline = None;
                }
            }
        }
    }

    fn register_failure(&self, retry: &mut SubscriptionRetry, fault: SubscriptionFault) -> Option<Duration> {
        let outcome = retry.record_failure(fault);
        *retry = outcome.state;
        let mut status = locked(&self.status);
        status.degraded |= outcome.reports_degraded;
        if fault == SubscriptionFault::Transient {
            return Some(RETRY_BACKOFF.delay(retry.attempts.saturating_sub(1)));
        }
        status.stopped = true;
        None
    }

    async fn run(self, request: GraphQLRequest, first: Result<Subscription, TransportError>) {
        let mut retry = SubscriptionRetry::default();
        let mut opened = Some(first);
        loop {
            let attempt = match opened.take() {
                Some(attempt) => attempt,
                None if !self.inner.connectivity.is_online() => {
                    self.inner.connectivity.online().await;
                    continue;
                }
                None => self.inner.transport.subscribe(request.clone()).await,
            };
            let mut connection = match attempt {
                Ok(connection) => {
                    let reconnected = {
                        let mut status = locked(&self.status);
                        status.connects += 1;
                        status.connects > 1
                    };
                    if reconnected {
                        self.inner.reload_watched(&self.def.model, None, false).await;
                    }
                    connection
                }
                Err(error) => {
                    let failure = SubscriptionFailure::from_error(error);
                    let fault = failure.kind.fault();
                    if fault == SubscriptionFault::Transient && !self.inner.connectivity.is_online() { continue; }
                    self.notify_failure(&failure);
                    let Some(delay) = self.register_failure(&mut retry, fault) else { return };
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };
            let fault = self.pump(&mut connection, &mut retry).await.map_or(SubscriptionFault::Transient, SubscriptionFailureKind::fault);
            drop(connection);
            if fault != SubscriptionFault::Transient {
                self.register_failure(&mut retry, fault);
                return;
            }
            if !self.inner.connectivity.is_online() {
                continue;
            }
            let Some(delay) = self.register_failure(&mut retry, fault) else { return };
            tokio::time::sleep(delay).await;
        }
    }
}

impl Sync {
    pub async fn subscribe(&self, def: SubscriptionDef) -> Result<EventSubscription, SyncError> {
        self.inner.subscribe(def).await
    }
}

impl Inner {
    pub(crate) async fn subscribe(self: &Arc<Self>, def: SubscriptionDef) -> Result<EventSubscription, SyncError> {
        if let Some(fault) = def.event.fault() {
            return Err(SyncError::Event(format!("{fault:?}")));
        }
        self.db.spec(&def.model).map_err(|_| SyncError::UnknownModel(def.model.clone()))?;
        let operation = locked(&self.operations)
            .get(&def.operation)
            .cloned()
            .ok_or_else(|| SyncError::UnknownOperation(def.operation.clone()))?;
        let request = GraphQLRequest::new(&operation.document, &operation.name, def.variables.clone());
        let first = self.transport.subscribe(request.clone()).await;
        let status = Arc::new(Mutex::new(SubscriptionStatus::default()));
        let definition = def.clone();
        let def_root = def.root.clone();
        let feed = Feed { inner: self.clone(), def, root_path: def_root.unwrap_or(operation.root_path), status: status.clone() };
        let task = tokio::spawn(self.until_closed(feed.run(request, first)));
        Ok(EventSubscription { abort: task.abort_handle(), status, definition })
    }
}

impl Inner {
    fn joins_of(&self, reader: &crate::store::Reader<'_>, model: &str, id: &RowId, row: &Map<String, Value>) -> Result<Vec<ScopeOp>, SyncError> {
        let spec = self.db.spec(model)?;
        let mut joins = Vec::new();
        for relation in spec.relations.iter().filter(|relation| relation.joins_events && relation.remote.is_some() && relation.text_params.is_empty()) {
            let params: Map<String, Value> = relation.by_columns.iter().filter_map(|column| row.get(column).filter(|value| !value.is_null()).map(|value| (column.clone(), value.clone()))).collect();
            if params.len() != relation.by_columns.len() {
                continue;
            }
            let Ok(resolved) = self.resolve(model, &relation.name, &params) else { continue };
            let loaded = rusqlite::OptionalExtension::optional(reader.connection().query_row("SELECT 1 FROM query_window WHERE key = ?1", [&resolved.window_key], |row| row.get::<_, i64>(0)))
                .map_err(|error| SyncError::Event(error.to_string()))?;
            if loaded.is_none() {
                continue;
            }
            if reader.scope_has_member(model, resolved.scope.key.as_str(), id.as_str())? {
                continue;
            }
            let last = reader.scope_last_key(model, resolved.scope.key.as_str())?;
            let order_key = fractional_between(last.as_deref(), None).map_err(|error| SyncError::Event(error.to_string()))?;
            joins.push(ScopeOp::Append { scope: resolved.scope, row: id.clone(), order_key });
        }
        Ok(joins)
    }
}
