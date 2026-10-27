use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::task::AbortHandle;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, FileOp, OperationTransition, ScopeOp, WriteOrigin};
use crate::db::{read_outbox, Prior};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::pacing::{Backoff, Debounce, RetryPolicy};
use crate::store::PlanCommitError;
use crate::syncstate::operation_keys;
use crate::transport::{
    EventSpec, GraphQLRequest, Response, ServerSubscription, SubscriptionEvent, SubscriptionFailureKind, SubscriptionFailure,
    TransportError,
};
use crate::value::read_id;

use super::error::SyncError;
use super::extract::root_value;
use super::normalize::normalize_nodes;
use super::{locked, Inner, Sync};

mod machine;
use machine::{Event, Lifecycle, Step};

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
    pub deleted_unless: Option<String>,
    pub set: Map<String, Value>,
    pub root: Option<String>,
    pub id: Vec<String>,
    pub apply: Option<crate::schema::SubscriptionApply>,
}

impl SubscriptionDef {
    pub fn new(event: EventSpec, operation: &str, model: &str, effect: EventEffect) -> Self {
        Self { event, operation: operation.to_string(), model: model.to_string(), effect, variables: json!({}), deleted_by: None, deleted_unless: None, set: Map::new(), root: None, id: Vec::new(), apply: None }
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

pub struct Updates {
    abort: AbortHandle,
    status: SharedStatus,
    pub(crate) definition: SubscriptionDef,
}

impl Updates {
    pub fn status(&self) -> SubscriptionStatus {
        locked(&self.status).clone()
    }

    pub fn cancel(&self) {
        self.abort.abort();
    }
}

impl Drop for Updates {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

struct Pending {
    definition: Arc<SubscriptionDef>,
    bucket: String,
    id: RowId,
    node: Map<String, Value>,
    generation: Generation,
}

struct Feed {
    inner: Arc<Inner>,
    def: Arc<SubscriptionDef>,
    recipients: Vec<Feed>,
    root_path: String,
    status: SharedStatus,
    backoff: Backoff,
    ready: Option<tokio::sync::watch::Sender<Option<Result<(), SyncError>>>>,
}

impl Feed {
    fn opening(&self, result: Result<(), SyncError>) {
        if let Some(ready) = &self.ready { ready.send_replace(Some(result)); }
    }

    fn notify_failure(&self, failure: &SubscriptionFailure) {
        self.opening(Err(SyncError::Transport(failure.error.clone())));
        locked(&self.status).failure = Some(failure.clone());
        if matches!(failure.error, TransportError::Session { .. }) {
            let _ = self.inner.subscription_failures.send(Some((super::live::live_key(&self.def.operation, &self.def.variables), failure.clone())));
        }
    }

    async fn invalidation(&self, response: &Response) -> bool {
        let Some(data) = response.data.as_ref() else { return false };
        let Some((_, feed)) = self.inner.change_feed().filter(|(_, feed)| feed.subscription == self.def.operation) else { return false };
        let Some(changes) = root_value(data, &feed.root).and_then(Value::as_array) else {
            locked(&self.status).rejected += 1;
            return true;
        };
        for change in changes {
            let (Some(area), Some(id), Some(kind)) = (root_value(&change, &feed.area).and_then(Value::as_str), root_value(&change, &feed.id).and_then(Value::as_str), root_value(&change, &feed.kind).and_then(Value::as_str)) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            let mut targets: BTreeMap<String, Vec<crate::schema::ChangeRead>> = BTreeMap::new();
            for spec in self.inner.db.shared.specs.values() {
                let reads: Vec<_> = spec.changes.iter().filter(|read| read.area == area).cloned().collect();
                if !reads.is_empty() { targets.insert(spec.key.to_string(), reads); }
            }
            if targets.is_empty() {
                locked(&self.status).rejected += 1;
                continue;
            }
            let Ok(row_id) = RowId::new(id) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            let derived: Vec<(String, crate::schema::ChangeRead)> = targets.iter().flat_map(|(model, reads)| reads.iter().filter(|read| read.id_of.is_some()).map(|read| (model.clone(), read.clone()))).collect();
            let sources_before: Vec<Option<RowId>> = derived.iter().map(|(_, read)| self.source_id(read, &row_id)).collect();
            let mut settled: BTreeMap<String, bool> = BTreeMap::new();
            for (model, reads) in &targets {
                let direct: Vec<_> = reads.iter().filter(|read| read.id_of.is_none()).cloned().collect();
                if direct.is_empty() { continue; }
                let ok = self.reread_target(model, &direct, area, kind, id, &row_id).await;
                settled.insert(model.clone(), ok);
            }
            for ((model, read), before) in derived.iter().zip(sources_before) {
                let Some(source) = read.id_of.as_ref() else { continue };
                if settled.get(&source.model) != Some(&true) {
                    locked(&self.status).rejected += 1;
                    continue;
                }
                let mut ids: Vec<RowId> = self.source_id(read, &row_id).into_iter().chain(before).collect();
                ids.dedup();
                if ids.is_empty() {
                    locked(&self.status).rejected += 1;
                    continue;
                }
                for target_id in ids {
                    self.reread_target(model, std::slice::from_ref(&crate::schema::ChangeRead { id_of: None, ..read.clone() }), area, kind, target_id.as_str(), &target_id).await;
                }
            }
        }
        true
    }

    fn source_id(&self, read: &crate::schema::ChangeRead, event_id: &RowId) -> Option<RowId> {
        let source = read.id_of.as_ref()?;
        let row = self.inner.db.find(&source.model, event_id).ok()??;
        row.get(&source.field).and_then(read_id)
    }

    async fn reread_target(&self, model: &str, reads: &[crate::schema::ChangeRead], area: &str, kind: &str, id: &str, row_id: &RowId) -> bool {
        let watched = self.inner.has_event_readers(model);
        let before = watched.then(|| self.inner.db.find(model, row_id));
        if reads.iter().any(|read| read.removes_on.iter().any(|removal| removal == kind)) {
            let definition = Arc::new(SubscriptionDef { model: model.to_string(), effect: EventEffect::Destroy, ..(*self.def).clone() });
            let removed = self.apply(vec![Pending { definition, bucket: String::new(), id: row_id.clone(), node: Map::new(), generation: self.inner.db.generation() }], model).await;
            if let Some(before) = before {
                if !removed {
                    self.inner.publish_model_gap(model);
                } else if !self.inner.publish_rows(model, area, kind, id, before, Ok(None)) {
                    locked(&self.status).rejected += 1;
                }
            }
            return removed;
        }
        let mut result = Ok(());
        for read in reads {
            let outcome = if read.observed { self.reread_observed(model).await } else { self.reread(model, &read.relation, read.parameter.as_deref(), Some(id)).await };
            if let (Err(error), Ok(())) = (outcome, &result) { result = Err(error); }
        }
        match result {
            Ok(()) => {
                locked(&self.status).applied += 1;
                if let Some(before) = before {
                    let after = self.inner.db.find(model, row_id);
                    if !self.inner.publish_rows(model, area, kind, id, before, after) { locked(&self.status).rejected += 1; }
                }
                true
            }
            Err(_) => {
                locked(&self.status).rejected += 1;
                if before.is_some() { self.inner.publish_model_gap(model); }
                false
            }
        }
    }

    async fn reread_observed(&self, model: &str) -> Result<(), SyncError> {
        let mut targets: BTreeMap<String, (String, Map<String, Value>)> = BTreeMap::new();
        for entry in self.inner.db.observed_reads().into_iter().filter(|entry| entry.model == model) {
            targets.insert(entry.key, (entry.relation, entry.params));
        }
        let mut failure = None;
        for (relation, params) in targets.into_values() {
            let outcome = match self.inner.resolve(model, &relation, &params) {
                Ok(resolved) => self.inner.load_scope(&resolved, true).await.map(|_| ()),
                Err(error) => Err(error),
            };
            if let Err(error) = outcome { failure.get_or_insert(error); }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn reread(&self, model: &str, name: &str, parameter: Option<&str>, id: Option<&str>) -> Result<(), SyncError> {
        let spec = self.inner.db.spec(model)?;
        let relation = spec.relations.iter().find(|relation| relation.name == name).ok_or_else(|| SyncError::Event(format!("unknown read {name}")))?;
        let parameter = if relation.node == Some(crate::schema::NodeRead::One) { Some(crate::schema::NODE_PARAM) } else { parameter.or_else(|| relation.text_params.iter().chain(&relation.by_columns).next().map(String::as_str)) };
        let params = match parameter {
            Some(parameter) => Map::from_iter([(parameter.to_string(), json!(id.ok_or_else(|| SyncError::Event("missing event id".into()))?))]),
            None => Map::new(),
        };
        let mut resolved = self.inner.resolve(model, name, &params)?;
        resolved.node_reread = relation.node == Some(crate::schema::NodeRead::One);
        self.inner.load_scope(&resolved, true).await.map(|_| ())
    }

    async fn apply_event(&self, response: &Response) -> bool {
        use crate::schema::SubscriptionAction;
        let Some(apply) = &self.def.apply else { return false };
        if matches!(apply.action, SubscriptionAction::Upsert | SubscriptionAction::Delete) { return false; }
        if matches!(apply.action, SubscriptionAction::Ignore) { return true; }
        let Some(node) = response.data.as_ref().and_then(|data| root_value(data, &self.root_path)) else {
            locked(&self.status).rejected += 1;
            return true;
        };
        if !self.matches(node) { return true; }
        let id = self.event_id(node);
        let result = match &apply.action {
            SubscriptionAction::Ephemeral { fields } => {
                let values: Option<Map<String, Value>> = fields.iter().map(|(name, path)| root_value(node, path).cloned().map(|value| (name.clone(), value))).collect();
                match (id.as_ref(), values) {
                    (Some(id), Some(values)) => self.inner.db.set_ephemeral(&self.def.model, id, values).map_err(SyncError::from),
                    _ => Err(SyncError::Event("missing event id or fields".into())),
                }
            }
            SubscriptionAction::Reread { via } => self.reread(&self.def.model, via, None, id.as_ref().map(RowId::as_str)).await,
            SubscriptionAction::Ignore => Ok(()),
            SubscriptionAction::Upsert | SubscriptionAction::Delete => return false,
        };
        match result {
            Ok(()) => locked(&self.status).applied += 1,
            Err(_) => locked(&self.status).rejected += 1,
        }
        true
    }

    fn matches(&self, node: &Value) -> bool {
        self.def.apply.as_ref().is_none_or(|apply| apply.matches.iter().all(|(path, predicate)| root_value(node, path).is_some_and(|value| predicate.accepts(value))))
    }

    fn event_id(&self, node: &Value) -> Option<RowId> {
        match self.def.id.as_slice() {
            [] => root_value(node, "id").and_then(read_id),
            [path] => root_value(node, path).and_then(read_id),
            paths => {
                let parts: Option<Vec<RowId>> = paths.iter().map(|path| root_value(node, path).and_then(read_id)).collect();
                RowId::new(&parts?.iter().map(RowId::as_str).collect::<Vec<_>>().join(":")).ok()
            }
        }
    }

    fn parse(&self, response: &Response) -> Option<Pending> {
        let node = root_value(response.data.as_ref()?, &self.root_path)?;
        if !self.matches(node) { return None; }
        let id = match self.event_id(node) {
            Some(id) => id,
            None if self.def.id.len() <= 1 && matches!(root_value(node, self.def.id.first().map_or("id", String::as_str)), None | Some(Value::Null)) && self.inner.db.spec(&self.def.model).is_ok_and(|spec| spec.is_memory()) => RowId::new(&uuid::Uuid::new_v4().to_string()).ok()?,
            None => return None,
        };
        let mut node = node.as_object()?.clone();
        node.insert("id".to_string(), Value::String(id.to_string()));
        Some(Pending { definition: self.def.clone(), bucket: format!("{}|{}", self.def.model, self.def.event.bucket_key(id.as_str())), id, node, generation: self.inner.db.generation() })
    }

    async fn apply(&self, mut items: Vec<Pending>, model: &str) -> bool {
        let current = self.inner.db.generation();
        items.retain(|item| item.generation == current);
        if items.is_empty() {
            return false;
        }
        let applied = match self.try_apply(items, current) {
            Ok((stale, held_files)) => self.inner.discard_files(held_files).await.map(|()| stale),
            Err(error) => Err(error),
        };
        match applied {
            Ok(stale) => {
                for model in stale {
                    let _ = self.inner.reload_lists_of_absent_parent(&model).await;
                }
                true
            }
            Err(_) => {
                locked(&self.status).rejected += 1;
                if self.reread_observed(model).await.is_err() {
                    self.inner.publish_model_gap(model);
                }
                false
            }
        }
    }

    fn try_apply(&self, items: Vec<Pending>, generation: Generation) -> Result<(Vec<String>, Vec<String>), SyncError> {
        let db = &self.inner.db;
        let items = items.into_iter().map(|item| {
            let def = &item.definition;
            let payload = Value::Object(item.node.clone());
            let deleted = def.deleted_by.as_ref().and_then(|field| root_value(&payload, field)).is_some_and(is_truthy)
                || def.deleted_unless.as_ref().and_then(|field| root_value(&payload, field)).is_some_and(|value| !is_truthy(value));
            let normalized = if deleted || def.effect == EventEffect::Destroy {
                None
            } else {
                Some(normalize_nodes(db, &def.model, std::slice::from_ref(&item.node))?)
            };
            Ok::<_, SyncError>((item, normalized))
        }).collect::<Result<Vec<_>, _>>()?;
        let change_feed = self.inner.change_feed().is_some_and(|(_, feed)| feed.subscription == self.def.operation);
        let before = items.iter().filter(|(item, _)| !change_feed && self.inner.has_event_readers(&item.definition.model)).map(|(item, _)| {
            let def = &item.definition;
            (def.model.clone(), item.id.clone(), item.node.get("kind").and_then(Value::as_str).unwrap_or(if def.effect == EventEffect::Destroy { "DELETED" } else { "UPDATED" }).to_string(), db.find(&def.model, &item.id))
        }).collect::<Vec<_>>();
        let mut count = 0;
        let mut announced: Vec<(String, RowId, Map<String, Value>)> = Vec::new();
        let mut held_files: Vec<String> = Vec::new();
        let committed = db.commit_following_report(generation, |reader| {
            let mut operations: BTreeMap<(String, String), EntityOp> = BTreeMap::new();
            let mut touches: Vec<EntityOp> = Vec::new();
            let mut joins: Vec<ScopeOp> = Vec::new();
            let mut destroyed: BTreeSet<(String, String)> = BTreeSet::new();
            count = 0;
            announced.clear();
            held_files.clear();
            for (item, normalized) in &items {
                let def = &item.definition;
                let model = def.model.as_str();
                let spec = db.spec(model)?;
                let key = (model.to_string(), item.id.as_str().to_string());
                if normalized.is_some() && !def.set.is_empty() {
                    announced.push((model.to_string(), item.id.clone(), def.set.clone()));
                }
                if let Some(normalized) = normalized {
                    let stored = reader.point(model, item.id.as_str())?;
                    if stored.is_none() && def.effect == EventEffect::Patch {
                        continue;
                    }
                    let Some(patch) = normalized.roots.first() else { continue };
                    let Some(row) = db.admitted(reader, WriteOrigin::Event, None, None, model, patch, &mut 0)? else { continue };
                    let own = RowRef::new(ModelKey::new(model), item.id.clone());
                    let seen: Vec<&Map<String, Value>> = std::iter::once(&row).chain(stored.as_ref()).collect();
                    touches.extend(db.touch_ops(reader, spec, &seen, &own)?);
                    for (related_model, patch) in &normalized.related {
                        let Some(row) = db.admitted(reader, WriteOrigin::Event, None, None, related_model.as_str(), patch, &mut 0)? else { continue };
                        let Some(id) = row.get("id").and_then(read_id) else { continue };
                        operations.insert((related_model.to_string(), id.to_string()), EntityOp::Upsert { model: related_model.clone(), id, row });
                    }
                    joins.extend(self.inner.joins_of(reader, model, &item.id, &row)?);
                    operations.insert(key, EntityOp::Upsert { model: ModelKey::new(model), id: item.id.clone(), row });
                } else {
                    let own = RowRef::new(ModelKey::new(model), item.id.clone());
                    for target in db.destroy_targets(reader, &own, WriteOrigin::Event)? {
                        destroyed.insert((target.model.to_string(), target.id.to_string()));
                        operations.insert((target.model.to_string(), target.id.to_string()), EntityOp::Destroy { model: target.model, id: target.id, reason: DestroyReason::Server });
                    }
                }
                count += 1;
            }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
            let mut written: BTreeSet<(String, String)> = operations.keys().cloned().collect();
            let touches = touches.into_iter().filter(|touch| {
                let row = touch.row_ref();
                written.insert((row.model.to_string(), row.id.to_string()))
            });
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
        for (model, id, values) in &announced {
            db.set_ephemeral(model, id, values.clone())?;
        }
        for (model, id, kind, before) in before {
            self.inner.publish_rows(&model, &self.def.operation, &kind, id.as_str(), before, db.find(&model, &id));
        }
        locked(&self.status).applied += count;
        let stale: Vec<String> = followed.absent.iter().map(ToString::to_string).collect();
        Ok((stale, held_files))
    }

    fn publish(&self, lifecycle: &Lifecycle) {
        let mut status = locked(&self.status);
        status.degraded = lifecycle.degraded();
        status.stopped = lifecycle.stopped();
    }

    async fn pump(&self, connection: &mut ServerSubscription, lifecycle: &mut Lifecycle) -> SubscriptionFailureKind {
        let debounce = self.def.event.debounce_interval.map(|interval| Debounce::new(interval, |item: &Pending| item.bucket.clone()));
        let mut pending: Vec<Pending> = Vec::new();
        let mut deadline: Option<Duration> = None;
        loop {
            let wait = deadline.unwrap_or_default().saturating_sub(self.inner.clock.now());
            tokio::select! {
                event = connection.next() => match event {
                    None => {
                        self.opening(Err(SyncError::Event("subscription closed".into())));
                        self.apply(std::mem::take(&mut pending), &self.def.model).await;
                        return SubscriptionFailureKind::Transport;
                    }
                    Some(SubscriptionEvent::Ready) => {
                        self.opening(Ok(()));
                        lifecycle.handle(Event::Ready);
                        if lifecycle.take_reload() {
                            let models: Vec<_> = self.inner.db.shared.specs.values().filter(|spec| !spec.changes.is_empty() || spec.key.as_str() == self.def.model).map(|spec| spec.key.to_string()).collect();
                            for model in models { self.inner.reload_watched(&model, None, false).await; }
                        }
                    }
                    Some(SubscriptionEvent::Next(response)) => {
                        lifecycle.handle(Event::Next);
                        self.publish(lifecycle);
                        if self.invalidation(&response).await { continue; }
                        let mut items = Vec::new();
                        for feed in &self.recipients {
                            if !feed.apply_event(&response).await {
                                if let Some(item) = feed.parse(&response) { items.push(item); }
                            }
                        }
                        if items.is_empty() { continue; }
                        match &debounce {
                            Some(debounce) => {
                                for item in items { pending = debounce.coalesce(std::mem::take(&mut pending), item); }
                                deadline.get_or_insert_with(|| self.inner.clock.now() + debounce.interval);
                            }
                            None => { self.apply(items, &self.def.model).await; }
                        }
                    }
                    Some(SubscriptionEvent::Failed(failure)) => {
                        self.notify_failure(&failure);
                        self.apply(std::mem::take(&mut pending), &self.def.model).await;
                        return failure.kind;
                    }
                },
                _ = self.inner.clock.sleep(wait), if deadline.is_some() => {
                    self.apply(std::mem::take(&mut pending), &self.def.model).await;
                    deadline = None;
                }
            }
        }
    }

    async fn run(self, request: GraphQLRequest, first: Option<Result<ServerSubscription, TransportError>>) {
        let mut lifecycle = Lifecycle::new(self.backoff);
        let mut opened = first;
        loop {
            lifecycle.handle(Event::Resumed);
            let attempt = match opened.take() {
                Some(attempt) => attempt,
                None => match self.inner.open_subscription(request.clone()).await {
                    Some(attempt) => attempt,
                    None => return,
                },
            };
            let kind = match attempt {
                Ok(mut connection) => {
                    let reconnected = {
                        let mut status = locked(&self.status);
                        status.connects += 1;
                        status.connects > 1
                    };
                    if reconnected {
                        let models: BTreeSet<_> = self.recipients.iter().map(|feed| feed.def.model.as_str()).collect();
                        for model in models { self.inner.reload_watched(model, None, false).await; }
                    }
                    lifecycle.handle(Event::Connected);
                    self.pump(&mut connection, &mut lifecycle).await
                }
                Err(error) => {
                    let failure = SubscriptionFailure::from_error(error);
                    if failure.kind != SubscriptionFailureKind::Transport || self.inner.connectivity.is_online() {
                        self.notify_failure(&failure);
                    }
                    failure.kind
                }
            };
            lifecycle.handle(Event::Lost { kind, online: self.inner.connectivity.is_online() });
            self.publish(&lifecycle);
            match lifecycle.step() {
                Step::Stop => return,
                Step::Wait(delay) => self.inner.connectivity.wait(self.inner.clock.as_ref(), delay).await,
                Step::Open => {}
            }
        }
    }
}

impl Sync {
    pub async fn subscribe(&self, def: SubscriptionDef) -> Result<Updates, SyncError> {
        self.inner.subscribe(def, None).await
    }
}

impl Inner {
    async fn open_subscription(&self, request: GraphQLRequest) -> Option<Result<ServerSubscription, TransportError>> {
        let mut stopped = self.stopped.clone();
        tokio::select! {
            biased;
            _ = stopped.changed() => None,
            result = async {
                self.connectivity.online().await;
                self.transport.subscribe(request).await
            } => Some(result),
        }
    }

    pub(crate) async fn subscribe(self: &Arc<Self>, def: SubscriptionDef, ready: Option<tokio::sync::watch::Sender<Option<Result<(), SyncError>>>>) -> Result<Updates, SyncError> {
        if let Some(fault) = def.event.fault() {
            return Err(SyncError::Event(format!("{fault:?}")));
        }
        self.db.spec(&def.model).map_err(|_| SyncError::UnknownModel(def.model.clone()))?;
        let operation = locked(&self.operations)
            .get(&def.operation)
            .cloned()
            .ok_or_else(|| SyncError::UnknownOperation(def.operation.clone()))?;
        let request = GraphQLRequest::new(&operation.document, &operation.name, def.variables.clone());
        let first = if self.connectivity.is_online() { self.open_subscription(request.clone()).await } else { None };
        let status = Arc::new(Mutex::new(SubscriptionStatus::default()));
        let definition = def.clone();
        let def_root = def.root.clone();
        let mut recipients = Vec::new();
        if let Some(targets) = self.subscriptions.get(&def.operation) {
            for (model, updates) in targets {
                let mut target = SubscriptionDef::new(def.event.clone(), &def.operation, model, if updates.removes { EventEffect::Destroy } else { EventEffect::Upsert });
                target.variables = def.variables.clone();
                target.deleted_by = updates.deleted_by.clone();
                target.deleted_unless = updates.deleted_unless.clone();
                target.set = updates.set.clone();
                target.root = updates.root.clone();
                target.id = updates.id.clone();
                target.apply = updates.apply.clone();
                if !recipients.contains(&target) { recipients.push(target); }
            }
        }
        if recipients.is_empty() { recipients.push(def.clone()); }
        let backoff = RetryPolicy::Subscription.backoff().randomized();
        let root_path = def_root.unwrap_or_else(|| operation.root_path.clone());
        let recipients = recipients.into_iter().map(|target| Feed { inner: self.clone(), root_path: target.root.clone().unwrap_or_else(|| operation.root_path.clone()), def: Arc::new(target), recipients: Vec::new(), status: status.clone(), backoff, ready: None }).collect();
        let feed = Feed { inner: self.clone(), def: Arc::new(def), recipients, root_path, status: status.clone(), backoff, ready };
        let task = tokio::spawn(self.until_closed(feed.run(request, first)));
        Ok(Updates { abort: task.abort_handle(), status, definition })
    }
}
