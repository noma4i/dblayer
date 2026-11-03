use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::task::AbortHandle;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, FileOp, OperationTransition, ScopeOp, WriteOrigin};
use crate::db::{read_outbox, Admission, Prior};
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
use super::model_event::Heard;
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

fn validate_payload(fields: &[crate::schema::FieldSpec], input: &Map<String, Value>) -> Result<(), SyncError> {
    for field in fields.iter().filter(|field| !field.is_local()) {
        let Some(value) = field.read(input) else {
            if field.optional { continue; }
            return Err(SyncError::Event(format!("incomplete payload: missing {}", field.name)));
        };
        if value.is_null() && field.nullable { continue; }
        if field.decode(&value).is_none() {
            return Err(SyncError::Event(format!("invalid payload field {}", field.name)));
        }
        if field.fields.is_empty() { continue; }
        let values = match &value { Value::Array(values) => values.iter().collect(), other => vec![other] };
        for value in values {
            let object = value.as_object().ok_or_else(|| SyncError::Event(format!("invalid payload object {}", field.name)))?;
            if matches!(field.kind, crate::schema::FieldKind::Union | crate::schema::FieldKind::UnionArray) {
                let typename = object.get("__typename").and_then(Value::as_str);
                let variant = field.fields.iter().find(|variant| variant.app_type.as_deref() == typename)
                    .ok_or_else(|| SyncError::Event(format!("unknown payload variant {}", field.name)))?;
                validate_payload(&field.shared, object)?;
                validate_payload(&variant.fields, object)?;
            } else {
                validate_payload(&field.fields, object)?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionStatus {
    pub connects: u32,
    pub applied: usize,
    pub rejected: usize,
    pub degraded: bool,
    pub stopped: bool,
    pub connected: bool,
    pub opening: bool,
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
        crate::db::lock::effect(|| self.abort.abort());
    }

    pub(crate) fn reopening(&self) {
        let mut status = locked(&self.status);
        if !status.stopped { status.connected = false; status.opening = true; }
    }
}

impl Drop for Updates {
    fn drop(&mut self) {
        crate::db::lock::effect(|| self.abort.abort());
    }
}

#[derive(Clone, Copy)]
struct PayloadEvent<'a> {
    change: &'a Value,
    area: &'a str,
    kind: &'a str,
}

struct Pending {
    definition: Arc<SubscriptionDef>,
    bucket: String,
    id: RowId,
    node: Map<String, Value>,
    generation: Generation,
    stamp: crate::lifecycle::Stamp,
}

struct Feed {
    inner: Arc<Inner>,
    def: Arc<SubscriptionDef>,
    shared: bool,
    operation_root: String,
    root_path: String,
    status: SharedStatus,
    backoff: Backoff,
    ready: Option<tokio::sync::watch::Sender<Option<Result<(), SyncError>>>>,
    received: crate::lifecycle::Stamp,
}

impl Feed {
    fn recipients(&self, received: crate::lifecycle::Stamp) -> Vec<Feed> {
        let definitions = if self.shared { self.inner.live_definitions(&self.def.operation, &self.def.variables) } else { vec![(*self.def).clone()] };
        definitions.into_iter().map(|def| Feed {
            inner: self.inner.clone(), root_path: def.root.clone().unwrap_or_else(|| self.operation_root.clone()),
            def: Arc::new(def), shared: false, operation_root: self.operation_root.clone(),
            status: self.status.clone(), backoff: self.backoff, ready: None, received,
        }).collect()
    }

    fn mark(&self, change: impl FnOnce(&mut SubscriptionStatus)) {
        change(&mut locked(&self.status));
    }

    fn opening(&self, result: Result<(), SyncError>) {
        if let Some(ready) = &self.ready { crate::db::lock::effect(|| ready.send_replace(Some(result))); }
    }

    fn notify_failure(&self, failure: &SubscriptionFailure) {
        self.opening(Err(SyncError::Transport(failure.error.clone())));
        locked(&self.status).failure = Some(failure.clone());
        if matches!(failure.error, TransportError::Session { .. }) {
            crate::db::lock::effect(|| { let _ = self.inner.subscription_failures.send(Some((super::live::live_key(&self.def.operation, &self.def.variables), failure.clone()))); });
        }
    }

    async fn invalidation(&self, response: &Response, generation: Generation, received: crate::lifecycle::Stamp) -> bool {
        let Some(data) = response.data.as_ref() else { return false };
        let Some((_, feed)) = self.inner.change_feed().filter(|(_, feed)| feed.subscription == self.def.operation) else { return false };
        if generation != self.inner.db.generation() { return true; }
        let Some(changes) = root_value(data, &feed.root).and_then(Value::as_array) else {
            locked(&self.status).rejected += 1;
            return true;
        };
        for change in changes {
            let (Some(area), Some(id), Some(kind)) = (root_value(change, &feed.area).and_then(Value::as_str), root_value(change, &feed.id).and_then(Value::as_str), root_value(change, &feed.kind).and_then(Value::as_str)) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            let Some(targets) = self.inner.change_targets.get(area) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            let Ok(row_id) = RowId::new(id) else {
                locked(&self.status).rejected += 1;
                continue;
            };
            let derived: Vec<(String, crate::schema::ChangeRead)> = targets.iter().flat_map(|(model, reads)| reads.iter().filter(|read| read.id_of.is_some()).map(|read| (model.clone(), read.clone()))).collect();
            let sources_before = match derived.iter().map(|(_, read)| self.source_id(read, &row_id)).collect::<Result<Vec<_>, _>>() {
                Ok(sources) => sources,
                Err(_) => {
                    locked(&self.status).rejected += 1;
                    for (model, _) in &derived { self.inner.publish_model_gap(model); }
                    return false;
                }
            };
            let mut settled: BTreeMap<String, bool> = BTreeMap::new();
            for (model, reads) in targets {
                for read in reads.iter().filter(|read| read.payload.is_some()) {
                    let result = self.payload_target(model, read, PayloadEvent { change, area, kind }, &row_id, generation, received).await;
                    match result {
                        Ok(()) => { settled.insert(model.clone(), true); }
                        Err(_) => {
                            settled.insert(model.clone(), false);
                            locked(&self.status).rejected += 1;
                            self.inner.publish_model_gap(model);
                        }
                    }
                }
            }
            for (model, reads) in targets {
                if generation != self.inner.db.generation() { return true; }
                let direct: Vec<_> = reads.iter().filter(|read| read.id_of.is_none() && read.payload.is_none()).cloned().collect();
                if direct.is_empty() { continue; }
                let ok = self.reread_target(model, &direct, Heard { stamp: received, area, kind }, id, &row_id).await;
                settled.insert(model.clone(), ok);
            }
            for ((model, read), before) in derived.iter().zip(sources_before) {
                let Some(source) = read.id_of.as_ref() else { continue };
                if settled.get(&source.model) != Some(&true) {
                    locked(&self.status).rejected += 1;
                    continue;
                }
                let after = match self.source_id(read, &row_id) {
                    Ok(after) => after,
                    Err(_) => {
                        locked(&self.status).rejected += 1;
                        self.inner.publish_model_gap(model);
                        continue;
                    }
                };
                let mut ids: Vec<RowId> = after.into_iter().chain(before).collect();
                ids.dedup();
                if ids.is_empty() {
                    locked(&self.status).rejected += 1;
                    continue;
                }
                for target_id in ids {
                    self.reread_target(model, std::slice::from_ref(&crate::schema::ChangeRead { id_of: None, ..read.clone() }), Heard { stamp: received, area, kind }, target_id.as_str(), &target_id).await;
                }
            }
        }
        true
    }

    async fn payload_target(&self, model: &str, read: &crate::schema::ChangeRead, event: PayloadEvent<'_>, event_id: &RowId, generation: Generation, received: crate::lifecycle::Stamp) -> Result<(), SyncError> {
        let PayloadEvent { change, area, kind } = event;
        let terminal = read.removes_on.iter().any(|removal| removal == kind);
        let node = if terminal { Map::new() } else {
            let path = read.payload.as_deref().ok_or_else(|| SyncError::Event("missing payload path".into()))?;
            root_value(change, path).and_then(Value::as_object).cloned().ok_or_else(|| SyncError::Event(format!("missing full payload {model}.{path}")))?
        };
        let id = if terminal { event_id.clone() } else {
            node.get("id").and_then(read_id).ok_or_else(|| SyncError::Event(format!("missing payload id for {model}")))?
        };
        let before = self.inner.has_event_readers(model).then(|| self.inner.db.find(model, &id));
        if !terminal {
            let spec = self.inner.db.spec(model)?;
            let mut related = Vec::new();
            let input = super::normalize::nested_input(&self.inner.db, model, &node, &mut related)?;
            validate_payload(&spec.fields, &input)?;
        }
        let definition = Arc::new(SubscriptionDef { model: model.to_string(), effect: if terminal { EventEffect::Destroy } else { EventEffect::Upsert }, ..(*self.def).clone() });
        let (_, held_files) = self.try_apply(vec![Pending { definition, bucket: String::new(), id: id.clone(), node, generation, stamp: received }], generation)?;
        self.inner.discard_files(held_files).await?;
        if let Some(before) = before {
            if !self.inner.publish_rows(Heard { stamp: received, area, kind }, model, id.as_str(), before, self.inner.db.find(model, &id)) {
                return Err(SyncError::Event("payload event could not be published".into()));
            }
        }
        Ok(())
    }

    fn source_id(&self, read: &crate::schema::ChangeRead, event_id: &RowId) -> Result<Option<RowId>, SyncError> {
        let Some(source) = read.id_of.as_ref() else { return Ok(None) };
        Ok(self.inner.db.find(&source.model, event_id)?.and_then(|row| row.get(&source.field).and_then(read_id)))
    }

    async fn reread_target(&self, model: &str, reads: &[crate::schema::ChangeRead], heard: Heard<'_>, id: &str, row_id: &RowId) -> bool {
        let Heard { stamp: received, area, kind } = heard;
        let watched = self.inner.has_event_readers(model);
        let before = watched.then(|| self.inner.db.find(model, row_id));
        if reads.iter().any(|read| read.removes_on.iter().any(|removal| removal == kind)) {
            let definition = Arc::new(SubscriptionDef { model: model.to_string(), effect: EventEffect::Destroy, ..(*self.def).clone() });
            let removed = self.apply(vec![Pending { definition, bucket: String::new(), id: row_id.clone(), node: Map::new(), generation: self.inner.db.generation(), stamp: received }]).await;
            if let Some(before) = before {
                if !removed {
                    self.inner.publish_model_gap(model);
                } else if !self.inner.publish_rows(Heard { stamp: received, area, kind }, model, id, before, Ok(None)) {
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
                    if !self.inner.publish_rows(Heard { stamp: received, area, kind }, model, id, before, after) { locked(&self.status).rejected += 1; }
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
            let outcome = match self.inner.resolve(crate::lifecycle::Origin::Session, model, &relation, &params) {
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
        let mut resolved = self.inner.resolve(crate::lifecycle::Origin::Session, model, name, &params)?;
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
        let before = id.as_ref().filter(|_| self.inner.has_event_readers(&self.def.model)).map(|id| self.inner.db.find(&self.def.model, id));
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
            Ok(()) => {
                locked(&self.status).applied += 1;
                if let (Some(id), Some(before)) = (id.as_ref(), before) {
                    let kind = node.get("kind").and_then(Value::as_str).unwrap_or("UPDATED");
                    self.inner.publish_rows(Heard { stamp: self.received, area: &self.def.operation, kind }, &self.def.model, id.as_str(), before, self.inner.db.find(&self.def.model, id));
                }
            }
            Err(_) => {
                locked(&self.status).rejected += 1;
                if before.is_some() { self.inner.publish_model_gap(&self.def.model); }
            }
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
        Some(Pending { definition: self.def.clone(), bucket: format!("{}|{}", self.def.model, self.def.event.bucket_key(id.as_str())), id, node, generation: self.inner.db.generation(), stamp: self.received })
    }

    async fn apply_active(&self, mut items: Vec<Pending>) -> bool {
        if self.shared {
            let definitions = self.inner.live_definitions(&self.def.operation, &self.def.variables);
            items.retain(|item| definitions.contains(&item.definition));
        }
        self.apply(items).await
    }

    async fn apply(&self, mut items: Vec<Pending>) -> bool {
        let current = self.inner.db.generation();
        items.retain(|item| item.generation == current);
        if items.is_empty() {
            return false;
        }
        let models: BTreeSet<_> = items.iter().map(|item| item.definition.model.clone()).collect();
        let applied = match self.try_apply(items, current) {
            Ok((stale, held_files)) => self.inner.discard_files(held_files).await.map(|()| stale),
            Err(error) => Err(error),
        };
        match applied {
            Ok(stale) => {
                let mut reloaded = true;
                for model in stale {
                    if self.inner.reload_lists_of_absent_parent(&model).await.is_err() {
                        self.inner.publish_model_gap(&model);
                        locked(&self.status).rejected += 1;
                        reloaded = false;
                    }
                }
                reloaded
            }
            Err(_) => {
                locked(&self.status).rejected += 1;
                for model in models {
                    if self.reread_observed(&model).await.is_err() {
                        self.inner.publish_model_gap(&model);
                    }
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
        let patches: BTreeMap<_, _> = crate::store::coalesce_rows(items.iter().filter_map(|(item, normalized)| normalized.as_ref().map(|normalized| (item, normalized))).flat_map(|(item, normalized)| {
            normalized.related.iter().cloned().chain(normalized.roots.iter().cloned().map(|row| (ModelKey::new(&item.definition.model), row)))
        })).into_iter().filter_map(|(model, row)| Some(((model.to_string(), row.get("id").and_then(read_id)?.to_string()), row))).collect();
        let change_feed = self.inner.change_feed().is_some_and(|(_, feed)| feed.subscription == self.def.operation);
        let before = items.iter().filter(|(item, _)| !change_feed && self.inner.has_event_readers(&item.definition.model)).map(|(item, _)| {
            let def = &item.definition;
            (item.stamp, def.model.clone(), item.id.clone(), item.node.get("kind").and_then(Value::as_str).unwrap_or(if def.effect == EventEffect::Destroy { "DELETED" } else { "UPDATED" }).to_string(), db.find(&def.model, &item.id))
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
                    let Some(patch) = patches.get(&key) else { continue };
                    let Some(row) = db.admitted(reader, WriteOrigin::Event, Admission::default(), model, patch, &mut 0)? else { continue };
                    let own = RowRef::new(ModelKey::new(model), item.id.clone());
                    let seen: Vec<&Map<String, Value>> = std::iter::once(&row).chain(stored.as_ref()).collect();
                    touches.extend(db.touch_ops(reader, spec, &seen, &own)?);
                    for (related_model, patch) in &normalized.related {
                        let Some(id) = patch.get("id").and_then(read_id) else { continue };
                        let Some(patch) = patches.get(&(related_model.to_string(), id.to_string())) else { continue };
                        let Some(row) = db.admitted(reader, WriteOrigin::Event, Admission::default(), related_model.as_str(), patch, &mut 0)? else { continue };
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
                    for file in Prior::of(&entry)?.map(|prior| prior.held_files()).unwrap_or_default() {
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
        for (stamp, model, id, kind, before) in before {
            self.inner.publish_rows(Heard { stamp, area: &self.def.operation, kind: &kind }, &model, id.as_str(), before, db.find(&model, &id));
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

    async fn pump(&self, connection: &mut ServerSubscription, lifecycle: &mut Lifecycle, generation: Generation) -> SubscriptionFailureKind {
        let mut pending: Vec<(Arc<SubscriptionDef>, Duration, Vec<Pending>)> = Vec::new();
        loop {
            let deadline = pending.iter().map(|(_, deadline, _)| *deadline).min();
            let wait = deadline.unwrap_or_default().saturating_sub(self.inner.clock.now());
            tokio::select! {
                event = connection.next() => match event {
                    None => {
                        self.opening(Err(SyncError::Event("subscription closed".into())));
                        self.apply_active(std::mem::take(&mut pending).into_iter().flat_map(|(_, _, items)| items).collect()).await;
                        return SubscriptionFailureKind::Transport;
                    }
                    Some(SubscriptionEvent::Ready) => {
                        if generation != self.inner.db.generation() { continue; }
                        self.opening(Ok(()));
                        lifecycle.handle(Event::Ready);
                        if lifecycle.take_reload() {
                            let models: Vec<_> = self.inner.db.shared.specs.values().filter(|spec| !spec.changes.is_empty() || spec.key.as_str() == self.def.model).map(|spec| spec.key.to_string()).collect();
                            for model in models {
                                if self.inner.reload_watched(&model, None, false).await.is_err() {
                                    self.inner.publish_model_gap(&model);
                                    locked(&self.status).rejected += 1;
                                }
                            }
                        }
                    }
                    Some(SubscriptionEvent::Next(response)) => {
                        if generation != self.inner.db.generation() { continue; }
                        lifecycle.handle(Event::Next);
                        self.publish(lifecycle);
                        let received = self.inner.db.shared.store.lifecycle.snapshot().stamp();
                        let invalidation = self.invalidation(&response, generation, received).await;
                        let mut items = Vec::new();
                        for feed in self.recipients(received) {
                            if !feed.apply_event(&response).await && !invalidation {
                                if let Some(item) = feed.parse(&response) {
                                    match feed.def.event.debounce_interval {
                                        Some(interval) => {
                                            let index = pending.iter().position(|(def, _, _)| def == &feed.def).unwrap_or_else(|| {
                                                pending.push((feed.def.clone(), self.inner.clock.now() + interval, Vec::new()));
                                                pending.len() - 1
                                            });
                                            let debounce = Debounce::new(interval, |item: &Pending| item.bucket.clone());
                                            pending[index].2 = debounce.coalesce(std::mem::take(&mut pending[index].2), item);
                                        }
                                        None => items.push(item),
                                    }
                                }
                            }
                        }
                        self.apply_active(items).await;
                    }
                    Some(SubscriptionEvent::Failed(failure)) => {
                        self.notify_failure(&failure);
                        self.apply_active(std::mem::take(&mut pending).into_iter().flat_map(|(_, _, items)| items).collect()).await;
                        return failure.kind;
                    }
                },
                _ = self.inner.clock.sleep(wait), if deadline.is_some() => {
                    let mut items = Vec::new();
                    let now = self.inner.clock.now();
                    pending.retain_mut(|(_, deadline, queued)| {
                        if *deadline > now { return true; }
                        items.append(queued);
                        false
                    });
                    self.apply_active(items).await;
                }
            }
        }
    }

    async fn run(self, request: GraphQLRequest, first: Option<(Generation, Result<ServerSubscription, TransportError>)>, mut resets: crate::lifecycle::Watch) {
        let mut lifecycle = Lifecycle::new(self.backoff);
        let mut opened = first;
        loop {
            if !self.inner.admitted(crate::lifecycle::Origin::Session) {
                opened = None;
                self.mark(|status| status.opening = false);
                if resets.changed().await.is_none() { return; }
                continue;
            }
            lifecycle.handle(Event::Resumed);
            self.mark(|status| status.opening = true);
            let (generation, attempt) = tokio::select! {
                biased;
                _ = invalidated(&mut resets) => { opened = None; lifecycle = Lifecycle::new(self.backoff); continue; }
                attempt = async {
                    match opened.take() {
                        Some(attempt) => Some(attempt),
                        None => self.inner.open_subscription(request.clone()).await,
                    }
                } => match attempt { Some(attempt) => attempt, None => { self.mark(|status| status.opening = false); return } },
            };
            self.mark(|status| status.opening = false);
            let kind = match attempt {
                Ok(mut connection) => {
                    let reconnected = {
                        let mut status = locked(&self.status);
                        status.connects += 1;
                        status.connected = true;
                        status.connects > 1
                    };
                    if reconnected {
                        let models: BTreeSet<_> = self.recipients(self.received).iter().map(|feed| feed.def.model.clone()).collect();
                        for model in models {
                            if self.inner.reload_watched(&model, None, false).await.is_err() {
                                self.inner.publish_model_gap(&model);
                                locked(&self.status).rejected += 1;
                            }
                        }
                    }
                    lifecycle.handle(Event::Connected);
                    tokio::select! {
                        biased;
                        _ = invalidated(&mut resets) => { self.mark(|status| status.connected = false); lifecycle = Lifecycle::new(self.backoff); continue; },
                        kind = self.pump(&mut connection, &mut lifecycle, generation) => kind,
                    }
                }
                Err(error) => {
                    let failure = SubscriptionFailure::from_error(error);
                    if failure.kind != SubscriptionFailureKind::Transport || self.inner.connectivity.is_online() {
                        self.notify_failure(&failure);
                    }
                    failure.kind
                }
            };
            self.mark(|status| status.connected = false);
            lifecycle.handle(Event::Lost { kind, online: self.inner.connectivity.is_online() });
            self.publish(&lifecycle);
            match lifecycle.step() {
                Step::Stop => return,
                Step::Wait(delay) => tokio::select! {
                    _ = invalidated(&mut resets) => { lifecycle = Lifecycle::new(self.backoff); },
                    _ = self.inner.connectivity.wait(self.inner.clock.as_ref(), delay) => {},
                },
                Step::Open => {}
            }
        }
    }
}

async fn invalidated(resets: &mut crate::lifecycle::Watch) -> Option<crate::lifecycle::Snapshot> {
    resets.invalidated().await
}

impl Sync {
    pub async fn subscribe(&self, def: SubscriptionDef) -> Result<Updates, SyncError> {
        self.inner.subscribe(def, None).await
    }
}

impl Inner {
    async fn open_subscription(&self, request: GraphQLRequest) -> Option<(Generation, Result<ServerSubscription, TransportError>)> {
        let mut stopped = self.stopped.clone();
        tokio::select! {
            biased;
            _ = stopped.wait_for(|stopped| *stopped) => None,
            result = async {
                self.connectivity.online().await;
                let lifecycle = &self.db.shared.store.lifecycle;
                let lease = loop {
                    match self.db.shared.store.lifecycle.lease_for_session(crate::lifecycle::Origin::Session, "subscription open") {
                        Ok(lease) => break lease,
                        Err(_) if !lifecycle.admission(crate::lifecycle::Origin::Session).await => return None,
                        Err(_) => {}
                    }
                };
                let generation = self.db.generation();
                crate::db::lock::assert_no_library_lock("Transport::subscribe");
                let opened = lease.run(self.transport.subscribe(request)).await;
                Some((generation, opened.unwrap_or_else(|ended| Err(ended.into()))))
            } => result,
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
        let mut resets = self.db.shared.store.lifecycle.watch();
        let first = if self.connectivity.is_online() && self.admitted(crate::lifecycle::Origin::Session) {
            tokio::select! {
                biased;
                _ = invalidated(&mut resets) => None,
                attempt = self.open_subscription(request.clone()) => attempt,
            }
        } else { None };
        let status = Arc::new(Mutex::new(SubscriptionStatus::default()));
        let definition = def.clone();
        let def_root = def.root.clone();
        let backoff = RetryPolicy::Subscription.backoff().randomized();
        let root_path = def_root.unwrap_or_else(|| operation.root_path.clone());
        let feed = Feed { inner: self.clone(), def: Arc::new(def), shared: ready.is_some(), operation_root: operation.root_path.clone(), root_path, status: status.clone(), backoff, ready, received: self.db.shared.store.lifecycle.snapshot().stamp() };
        let task = crate::task::spawn(self.until_closed(feed.run(request, first, resets)));
        Ok(Updates { abort: task.abort_handle(), status, definition })
    }
}
