//! The one reading of the frames of the subscription from the declarations of the `.dbl`: `change_feed` and `changes` (payload, `removes_on`, `id_of`,
//! direct and `observed` reads), `listens` (`apply`, `deleted_by`, `deleted_unless`, `set`, `id`, `root`), the scope joins of an upserted row.
//! The codec has no state. Its events carry the payload as JSON text with the field `op`: `upsert`, `delete`, `ephemeral`, `reread`, `invalid`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use super::feed::{subscription_of, FramedEvent, ScopedChannel};
use super::feed_io::FeedCodec;
use super::windows::{StaleKeys, WindowKey};
use crate::store::Store;
use crate::identity::RowId;
use crate::schema::{ChangeFeed, ChangeRead, ModelSpec, NodeRead, SubscriptionAction, UpdatesSpec, NODE_PARAM};
use super::codec::{mapped_variables, normalize_nodes, root_value};
use crate::transport::{GraphQLRequest, Response};
use crate::value::read_id;

pub type Operations = super::ReadOperations;

pub struct SchemaFeedCodec {
    db: std::sync::Arc<Store>,
    operations: Operations,
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        _ => true,
    }
}

fn event(model: &str, payload: Value, reread: StaleKeys) -> FramedEvent { FramedEvent { model: model.into(), payload: payload.to_string(), reread, refused: false } }

fn invalid(model: &str, reason: impl std::fmt::Display) -> FramedEvent { FramedEvent { refused: true, ..event(model, json!({"op":"invalid", "model":model, "reason":reason.to_string()}), StaleKeys::none()) } }

fn delete(model: &str, id: &RowId) -> FramedEvent { event(model, json!({"op":"delete", "model":model, "id":id.as_str()}), StaleKeys::none()) }

impl SchemaFeedCodec {
    pub fn new(db: std::sync::Arc<Store>, operations: Operations) -> Self { Self { db, operations } }

    fn feed(&self) -> Option<ChangeFeed> { self.specs().find_map(|spec| spec.change_feed.clone()) }

    fn specs(&self) -> impl Iterator<Item = &ModelSpec> { self.db.specs().values() }

    fn change_events(&self, data: &Value) -> Vec<FramedEvent> {
        let Some(feed) = self.feed() else { return Vec::new() };
        let Some(changes) = root_value(data, &feed.root) else { return Vec::new() };
        let Some(changes) = changes.as_array() else {
            let events: Vec<FramedEvent> = self.specs().filter(|spec| !spec.changes.is_empty()).map(|spec| invalid(spec.key.as_str(), "change feed is no list")).collect();
            return if events.is_empty() { vec![invalid("", format!("{} is no list", feed.root))] } else { events };
        };
        let mut events = Vec::new();
        for change in changes {
            let read = |path: &str| root_value(change, path).and_then(Value::as_str);
            let (Some(area), Some(id), Some(kind)) = (read(&feed.area), read(&feed.id), read(&feed.kind)) else {
                events.push(invalid("", "a change without area, id or kind"));
                continue;
            };
            let Ok(row_id) = RowId::new(id) else {
                events.push(invalid("", format!("change id {id}")));
                continue;
            };
            self.change_targets(change, area, kind, &row_id, &mut events);
        }
        events
    }

    fn change_targets(&self, change: &Value, area: &str, kind: &str, id: &RowId, events: &mut Vec<FramedEvent>) {
        let first = events.len();
        for spec in self.specs() {
            let model = spec.key.as_str();
            let reads: Vec<&ChangeRead> = spec.changes.iter().filter(|read| read.area == area).collect();
            for read in reads.iter().filter(|read| read.payload.is_some()) {
                events.push(self.payload_event(model, read, change, kind, id));
            }
            let direct: Vec<&&ChangeRead> = reads.iter().filter(|read| read.id_of.is_none() && read.payload.is_none()).collect();
            if direct.iter().any(|read| read.removes_on.iter().any(|removal| removal == kind)) {
                events.push(delete(model, id));
            } else if !direct.is_empty() {
                let keys = direct.iter().fold(StaleKeys::none(), |keys, read| keys.union(self.reread_keys(spec, read, id)));
                events.push(event(model, json!({"op":"reread", "model":model, "id":id.as_str()}), keys));
            }
            for read in reads.iter().filter(|read| read.id_of.is_some()) {
                events.push(self.derived_event(spec, read, id));
            }
        }
        for event in events[first..].iter_mut().filter(|event| !event.refused) {
            if let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(&event.payload) {
                fields.insert("area".into(), json!(area));
                fields.insert("kind".into(), json!(kind));
                event.payload = Value::Object(fields).to_string();
            }
        }
    }

    fn payload_event(&self, model: &str, read: &ChangeRead, change: &Value, kind: &str, event_id: &RowId) -> FramedEvent {
        if read.removes_on.iter().any(|removal| removal == kind) {
            return delete(model, event_id);
        }
        let path = read.payload.as_deref().unwrap_or_default();
        if read.relation == "node" && matches!(root_value(change, path), None | Some(Value::Null)) {
            let Some(spec) = self.specs().find(|spec| spec.key.as_str() == model) else { return invalid(model, format!("unknown model {model}")) };
            return event(model, json!({"op":"reread", "model":model, "id":event_id.as_str()}), self.reread_keys(spec, read, event_id));
        }
        let Some(node) = root_value(change, path).and_then(Value::as_object) else { return invalid(model, format!("missing full payload {model}.{path}")) };
        let Some(spec) = self.specs().find(|spec| spec.key.as_str() == model) else { return invalid(model, format!("unknown model {model}")) };
        match self.upsert(model, node.clone(), &Map::new()) {
            Ok(Some(payload)) => match payload["row"].as_object().and_then(|row| spec.missing_field(row)) {
                Some(name) => invalid(model, format!("{name} is required")),
                None => event(model, payload, StaleKeys::none()),
            },
            Ok(None) => invalid(model, "payload without id"),
            Err(error) => invalid(model, error),
        }
    }

    /// The windows of a derived read: the target ids are the stored ids the source row names.
    fn derived_event(&self, spec: &ModelSpec, read: &ChangeRead, event_id: &RowId) -> FramedEvent {
        let model = spec.key.as_str();
        let Some(source) = read.id_of.as_ref() else { return invalid(model, "derived read without source") };
        let target = match self.db.find(&source.model, event_id) {
            Ok(row) => row.and_then(|row| row.get(&source.field).and_then(read_id)),
            Err(error) => return invalid(model, error),
        };
        let keys = target.map(|target| self.reread_keys(spec, &ChangeRead { id_of: None, ..read.clone() }, &target)).unwrap_or_else(StaleKeys::none);
        event(model, json!({"op":"reread", "model":model, "id":event_id.as_str()}), keys)
    }

    fn reread_keys(&self, spec: &ModelSpec, read: &ChangeRead, id: &RowId) -> StaleKeys {
        let model = spec.key.as_str();
        if read.observed {
            return StaleKeys::all();
        }
        let Some(relation) = spec.relations.iter().find(|relation| relation.name == read.relation) else { return StaleKeys::none() };
        let parameter = if relation.node == Some(NodeRead::One) { Some(NODE_PARAM) } else { read.parameter.as_deref().or_else(|| relation.text_params.iter().chain(&relation.by_columns).next().map(String::as_str)) };
        let params = parameter.map(|parameter| Map::from_iter([(parameter.to_string(), json!(id.as_str()))])).unwrap_or_default();
        StaleKeys::Set(BTreeSet::from([WindowKey::relation(model, &read.relation, &params)]))
    }

    fn listen_events(&self, channel: &str, data: &Value) -> Vec<FramedEvent> {
        let mut events = Vec::new();
        for spec in self.specs() {
            for updates in spec.listens.iter().chain(spec.relations.iter().flat_map(|relation| &relation.updates)).filter(|updates| updates.subscription == channel) {
                let Some(operation) = self.operations.get(&updates.subscription) else { continue };
                let root = updates.root.clone().unwrap_or_else(|| operation.root_path.clone());
                let Some(node) = root_value(data, &root) else { continue };
                events.extend(self.listen_event(spec, updates, node));
            }
        }
        events
    }

    fn listen_event(&self, spec: &ModelSpec, updates: &UpdatesSpec, node: &Value) -> Option<FramedEvent> {
        let model = spec.key.as_str();
        if let Some(apply) = &updates.apply {
            if matches!(apply.action, SubscriptionAction::Ignore) { return None; }
            if !apply.matches.iter().all(|(path, accepted)| root_value(node, path).is_some_and(|value| accepted.accepts(value))) { return None; }
        }
        let id = Self::event_id(&updates.id, node);
        let removed = matches!(updates.apply.as_ref().map(|apply| &apply.action), Some(SubscriptionAction::Delete))
            || updates.deleted_by.as_ref().and_then(|field| root_value(node, field)).is_some_and(truthy)
            || updates.deleted_unless.as_ref().and_then(|field| root_value(node, field)).is_some_and(|value| !truthy(value));
        let id = id.or_else(|| spec.is_memory().then(|| RowId::new(&uuid::Uuid::new_v4().to_string()).ok()).flatten());
        let Some(id) = id else { return Some(invalid(model, "event without id")) };
        Some(match updates.apply.as_ref().map(|apply| &apply.action) {
            Some(SubscriptionAction::Ephemeral { fields }) => {
                let values: Option<Map<String, Value>> = fields.iter().map(|(name, path)| root_value(node, path).cloned().map(|value| (name.clone(), value))).collect();
                match values {
                    Some(values) => event(model, json!({"op":"ephemeral", "model":model, "id":id.as_str(), "values":values}), StaleKeys::none()),
                    None => invalid(model, "missing event fields"),
                }
            }
            _ if removed => delete(model, &id),
            _ => {
                let Some(fields) = node.as_object() else { return Some(invalid(model, "event is no object")) };
                let mut fields = fields.clone();
                fields.insert("id".into(), Value::String(id.to_string()));
                match self.upsert(model, fields, &updates.set) {
                    Ok(Some(payload)) => event(model, payload, StaleKeys::none()),
                    Ok(None) => invalid(model, "event without id"),
                    Err(error) => invalid(model, error),
                }
            }
        })
    }

    fn event_id(paths: &[String], node: &Value) -> Option<RowId> {
        match paths {
            [] => root_value(node, "id").and_then(read_id),
            [path] => root_value(node, path).and_then(read_id),
            paths => {
                let parts: Option<Vec<RowId>> = paths.iter().map(|path| root_value(node, path).and_then(read_id)).collect();
                RowId::new(&parts?.iter().map(RowId::as_str).collect::<Vec<_>>().join(":")).ok()
            }
        }
    }

    /// The upsert payload: the normalized row and the rows of the related models; the Store decides the windows the row enters.
    fn upsert(&self, model: &str, node: Map<String, Value>, set: &Map<String, Value>) -> Result<Option<Value>, String> {
        let normalized = normalize_nodes(&self.db, model, std::slice::from_ref(&node)).map_err(|error| error.to_string())?;
        let Some(root) = normalized.roots.into_iter().next() else { return Ok(None) };
        let Some(id) = root.get("id").and_then(read_id) else { return Ok(None) };
        let related: Vec<Value> = normalized.related.into_iter().map(|(related, row)| json!({"model":related.as_str(), "row":row})).collect();
        Ok(Some(json!({"op":"upsert", "model":model, "id":id.as_str(), "row":root, "related":related, "set":set})))
    }
}

impl FeedCodec for SchemaFeedCodec {
    fn request(&self, channel: &str, variables: Value) -> GraphQLRequest {
        let subscription = subscription_of(channel);
        match self.operations.get(subscription) {
            Some(operation) => GraphQLRequest::new(&operation.document, &operation.name, variables),
            None => GraphQLRequest::new("", subscription, variables),
        }
    }

    fn events(&self, channel: &str, response: Response) -> Vec<FramedEvent> {
        let Some(data) = response.data else { return Vec::new() };
        let subscription = subscription_of(channel);
        let mut events = if self.feed().is_some_and(|feed| feed.subscription == subscription) { self.change_events(&data) } else { Vec::new() };
        events.extend(self.listen_events(subscription, &data));
        events
    }
}

/// The channels each model reads: the change feed of the project when the model declares `changed_by`, the subscription of each `listens` and each relation `updates` that takes no variables of a window.
pub fn channels_of(db: &Store) -> BTreeMap<String, Vec<String>> {
    let feed = db.specs().values().find_map(|spec| spec.change_feed.as_ref().map(|feed| feed.subscription.clone()));
    db.specs().values().filter_map(|spec| {
        let plain = spec.relations.iter().flat_map(|relation| relation.updates.iter().filter(|updates| !updates.takes_variables(relation)));
        let mut channels: BTreeSet<String> = spec.listens.iter().chain(plain).map(|updates| updates.subscription.clone()).collect();
        if let Some(feed) = feed.as_ref().filter(|_| !spec.changes.is_empty()) {
            channels.insert(feed.clone());
        }
        (!channels.is_empty()).then(|| (spec.key.to_string(), channels.into_iter().collect()))
    }).collect()
}

/// The operations the library sends by itself with no call of the app: the change feed, each `listens` and `updates` subscription, and the read of each relation a `changed_by` event starts.
pub fn own_operations(db: &Store) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for spec in db.specs().values() {
        if let Some(feed) = spec.change_feed.as_ref().filter(|_| !spec.changes.is_empty()) { names.insert(feed.subscription.clone()); }
        names.extend(spec.listens.iter().chain(spec.relations.iter().flat_map(|relation| &relation.updates)).map(|updates| updates.subscription.clone()));
        for read in &spec.changes {
            names.extend(spec.relations.iter().filter(|relation| relation.name == read.relation).filter_map(|relation| relation.remote.as_ref()).map(|remote| remote.operation.clone()));
        }
    }
    names
}

pub fn scoped_channels(spec: &ModelSpec, relation: &str, params: &Map<String, Value>) -> Vec<ScopedChannel> {
    let Some(relation) = spec.relations.iter().find(|declared| declared.name == relation) else { return Vec::new() };
    let Some(remote) = relation.remote.as_ref() else { return Vec::new() };
    relation.updates.iter().filter(|updates| updates.takes_variables(relation)).map(|updates| ScopedChannel { subscription: updates.subscription.clone(), variables: mapped_variables(params, &remote.variables) }).collect()
}
