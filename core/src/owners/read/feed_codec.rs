//! The one reading of the frames of the subscription from the declarations of the `.dbl`: `change_feed` and `changes` (payload, `removes_on`, `id_of`,
//! direct and `observed` reads), `listens` (`apply`, `deleted_by`, `deleted_unless`, `set`, `id`, `root`), the scope joins of an upserted row.
//! The codec has no state. Its events carry the payload as JSON text with the field `op`: `upsert`, `delete`, `ephemeral`, `reread`, `invalid`.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::feed::FramedEvent;
use super::feed_io::FeedCodec;
use super::schema_io::Operations;
use super::windows::WindowKey;
use crate::db::Db;
use crate::identity::RowId;
use crate::schema::{ChangeRead, ModelSpec, NodeRead, SubscriptionAction, UpdatesSpec, NODE_PARAM};
use crate::sync::{normalize_nodes, root_value};
use crate::transport::{GraphQLRequest, Response};
use crate::value::read_id;

pub struct SchemaFeedCodec {
    db: Db,
    operations: Operations,
    request: GraphQLRequest,
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

fn event(model: &str, payload: Value, reread: BTreeSet<WindowKey>) -> FramedEvent { FramedEvent { model: model.into(), payload: payload.to_string(), reread } }

fn invalid(model: &str, reason: impl std::fmt::Display) -> FramedEvent { event(model, json!({"op":"invalid", "model":model, "reason":reason.to_string()}), BTreeSet::new()) }

fn delete(model: &str, id: &RowId) -> FramedEvent { event(model, json!({"op":"delete", "model":model, "id":id.as_str()}), BTreeSet::new()) }

impl SchemaFeedCodec {
    /// Fails when the schema declares no subscription or the operation of it is not registered.
    pub fn new(db: Db, operations: Operations) -> Result<Self, String> {
        let name = Self::subscription_of(&db).ok_or("the schema declares no subscription")?;
        let operation = operations(&name).ok_or_else(|| format!("operation {name} is not registered"))?;
        let request = GraphQLRequest::new(&operation.document, &operation.name, json!({}));
        Ok(Self { db, operations, request })
    }

    fn subscription_of(db: &Db) -> Option<String> {
        let specs = db.shared.specs.values();
        let mut feeds = specs.clone().filter_map(|spec| spec.change_feed.as_ref().map(|feed| feed.subscription.clone()));
        feeds.next().or_else(|| specs.flat_map(|spec| &spec.listens).map(|updates| updates.subscription.clone()).next())
    }

    fn specs(&self) -> impl Iterator<Item = &ModelSpec> { self.db.shared.specs.values() }

    fn change_events(&self, data: &Value) -> Vec<FramedEvent> {
        let Some(feed) = self.specs().find_map(|spec| spec.change_feed.clone()) else { return Vec::new() };
        let Some(changes) = root_value(data, &feed.root) else { return Vec::new() };
        let Some(changes) = changes.as_array() else { return vec![invalid("", format!("{} is no list", feed.root))] };
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
                let keys = direct.iter().flat_map(|read| self.reread_keys(spec, read, id)).collect();
                events.push(event(model, json!({"op":"reread", "model":model, "id":id.as_str()}), keys));
            }
            for read in reads.iter().filter(|read| read.id_of.is_some()) {
                events.push(self.derived_event(spec, read, id));
            }
        }
    }

    fn payload_event(&self, model: &str, read: &ChangeRead, change: &Value, kind: &str, event_id: &RowId) -> FramedEvent {
        if read.removes_on.iter().any(|removal| removal == kind) {
            return delete(model, event_id);
        }
        let path = read.payload.as_deref().unwrap_or_default();
        let Some(node) = root_value(change, path).and_then(Value::as_object) else { return invalid(model, format!("missing full payload {model}.{path}")) };
        match self.upsert(model, node.clone(), &Map::new()) {
            Ok(Some(payload)) => event(model, payload, BTreeSet::new()),
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
        let keys = target.map(|target| self.reread_keys(spec, &ChangeRead { id_of: None, ..read.clone() }, &target)).unwrap_or_default();
        event(model, json!({"op":"reread", "model":model, "id":event_id.as_str()}), keys)
    }

    fn reread_keys(&self, spec: &ModelSpec, read: &ChangeRead, id: &RowId) -> BTreeSet<WindowKey> {
        let model = spec.key.as_str();
        if read.observed {
            return self.db.observed_reads().into_iter().filter(|entry| entry.model == model).map(|entry| WindowKey::relation(model, &entry.relation, &entry.params)).collect();
        }
        let Some(relation) = spec.relations.iter().find(|relation| relation.name == read.relation) else { return BTreeSet::new() };
        let parameter = if relation.node == Some(NodeRead::One) { Some(NODE_PARAM) } else { read.parameter.as_deref().or_else(|| relation.text_params.iter().chain(&relation.by_columns).next().map(String::as_str)) };
        let params = parameter.map(|parameter| Map::from_iter([(parameter.to_string(), json!(id.as_str()))])).unwrap_or_default();
        BTreeSet::from([WindowKey::relation(model, &read.relation, &params)])
    }

    fn listen_events(&self, data: &Value) -> Vec<FramedEvent> {
        let mut events = Vec::new();
        for spec in self.specs() {
            for updates in &spec.listens {
                let Some(operation) = (self.operations)(&updates.subscription) else { continue };
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
        let removing = updates.removes || matches!(updates.apply.as_ref().map(|apply| &apply.action), Some(SubscriptionAction::Delete));
        let removed = removing
            || updates.deleted_by.as_ref().and_then(|field| root_value(node, field)).is_some_and(truthy)
            || updates.deleted_unless.as_ref().and_then(|field| root_value(node, field)).is_some_and(|value| !truthy(value));
        let Some(id) = id else { return Some(invalid(model, "event without id")) };
        Some(match updates.apply.as_ref().map(|apply| &apply.action) {
            Some(SubscriptionAction::Ephemeral { fields }) => {
                let values: Option<Map<String, Value>> = fields.iter().map(|(name, path)| root_value(node, path).cloned().map(|value| (name.clone(), value))).collect();
                match values {
                    Some(values) => event(model, json!({"op":"ephemeral", "model":model, "id":id.as_str(), "values":values}), BTreeSet::new()),
                    None => invalid(model, "missing event fields"),
                }
            }
            Some(SubscriptionAction::Reread { via }) => {
                let read = ChangeRead { area: String::new(), relation: via.clone(), parameter: None, removes_on: Vec::new(), observed: false, id_of: None, payload: None };
                event(model, json!({"op":"reread", "model":model, "id":id.as_str()}), self.reread_keys(spec, &read, &id))
            }
            _ if removed => delete(model, &id),
            _ => {
                let Some(fields) = node.as_object() else { return Some(invalid(model, "event is no object")) };
                let mut fields = fields.clone();
                fields.insert("id".into(), Value::String(id.to_string()));
                match self.upsert(model, fields, &updates.set) {
                    Ok(Some(payload)) => event(model, payload, BTreeSet::new()),
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

    /// The upsert payload: the normalized row, the rows of the related models and the scopes the row joins.
    fn upsert(&self, model: &str, node: Map<String, Value>, set: &Map<String, Value>) -> Result<Option<Value>, String> {
        let normalized = normalize_nodes(&self.db, model, std::slice::from_ref(&node)).map_err(|error| error.to_string())?;
        let Some(root) = normalized.roots.into_iter().next() else { return Ok(None) };
        let Some(id) = root.get("id").and_then(read_id) else { return Ok(None) };
        let related: Vec<Value> = normalized.related.into_iter().map(|(related, row)| json!({"model":related.as_str(), "row":row})).collect();
        let joins = self.joins_of(model, &root);
        Ok(Some(json!({"op":"upsert", "model":model, "id":id.as_str(), "row":root, "related":related, "joins":joins, "set":set})))
    }

    /// The relations an upserted row joins by its columns: the declared part of a join, the Store checks the loaded window and the membership.
    pub fn joins_of(&self, model: &str, row: &Map<String, Value>) -> Vec<Value> {
        let Ok(spec) = self.db.spec(model) else { return Vec::new() };
        spec.relations
            .iter()
            .filter(|relation| relation.joins_events && relation.remote.is_some() && relation.text_params.is_empty() && relation.constants_described() && relation.row_passes_literals(row))
            .filter_map(|relation| {
                let params: Map<String, Value> = relation.by_columns.iter().filter_map(|column| row.get(column).filter(|value| !value.is_null()).map(|value| (column.clone(), value.clone()))).collect();
                (params.len() == relation.by_columns.len()).then(|| json!({"relation":relation.name, "params":params}))
            })
            .collect()
    }
}

impl FeedCodec for SchemaFeedCodec {
    fn request(&self, _models: &[String]) -> GraphQLRequest { self.request.clone() }

    fn events(&self, response: Response) -> Vec<FramedEvent> {
        let Some(data) = response.data else { return Vec::new() };
        let mut events = self.change_events(&data);
        events.extend(self.listen_events(&data));
        events
    }
}
