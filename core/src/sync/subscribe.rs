use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::pacing::Debounce;
use crate::transport::{
    EventSpec, GraphQLRequest, Response, Subscription, SubscriptionEvent, SubscriptionFailureKind, SubscriptionFault,
    SubscriptionRetry, RETRY_BACKOFF,
};
use crate::value::read_id;

use super::error::SyncError;
use super::extract::root_value;
use super::normalize::{normalize_node, Related};
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
}

impl SubscriptionDef {
    pub fn new(event: EventSpec, operation: &str, model: &str, effect: EventEffect) -> Self {
        Self { event, operation: operation.to_string(), model: model.to_string(), effect, variables: json!({}), deleted_by: None }
    }

    pub fn deleting_when(self, field: &str) -> Self {
        Self { deleted_by: Some(field.to_string()), ..self }
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscriptionStatus {
    pub connects: u32,
    pub applied: usize,
    pub rejected: usize,
    pub degraded: bool,
    pub stopped: bool,
}

type SharedStatus = Arc<Mutex<SubscriptionStatus>>;

pub struct EventSubscription {
    abort: AbortHandle,
    status: SharedStatus,
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
}

struct Feed {
    inner: Arc<Inner>,
    def: SubscriptionDef,
    root_path: String,
    status: SharedStatus,
}

impl Feed {
    fn parse(&self, response: &Response) -> Option<Pending> {
        let node = root_value(response.data.as_ref()?, &self.root_path)?.as_object()?;
        let id = read_id(node.get("id")?)?;
        Some(Pending { bucket: self.def.event.bucket_key(id.as_str()), id, node: node.clone() })
    }

    fn apply(&self, items: Vec<Pending>) {
        if items.is_empty() {
            return;
        }
        let rejected = self.try_apply(items).is_err();
        let mut status = locked(&self.status);
        status.rejected += usize::from(rejected);
    }

    fn try_apply(&self, items: Vec<Pending>) -> Result<(), SyncError> {
        let db = &self.inner.db;
        let model = self.def.model.as_str();
        let mut operations: BTreeMap<(String, String), EntityOp> = BTreeMap::new();
        let mut count = 0;
        for item in items {
            let key = (model.to_string(), item.id.as_str().to_string());
            let deleted = self.def.deleted_by.as_ref().and_then(|field| item.node.get(field)).is_some_and(is_truthy);
            match self.def.effect {
                _ if deleted => {
                    let operation = EntityOp::Destroy { model: ModelKey::new(model), id: item.id, reason: DestroyReason::Server };
                    operations.insert(key, operation);
                }
                EventEffect::Upsert => {
                    let mut related = Related::new();
                    let Some(row) = normalize_node(db, model, &item.node, &mut related)? else { continue };
                    for ((related_model, related_id), row) in related {
                        let id = RowId::new(&related_id).map_err(|error| SyncError::Event(error.to_string()))?;
                        operations.insert((related_model.clone(), related_id), EntityOp::Upsert { model: ModelKey::new(&related_model), id, row });
                    }
                    operations.insert(key, EntityOp::Upsert { model: ModelKey::new(model), id: item.id, row });
                }
                EventEffect::Patch => {
                    let Some(existing) = db.find(model, &item.id)? else { continue };
                    let mut row = existing.data.clone();
                    row.extend(db.spec(model)?.patch(&item.node));
                    operations.insert(key, EntityOp::Upsert { model: ModelKey::new(model), id: item.id, row });
                }
                EventEffect::Destroy => {
                    let operation = EntityOp::Destroy { model: ModelKey::new(model), id: item.id, reason: DestroyReason::Server };
                    operations.insert(key, operation);
                }
            }
            count += 1;
        }
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops = operations.into_values().collect();
        db.land(envelope)?;
        locked(&self.status).applied += count;
        Ok(())
    }

    async fn pump(&self, connection: &mut Subscription, retry: &mut SubscriptionRetry) -> Option<SubscriptionFailureKind> {
        let debounce = self.def.event.debounce_interval.map(|interval| Debounce::new(interval, |item: &Pending| item.bucket.clone()));
        let mut pending: Vec<Pending> = Vec::new();
        let mut deadline: Option<Instant> = None;
        loop {
            let wake = deadline.unwrap_or_else(Instant::now);
            tokio::select! {
                event = connection.next() => match event {
                    None => {
                        self.apply(std::mem::take(&mut pending));
                        return None;
                    }
                    Some(SubscriptionEvent::Ready) => {
                        *retry = retry.confirmed();
                        locked(&self.status).degraded = false;
                    }
                    Some(SubscriptionEvent::Next(response)) => {
                        let Some(item) = self.parse(&response) else { continue };
                        match &debounce {
                            Some(debounce) => {
                                pending = debounce.coalesce(std::mem::take(&mut pending), item);
                                deadline.get_or_insert_with(|| Instant::now() + debounce.interval);
                            }
                            None => self.apply(vec![item]),
                        }
                    }
                    Some(SubscriptionEvent::Failed(failure)) => {
                        self.apply(std::mem::take(&mut pending));
                        return Some(failure.kind);
                    }
                },
                _ = tokio::time::sleep_until(wake), if deadline.is_some() => {
                    self.apply(std::mem::take(&mut pending));
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

    async fn run(self, request: GraphQLRequest, first: Subscription) {
        let mut retry = SubscriptionRetry::default();
        let mut opened = Some(first);
        loop {
            let mut connection = match opened.take() {
                Some(connection) => connection,
                None => match self.inner.transport.subscribe(request.clone()).await {
                    Ok(connection) => {
                        locked(&self.status).connects += 1;
                        self.inner.reload_watched(&self.def.model, None, false).await;
                        connection
                    }
                    Err(_) => {
                        let Some(delay) = self.register_failure(&mut retry, SubscriptionFault::Transient) else { return };
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                },
            };
            let fault = self.pump(&mut connection, &mut retry).await.map_or(SubscriptionFault::Transient, SubscriptionFailureKind::fault);
            drop(connection);
            let Some(delay) = self.register_failure(&mut retry, fault) else { return };
            tokio::time::sleep(delay).await;
        }
    }
}

impl Sync {
    pub async fn subscribe(&self, def: SubscriptionDef) -> Result<EventSubscription, SyncError> {
        if let Some(fault) = def.event.fault() {
            return Err(SyncError::Event(format!("{fault:?}")));
        }
        self.inner.db.spec(&def.model).map_err(|_| SyncError::UnknownModel(def.model.clone()))?;
        let operation = locked(&self.inner.operations)
            .get(&def.operation)
            .cloned()
            .ok_or_else(|| SyncError::UnknownOperation(def.operation.clone()))?;
        let request = GraphQLRequest::new(&operation.document, &operation.name, def.variables.clone());
        let first = self.inner.transport.subscribe(request.clone()).await.map_err(|error| SyncError::Transport(error.to_string()))?;
        let status = Arc::new(Mutex::new(SubscriptionStatus { connects: 1, ..SubscriptionStatus::default() }));
        let feed = Feed { inner: self.inner.clone(), def, root_path: operation.root_path, status: status.clone() };
        let task = tokio::spawn(feed.run(request, first));
        Ok(EventSubscription { abort: task.abort_handle(), status })
    }
}
