//! The calls of the platform that wait for a window: `begin_load`, `begin_load_all`, `refresh`, `load_more`, `node`, `nodes`.
//! Each registers one waiter in the owner `Windows` and returns when the window answers.

use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};

use super::database::{executor, Database};
use super::wire::WireRow;
use super::DbError;
use crate::identity::RowId;
use crate::owners::life::LifeReply;
use crate::owners::read::windows::{Goal, WindowKey, WindowOutcome};
use crate::schema::{AssociationKind, NodeRead, NODE_PARAM};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum LoadMoreResult {
    Loaded { has_more: bool },
    NoMore,
}

/// The interest of one caller in a load that `Database::begin_load` started.
#[derive(uniffi::Object)]
pub struct LoadRequest {
    outcome: tokio::sync::watch::Receiver<Option<Result<(), DbError>>>,
    /// The sender the task answers with: a cancel answers with it too, so the wait ends with its cause.
    answer: Arc<tokio::sync::watch::Sender<Option<Result<(), DbError>>>>,
    held: Mutex<Option<tokio::task::AbortHandle>>,
    life: Arc<crate::owners::life::Life>,
    /// The count of the owners barrier while an aborted task is still being dropped: the task lets it go with its last field.
    dropping: Arc<Mutex<Option<crate::runtime::Launch>>>,
}

struct ReleaseBarrier(Arc<Mutex<Option<crate::runtime::Launch>>>);

impl Drop for ReleaseBarrier {
    fn drop(&mut self) {
        let held = crate::lock::lock(&self.0).take();
        drop(held);
    }
}

/// Held by the task of a load: while the task panics, the waiters get `Runtime` and the database closes.
struct LoadPanic {
    answer: Arc<tokio::sync::watch::Sender<Option<Result<(), DbError>>>>,
    life: Arc<crate::owners::life::Life>,
}

impl Drop for LoadPanic {
    fn drop(&mut self) {
        if !std::thread::panicking() { return; }
        crate::lock::effect(|| self.answer.send_if_modified(|seen| seen.is_none().then(|| *seen = Some(Err(DbError::Runtime { detail: "the load task failed".into() }))).is_some()));
        self.life.executor_failed("load");
    }
}

fn load_cancelled() -> DbError { super::cause_error(&super::EndCause::Cancelled) }

#[uniffi::export]
impl LoadRequest {
    /// Waits for the end of the load. A request cancelled before its end fails with `Interrupted`.
    pub async fn result(&self) -> Result<(), DbError> {
        let outcome = self.outcome.clone();
        let waiting = self.outcome.clone();
        let life = self.life.clone();
        let seen = super::reading::poll_then_wait(|| match &*outcome.borrow() {
            Some(seen) => std::task::Poll::Ready(Some(seen.clone())),
            None if outcome.has_changed().is_err() => std::task::Poll::Ready(Some(Err(DbError::Internal { detail: "the load ended without an outcome".into() }))),
            None => std::task::Poll::Pending,
        }, || {
            let mut waiting = waiting.clone();
            Box::pin(super::database::on_core(super::database::Scope::counted(life.clone()), async move { let _ = waiting.wait_for(Option::is_some).await; }))
        }).await;
        seen.unwrap_or_else(|| Err(load_cancelled()))
    }

    /// Gives up this caller's interest. The request goes on while another caller waits for it.
    pub fn cancel(&self) {
        let held = crate::lock::lock(&self.held).take();
        let Some(task) = held else { return };
        crate::lock::effect(|| self.answer.send_if_modified(|seen| seen.is_none().then(|| *seen = Some(Err(load_cancelled()))).is_some()));
        let previous = crate::lock::lock(&self.dropping).replace(self.life.launch());
        drop(previous);
        crate::lock::effect(|| task.abort());
        if task.is_finished() {
            let held = crate::lock::lock(&self.dropping).take();
            drop(held);
        }
    }
}

impl Drop for LoadRequest {
    fn drop(&mut self) { self.cancel(); }
}

struct LoadArgs {
    epoch: u64,
    model: String,
    relation: String,
    params_json: String,
    goal: Goal,
    force: bool,
}

struct LoadCall {
    waiter: crate::runtime::Waiter<LifeReply>,
    leave: LeaveLoad,
}

/// The place of a call in the order of the loads: `Windows` registers the loads in the order of the places.
struct LoadSlot {
    life: Arc<crate::owners::life::Life>,
    waiter: Option<crate::runtime::Waiter<LifeReply>>,
    id: u64,
    order: Option<Arc<crate::runtime::CallOrder>>,
}

impl Drop for LoadSlot {
    fn drop(&mut self) {
        if self.waiter.is_none() { return; }
        let (life, id) = (self.life.clone(), self.id);
        match &self.order {
            Some(order) => order.enter(move || life.unreserve_load(id)),
            None => life.unreserve_load(id),
        }
    }
}

struct LeaveLoad {
    life: Arc<crate::owners::life::Life>,
    key: WindowKey,
    id: u64,
    settled: bool,
}

impl Drop for LeaveLoad {
    fn drop(&mut self) {
        if !self.settled { self.life.leave_load(self.key.clone(), self.id); }
    }
}

/// Swaps the temporary ids among the params for server ids with `resolve(model of the param, id)`, the model is the target of the `BelongsTo` or `References` association of the model whose foreign key the param names. Returns the temporary ids that have no server id yet. A temporary id that no association names, or one inside an array or object, is `Invalid`: no request carries it.
pub(super) fn resolve_params(db: &crate::store::Store, model: &str, params: &mut Map<String, Value>, read: bool) -> Result<Vec<(String, String)>, DbError> {
    let spec = db.spec(model)?;
    let mut waiting = Vec::new();
    for (name, value) in params.iter_mut() {
        let mut holds = false;
        crate::identity::walk_temp_ids(value, &mut |_| holds = true);
        if !holds { continue; }
        let target = value.is_string().then(|| spec.associations.iter().find(|association| matches!(association.kind, AssociationKind::BelongsTo | AssociationKind::References) && association.foreign_key == *name).map(|association| association.target.as_str())).flatten();
        let (Some(temp), Some(target)) = (value.as_str(), target) else { return Err(DbError::Invalid { detail: format!("the param {name} of {model} holds a temporary id") }) };
        let temp = RowId::new(temp).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let real = if read { db.resolve(target, &temp)? } else { temp.clone() };
        if real.is_temp() { waiting.push((target.to_string(), temp.as_str().to_string())); } else { *value = Value::String(real.as_str().to_string()); }
    }
    Ok(waiting)
}

/// A call without a relation has nothing to load.
pub(super) fn relation_named(model: &str, relation: Option<String>) -> Result<String, DbError> {
    relation.ok_or_else(|| DbError::Invalid { detail: format!("{model} query has no relation to load") })
}

#[uniffi::export]
impl Database {
    /// Starts loading a relation and returns this caller's interest in the load at once. Callers of the same scope share one request: the request stops when every interest in it was cancelled.
    pub fn begin_load(&self, epoch: u64, model: String, relation: Option<String>, params_json: String) -> Result<Arc<LoadRequest>, DbError> {
        self.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        self.begin(LoadArgs { epoch, model, relation, params_json, goal: Goal::First, force: false })
    }

    pub async fn refresh(&self, epoch: u64, model: String, relation: Option<String>, params_json: String) -> Result<(), DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        let Some(call) = this_db.start_load(epoch, &model, &relation, &params_json, (Goal::First, true), Some("refresh")).await? else { return Ok(()) };
        this_db.finish_load(call).await.map(drop)
    }).await? }

    /// Starts reading every page of the relation and returns this caller's request at once. Cancelling it stops the pages that no other caller waits for.
    pub fn begin_load_all(&self, epoch: u64, model: String, relation: Option<String>, params_json: String, force: bool) -> Result<Arc<LoadRequest>, DbError> {
        self.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        self.begin(LoadArgs { epoch, model, relation, params_json, goal: Goal::All, force })
    }

    pub async fn load_more(&self, epoch: u64, model: String, relation: Option<String>, params_json: String) -> Result<LoadMoreResult, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        let Some(call) = this_db.start_load(epoch, &model, &relation, &params_json, (Goal::Next, false), Some("load more")).await? else { return Ok(LoadMoreResult::NoMore) };
        this_db.finish_load(call).await
    }).await? }

    pub async fn node(&self, epoch: u64, model: String, relation: String, row_id: String, force: bool) -> Result<Option<WireRow>, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        this_db.node_relation(&model, &relation, NodeRead::One)?;
        let id = RowId::new(&row_id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let id = this_db.resolved(epoch, &model, id).await?;
        let wire = this_db.wire(&model)?.clone();
        if id.is_temp() {
            let row = this_db.read_in_epoch(epoch, move |database| Ok(database.db.find(&model, &id)?)).await?;
            return Ok(row.map(|row| wire.row(&row)));
        }
        let params = Map::from_iter([(NODE_PARAM.to_string(), json!(id.as_str()))]);
        let Some(call) = this_db.start_load(epoch, &model, &relation, &Value::Object(params.clone()).to_string(), (Goal::First, force), force.then_some("node")).await? else { return Ok(None) };
        this_db.finish_load(call).await?;
        let row = this_db.read_in_epoch(epoch, move |database| Ok(if database.db.window_member(&model, &relation, &params, id.as_str())? { database.db.find(&model, &id)? } else { None })).await?;
        Ok(row.map(|row| wire.row(&row)))
    }).await? }

    /// The rows of the ids that the answer of the server returned, in the order of the call.
    pub async fn nodes(&self, epoch: u64, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.admit_now(epoch)?;
        this_db.node_relation(&model, &relation, NodeRead::Many)?;
        let asked = row_ids.iter().map(|id| RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })).collect::<Result<Vec<_>, _>>()?;
        let mut ids = Vec::with_capacity(asked.len());
        for id in asked {
            let id = this_db.resolved(epoch, &model, id).await?;
            if !id.is_temp() { ids.push(id); }
        }
        if ids.is_empty() { return Ok(Vec::new()); }
        let wire = this_db.wire(&model)?.clone();
        let key = WindowKey::nodes(&model, &ids.iter().map(RowId::as_str).collect::<Vec<_>>());
        let name = key.name.clone();
        let call = this_db.start_window(epoch, this_db.reserve_load(epoch), key, (Goal::First, true), None)?;
        this_db.finish_load(call).await?;
        let rows = this_db.read_in_epoch(epoch, move |database| {
            let mut rows = Vec::with_capacity(ids.len());
            for id in database.db.nodes_answered(&model, &name, &ids)? {
                rows.extend(database.db.find(&model, &id)?);
            }
            Ok(rows)
        }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }).await? }
}

impl Database {
    fn begin(&self, args: LoadArgs) -> Result<Arc<LoadRequest>, DbError> {
        self.remote()?;
        let (sender, outcome) = tokio::sync::watch::channel(None);
        let sender = Arc::new(sender);
        let answer = sender.clone();
        let this = self.clone();
        let launch = self.life.launch();
        let handle = executor()?.handle().clone();
        let (waiter, id) = self.life.register_load(args.epoch);
        let slot = LoadSlot { life: self.life.clone(), waiter: Some(waiter), id, order: Some(self.order.clone()) };
        let (reserved, sent) = tokio::sync::oneshot::channel();
        let (life, epoch) = (self.life.clone(), args.epoch);
        self.order.enter(move || { life.send_reservation(epoch, id); let _ = crate::lock::effect(|| reserved.send(())); });
        let dropping: Arc<Mutex<Option<crate::runtime::Launch>>> = Arc::default();
        let release = ReleaseBarrier(dropping.clone());
        let panic = LoadPanic { answer: answer.clone(), life: self.life.clone() };
        let task = crate::task::spawn_on(&handle, crate::runtime::counted(Some(launch), async move {
            let _release = release;
            let _panic = panic;
            let sender = sender;
            let _ = sent.await;
            let this = this;
            let LoadArgs { epoch, model, relation, params_json, goal, force } = args;
            let started = match this.prepare_load(&model, &relation, &params_json).await {
                Ok(Some(params)) => this.start_window(epoch, slot, WindowKey::relation(&model, &relation, &params), (goal, force), None).map(Some),
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
            let result = match started {
                Ok(Some(call)) => this.finish_load(call).await.map(drop),
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            crate::lock::effect(|| sender.send_if_modified(|seen| seen.is_none().then(|| *seen = Some(result)).is_some()));
        }));
        Ok(Arc::new(LoadRequest { outcome, answer, held: Mutex::new(Some(task.abort_handle())), life: self.life.clone(), dropping }))
    }

    fn reserve_load(&self, epoch: u64) -> LoadSlot {
        let (waiter, id) = self.life.reserve_load(epoch);
        LoadSlot { life: self.life.clone(), waiter: Some(waiter), id, order: None }
    }

    #[cfg(test)]
    pub(crate) async fn load_all(&self, epoch: u64, model: String, relation: Option<String>, params_json: String, force: bool) -> Result<(), DbError> {
        self.begin_load_all(epoch, model, relation, params_json, force)?.result().await
    }

    fn node_relation(&self, model: &str, relation: &str, kind: NodeRead) -> Result<(), DbError> {
        let spec = self.db.spec(model)?;
        match spec.relations.iter().find(|candidate| candidate.name == relation) {
            Some(declared) if declared.node == Some(kind) => Ok(()),
            _ => Err(DbError::Invalid { detail: format!("relation {relation} of {model} is not a {kind:?} node read") }),
        }
    }

    /// Registers a call that waits for a load of the window of a relation. The window keeps the request while this call or another one waits for it.
    async fn start_load(&self, epoch: u64, model: &str, relation: &str, params_json: &str, (goal, force): (Goal, bool), no_wait: Option<&'static str>) -> Result<Option<LoadCall>, DbError> {
        let slot = self.reserve_load(epoch);
        let Some(params) = self.prepare_load(model, relation, params_json).await? else { return Ok(None) };
        self.start_window(epoch, slot, WindowKey::relation(model, relation, &params), (goal, force), no_wait).map(Some)
    }

    /// The params of a load, or `None` when one names a temporary id that has no server id yet: such a call sends no request and opens no window.
    async fn prepare_load(&self, model: &str, relation: &str, params_json: &str) -> Result<Option<Map<String, Value>>, DbError> {
        self.remote()?;
        let _kept = crate::runtime::hold_until_woken(self.life.launch());
        let (db, found, name, text) = (self.db.clone(), model.to_string(), relation.to_string(), params_json.to_string());
        let prepared = super::database::blocking(move || {
            let mut params: Map<String, Value> = serde_json::from_str(&text)?;
            if !resolve_params(&db, &found, &mut params, true)?.is_empty() { return Ok(None); }
            let params = db.spec(&found)?.selection_values(&name, &params).map_err(crate::store::CoreError::from)?;
            db.check_window(&found, &name, &params)?;
            Ok(Some(params))
        }).await;
        crate::runtime::release_holds();
        prepared
    }

    fn start_window(&self, epoch: u64, mut slot: LoadSlot, key: WindowKey, (goal, force): (Goal, bool), no_wait: Option<&'static str>) -> Result<LoadCall, DbError> {
        self.remote()?;
        self.account_call(epoch)?;
        let published = self.life.published();
        if published.epoch != epoch { return Err(super::cause_error(&super::EndCause::SessionEnded)); }
        if published.identity.is_none() { return Err(DbError::NoSession { detail: "the database has no session in this epoch".into() }); }
        let (id, waiter) = (slot.id, slot.waiter.take().ok_or_else(super::database::foreign_reply)?);
        self.life.load(epoch, id, key.clone(), (goal, force), crate::blocking::blocking_unit(|| self.db.revision_seq())?, no_wait);
        Ok(LoadCall { waiter, leave: LeaveLoad { life: self.life.clone(), key, id, settled: false } })
    }

    async fn finish_load(&self, mut call: LoadCall) -> Result<LoadMoreResult, DbError> {
        let reply = self.await_life(call.waiter).await;
        call.leave.settled = true;
        let LifeReply::Window(outcome) = reply? else { return Err(super::database::foreign_reply()) };
        match *outcome {
            WindowOutcome::Loaded { has_more, fetched, .. } => {
                Ok(if fetched { LoadMoreResult::Loaded { has_more } } else { LoadMoreResult::NoMore })
            }
            WindowOutcome::Failed { failure } => Err(failure),
            WindowOutcome::Offline { message } => Err(DbError::Offline { detail: message }),
            WindowOutcome::PageLimit => Err(DbError::Invalid { detail: "the window reached its page limit".into() }),
            WindowOutcome::Ended { cause } => Err(super::cause_error(&cause)),
        }
    }
}
