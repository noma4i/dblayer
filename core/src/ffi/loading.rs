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
use crate::schema::{NodeRead, NODE_PARAM};

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

fn load_cancelled() -> DbError { super::cause_error(&super::EndCause::Cancelled) }

#[uniffi::export]
impl LoadRequest {
    /// Waits for the end of the load. A request cancelled before its end fails with `Interrupted`.
    pub async fn result(&self) -> Result<(), DbError> {
        let mut outcome = self.outcome.clone();
        let seen = match outcome.wait_for(Option::is_some).await {
            Ok(seen) => seen.clone(),
            Err(_) => return Err(DbError::Internal { detail: "the load ended without an outcome".into() }),
        };
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
}

impl Drop for LoadSlot {
    fn drop(&mut self) {
        if self.waiter.is_some() { self.life.unreserve_load(self.id); }
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

    pub async fn refresh(&self, epoch: u64, model: String, relation: Option<String>, params_json: String) -> Result<(), DbError> {
        self.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        let call = self.start_load(epoch, &model, &relation, &params_json, (Goal::First, true), Some("refresh")).await?;
        self.finish_load(call).await.map(drop)
    }

    /// Starts reading every page of the relation and returns this caller's request at once. Cancelling it stops the pages that no other caller waits for.
    pub fn begin_load_all(&self, epoch: u64, model: String, relation: Option<String>, params_json: String, force: bool) -> Result<Arc<LoadRequest>, DbError> {
        self.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        self.begin(LoadArgs { epoch, model, relation, params_json, goal: Goal::All, force })
    }

    pub async fn load_more(&self, epoch: u64, model: String, relation: Option<String>, params_json: String) -> Result<LoadMoreResult, DbError> {
        self.account_call(epoch)?;
        let relation = relation_named(&model, relation)?;
        let call = self.start_load(epoch, &model, &relation, &params_json, (Goal::Next, false), Some("load more")).await?;
        self.finish_load(call).await
    }

    pub async fn node(&self, epoch: u64, model: String, relation: String, row_id: String, force: bool) -> Result<Option<WireRow>, DbError> {
        self.account_call(epoch)?;
        let params = Map::from_iter([(NODE_PARAM.to_string(), json!(row_id))]);
        self.node_relation(&model, &relation, NodeRead::One)?;
        let id = RowId::new(&row_id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let wire = self.wire(&model)?.clone();
        let call = self.start_load(epoch, &model, &relation, &Value::Object(params.clone()).to_string(), (Goal::First, force), force.then_some("node")).await?;
        self.finish_load(call).await?;
        let row = self.read_in_epoch(epoch, "node", move |database| Ok(if database.db.window_member(&model, &relation, &params, id.as_str())? { database.db.find(&model, &id)? } else { None })).await?;
        Ok(row.map(|row| wire.row(&row)))
    }

    /// The rows of the ids that the answer of the server returned, in the order of the call.
    pub async fn nodes(&self, epoch: u64, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> {
        self.admit_now(epoch)?;
        self.node_relation(&model, &relation, NodeRead::Many)?;
        let ids = row_ids.iter().map(|id| RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })).collect::<Result<Vec<_>, _>>()?;
        let wire = self.wire(&model)?.clone();
        let key = WindowKey::nodes(&model, &row_ids.iter().map(String::as_str).collect::<Vec<_>>());
        let name = key.name.clone();
        let call = self.start_window(epoch, self.reserve_load(epoch), key, (Goal::First, true), None)?;
        self.finish_load(call).await?;
        let rows = self.read_in_epoch(epoch, "nodes", move |database| {
            let mut rows = Vec::with_capacity(ids.len());
            for id in database.db.nodes_answered(&model, &name, &ids)? {
                rows.extend(database.db.find(&model, &id)?);
            }
            Ok(rows)
        }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }
}

impl Database {
    fn begin(&self, args: LoadArgs) -> Result<Arc<LoadRequest>, DbError> {
        self.remote()?;
        let (sender, outcome) = tokio::sync::watch::channel(None);
        let sender = Arc::new(sender);
        let answer = sender.clone();
        let this = self.clone();
        let launch = self.life.launch();
        let slot = self.reserve_load(args.epoch);
        let dropping: Arc<Mutex<Option<crate::runtime::Launch>>> = Arc::default();
        let release = ReleaseBarrier(dropping.clone());
        let task = crate::task::spawn_on(executor()?.handle(), crate::runtime::counted(Some(launch), async move {
            let _release = release;
            let sender = sender;
            let this = this;
            let LoadArgs { epoch, model, relation, params_json, goal, force } = args;
            let started = match this.prepare_load(&model, &relation, &params_json).await {
                Ok(params) => this.start_window(epoch, slot, WindowKey::relation(&model, &relation, &params), (goal, force), None),
                Err(error) => Err(error),
            };
            let result = match started {
                Ok(call) => this.finish_load(call).await.map(drop),
                Err(error) => Err(error),
            };
            crate::lock::effect(|| sender.send_if_modified(|seen| seen.is_none().then(|| *seen = Some(result)).is_some()));
        }));
        Ok(Arc::new(LoadRequest { outcome, answer, held: Mutex::new(Some(task.abort_handle())), life: self.life.clone(), dropping }))
    }

    fn reserve_load(&self, epoch: u64) -> LoadSlot {
        let (waiter, id) = self.life.reserve_load(epoch);
        LoadSlot { life: self.life.clone(), waiter: Some(waiter), id }
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
    async fn start_load(&self, epoch: u64, model: &str, relation: &str, params_json: &str, (goal, force): (Goal, bool), no_wait: Option<&'static str>) -> Result<LoadCall, DbError> {
        let slot = self.reserve_load(epoch);
        let params = self.prepare_load(model, relation, params_json).await?;
        self.start_window(epoch, slot, WindowKey::relation(model, relation, &params), (goal, force), no_wait)
    }

    async fn prepare_load(&self, model: &str, relation: &str, params_json: &str) -> Result<Map<String, Value>, DbError> {
        self.remote()?;
        let _kept = crate::runtime::hold_until_woken(self.life.launch());
        let (db, found, name, text) = (self.db.clone(), model.to_string(), relation.to_string(), params_json.to_string());
        let prepared = super::database::blocking(move || {
            let params: Map<String, Value> = serde_json::from_str(&text)?;
            let params = db.spec(&found)?.selection_values(&name, &params).map_err(crate::store::CoreError::from)?;
            db.check_window(&found, &name, &params)?;
            Ok(params)
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
        self.life.load(epoch, id, key.clone(), (goal, force), self.db.revision_seq()?, no_wait);
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
