use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use serde_json::{Map, Value};
use tokio::runtime::Runtime;
use crate::db::{Db, DbConfig, OpenReport};
use crate::identity::RowId;
use crate::pacing::{Clock, ManualClock, SystemClock};
use crate::query::{Query, Where};
use crate::schema::ModelSpec;
use crate::sync::{LoadOptions, OperationDef, Sync, SyncConfig};
use crate::transport::{self as core, EncryptedClient};
use super::entities::{self, EntityListener, LiveEntities};
use super::live::LiveHandles;
use super::transport::ForeignTransport;
use super::wire::{WireModel, WireRow};
use super::{DbError, LiveChange, LiveSubscription, OnlineSink, QueryListener, RelationStateChange, StateListener, StateSubscription, Transport};

pub(super) fn executor() -> Result<&'static Runtime, DbError> {
    static EXECUTOR: Starter = Starter::new();
    started(&EXECUTOR, || tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build())
}

pub(super) struct Starter {
    runtime: OnceLock<Runtime>,
    starting: Mutex<()>,
}

impl Starter {
    pub(super) const fn new() -> Self {
        Self { runtime: OnceLock::new(), starting: Mutex::new(()) }
    }
}

pub(super) fn started(slot: &Starter, build: impl FnOnce() -> std::io::Result<Runtime>) -> Result<&Runtime, DbError> {
    if let Some(runtime) = slot.runtime.get() {
        return Ok(runtime);
    }
    let _starting = slot.starting.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(runtime) = slot.runtime.get() {
        return Ok(runtime);
    }
    let runtime = build().map_err(|error| DbError::Database { detail: format!("the database executor did not start: {error}") })?;
    Ok(slot.runtime.get_or_init(|| runtime))
}

async fn on_executor<T: Send + 'static>(work: impl std::future::Future<Output = Result<T, crate::sync::SyncError>> + Send + 'static) -> Result<T, DbError> {
    executor()?.spawn(work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(DbError::from)
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
    executor()?.spawn_blocking(work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
}

#[derive(uniffi::Record)]
pub struct Config { pub path: Option<String>, pub data_version: Option<String>, pub cache_rows: u32 }

#[derive(uniffi::Record)]
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64 }

#[derive(uniffi::Record)]
pub struct QueuedAction { pub row: WireRow, pub operation: i64 }

#[derive(uniffi::Object)]
pub struct TestClock { clock: Arc<ManualClock> }

#[uniffi::export]
impl TestClock {
    #[uniffi::constructor]
    pub fn new(start_millis: u64) -> Arc<Self> { Arc::new(Self { clock: Arc::new(ManualClock::new(Duration::from_millis(start_millis))) }) }
    pub fn advance(&self, millis: u64) { self.clock.advance(Duration::from_millis(millis)) }
}

#[derive(Clone, uniffi::Object)]
pub struct Database { pub(super) db: Db, pub(super) sync: Option<Sync>, pub(super) session: Option<Arc<EncryptedClient>>, wires: Arc<HashMap<String, Arc<WireModel>>> }

pub(super) type Wiring = (Option<Arc<dyn core::Transport>>, Option<Arc<EncryptedClient>>);

impl Database {
    fn start(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, clock, |_, _| Ok((transport.map(|transport| Arc::new(ForeignTransport(transport)) as Arc<dyn core::Transport>), None)))
    }

    pub(super) fn start_with(
        config: Config,
        models_json: String,
        clock: Arc<dyn Clock>,
        wire: impl FnOnce(&Db, &Arc<dyn Clock>) -> Result<Wiring, DbError>,
    ) -> Result<Arc<Self>, DbError> {
        let models: Vec<ModelSpec> = serde_json::from_str(&models_json)?;
        let keys: Vec<String> = models.iter().map(|model| model.key.to_string()).collect();
        let db = Db::open_with(config.path.as_deref().map(Path::new), models, config.data_version.as_deref(), DbConfig { cache_rows: config.cache_rows as usize })?;
        db.set_clock(clock.clone());
        let (transport, session) = wire(&db, &clock)?;
        let sync = transport.map(|transport| Sync::new(db.clone(), transport, clock, SyncConfig::default()));
        let wires = keys.into_iter().map(|key| db.spec(&key).map(|spec| (key, Arc::new(WireModel::of(spec)))).map_err(DbError::from)).collect::<Result<_, _>>()?;
        Ok(Arc::new(Self { db, sync, session, wires: Arc::new(wires) }))
    }

    fn wire(&self, model: &str) -> Result<&Arc<WireModel>, DbError> {
        self.wires.get(model).ok_or_else(|| DbError::Invalid { detail: format!("model {model} is not registered") })
    }
}

fn id(value: &str) -> Result<RowId, DbError> {
    RowId::new(value).map_err(|error| DbError::Invalid { detail: error.to_string() })
}

#[uniffi::export]
impl Database {
    #[uniffi::constructor]
    pub fn open(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>) -> Result<Arc<Self>, DbError> {
        Self::start(config, models_json, transport, Arc::new(SystemClock))
    }

    #[uniffi::constructor]
    pub fn open_with_clock(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, clock: Arc<TestClock>) -> Result<Arc<Self>, DbError> {
        Self::start(config, models_json, transport, clock.clock.clone())
    }

    pub fn live_find(&self, model: String, row_id: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let row_id = id(&row_id)?;
        self.connect(Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))), None, false, LoadOptions::default(), listener)
    }
    pub fn peek_row(&self, model: String, row_id: String) -> Result<Option<WireRow>, DbError> {
        let wire = self.wire(&model)?;
        Ok(self.db.peek_row(&model, &id(&row_id)?).map(|row| wire.row(&row)))
    }
    pub fn peek_query(&self, query_json: String) -> Result<Option<LiveChange>, DbError> {
        let (query, _) = self.parsed_query(&query_json)?;
        let wire = self.wire(query.model.as_str())?;
        Ok(self.db.peek_query(&query).map(|peeked| LiveChange::whole(peeked.revision, peeked.rows.iter().map(|row| wire.row(row)).collect())))
    }
    pub async fn warm(&self, query_json: String) -> Result<(), DbError> {
        let (query, _) = self.parsed_query(&query_json)?;
        let db = self.db.clone();
        blocking(move || Ok(db.warm(&query)?)).await
    }
    pub fn open_report(&self) -> OpenReport { self.db.open_report().clone() }
    pub fn subscription_census(&self) -> SubscriptionCensus {
        let census = self.db.observer_census();
        SubscriptionCensus { observed_queries: census.observed_queries as u64, subscriptions: census.subscriptions as u64 }
    }
    pub fn live_entities(&self, model: String, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
        entities::open(&self.db, &model, self.wire(&model)?.clone(), listener)
    }
    pub fn live_query(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.connect_json(&query_json, false, listener)
    }
    pub fn live_count(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.connect_json(&query_json, true, listener)
    }
    pub fn live_state(&self, model: String, relation: String, params_json: String, listener: Arc<dyn StateListener>) -> Result<Arc<StateSubscription>, DbError> {
        let params: Map<String, Value> = serde_json::from_str(&params_json)?;
        let mut receiver = self.sync()?.relation_state(&model, &relation, &params)?;
        let db = self.db.clone();
        let task = executor()?.spawn(async move {
            loop {
                let change = RelationStateChange::of(&receiver.borrow_and_update());
                let db = db.clone();
                if tokio::task::spawn_blocking(move || db.settle()).await.is_err() { return; }
                listener.changed(change);
                if receiver.changed().await.is_err() { return; }
            }
        });
        Ok(Arc::new(StateSubscription { task: Mutex::new(Some(task.abort_handle())) }))
    }
    pub fn live_has_many(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.related(model, row_id, name, listener)
    }
    pub async fn create(&self, model: String, values_json: String, args_json: String) -> Result<WireRow, DbError> {
        let database = self.clone();
        blocking(move || {
            let row = database.db.create_with(&model, serde_json::from_str(&values_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(database.wire(&model)?.row(&row))
        }).await
    }
    pub async fn update(&self, model: String, row_id: String, patch_json: String, args_json: String) -> Result<WireRow, DbError> {
        let database = self.clone();
        blocking(move || {
            let row = database.db.update_with(&model, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(database.wire(&model)?.row(&row))
        }).await
    }
    pub async fn set_local(&self, model: String, row_id: String, path: String, value_json: String) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            Ok(database.db.set_local(&model, &id(&row_id)?, &path, serde_json::from_str(&value_json)?)?)
        }).await
    }
    pub async fn act(&self, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<QueuedAction, DbError> {
        let database = self.clone();
        blocking(move || {
            let acted = database.db.act(&model, &action, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(QueuedAction { row: database.wire(&model)?.row(&acted.row), operation: acted.operation })
        }).await
    }
    pub async fn destroy(&self, model: String, row_id: String) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            database.db.destroy(&model, &id(&row_id)?)?;
            database.send_writes();
            Ok(())
        }).await
    }
    pub async fn set_ephemeral(&self, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            Ok(database.db.set_ephemeral(&model, &id(&row_id)?, serde_json::from_str(&values_json)?)?)
        }).await
    }
    pub async fn attach(&self, model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String) -> Result<String, DbError> {
        let (db, row_id) = (self.db.clone(), id(&row_id)?);
        blocking(move || Ok(db.attach(&model, &row_id, &field, &bytes, &name, &mime)?.to_value().to_string())).await
    }
    pub async fn file(&self, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        let (db, row_id) = (self.db.clone(), id(&row_id)?);
        blocking(move || Ok(db.file(&model, &row_id, &field)?.map(|file| file.to_value().to_string()))).await
    }
    pub async fn read(&self, model: String, row_id: String, field: String) -> Result<Option<Vec<u8>>, DbError> {
        let (db, row_id) = (self.db.clone(), id(&row_id)?);
        blocking(move || Ok(db.read(&model, &row_id, &field)?)).await
    }
    pub async fn url(&self, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        let (db, row_id) = (self.db.clone(), id(&row_id)?);
        blocking(move || Ok(db.url(&model, &row_id, &field)?.map(|path| path.to_string_lossy().into_owned()))).await
    }
    pub async fn detach(&self, model: String, row_id: String, field: String) -> Result<bool, DbError> {
        let (db, row_id) = (self.db.clone(), id(&row_id)?);
        blocking(move || Ok(db.detach(&model, &row_id, &field)?)).await
    }
    pub fn register_operations(&self, defs_json: String) -> Result<(), DbError> {
        let defs: Vec<Value> = serde_json::from_str(&defs_json)?;
        let sync = self.sync()?;
        for def in defs {
            let text = |key: &str| def[key].as_str().map(str::to_string).ok_or_else(|| DbError::Invalid { detail: format!("operation needs {key}") });
            let (name, document) = (text("name")?, text("document")?);
            let mut operation = match def["rootPath"].as_str() {
                Some(root) => OperationDef::new(&name, &document, root),
                None => OperationDef::from_document(&name, &document).ok_or_else(|| DbError::Invalid { detail: format!("operation {name} has no root field") })?,
            };
            if let Some(size) = def["pageSize"].as_u64() { operation = operation.paged(u32::try_from(size).map_err(|error| DbError::Invalid { detail: error.to_string() })?); }
            sync.register(operation);
        }
        Ok(())
    }
    pub async fn run(&self, operation: String, variables_json: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let variables: Value = serde_json::from_str(&variables_json)?;
        Ok(on_executor(async move { sync.run(&operation, variables).await }).await?.to_string())
    }
    pub async fn load(&self, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let params = serde_json::from_str(&params_json)?;
        on_executor(async move { sync.load(&model, &relation, &params, LoadOptions::default()).await }).await?;
        Ok(())
    }
    pub async fn refresh(&self, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let params = serde_json::from_str(&params_json)?;
        on_executor(async move { sync.refresh(&model, &relation, &params).await }).await?;
        Ok(())
    }
    pub async fn load_more(&self, model: String, relation: String, params_json: String) -> Result<bool, DbError> {
        let sync = self.sync()?.clone();
        let params = serde_json::from_str(&params_json)?;
        Ok(on_executor(async move { sync.load_more(&model, &relation, &params).await }).await?.has_next)
    }
    pub fn listen(&self) -> Result<(), DbError> {
        let _entered = executor()?.enter();
        self.sync()?.listen();
        Ok(())
    }
    pub fn unlisten(&self) -> Result<(), DbError> {
        let _entered = executor()?.enter();
        self.sync()?.unlisten();
        Ok(())
    }
    pub async fn flush(&self) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        on_executor(async move { sync.flush().await }).await?;
        Ok(())
    }
    pub async fn retry(&self, model: String, row_id: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move { sync.retry(&model, &row).await }).await?;
        Ok(())
    }
    pub async fn retry_all(&self) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        on_executor(async move { sync.retry_all().await }).await?;
        Ok(())
    }
    pub async fn discard(&self, model: String, row_id: String) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            database.db.discard(&model, &id(&row_id)?)?; Ok(())
        }).await
    }
    pub async fn resume(&self) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        on_executor(async move { sync.resume().await }).await?;
        Ok(())
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }
    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync()?.clone() })) }
    pub async fn upload(&self, model: String, row_id: String, input_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        let input = serde_json::from_str(&input_json)?;
        on_executor(async move { sync.upload(&model, &row, input).await }).await?;
        Ok(())
    }
    pub async fn upload_create(&self, model: String, values_json: String, input_json: String) -> Result<WireRow, DbError> {
        let sync = self.sync()?.clone();
        let (values, input) = (serde_json::from_str(&values_json)?, serde_json::from_str(&input_json)?);
        let owner = model.clone();
        let row_id = on_executor(async move { sync.upload_create(&owner, values, input).await }).await?;
        let row = self.db.find(&model, &row_id)?.ok_or_else(|| DbError::Invalid { detail: format!("{model} {row_id} was not created") })?;
        Ok(self.wire(&model)?.row(&row))
    }
    pub async fn confirmed(&self, model: String, row_id: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        let real = on_executor(async move { sync.confirmed(&model, &row).await }).await?;
        Ok(real.to_string())
    }
    pub async fn settled(&self, model: String, row_id: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move { sync.settled(&model, &row).await }).await?;
        Ok(())
    }
    pub async fn answer(&self, model: String, operation: i64) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let answer = on_executor(async move { sync.answer(&model, operation).await }).await?;
        Ok(answer.to_string())
    }
    pub async fn cancel_upload(&self, model: String, row_id: String) -> Result<bool, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move { sync.cancel_upload(&model, &row).await }).await
    }
    pub async fn maintain(&self) -> Result<(), DbError> {
        let db = self.db.clone();
        blocking(move || Ok(db.maintain(&[]).map(|_| ())?)).await
    }
    pub async fn logout(&self) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            database.db.reset_all()?; Ok(())
        }).await
    }
}

impl Database {
    pub(super) fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "transport is required for server operations".into() }) }
    fn send_writes(&self) {
        if let (Some(sync), Ok(executor)) = (self.sync.as_ref(), executor()) {
            let _entered = executor.enter();
            sync.inner.spawn_flush(Duration::ZERO);
        }
    }
    fn parsed_query(&self, json: &str) -> Result<(Query, Option<(String, Map<String, Value>)>), DbError> {
        let description: Value = serde_json::from_str(json)?;
        let mut query: Query = serde_json::from_value(description.clone())?;
        let remote = description["relation"].as_str().map(|name| -> Result<_, DbError> {
            let params: Map<String, Value> = description.get("params").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or_default();
            let relation_query = self.db.remote_relation_query(query.model.as_str(), name, &params).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
            Ok(relation_query.map(|relation_query| {
                query.scope = relation_query.scope;
                if query.order.is_empty() { query.order = relation_query.order; }
                (name.to_string(), params)
            }))
        }).transpose()?.flatten();
        Ok((query, remote))
    }
    fn connect_json(&self, json: &str, count: bool, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let (query, remote) = self.parsed_query(json)?;
        let description: Value = serde_json::from_str(json)?;
        let force = description.get("refreshOnConnect").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or(false);
        self.connect(query, remote, count, LoadOptions { force }, listener)
    }
    fn connect(&self, query: Query, remote: Option<(String, Map<String, Value>)>, count: bool, options: LoadOptions, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let _entered = executor()?.enter();
        let (latest, _) = tokio::sync::watch::channel(None);
        let post = latest.clone();
        let callback = listener.clone();
        if !count {
            let _ = self.db.warm(&query);
        }
        let wire = self.wire(query.model.as_str())?.clone();
        let observer = if count {
            self.db.observe_count(query.clone(), move |change| {
                let value = LiveChange::counted(change.revision, change.count);
                post.send_replace(Some(value.clone())); callback.changed(value);
            })?
        } else {
            self.db.observe(query.clone(), move |change| { let value = LiveChange::rows(change, &wire); post.send_replace(Some(value.clone())); callback.changed(value); })?
        };
        let (completed, initial_load) = tokio::sync::watch::channel(None);
        let (watch, load) = match remote {
            Some((relation, params)) => {
                let sync = self.sync()?.clone();
                let watch = sync.watch(query.model.as_str(), &relation, &params)?;
                let model = query.model.to_string();
                let errors = latest.clone();
                let task = executor()?.spawn(async move {
                    let result = sync.load(&model, &relation, &params, options).await.map(|_| ());
                    if let Err(error) = &result {
                        let revision = errors.borrow().as_ref().map_or(0, |change| change.revision);
                        let change = LiveChange::failed(revision, error.to_string());
                        errors.send_replace(Some(change.clone()));
                        listener.changed(change);
                    }
                    completed.send_replace(Some(result));
                });
                (Some(watch), Some(task.abort_handle()))
            }
            None => { completed.send_replace(Some(Ok(()))); (None, None) },
        };
        Ok(Arc::new(LiveSubscription { latest, initial_load, handles: Mutex::new(Some(LiveHandles { _observer: observer, _watch: watch, load })) }))
    }
    fn related(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let row_id = id(&row_id)?;
        let (latest, _) = tokio::sync::watch::channel(None);
        let post = latest.clone();
        let target = self.db.spec(&model)?.associations.iter().find(|association| association.name == name).map(|association| association.target.to_string()).ok_or_else(|| DbError::Invalid { detail: format!("{model} has no association {name}") })?;
        let wire = self.wire(&target)?.clone();
        let query = Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))).including(&[&name]);
        let observer = self.db.observe(query, move |change| {
            let rows = change.related.get(&name).and_then(|parents| parents.get(&row_id)).cloned().unwrap_or_default();
            let value = LiveChange::whole(change.revision, rows.iter().map(|row| wire.row(row)).collect());
            post.send_replace(Some(value.clone())); listener.changed(value);
        })?;
        let (_, initial_load) = tokio::sync::watch::channel(Some(Ok(())));
        Ok(Arc::new(LiveSubscription { latest, initial_load, handles: Mutex::new(Some(LiveHandles { _observer: observer, _watch: None, load: None })) }))
    }
}
