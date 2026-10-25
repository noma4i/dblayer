use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use serde_json::{Map, Value};
use tokio::runtime::Runtime;
use crate::db::{Db, DbConfig, Feed, OpenReport, WindowSpec};
use crate::identity::RowId;
use crate::pacing::{Clock, ManualClock, SystemClock};
use crate::query::{Query, Where};
use crate::schema::ModelSpec;
use crate::read::LoadMoreVerdict;
use crate::sync::{LoadOptions, LoadStatus, OperationDef, Sync, SyncConfig};
use crate::transport::{self as core, EncryptedClient};
use crate::upload::UploadOperations;
use super::entities::{self, EntityListener, LiveEntities};
use super::live::{follow_failures, intake, LiveHandles};
use super::live_feed::LiveFeed;
use super::open_slot::OpenLease;
use super::transport::ForeignTransport;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use super::wire::{WireModel, WireRow};
use super::{DbError, LiveChange, LiveSubscription, LoadMoreResult, OnlineSink, QueryListener, Transport};

pub(super) fn executor() -> Result<&'static Runtime, DbError> {
    static EXECUTOR: Starter = Starter::new();
    started(&EXECUTOR, || tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build())
}

pub(super) struct Starter {
    runtime: OnceLock<Runtime>,
    starting: Mutex<()>,
    #[cfg(test)]
    cold_entries: (Mutex<usize>, std::sync::Condvar),
}

impl Starter {
    pub(super) const fn new() -> Self {
        Self {
            runtime: OnceLock::new(),
            starting: Mutex::new(()),
            #[cfg(test)]
            cold_entries: (Mutex::new(0), std::sync::Condvar::new()),
        }
    }

    #[cfg(test)]
    pub(super) fn cold_entries_reach(&self, count: usize, within: std::time::Duration) -> bool {
        let (entries, reached) = &self.cold_entries;
        let guard = entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (guard, _) = reached.wait_timeout_while(guard, within, |entries| *entries < count).unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard >= count
    }
}

pub(super) fn started(slot: &Starter, build: impl FnOnce() -> std::io::Result<Runtime>) -> Result<&Runtime, DbError> {
    if let Some(runtime) = slot.runtime.get() {
        return Ok(runtime);
    }
    #[cfg(test)]
    {
        *slot.cold_entries.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        slot.cold_entries.1.notify_all();
    }
    let _starting = slot.starting.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(runtime) = slot.runtime.get() {
        return Ok(runtime);
    }
    let runtime = build().map_err(|error| DbError::Database { detail: format!("the database executor did not start: {error}") })?;
    Ok(slot.runtime.get_or_init(|| runtime))
}

async fn on_executor<T: Send + 'static, E: Into<DbError> + Send + 'static>(work: impl std::future::Future<Output = Result<T, E>> + Send + 'static) -> Result<T, DbError> {
    executor()?.spawn(work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(Into::into)
}

pub(super) async fn cancellable<T: Send + 'static>(work: impl std::future::Future<Output = T> + Send + 'static) -> Result<T, DbError> {
    struct Cancel(tokio::task::AbortHandle);
    impl Drop for Cancel { fn drop(&mut self) { self.0.abort(); } }
    let task = executor()?.spawn(work);
    let _cancel = Cancel(task.abort_handle());
    task.await.map_err(|error| DbError::Invalid { detail: error.to_string() })
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
    #[cfg(test)]
    let work = {
        let caller = std::thread::current().id();
        move || {
            super::offload_tests::note_blocking_thread(caller);
            work()
        }
    };
    executor()?.spawn_blocking(work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
}

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct Config { pub path: Option<String>, pub data_version: Option<String>, pub cache_rows: u32 }

#[derive(uniffi::Record)]
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64 }

#[derive(uniffi::Record)]
pub struct EntityCensus { pub facts_told: u64, pub position_skips: u64, pub reads_failed: u64 }

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
pub struct Database { lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) db: Db, pub(super) sync: Option<Sync>, pub(super) session: Option<Arc<EncryptedClient>>, pub(super) clock: Arc<dyn Clock>, wires: Arc<HashMap<String, Arc<WireModel>>> }

pub(super) type Wiring = (Option<Arc<dyn core::Transport>>, Option<Arc<EncryptedClient>>);

impl Database {
    fn start(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<UploadOperations>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, uploads, clock, |_, _| Ok((transport.map(|transport| Arc::new(ForeignTransport(transport)) as Arc<dyn core::Transport>), None)))
    }

    pub(super) fn start_with(
        config: Config,
        models_json: String,
        uploads: Option<UploadOperations>,
        clock: Arc<dyn Clock>,
        wire: impl FnOnce(&Db, &Arc<dyn Clock>) -> Result<Wiring, DbError>,
    ) -> Result<Arc<Self>, DbError> {
        let models: Vec<ModelSpec> = serde_json::from_str(&models_json)?;
        let keys: Vec<String> = models.iter().map(|model| model.key.to_string()).collect();
        let db = Db::open_with(config.path.as_deref().map(Path::new), models, config.data_version.as_deref(), DbConfig { cache_rows: config.cache_rows as usize })?;
        db.set_clock(clock.clone());
        let (transport, session) = wire(&db, &clock)?;
        if uploads.is_some() && transport.is_none() {
            return Err(DbError::Invalid { detail: "upload operations require a transport".into() });
        }
        let sync = transport.map(|transport| Sync::new(db.clone(), transport, clock.clone(), SyncConfig { uploads, ..SyncConfig::default() }));
        let wires = keys.into_iter().map(|key| db.spec(&key).map(|spec| (key, Arc::new(WireModel::of(spec)))).map_err(DbError::from)).collect::<Result<_, _>>()?;
        Ok(Arc::new(Self { lease: Arc::new(OnceLock::new()), db, sync, session, clock, wires: Arc::new(wires) }))
    }

    pub async fn open(
        config: Config,
        models_json: String,
        transport: Option<Arc<dyn Transport>>,
        encrypted: Option<super::EncryptedConfig>,
        credentials: Option<Arc<dyn super::CredentialStore>>,
        http: Option<Arc<dyn super::EncryptedHttp>>,
        uploads: Option<UploadOperations>,
    ) -> Result<Arc<Self>, DbError> {
        blocking(move || {
            let Some(encrypted) = encrypted else {
                if credentials.is_some() || http.is_some() {
                    return Err(DbError::Invalid { detail: "credentials and http require encrypted".into() });
                }
                return Self::start(config, models_json, transport, uploads, Arc::new(SystemClock));
            };
            let credentials = credentials.ok_or_else(|| DbError::Invalid { detail: "encrypted requires credentials".into() })?;
            let http = http.ok_or_else(|| DbError::Invalid { detail: "encrypted requires http".into() })?;
            let transport = transport.ok_or_else(|| DbError::Invalid { detail: "encrypted requires transport".into() })?;
            let _entered = executor()?.enter();
            Self::start_with(config, models_json, uploads, Arc::new(SystemClock), |db, clock| {
                super::encrypted::wire(db, clock, encrypted, http, credentials, transport)
            })
        }).await
    }

    pub async fn open_with_clock(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<UploadOperations>, clock: Arc<TestClock>) -> Result<Arc<Self>, DbError> {
        blocking(move || Self::start(config, models_json, transport, uploads, clock.clock.clone())).await
    }

    pub(super) fn hold(&self, lease: Arc<OpenLease>) { let _ = self.lease.set(lease); }

    fn wire(&self, model: &str) -> Result<&Arc<WireModel>, DbError> {
        self.wires.get(model).ok_or_else(|| DbError::Invalid { detail: format!("model {model} is not registered") })
    }
}

fn id(value: &str) -> Result<RowId, DbError> {
    RowId::new(value).map_err(|error| DbError::Invalid { detail: error.to_string() })
}

#[uniffi::export]
impl Database {
    pub fn live_find(&self, model: String, row_id: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| {
            let row_id = id(&row_id)?;
            self.connect(Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))), None, false, LoadOptions::default(), feed)
        })
    }
    pub fn peek_row(&self, model: String, row_id: String) -> Result<Option<WireRow>, DbError> {
        let wire = self.wire(&model)?;
        Ok(self.db.peek_row(&model, &id(&row_id)?).map(|row| wire.row(&row)))
    }
    pub async fn edge_values(&self, model: String, relation: String, params_json: String, row_ids: Vec<String>) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        blocking(move || {
            let params = serde_json::from_str(&params_json)?;
            Ok(serde_json::to_string(&sync.edge_values(&model, &relation, &params, &row_ids)?)?)
        })
        .await
    }
    pub async fn peek_query(&self, query_json: String) -> Result<Option<LiveChange>, DbError> {
        let this = self.clone();
        blocking(move || {
            let (query, _) = this.parsed_query(&query_json)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.peek_query(&query).map(|peeked| LiveChange::whole(peeked.revision, peeked.rows.iter().map(|row| wire.row(row)).collect())))
        })
        .await
    }
    pub async fn warm(&self, query_json: String) -> Result<(), DbError> {
        let this = self.clone();
        blocking(move || {
            let (query, _) = this.parsed_query(&query_json)?;
            Ok(this.db.warm(&query)?)
        })
        .await
    }
    pub fn open_report(&self) -> OpenReport { self.db.open_report().clone() }
    pub fn subscription_census(&self) -> SubscriptionCensus {
        let census = self.db.observer_census();
        SubscriptionCensus { observed_queries: census.observed_queries as u64, subscriptions: census.subscriptions as u64 }
    }
    pub fn entity_census(&self) -> EntityCensus {
        let counters = self.db.counters();
        EntityCensus { facts_told: counters.entity_facts_told as u64, position_skips: counters.entity_position_skips as u64, reads_failed: counters.entity_reads_failed as u64 }
    }
    pub fn live_entities(&self, model: String, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
        entities::open(&self.db, &model, self.wire(&model)?.clone(), listener)
    }
    pub fn live_query(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.connect_json(&query_json, false, feed))
    }
    pub fn live_window(&self, query_json: String, tail: u32, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| {
            let (query, relation) = self.parsed_query(&query_json)?;
            let _entered = executor()?.enter();
            let mut watch = match (self.sync.as_ref(), relation) {
                (Some(sync), Some((relation, params))) => Some(sync.watch(query.model.as_str(), &relation, &params)?),
                _ => None,
            };
            let wire = self.wire(query.model.as_str())?.clone();
            let sink = feed.clone();
            let window = self.db.observe_window_feed(query, WindowSpec::Tail { size: tail as usize }, move |item| intake(&sink, &wire, item))?;
            let failure = follow_failures(watch.as_mut(), feed);
            Ok(LiveHandles { _observer: None, window: Some(window), _watch: watch, load: None, failure, state: None })
        })
    }
    pub fn live_count(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.connect_json(&query_json, true, feed))
    }
    pub fn live_has_many(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.related(model, row_id, name, feed))
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
        Ok(on_executor(async move {
            let variables: Value = serde_json::from_str(&variables_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.run(&operation, variables).await?)
        })
        .await?
        .to_string())
    }
    pub async fn load(&self, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        on_executor(async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load(&model, &relation, &params, LoadOptions::default()).await?)
        })
        .await?;
        Ok(())
    }
    pub async fn refresh(&self, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        on_executor(async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.refresh(&model, &relation, &params).await?)
        })
        .await?;
        Ok(())
    }
    pub async fn load_all(&self, model: String, relation: String, params_json: String, force: bool) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        cancellable(async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_all(&model, &relation, &params, LoadOptions { force }).await?)
        })
        .await?
    }
    pub async fn load_more(&self, model: String, relation: String, params_json: String) -> Result<LoadMoreResult, DbError> {
        let sync = self.sync()?.clone();
        let outcome = on_executor(async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_more(&model, &relation, &params).await?)
        })
        .await?;
        Ok(match outcome.status {
            LoadStatus::NoMorePages(LoadMoreVerdict::AlreadyLoading) => LoadMoreResult::AlreadyLoading,
            LoadStatus::NoMorePages(_) => LoadMoreResult::NoMore,
            _ => LoadMoreResult::Loaded { has_more: outcome.has_next },
        })
    }
    pub async fn node(&self, model: String, relation: String, row_id: String, force: bool) -> Result<Option<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let row = on_executor(async move { sync.node(&model, &relation, &row_id, force).await }).await?;
        Ok(row.map(|row| wire.row(&row)))
    }
    pub async fn nodes(&self, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let rows = on_executor(async move { sync.nodes(&model, &relation, &row_ids.iter().map(String::as_str).collect::<Vec<_>>()).await }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }
    pub fn listen(&self) -> Result<(), DbError> {
        let _entered = executor()?.enter();
        if let Some(sync) = &self.sync { sync.listen(); }
        Ok(())
    }
    pub fn unlisten(&self) -> Result<(), DbError> {
        let _entered = executor()?.enter();
        if let Some(sync) = &self.sync { sync.unlisten(); }
        Ok(())
    }
    pub async fn flush(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.flush().await }).await?;
        Ok(())
    }
    pub async fn retry(&self, model: String, row_id: String, action: Option<String>) -> Result<u64, DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(0) };
        let row = id(&row_id)?;
        let db = self.db.clone();
        on_executor(async move {
            let count = db.retry(&model, &row, action.as_deref())?;
            sync.flush().await?;
            Ok::<_, crate::sync::SyncError>(count as u64)
        }).await
    }
    pub async fn retry_all(&self) -> Result<u64, DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(0) };
        let db = self.db.clone();
        on_executor(async move {
            let count = db.retry_all()?;
            sync.flush().await?;
            Ok::<_, crate::sync::SyncError>(count as u64)
        }).await
    }
    pub async fn discard(&self, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        if let Some(sync) = self.sync.clone() {
            let row = id(&row_id)?;
            on_executor(async move { sync.discard(&model, &row, action.as_deref()).await }).await?;
            return Ok(());
        }
        let database = self.clone();
        blocking(move || {
            database.db.discard(&model, &id(&row_id)?, action.as_deref())?; Ok(())
        }).await
    }
    pub async fn resume(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.resume().await }).await?;
        Ok(())
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }
    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync.clone(), online: self.db.connectivity() })) }
    pub async fn upload(&self, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move {
            let file = request.into_file()?;
            Ok::<_, DbError>(sync.upload(&model, &row, &action, file).await?)
        })
        .await?;
        Ok(())
    }
    pub async fn upload_create(&self, model: String, action: String, values_json: String, request: WireUploadRequest) -> Result<WireRow, DbError> {
        let sync = self.sync()?.clone();
        let (db, wire) = (self.db.clone(), self.wire(&model)?.clone());
        on_executor(async move {
            let (values, file) = (serde_json::from_str(&values_json).map_err(DbError::from)?, request.into_file()?);
            let row_id = sync.upload_create(&model, &action, values, file).await?;
            let row = db.find(&model, &row_id)?.ok_or_else(|| DbError::Invalid { detail: format!("{model} {row_id} was not created") })?;
            Ok::<_, DbError>(wire.row(&row))
        })
        .await
    }
    pub async fn upload_create_batch(&self, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        on_executor(async move {
            let items = items.into_iter().map(WireUploadItem::into_item).collect::<Result<Vec<_>, _>>()?;
            let input = request.into_input()?;
            let rows = sync.upload_create_batch(&model, items, input).await?;
            Ok::<_, DbError>(rows.iter().map(|row| wire.row(row)).collect())
        })
        .await
    }
    pub async fn confirmed(&self, model: String, row_id: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        let real = on_executor(async move { sync.confirmed(&model, &row).await }).await?;
        Ok(real.to_string())
    }
    pub async fn settled(&self, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move { sync.settled(&model, &row, action.as_deref()).await }).await?;
        Ok(())
    }
    pub async fn answer(&self, model: String, operation: i64) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let answer = on_executor(async move { sync.answer(&model, operation).await }).await?;
        Ok(answer.to_string())
    }
    pub async fn cancel_upload(&self, model: String, row_id: String, action: String) -> Result<bool, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move { sync.cancel_upload(&model, &row, &action).await }).await
    }
    pub async fn maintain(&self) -> Result<(), DbError> {
        let db = self.db.clone();
        blocking(move || Ok(db.maintain(&[]).map(|_| ())?)).await
    }
    pub fn shutdown(&self) { if let Some(lease) = self.lease.get() { lease.release(); } }
    pub async fn logout(&self) -> Result<(), DbError> {
        let database = self.clone();
        blocking(move || {
            database.db.reset_all()?;
            database.db.await_revision(database.db.revision_seq()?);
            Ok(())
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
            let watch = relation_query.is_some() || self.db.spec(query.model.as_str())?.relations.iter().any(|relation| relation.name == name && !relation.updates.is_empty());
            if let Some(relation_query) = relation_query {
                query.scope = relation_query.scope;
                query.stable = relation_query.stable;
                if query.order.is_empty() { query.order = relation_query.order; }
            }
            Ok(watch.then_some((name.to_string(), params)))
        }).transpose()?.flatten();
        Ok((query, remote))
    }
    fn subscribed(&self, listener: Arc<dyn QueryListener>, open: impl FnOnce(Arc<LiveFeed>) -> Result<LiveHandles, DbError>) -> Arc<LiveSubscription> {
        let feed = LiveFeed::new(listener);
        let handles = match open(feed.clone()) {
            Ok(handles) => Some(handles),
            Err(error) => {
                let failed = feed.clone();
                match executor() {
                    Ok(executor) => { executor.spawn(async move { failed.invalid(error) }); }
                    Err(_) => failed.invalid(error),
                }
                None
            }
        };
        Arc::new(LiveSubscription { feed, handles: Mutex::new(handles) })
    }
    fn connect_json(&self, json: &str, count: bool, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let (query, remote) = self.parsed_query(json)?;
        let description: Value = serde_json::from_str(json)?;
        let force = description.get("refreshOnConnect").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or(false);
        self.connect(query, remote, count, LoadOptions { force }, feed)
    }
    fn connect(&self, query: Query, remote: Option<(String, Map<String, Value>)>, count: bool, options: LoadOptions, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let _entered = executor()?.enter();
        let model = query.model.to_string();
        let mut tracked = None;
        let mut watch = match remote {
            Some((relation, params)) => {
                let sync = self.sync()?.clone();
                let watch = sync.watch(&model, &relation, &params)?;
                if self.db.spec(&model)?.relations.iter().any(|declared| declared.name == relation && declared.remote.is_some()) {
                    feed.expect_load();
                    tracked = Some((sync, relation, params));
                }
                Some(watch)
            }
            None => None,
        };
        if !count {
            self.db.retain_query(&query)?;
        }
        let wire = self.wire(&model)?.clone();
        let sink = feed.clone();
        let observer = self.db.observe_query_feed(query, count, tracked.is_some(), move |item| intake(&sink, &wire, item))?;
        let (load, state) = match tracked {
            Some((sync, relation, params)) => {
                let control = observer.control();
                let ask = control.clone();
                feed.on_ahead(Arc::new(move |revision| ask.want_reach(revision)));
                let (follower, loader) = (feed.clone(), feed.clone());
                let (ready, seeded) = tokio::sync::oneshot::channel();
                let state = executor()?.spawn(async move {
                    let mut changes = match sync.relation_state(&model, &relation, &params).await {
                        Ok(changes) => changes,
                        Err(error) => {
                            follower.load_finished(Some(DbError::from(error)));
                            return control.resume();
                        }
                    };
                    follower.state(changes.borrow_and_update().clone());
                    control.resume();
                    let _ = ready.send((sync, model, relation, params, changes.clone()));
                    while changes.changed().await.is_ok() { follower.state(changes.borrow_and_update().clone()); }
                }).abort_handle();
                let load = executor()?.spawn(async move {
                    let Ok((sync, model, relation, params, receiver)) = seeded.await else { return };
                    let result = sync.load(&model, &relation, &params, options).await;
                    loader.state(receiver.borrow().clone());
                    loader.load_finished(result.err().map(DbError::from));
                }).abort_handle();
                (Some(load), Some(state))
            }
            None => (None, None),
        };
        let failure = follow_failures(watch.as_mut(), feed);
        Ok(LiveHandles { _observer: Some(observer), window: None, _watch: watch, load, failure, state })
    }
    fn related(&self, model: String, row_id: String, name: String, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let row_id = id(&row_id)?;
        let target = self.db.spec(&model)?.associations.iter().find(|association| association.name == name).map(|association| association.target.to_string()).ok_or_else(|| DbError::Invalid { detail: format!("{model} has no association {name}") })?;
        let wire = self.wire(&target)?.clone();
        let query = Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))).including(&[&name]);
        let observer = self.db.observe_query_feed(query, false, false, move |item| match item {
            Feed::Rows(change) => {
                let rows = change.related.get(&name).and_then(|parents| parents.get(&row_id)).cloned().unwrap_or_default();
                feed.rows(LiveChange::whole(change.revision, rows.iter().map(|row| wire.row(row)).collect()));
            }
            Feed::Count(_) => {}
            Feed::Reached(revision) => feed.reached(revision),
            Feed::Failed(detail) => feed.read_failed(detail),
        })?;
        Ok(LiveHandles { _observer: Some(observer), window: None, _watch: None, load: None, failure: None, state: None })
    }
}
