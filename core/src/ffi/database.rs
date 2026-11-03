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
use super::related::RelatedProjection;
use crate::lifecycle::Lifecycle;
use super::open_slot::OpenLease;
use super::transport::ForeignTransport;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use super::wire::{WireModel, WireRow};
use super::{DbError, LiveChange, LiveSubscription, LoadMoreResult, OnlineSink, QueryListener, Transport};

type ParsedQuery = (Query, Option<(String, Map<String, Value>)>);

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

pub(super) async fn on_executor<T: Send + 'static, E: Into<DbError> + Send + 'static>(work: impl std::future::Future<Output = Result<T, E>> + Send + 'static) -> Result<T, DbError> {
    executor()?.spawn(work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(Into::into)
}

pub(super) async fn cancellable<T: Send + 'static>(work: impl std::future::Future<Output = T> + Send + 'static) -> Result<T, DbError> {
    struct Cancel(tokio::task::AbortHandle);
    impl Drop for Cancel { fn drop(&mut self) { crate::db::lock::effect(|| self.0.abort()); } }
    let task = executor()?.spawn(work);
    let _cancel = Cancel(task.abort_handle());
    task.await.map_err(|error| DbError::Invalid { detail: error.to_string() })
}

pub(super) async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
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
pub struct Database { lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) streams: Arc<Lifecycle>, pub(super) db: Db, pub(super) sync: Option<Sync>, pub(super) session: Option<Arc<EncryptedClient>>, pub(super) clock: Arc<dyn Clock>, pub(super) entity_cache: Arc<super::entity_pool::EntityCache>, testing: bool, wires: Arc<HashMap<String, Arc<WireModel>>> }

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
        let streams = db.shared.store.lifecycle.clone();
        Ok(Arc::new(Self { lease: Arc::new(OnceLock::new()), streams, db, sync, session, clock, entity_cache: super::entity_pool::EntityCache::new(), testing: false, wires: Arc::new(wires) }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: Config,
        models_json: String,
        transport: Option<Arc<dyn Transport>>,
        encrypted: Option<super::EncryptedConfig>,
        credentials: Option<Arc<dyn super::CredentialStore>>,
        http: Option<Arc<dyn super::EncryptedHttp>>,
        uploads: Option<UploadOperations>,
        retries: Option<Arc<super::RetryProgress>>,
    ) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, transport, encrypted, credentials, http, uploads, retries, Arc::new(SystemClock), false).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_clock(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<UploadOperations>, clock: Arc<TestClock>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, retries: Option<Arc<super::RetryProgress>>) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, transport, encrypted, credentials, http, uploads, retries, clock.clock.clone(), true).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_at(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, uploads: Option<UploadOperations>, retries: Option<Arc<super::RetryProgress>>, clock: Arc<dyn Clock>, testing: bool) -> Result<Arc<Self>, DbError> {
        blocking(move || {
            let database = match encrypted {
                Some(encrypted) => {
                    let credentials = credentials.ok_or_else(|| DbError::Invalid { detail: "encrypted requires credentials".into() })?;
                    let http = http.ok_or_else(|| DbError::Invalid { detail: "encrypted requires http".into() })?;
                    let transport = transport.ok_or_else(|| DbError::Invalid { detail: "encrypted requires transport".into() })?;
                    let _entered = executor()?.enter();
                    Self::start_with(config, models_json, uploads, clock, |db, clock| {
                        super::encrypted::wire(db, clock, encrypted, http, credentials, transport, retries)
                    })?
                }
                None => {
                    if credentials.is_some() || http.is_some() || retries.is_some() {
                        return Err(DbError::Invalid { detail: "credentials, http and retries require encrypted".into() });
                    }
                    Self::start(config, models_json, transport, uploads, clock)?
                }
            };
            Ok(Arc::new(Self { testing, ..Arc::unwrap_or_clone(database) }))
        }).await
    }

    pub(super) fn hold(&self, lease: Arc<OpenLease>) { let _ = self.lease.set(lease); }

    pub(super) fn wire(&self, model: &str) -> Result<&Arc<WireModel>, DbError> {
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
            self.connect(Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))), None, false, None, LoadOptions::default(), feed)
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
        super::entity_calls::entered();
        let source = entities::open(self.clone(), &model, self.wire(&model)?.clone(), listener)?;
        self.streams.enroll(&source);
        Ok(source)
    }
    pub fn live_query(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.connect_json(&query_json, false, None, feed))
    }
    pub fn live_window(&self, query_json: String, tail: u32, max: Option<u32>, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.window_handles(&query_json, (tail, max), None, feed))
    }
    pub fn live_window_related(&self, query_json: String, tail: u32, max: Option<u32>, name: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.window_handles(&query_json, (tail, max), Some(name), feed))
    }
    pub fn live_count(&self, query_json: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.connect_json(&query_json, true, None, feed))
    }
    pub fn live_related(&self, query_json: String, name: String, listener: Arc<dyn QueryListener>) -> Arc<LiveSubscription> {
        self.subscribed(listener, |feed| self.connect_json(&query_json, false, Some(name), feed))
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
        let _entered = executor()?.enter();
        sync.check_session();
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
    pub async fn refresh_preloads(&self) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let failures = on_executor(async move { sync.refresh_preloads().await }).await?;
        if failures.is_empty() { return Ok(()); }
        let scopes: Vec<String> = failures.iter().map(|failure| format!("{}.{} {}: {}", failure.model, failure.relation, serde_json::Value::Object(failure.params.clone()), failure.error)).collect();
        Err(DbError::Sync { detail: format!("{} preload scopes failed to refresh: {}", scopes.len(), scopes.join("; ")) })
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
    pub fn put_tasks(&self, tasks: Vec<crate::transport::PutTask>) {
        if let Some(sync) = &self.sync { sync.put_tasks(tasks); }
    }
    pub async fn resume_uploads(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.resume_uploads().await }).await?;
        Ok(())
    }
    pub async fn resume_uploads_for(&self, tags: Vec<String>) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.resume_uploads_for(tags).await }).await?;
        Ok(())
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }

    pub fn enter_foreground_for_testing(&self) -> Result<(), DbError> {
        if !self.testing { return Err(DbError::Invalid { detail: "enterForegroundForTesting requires openTesting".into() }); }
        self.online_sink()?.enter_foreground();
        Ok(())
    }
    pub async fn reset_for_testing(&self) -> Result<(), DbError> {
        if !self.testing { return Err(DbError::Invalid { detail: "resetForTesting requires openTesting".into() }); }
        let database = self.db.clone();
        blocking(move || { database.reset_all()?; Ok(()) }).await
    }

    pub async fn settle_for_testing(&self) -> Result<(), DbError> {
        if !self.testing { return Err(DbError::Invalid { detail: "settleForTesting requires openTesting".into() }); }
        let database = self.clone();
        on_executor(async move {
            let mut busy = "background work";
            let finished = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(sync) = &database.sync {
                        busy = "resume";
                        sync.resume_settled().await;
                        busy = "load flights";
                        sync.inner.app_loads_settled().await;
                    }
                    let checking = database.clone();
                    busy = match blocking(move || checking.testing_busy()).await? {
                        Some(part) => part,
                        None => return Ok::<(), DbError>(()),
                    };
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await;
            match finished {
                Ok(result) => result,
                Err(_) => Err(DbError::Invalid { detail: format!("settleForTesting timed out: {busy}") }),
            }
        }).await
    }
    pub fn set_listening(&self, allowed: bool) -> Result<(), DbError> {
        if let Some(sync) = &self.sync {
            let _entered = executor()?.enter();
            sync.set_listening(allowed);
        }
        Ok(())
    }
    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync.as_ref().map(Sync::downgrade), online: self.db.connectivity() })) }
    pub async fn upload(&self, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<i64, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        on_executor(async move {
            let file = request.into_file()?;
            Ok::<_, DbError>(sync.upload(&model, &row, &action, file).await?)
        })
        .await
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
    pub fn shutdown(&self) {
        self.streams.close();
        if let Some(sync) = &self.sync { sync.close(); }
        if let Some(lease) = self.lease.get() { lease.release(); }
    }
    pub async fn logout(&self) -> Result<(), DbError> {
        if let Some(sync) = &self.sync {
            let _entered = executor()?.enter();
            sync.session_ended();
        }
        let database = self.clone();
        blocking(move || {
            database.db.reset_all()?;
            database.db.await_revision(database.db.revision_seq()?);
            Ok(())
        }).await
    }
}

impl Database {
    pub(super) fn testing_busy(&self) -> Result<Option<&'static str>, DbError> {
        if let Some(sync) = &self.sync {
            let inner = &sync.inner;
            if !crate::sync::locked(&inner.flights).waiting.is_empty() { return Ok(Some("load flights")); }
            if !inner.resume.idle() { return Ok(Some("resume")); }
            if !inner.preload.idle() { return Ok(Some("preload")); }
            if inner.connectivity.is_online() {
                let queue = inner.operation_queue().map_err(DbError::from)?;
                let queue = crate::sync::locked(queue);
                if queue.entries.values().any(|entry| entry.uploading && entry.operation.status == crate::operation::OperationStatus::Pending && !inner.is_waiting(&entry.operation)) {
                    return Ok(Some("uploads"));
                }
                if queue.pending.iter().filter_map(|seq| queue.entries.get(seq)).any(|entry| !inner.is_waiting(&entry.operation) && !queue.blocked_before(&entry.operation).0 && !queue.blocked_before(&entry.operation).1) {
                    return Ok(Some("outbox send"));
                }
            }
            if inner.outbox_gate.try_lock().is_err() { return Ok(Some("outbox send")); }
            let (uploads_idle, finished) = crate::sync::locked(&inner.uploads).idle();
            for result in finished {
                result.map_err(|error| DbError::Invalid { detail: format!("upload task failed: {error}") })?;
            }
            if !uploads_idle || *inner.file_tasks.borrow() != 0 || !inner.upload_effects.is_idle() {
                return Ok(Some("uploads"));
            }
            if !inner.live_effects.is_idle() { return Ok(Some("live listeners")); }
        }
        if let Some(part) = self.db.testing_busy()? { return Ok(Some(part)); }
        if self.streams.busy() { return Ok(Some("listeners")); }
        Ok(None)
    }

    pub(super) fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "transport is required for server operations".into() }) }
    fn send_writes(&self) {
        if let (Some(sync), Ok(executor)) = (self.sync.as_ref(), executor()) {
            let _entered = executor.enter();
            sync.inner.spawn_flush(Duration::ZERO);
        }
    }
    fn parsed_query(&self, json: &str) -> Result<ParsedQuery, DbError> {
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
        if let Ok(sync) = self.sync() { feed.hold_first_screen(sync.first_screen()); }
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
        let subscription = Arc::new(LiveSubscription { feed, handles: Mutex::new(handles), registration: Default::default() });
        self.streams.enroll(&subscription);
        subscription
    }
    fn window_handles(&self, query_json: &str, (tail, max): (u32, Option<u32>), related: Option<String>, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let (query, relation) = self.parsed_query(query_json)?;
        let members = relation.as_ref().is_some_and(|(name, _)| self.db.spec(query.model.as_str()).is_ok_and(|spec| spec.relations.iter().any(|relation| relation.name == *name && relation.members_complete)));
        if members && (tail > 200 || max.is_some_and(|max| max > 200 || max < tail)) {
            return Err(DbError::Invalid { detail: "members_complete windows require tail <= max <= 200".into() });
        }
        let description: Value = serde_json::from_str(query_json)?;
        let force = description.get("refreshOnConnect").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or(false);
        let _entered = executor()?.enter();
        let model = query.model.to_string();
        let mut tracked = None;
        let mut watch = match (self.sync.as_ref(), relation) {
            (Some(sync), Some((relation, params))) => {
                let watch = sync.watch(&model, &relation, &params)?;
                if self.db.spec(&model)?.relations.iter().any(|declared| declared.name == relation && declared.remote.is_some()) {
                    feed.expect_load();
                    tracked = Some((sync.clone(), relation, params));
                }
                Some(watch)
            }
            _ => None,
        };
        let paused = tracked.is_some();
        let model_key = model.clone();
        let pager = Arc::new(Pager { page: tracked.clone().map(|(sync, relation, params)| (sync, model.clone(), relation, params)), busy: std::sync::atomic::AtomicBool::new(false), control: OnceLock::new() });
        let wire = self.wire(&model)?.clone();
        let sink = feed.clone();
        let spec = if members { WindowSpec::Members { size: tail as usize, max: max.unwrap_or(200) as usize } } else { WindowSpec::Tail { size: tail as usize, max: max.map(|max| max as usize), paged: paused } };
        let (window, targets) = match related {
            None => (self.db.observe_window_feed(query, spec, paused, {
                let pager = pager.clone();
                move |item| {
                    let wanted = match &item { Feed::Rows(change) => change.window.map_or(0, |edge| edge.wanted_before), _ => 0 };
                    intake(&sink, &wire, item);
                    pager.wanted(wanted);
                }
            })?, None),
            Some(name) => {
                let association = self.db.spec(&model)?.associations.iter().find(|association| association.name == name).ok_or_else(|| DbError::Invalid { detail: format!("{model} has no association {name}") })?;
                let (target, kind) = (association.target.to_string(), association.kind);
                feed.show_targets_only();
                let projection = Arc::new(Mutex::new(RelatedProjection::new(name.clone(), kind, self.db.shared.counters.clone())));
                let target_wire = self.wire(&target)?.clone();
                let held: Arc<Mutex<HashMap<RowId, crate::db::ObserverHandle>>> = Arc::default();
                let db = self.db.clone();
                let window = self.db.observe_window_feed(query, spec, paused, {
                    let held = held.clone();
                    let pager = pager.clone();
                    move |item| match item {
                        Feed::Rows(mut change) => {
                            match db.shared.related_for(&model, &name, &change.rows) {
                                Ok(found) => change.related = found,
                                Err(error) => return intake(&sink, &wire, Feed::Failed(error.to_string())),
                            }
                            let (shown, (gained, lost)) = {
                                let mut projection = projection.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                (projection.apply(&change, &target_wire), projection.take_watch_delta())
                            };
                            let mut retired = Vec::new();
                            {
                                let mut held = held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                for id in lost { retired.extend(held.remove(&id)); }
                            }
                            let opened: Vec<_> = gained.into_iter().filter_map(|id| {
                                let (projection, sink, target_wire) = (projection.clone(), sink.clone(), target_wire.clone());
                                let known = id.clone();
                                db.observe_row(&target, &id, move |seen| {
                                    let row = seen.rows.first().cloned();
                                    let shown = projection.lock().unwrap_or_else(std::sync::PoisonError::into_inner).target_changed(&known, row, seen.revision, &target_wire);
                                    if let Some(shown) = shown { sink.rows(shown); }
                                }).ok().map(|observer| (id, observer))
                            }).collect();
                            {
                                let mut held = held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                for (id, observer) in opened { retired.extend(held.insert(id, observer)); }
                            }
                            drop(retired);
                            match shown {
                                Some(shown) => sink.rows(shown),
                                None => sink.reached(change.revision),
                            }
                            pager.wanted(change.window.map_or(0, |edge| edge.wanted_before));
                        }
                        item => intake(&sink, &wire, item),
                    }
                })?;
                (window, Some(held))
            }
        };
        feed.on_rejoin(Arc::new(window.rejoiner()));
        let _ = pager.control.set(window.control());
        let (load, state) = self.follow_load(tracked, model_key, LoadOptions { force }, &feed, window.control())?;
        let failure = follow_failures(watch.as_mut(), feed);
        Ok(LiveHandles { _observer: None, window: Some(window), _watch: watch, load, failure, state, targets })
    }
    fn follow_load(&self, tracked: Option<(Sync, String, Map<String, Value>)>, model: String, options: LoadOptions, feed: &Arc<LiveFeed>, control: crate::db::ObserverControl) -> Result<(Option<tokio::task::AbortHandle>, Option<tokio::task::AbortHandle>), DbError> {
        Ok(match tracked {
            Some((sync, relation, params)) => {
                let mut transitions = self.streams.watch();
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
                    let seed = changes.borrow_and_update().clone();
                    follower.state(seed);
                    control.resume();
                    crate::db::lock::effect(|| { let _ = ready.send((sync, model, relation, params, changes.clone())); });
                    while changes.changed().await.is_ok() {
                        let current = changes.borrow_and_update().clone();
                        follower.state(current);
                    }
                }).abort_handle();
                let load = executor()?.spawn(async move {
                    let Ok((sync, model, relation, params, receiver)) = seeded.await else { return };
                    let mut force = options.force;
                    loop {
                        let snapshot = transitions.current();
                        if snapshot.closed { return; }
                        if !snapshot.admits(crate::lifecycle::Origin::Observed) {
                            loader.load_finished(None);
                            if transitions.changed().await.is_none() { return; }
                            continue;
                        }
                        let resolved = match sync.inner.resolve(crate::lifecycle::Origin::Observed, &model, &relation, &params) {
                            Ok(resolved) => resolved,
                            Err(error) => { loader.load_finished(Some(DbError::from(error))); return; }
                        };
                        let app_load = sync.app_load();
                        loader.reset_load();
                        let concerning = async {
                            loop {
                                match transitions.changed().await {
                                    None => break None,
                                    Some(next) if next.concerns(&model) => break Some(next),
                                    Some(_) => {}
                                }
                            }
                        };
                        let result = tokio::select! {
                            biased;
                            changed = concerning => {
                                if changed.is_none() { return; }
                                continue;
                            }
                            result = sync.inner.load_scope(&resolved, force) => result,
                        };
                        force = false;
                        let settled = receiver.borrow().clone();
                        loader.state(settled);
                        loader.load_finished(result.err().map(DbError::from));
                        drop(app_load);
                        loop {
                            let Some(next) = transitions.changed().await else { return };
                            if next.closed { return; }
                            if next.concerns(&model) { break; }
                        }
                    }
                }).abort_handle();
                (Some(load), Some(state))
            }
            None => (None, None),
        })
    }
    fn connect_json(&self, json: &str, count: bool, related: Option<String>, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let (query, remote) = self.parsed_query(json)?;
        let description: Value = serde_json::from_str(json)?;
        let force = description.get("refreshOnConnect").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or(false);
        self.connect(query, remote, count, related, LoadOptions { force }, feed)
    }
    fn connect(&self, mut query: Query, remote: Option<(String, Map<String, Value>)>, count: bool, related: Option<String>, options: LoadOptions, feed: Arc<LiveFeed>) -> Result<LiveHandles, DbError> {
        let _entered = executor()?.enter();
        let model = query.model.to_string();
        let projection = related.map(|name| -> Result<_, DbError> {
            let association = self.db.spec(&model)?.associations.iter().find(|association| association.name == name).ok_or_else(|| DbError::Invalid { detail: format!("{model} has no association {name}") })?;
            let (target, kind) = (association.target.to_string(), association.kind);
            query.include = vec![name.clone()];
            feed.show_targets_only();
            Ok((Mutex::new(RelatedProjection::new(name, kind, self.db.shared.counters.clone())), self.wire(&target)?.clone()))
        }).transpose()?;
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
        let observer = self.db.observe_query_feed(query, count, tracked.is_some(), move |item| match (&projection, item) {
            (Some((projection, target)), Feed::Rows(change)) => {
                let shown = projection.lock().unwrap_or_else(std::sync::PoisonError::into_inner).apply(&change, target);
                match shown {
                    Some(targets) => sink.rows(targets),
                    None => sink.reached(change.revision),
                }
            }
            (Some(_), Feed::Count(_)) => {}
            (_, item) => intake(&sink, &wire, item),
        })?;
        feed.on_rejoin(Arc::new(observer.rejoiner()));
        let (load, state) = self.follow_load(tracked, model, options, &feed, observer.control())?;
        let failure = follow_failures(watch.as_mut(), feed);
        Ok(LiveHandles { _observer: Some(observer), window: None, _watch: watch, load, failure, state, targets: None })
    }
}

struct Pager {
    page: Option<(Sync, String, String, Map<String, Value>)>,
    busy: std::sync::atomic::AtomicBool,
    control: OnceLock<crate::db::ObserverControl>,
}

impl Pager {
    /// The window asked for `rows` older rows that the database does not hold: one shared page is loaded and the window grows by what is still missing.
    fn wanted(self: &Arc<Self>, rows: usize) {
        use std::sync::atomic::Ordering;
        let Some((sync, model, relation, params)) = self.page.clone().filter(|_| rows > 0) else { return };
        if self.busy.swap(true, Ordering::SeqCst) { return }
        let Ok(runtime) = executor() else {
            self.busy.store(false, Ordering::SeqCst);
            return;
        };
        let pager = self.clone();
        runtime.spawn(async move {
            let loaded = sync.load_more(&model, &relation, &params).await;
            pager.busy.store(false, Ordering::SeqCst);
            if let (Ok(outcome), Some(control)) = (loaded, pager.control.get()) {
                if !matches!(outcome.status, LoadStatus::NoMorePages(_)) { control.grow(rows, 0); }
            }
        });
    }
}
