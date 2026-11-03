use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use serde_json::{Map, Value};
use tokio::runtime::Runtime;
use crate::db::{Db, DbConfig, OpenReport};
use crate::identity::RowId;
use crate::pacing::{Clock, ManualClock, SystemClock};
use crate::query::Query;
use crate::schema::ModelSpec;
use crate::read::LoadMoreVerdict;
use crate::sync::{LoadOptions, LoadStatus, OperationDef, Sync, SyncConfig};
use crate::transport::{self as core, EncryptedClient};
use crate::upload::UploadOperations;
use crate::lifecycle::Lifecycle;
use super::open_slot::OpenLease;
use super::transport::ForeignTransport;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use super::wire::{WireModel, WireRow};
use super::{DbError, LoadMoreResult, OnlineSink, Transport};

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
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64, pub retained_observers: u64 }

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
pub struct Database { lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) streams: Arc<Lifecycle>, pub(super) db: Db, pub(super) sync: Option<Sync>, pub(super) session: Option<Arc<EncryptedClient>>, pub(super) stream_table: Arc<super::stream::StreamTable>, testing: bool, wires: Arc<HashMap<String, Arc<WireModel>>> }

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
        Ok(Arc::new(Self { lease: Arc::new(OnceLock::new()), streams, db, sync, session, stream_table: Arc::default(), testing: false, wires: Arc::new(wires) }))
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

pub(super) fn id(value: &str) -> Result<RowId, DbError> {
    RowId::new(value).map_err(|error| DbError::Invalid { detail: error.to_string() })
}

#[uniffi::export]
impl Database {
    pub fn peek_row(&self, epoch: u64, model: String, row_id: String) -> Result<Option<WireRow>, DbError> {
        self.admit_now(epoch)?;
        let wire = self.wire(&model)?;
        let found = self.db.peek_row(&model, &id(&row_id)?).map(|row| wire.row(&row));
        self.admit_now(epoch)?;
        Ok(found)
    }
    pub async fn edge_values(&self, epoch: u64, model: String, relation: String, params_json: String, row_ids: Vec<String>) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        self.read_in_epoch(epoch, "edge values", move |_| {
            let params = serde_json::from_str(&params_json)?;
            Ok(serde_json::to_string(&sync.edge_values(&model, &relation, &params, &row_ids)?)?)
        }).await
    }
    pub async fn warm(&self, epoch: u64, query_json: String) -> Result<(), DbError> {
        self.read_in_epoch(epoch, "warm", move |this| {
            let (query, _) = this.parsed_query(&query_json)?;
            Ok(this.db.warm(&query)?)
        }).await
    }
    pub fn open_report(&self) -> OpenReport { self.db.open_report().clone() }
    pub fn subscription_census(&self) -> SubscriptionCensus {
        let census = self.db.observer_census();
        SubscriptionCensus { observed_queries: census.observed_queries as u64, subscriptions: census.subscriptions as u64, retained_observers: census.retained as u64 }
    }
    /// Creates a row. A model that keeps the row on the device writes it at once. A model whose create declares a server mutation needs the network, waits for the answer of the server and returns the confirmed row. A durable create (sending a message) goes through `send`.
    pub async fn create(&self, epoch: u64, model: String, values_json: String, args_json: String) -> Result<WireRow, DbError> {
        let declared = self.db.spec(&model)?.actions.iter().find(|action| action.name == "create").cloned();
        let remote = declared.as_ref().is_some_and(|action| action.mutation.is_some());
        if declared.as_ref().is_some_and(|action| action.durable) {
            return Err(DbError::Invalid { detail: format!("{model} sends its create as a durable command: use send") });
        }
        if remote { self.refuse_offline(&model, "create")?; }
        let written = self.write_in_epoch(epoch, "create", { let model = model.clone(); move |database| {
            let row = database.db.create_with(&model, serde_json::from_str(&values_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(row)
        } }).await?;
        if !remote { return Ok(self.wire(&model)?.row(&written)); }
        let sync = self.sync()?.clone();
        let (db, wire, temporary) = (self.db.clone(), self.wire(&model)?.clone(), written.id.clone());
        self.serve_in_epoch(epoch, "create", async move {
            let real = sync.confirmed(&model, &temporary).await?;
            let row = db.find(&model, &real)?.ok_or_else(|| DbError::Invalid { detail: format!("{model} {real} was not created") })?;
            Ok::<_, DbError>(wire.row(&row))
        }).await
    }
    pub async fn update(&self, epoch: u64, model: String, row_id: String, patch_json: String, args_json: String) -> Result<WireRow, DbError> {
        self.refuse_offline(&model, "update")?;
        self.write_in_epoch(epoch, "update", move |database| {
            let row = database.db.update_with(&model, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(database.wire(&model)?.row(&row))
        }).await
    }
    pub async fn set_local(&self, epoch: u64, model: String, row_id: String, path: String, value_json: String) -> Result<(), DbError> {
        self.write_in_epoch(epoch, "set local", move |database| {
            Ok(database.db.set_local(&model, &id(&row_id)?, &path, serde_json::from_str(&value_json)?)?)
        }).await
    }
    /// Queues the action. An action with no optimistic change and no declared result returns after the server answered, with the row at the root of the answer: the receiver row when the answer names the receiver, else the row the answer carried. The receiver row is never overwritten by an answer with another id.
    pub async fn act(&self, epoch: u64, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<QueuedAction, DbError> {
        self.refuse_offline(&model, &action)?;
        let waits = self.db.spec(&model)?.actions.iter().find(|declared| declared.name == action).is_some_and(|declared| declared.returns_answer());
        let (call_model, call_action, receiver) = (model.clone(), action.clone(), row_id.clone());
        let queued = self.write_in_epoch(epoch, "act", move |database| {
            let acted = database.db.act(&model, &action, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?)?;
            database.send_writes();
            Ok(QueuedAction { row: database.wire(&model)?.row(&acted.row), operation: acted.operation })
        }).await?;
        if !waits { return Ok(queued); }
        let (sync, db, wire, operation) = (self.sync()?.clone(), self.db.clone(), self.wire(&call_model)?.clone(), queued.operation);
        self.serve_in_epoch(epoch, "act answer", async move {
            let receiver = id(&receiver)?;
            sync.settled(&call_model, &receiver, Some(&call_action)).await?;
            let answered = match sync.answer_root(operation) {
                Some(Some(root)) => root,
                Some(None) => receiver.clone(),
                None => return Err(DbError::Invalid { detail: format!("the answer to {call_model}.{call_action} was not published") }),
            };
            let row = match db.find(&call_model, &answered)? {
                Some(row) => row,
                None => db.find(&call_model, &receiver)?.ok_or_else(|| DbError::Invalid { detail: format!("{call_model} {receiver} is gone after the answer") })?,
            };
            Ok::<_, DbError>(QueuedAction { row: wire.row(&row), operation })
        }).await
    }
    pub async fn destroy(&self, epoch: u64, model: String, row_id: String) -> Result<(), DbError> {
        self.refuse_offline(&model, "destroy")?;
        self.write_in_epoch(epoch, "destroy", move |database| {
            database.db.destroy(&model, &id(&row_id)?)?;
            database.send_writes();
            Ok(())
        }).await
    }
    pub async fn set_ephemeral(&self, epoch: u64, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        self.write_in_epoch(epoch, "set ephemeral", move |database| {
            Ok(database.db.set_ephemeral(&model, &id(&row_id)?, serde_json::from_str(&values_json)?)?)
        }).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(&self, epoch: u64, model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String) -> Result<String, DbError> {
        let row_id = id(&row_id)?;
        self.write_in_epoch(epoch, "attach", move |database| Ok(database.db.attach(&model, &row_id, &field, &bytes, &name, &mime)?.to_value().to_string())).await
    }
    pub async fn file(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "file", move |database| Ok(database.db.file(&model, &row_id, &field)?.map(|file| file.to_value().to_string()))).await
    }
    pub async fn read(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<Vec<u8>>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "read file", move |database| Ok(database.db.read(&model, &row_id, &field)?)).await
    }
    pub async fn url(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "file url", move |database| Ok(database.db.url(&model, &row_id, &field)?.map(|path| path.to_string_lossy().into_owned()))).await
    }
    pub async fn detach(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<bool, DbError> {
        let row_id = id(&row_id)?;
        self.write_in_epoch(epoch, "detach", move |database| Ok(database.db.detach(&model, &row_id, &field)?)).await
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
    pub async fn run(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let answer = self.serve_in_epoch(epoch, "run", async move {
            let variables: Value = serde_json::from_str(&variables_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.run(&operation, variables).await?)
        }).await?;
        Ok(answer.to_string())
    }
    pub async fn load(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        self.serve_in_epoch(epoch, "load", async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load(&model, &relation, &params, LoadOptions::default()).await?)
        }).await.map(drop)
    }
    pub async fn refresh(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.require_online(&format!("{model}.{relation} refresh"))?;
        let sync = self.sync()?.clone();
        self.serve_in_epoch(epoch, "refresh", async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.refresh(&model, &relation, &params).await?)
        }).await.map(drop)
    }
    pub async fn refresh_preloads(&self, epoch: u64) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let failures = self.serve_in_epoch(epoch, "refresh preloads", async move { Ok::<_, DbError>(sync.refresh_preloads().await?) }).await?;
        if failures.is_empty() { return Ok(()); }
        let scopes: Vec<String> = failures.iter().map(|failure| format!("{}.{} {}: {}", failure.model, failure.relation, serde_json::Value::Object(failure.params.clone()), failure.error)).collect();
        Err(DbError::Sync { detail: format!("{} preload scopes failed to refresh: {}", scopes.len(), scopes.join("; ")) })
    }
    pub async fn load_all(&self, epoch: u64, model: String, relation: String, params_json: String, force: bool) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        self.serve_in_epoch(epoch, "load all", async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_all(&model, &relation, &params, LoadOptions { force }).await?)
        }).await
    }
    pub async fn load_more(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<LoadMoreResult, DbError> {
        self.require_online(&format!("{model}.{relation} load more"))?;
        let sync = self.sync()?.clone();
        let outcome = self.serve_in_epoch(epoch, "load more", async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_more(&model, &relation, &params).await?)
        }).await?;
        Ok(match outcome.status {
            LoadStatus::NoMorePages(LoadMoreVerdict::AlreadyLoading) => LoadMoreResult::AlreadyLoading,
            LoadStatus::NoMorePages(_) => LoadMoreResult::NoMore,
            _ => LoadMoreResult::Loaded { has_more: outcome.has_next },
        })
    }
    pub async fn node(&self, epoch: u64, model: String, relation: String, row_id: String, force: bool) -> Result<Option<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let row = self.serve_in_epoch(epoch, "node", async move { sync.node(&model, &relation, &row_id, force).await }).await?;
        Ok(row.map(|row| wire.row(&row)))
    }
    pub async fn nodes(&self, epoch: u64, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let rows = self.serve_in_epoch(epoch, "nodes", async move { sync.nodes(&model, &relation, &row_ids.iter().map(String::as_str).collect::<Vec<_>>()).await }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }
    pub async fn flush(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.flush().await }).await?;
        Ok(())
    }
    pub async fn retry(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<u64, DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(0) };
        let row = id(&row_id)?;
        let db = self.db.clone();
        let this = self.clone();
        self.serve_in_epoch(epoch, "retry", async move {
            let count = this.pinned_write(epoch, || Ok(db.retry(&model, &row, action.as_deref())?))?;
            sync.flush().await?;
            Ok::<_, DbError>(count as u64)
        }).await
    }
    pub async fn retry_all(&self, epoch: u64) -> Result<u64, DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(0) };
        let db = self.db.clone();
        let this = self.clone();
        self.serve_in_epoch(epoch, "retry all", async move {
            let count = this.pinned_write(epoch, || Ok(db.retry_all()?))?;
            sync.flush().await?;
            Ok::<_, DbError>(count as u64)
        }).await
    }
    pub async fn discard(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        let sync = self.sync.clone();
        self.write_in_epoch(epoch, "discard", move |database| {
            let row = id(&row_id)?;
            database.db.discard(&model, &row, action.as_deref())?;
            if let Some(sync) = &sync {
                let _entered = executor()?.enter();
                sync.inner.start_uploads()?;
            }
            Ok(())
        }).await
    }
    pub async fn resume(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.resume().await }).await?;
        Ok(())
    }
    pub async fn resume_uploads(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        on_executor(async move { sync.resume_uploads().await }).await?;
        Ok(())
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }

    pub fn enter_foreground_for_testing(&self) -> Result<(), DbError> {
        if !self.testing { return Err(DbError::Invalid { detail: "enterForegroundForTesting requires openTesting".into() }); }
        self.online_sink()?.enter_foreground();
        Ok(())
    }
    pub fn set_listening(&self, allowed: bool) -> Result<(), DbError> {
        if let Some(sync) = &self.sync {
            let _entered = executor()?.enter();
            sync.set_listening(allowed);
        }
        Ok(())
    }
    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync.as_ref().map(Sync::downgrade), online: self.db.connectivity() })) }
    pub async fn upload(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<i64, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        let lease = self.streams.lease(crate::lifecycle::Origin::Session, "upload").map_err(|_| DbError::Interrupted { detail: "the session of this upload ended".into() })?;
        if lease.epoch() != epoch { return Err(DbError::Interrupted { detail: "the session of this upload ended".into() }); }
        let work = on_executor(async move {
            let file = request.into_file()?;
            Ok::<_, DbError>(sync.upload_in(&model, &row, &action, file, Some(epoch)).await?)
        });
        lease.run(work).await.map_err(|_| DbError::Interrupted { detail: "the session of this upload ended".into() })?
    }
    pub async fn upload_create_batch(&self, epoch: u64, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        let lease = self.streams.lease(crate::lifecycle::Origin::Session, "upload batch").map_err(|_| DbError::Interrupted { detail: "the session of this upload ended".into() })?;
        if lease.epoch() != epoch { return Err(DbError::Interrupted { detail: "the session of this upload ended".into() }); }
        let work = on_executor(async move {
            let items = items.into_iter().map(WireUploadItem::into_item).collect::<Result<Vec<_>, _>>()?;
            let input = request.into_input()?;
            let rows = sync.upload_create_batch_in(&model, items, input, Some(epoch)).await?;
            Ok::<_, DbError>(rows.iter().map(|row| wire.row(row)).collect())
        });
        lease.run(work).await.map_err(|_| DbError::Interrupted { detail: "the session of this upload ended".into() })?
    }
    pub async fn confirmed(&self, epoch: u64, model: String, row_id: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        self.serve_in_epoch(epoch, "confirmed", async move { Ok::<_, DbError>(sync.confirmed(&model, &row).await?.to_string()) }).await
    }
    pub async fn settled(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        self.serve_in_epoch(epoch, "settled", async move { Ok::<_, DbError>(sync.settled(&model, &row, action.as_deref()).await?) }).await
    }
    pub async fn answer(&self, epoch: u64, model: String, operation: i64) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        self.serve_in_epoch(epoch, "answer", async move { Ok::<_, DbError>(sync.answer(&model, operation).await?.to_string()) }).await
    }
    pub async fn cancel_upload(&self, epoch: u64, model: String, row_id: String, action: String) -> Result<bool, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        self.serve_in_epoch(epoch, "cancel upload", async move { sync.cancel_upload(&model, &row, &action).await }).await
    }
    pub async fn maintain(&self) -> Result<(), DbError> {
        let db = self.db.clone();
        blocking(move || Ok(db.maintain(&[]).map(|_| ())?)).await
    }
    /// Closes the database: every lease ends, every holder closes, the process slot opens for the next database. Closing again returns the same result. The name differs from `close` because Kotlin objects already carry `close()` for their native handle.
    pub async fn close_database(&self) -> Result<(), DbError> {
        let finished = self.close_holders();
        let lifecycle = self.streams.clone();
        let drained = on_executor(async move {
            tokio::time::timeout(Duration::from_secs(10), lifecycle.settled()).await.map_err(|_| DbError::Invalid { detail: format!("close timed out while leases were still held: {}", lifecycle.held().join("; ")) })
        }).await;
        if finished { if let Some(lease) = self.lease.get() { lease.release(); } }
        drained
    }
}

impl Database {
    /// Closes the lifecycle and every holder. True when this call finished the close.
    fn close_holders(&self) -> bool {
        let mut finished = self.streams.close();
        if let Some(sync) = &self.sync { finished &= sync.close(); }
        finished
    }

    /// The part of `close` that does not wait: for a caller that cannot await.
    #[cfg(test)]
    pub(super) fn close_now(&self) {
        if self.close_holders() { if let Some(lease) = self.lease.get() { lease.release(); } }
    }

    pub(super) fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "transport is required for server operations".into() }) }
    pub(super) fn send_writes(&self) {
        if let (Some(sync), Ok(executor)) = (self.sync.as_ref(), executor()) {
            let _entered = executor.enter();
            sync.inner.spawn_flush(Duration::ZERO);
        }
    }
    /// A call that always goes to the server fails at once while the network is away. It does not wait for the network and does not retry.
    pub(super) fn require_online(&self, what: &str) -> Result<(), DbError> {
        if self.db.connectivity().is_online() { return Ok(()); }
        Err(DbError::Network { detail: format!("{what} needs the network") })
    }

    /// An action that needs the server fails at once while the network is away. Only sending a message and a media upload wait for it (`send`, `upload`).
    fn refuse_offline(&self, model: &str, action: &str) -> Result<(), DbError> {
        let declared = self.db.spec(model)?.actions.iter().any(|declared| declared.name == action && declared.mutation.is_some());
        if declared && !self.db.connectivity().is_online() {
            return Err(DbError::Network { detail: format!("{model}.{action} needs the network") });
        }
        Ok(())
    }

    pub(super) fn parsed_query(&self, json: &str) -> Result<ParsedQuery, DbError> {
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
}

