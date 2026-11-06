use std::collections::HashMap;

/// The detail of the `Network` error a call gets while the network is down.
pub(crate) const NEEDS_NETWORK: &str = "the operation needs the network";

use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use serde_json::Value;
use tokio::runtime::Runtime;
use crate::db::{Db, DbConfig, OpenReport};
use crate::identity::RowId;
use crate::pacing::{Clock, ManualClock, SystemClock};
use crate::query::Query;
use crate::schema::ModelSpec;
use crate::transport::{self as core, EncryptedClient};
use crate::upload::UploadOperations;
use super::open_slot::OpenLease;
use super::transport::ForeignTransport;
use super::upload_wire::WireUploadRequest;
use super::receipt::{WireOperationOutcome, WireOperationReceipt};
use super::wire::{WireModel, WireRow};
use super::{DbError, OnlineSink, Transport};
use crate::owners::life::LifeReply;


pub(super) fn executor_enter() -> Result<tokio::runtime::EnterGuard<'static>, DbError> { Ok(executor()?.enter()) }

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
    crate::task::spawn_on(crate::task::Carry::here(), executor()?.handle(), work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(Into::into)
}

pub(super) async fn cancellable<T: Send + 'static>(work: impl std::future::Future<Output = T> + Send + 'static) -> Result<T, DbError> {
    struct Cancel(tokio::task::AbortHandle);
    impl Drop for Cancel { fn drop(&mut self) { crate::lock::effect(|| self.0.abort()); } }
    let task = crate::task::spawn_on(crate::task::Carry::here(), executor()?.handle(), work);
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
    crate::task::spawn_blocking_on(crate::task::Carry::here(), executor()?.handle(), work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
}

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct Config { pub path: Option<String>, pub data_version: Option<String>, pub cache_rows: u32 }

#[derive(uniffi::Record)]
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64, pub retained_observers: u64, pub refused_events: u64 }

#[derive(uniffi::Object)]
pub struct TestClock { clock: Arc<ManualClock> }

#[uniffi::export]
impl TestClock {
    #[uniffi::constructor]
    pub fn new(start_millis: u64) -> Arc<Self> { Arc::new(Self { clock: Arc::new(ManualClock::new(Duration::from_millis(start_millis))) }) }
    pub fn advance(&self, millis: u64) { self.clock.advance(Duration::from_millis(millis)) }
}

#[derive(Clone, uniffi::Object)]
pub struct Database { pub(super) lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) life: Arc<crate::owners::life::Life>, pub(super) streams: Arc<crate::ffi::admission::Leases>, pub(super) db: Db, pub(super) remote: bool, pub(super) session: Option<Arc<EncryptedClient>>, pub(super) reads: crate::owners::read::ReadOperations, pub(super) uploads: Option<UploadOperations>, pub(super) jobs: Arc<super::upload_rounds::RoundJobsSlot>, pub(super) shows: Arc<super::transfer_status::Shows>, pub(super) outcomes: Arc<Mutex<HashMap<(u64, u64, u64), WireOperationOutcome>>>, wires: Arc<HashMap<String, Arc<WireModel>>> }

pub(super) type Wiring = (Option<Arc<dyn core::Transport>>, Option<Arc<EncryptedClient>>, Option<Arc<dyn core::CableLines>>, Option<Arc<crate::owners::session_seal::SealConfig>>);

impl Database {
    fn start(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, clock, |db, _| {
            #[cfg(test)]
            if transport.is_some() { super::signed_session_tests::signed_session(db); }
            let _ = db;
            Ok((transport.map(|transport| Arc::new(ForeignTransport(transport)) as Arc<dyn core::Transport>), None, None, None))
        })
    }

    pub(super) fn start_with(
        config: Config,
        models_json: String,
        clock: Arc<dyn Clock>,
        wire: impl FnOnce(&Db, &Arc<dyn Clock>) -> Result<Wiring, DbError>,
    ) -> Result<Arc<Self>, DbError> {
        let models: Vec<ModelSpec> = serde_json::from_str(&models_json)?;
        let uploads = upload_steps_of(&models)?;
        let keys: Vec<String> = models.iter().map(|model| model.key.to_string()).collect();
        let db = Db::open_with(config.path.as_deref().map(Path::new), models, config.data_version.as_deref(), DbConfig { cache_rows: config.cache_rows as usize })?;
        db.set_clock(clock.clone());
        let opening = db.admit()?;
        db.expire_blobs_before_use(&opening)?;
        drop(opening);
        let (transport, session, cable, seal) = wire(&db, &clock)?;
        let reads = crate::owners::read::ReadOperations::default();
        let jobs: Arc<super::upload_rounds::RoundJobsSlot> = Arc::default();
        let shows: Arc<super::transfer_status::Shows> = Arc::default();
        let life = Self::build_life(&db, transport.clone(), &session, cable, seal, reads.clone(), (uploads.clone(), jobs.clone(), shows.clone()))?;
        let remote = transport.is_some();
        let wires = keys.into_iter().map(|key| db.spec(&key).map(|spec| (key, Arc::new(WireModel::of(spec)))).map_err(DbError::from)).collect::<Result<_, _>>()?;
        let streams = db.shared.store.lifecycle.clone();
        Ok(Arc::new(Self { lease: Arc::new(OnceLock::new()), life, streams, db, remote, session, reads, uploads, jobs, shows, outcomes: Arc::default(), wires: Arc::new(wires) }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: Config,
        models_json: String,
        transport: Option<Arc<dyn Transport>>,
        encrypted: Option<super::EncryptedConfig>,
        credentials: Option<Arc<dyn super::CredentialStore>>,
        http: Option<Arc<dyn super::EncryptedHttp>>,
        retries: Option<Arc<super::RetryProgress>>,
    ) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, transport, encrypted, credentials, http, retries, Arc::new(SystemClock)).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_clock(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, clock: Arc<TestClock>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, retries: Option<Arc<super::RetryProgress>>) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, transport, encrypted, credentials, http, retries, clock.clock.clone()).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_at(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, retries: Option<Arc<super::RetryProgress>>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        blocking(move || {
            let database = match encrypted {
                Some(encrypted) => {
                    let credentials = credentials.ok_or_else(|| DbError::Invalid { detail: "encrypted requires credentials".into() })?;
                    let http = http.ok_or_else(|| DbError::Invalid { detail: "encrypted requires http".into() })?;
                    let transport = transport.ok_or_else(|| DbError::Invalid { detail: "encrypted requires transport".into() })?;
                    let _entered = executor()?.enter();
                    Self::start_with(config, models_json, clock, |db, clock| {
                        super::encrypted::wire(db, clock, encrypted, http, credentials, transport, retries)
                    })?
                }
                None => {
                    if credentials.is_some() || http.is_some() || retries.is_some() {
                        return Err(DbError::Invalid { detail: "credentials, http and retries require encrypted".into() });
                    }
                    Self::start(config, models_json, transport, clock)?
                }
            };
            Ok(database)
        }).await
    }

    /// F1: the owners of direction A on the threads of the core, started from the booted `Db`. It returns after the first `Publish`.
    fn build_life(db: &Db, transport: Option<Arc<dyn core::Transport>>, session: &Option<Arc<EncryptedClient>>, cable: Option<Arc<dyn core::CableLines>>, seal: Option<Arc<crate::owners::session_seal::SealConfig>>, read_operations: crate::owners::read::ReadOperations, (uploads, jobs, shows): (Option<UploadOperations>, Arc<super::upload_rounds::RoundJobsSlot>, Arc<super::transfer_status::Shows>)) -> Result<Arc<crate::owners::life::Life>, DbError> {
        let executor = executor()?.handle().clone();
        let operations = operations_lookup(&read_operations);
        let transfer = uploads.filter(|_| session.is_some()).map(|operations| crate::owners::write::transfer_executor::TransferPort {
            codec: Arc::new(crate::owners::write::upload_codec::UploadCodec::new(operations, operations_lookup(&read_operations), crate::upload::UploadConfig::default())),
            jobs,
            shows: Arc::new(move |key, shown| shows.put(key, shown)),
            slots: 1,
        });
        let ports = crate::owners::life::Ports {
            db: db.clone(),
            transport,
            executor,
            operations,
            credentials: session.as_ref().map(|client| client.credential_slot()),
            session: session.clone(),
            seal,
            transfer,
            cable,
            read_operations: Some(read_operations),
        };
        let _entered = executor_enter()?;
        let life = Arc::new(crate::owners::life::Life::start(ports).map_err(|error| match error {
            crate::owners::life::LifeError::CorruptManifest(detail) => DbError::CorruptManifest { detail },
            other => DbError::Database { detail: format!("the owners did not start: {other:?}") },
        })?);
        if let Some(client) = session { client.attach(&life); }
        Ok(life)
    }

    pub(super) fn hold(&self, lease: Arc<OpenLease>) { let _ = self.lease.set(lease); }

    pub(super) fn wire(&self, model: &str) -> Result<&Arc<WireModel>, DbError> {
        self.wires.get(model).ok_or_else(|| DbError::Invalid { detail: format!("model {model} is not registered") })
    }
}

fn operations_lookup(reads: &crate::owners::read::ReadOperations) -> crate::owners::life::OperationLookup {
    let reads = reads.clone();
    Arc::new(move |name| reads.get(name).map(|operation| (operation.document.clone(), operation.root_path.clone())))
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
        self.read_in_epoch(epoch, "edge values", move |database| {
            let params = serde_json::from_str(&params_json)?;
            Ok(serde_json::to_string(&database.db.edge_values(&model, &relation, &params, &row_ids)?)?)
        }).await
    }
    pub fn open_report(&self) -> OpenReport { self.db.open_report().clone() }
    pub fn restore_failed(&self) -> Result<Vec<crate::db::DroppedRecord>, DbError> { Ok(self.db.restore_failed()?) }
    pub fn discard_restore_failed(&self, table: String, key: String) -> Result<bool, DbError> { Ok(self.db.discard_restore_failed(&table, &key)?) }
    pub async fn subscription_census(&self) -> Result<SubscriptionCensus, DbError> {
        let (observed, subscriptions, retained, refused_events) = self.life.subscription_census().await.map_err(|_| DbError::Interrupted { detail: "the census was interrupted".into() })?;
        Ok(SubscriptionCensus { observed_queries: observed as u64, subscriptions: subscriptions as u64, retained_observers: retained as u64, refused_events })
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
    pub fn register_operations(&self, defs_json: String) -> Result<(), DbError> {
        if self.life.published().closed { return Err(super::command::closed()); }
        let defs: Vec<Value> = serde_json::from_str(&defs_json)?;
        self.remote()?;
        let mut names = Vec::new();
        let mut declared = Vec::new();
        for def in defs {
            let text = |key: &str| def[key].as_str().map(str::to_string).ok_or_else(|| DbError::Invalid { detail: format!("operation needs {key}") });
            let (name, document) = (text("name")?, text("document")?);
            let mut operation = match def["rootPath"].as_str() {
                Some(root) => crate::owners::read::ReadOperation::new(&name, &document, root),
                None => crate::owners::read::ReadOperation::from_document(&name, &document).ok_or_else(|| DbError::Invalid { detail: format!("operation {name} has no root field") })?,
            };
            if let Some(size) = def.get("pageSize") {
                let size = size.as_u64().and_then(|size| u32::try_from(size).ok()).ok_or_else(|| DbError::Invalid { detail: format!("operation {name} has invalid pageSize") })?;
                operation = operation.paged(size);
            }
            names.push((name.clone(), if document.trim_start().starts_with("mutation") { crate::owners::calls::CallKind::Mutate } else { crate::owners::calls::CallKind::Query }));
            declared.push(operation);
        }
        let registered: Vec<(String, String)> = self.db.shared.specs.values().flat_map(|spec| spec.actions.iter().filter_map(|action| action.mutation.as_ref().filter(|mutation| names.iter().any(|(name, _)| name == *mutation)).map(|_| (spec.key.to_string(), action.name.clone())))).collect();
        let keys = crate::owners::read::preload_keys(&self.db, false).map_err(|error| DbError::Invalid { detail: format!("the preload of the schema is invalid: {error}") })?;
        self.reads.declare(declared);
        self.life.declare(names);
        self.life.register_operations(registered);
        self.life.preload(keys);
        Ok(())
    }

    /// Declared upload operations need their operations registered.
    pub(super) fn check_uploads(&self) -> Result<(), DbError> {
        let Some(uploads) = &self.uploads else { return Ok(()) };
        if self.reads.is_empty() {
            return Err(DbError::Invalid { detail: "uploads are declared but operations is empty: pass the upload GraphQLOperation definitions in operations to open".into() });
        }
        uploads.check(|name| self.reads.get(name).is_some()).map_err(|detail| DbError::Invalid { detail })
    }
    pub async fn run(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        self.remote()?;
        let variables_json = self.offload(move || { serde_json::from_str::<Value>(&variables_json)?; Ok(variables_json) }).await?;
        match self.await_life(self.life.run(epoch, operation, variables_json)).await? {
            LifeReply::Call(Ok(answer)) => Ok(answer),
            LifeReply::Call(Err(error)) => Err(call_failure(error)),
            _ => Err(super::command::ended()),
        }
    }
    pub async fn retry(&self, epoch: u64, model: String, row_id: String, action: Option<String>, source: Option<WireUploadRequest>) -> Result<WireOperationReceipt, DbError> {
        if source.is_some() || action.as_deref().is_some_and(|action| self.db.spec(&model).is_ok_and(|spec| spec.upload_action(action).is_some())) {
            return self.retry_transfer(epoch, model, row_id, action, source).await;
        }
        self.account_call(epoch)?;
        if let Some(receipt) = self.retry_declared_transfer(epoch, &model, &row_id).await? { return Ok(receipt); }
        self.retry_operation(epoch, model, row_id).await
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }

    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { life: Arc::downgrade(&self.life) })) }
    /// Closes the database: every lease ends, every holder closes, every database thread lets go of the file, then the process slot opens for the next database. Closing again returns the same result. The name differs from `close` because Kotlin objects already carry `close()` for their native handle.
    pub async fn close_database(&self) -> Result<(), DbError> {
        if self.life.is_shut() { return Err(super::command::closed()); }
        let waiter = self.life.close();
        let _closing = crate::runtime::hold_until_woken(self.life.launch());
        let answered = on_executor(crate::runtime::counted(Some(self.life.launch()), async move {
            tokio::time::timeout(Duration::from_secs(10), async { waiter.wait().await }).await.map_err(|_| DbError::Invalid { detail: "close timed out while the owners were closing".into() })
        })).await;
        crate::runtime::release_held();
        let answered = answered.and_then(|outcome| match outcome {
            crate::runtime::Outcome::Value(LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Closed))) => Err(super::command::closed()),
            _ => Ok(()),
        });
        let leases = self.streams.clone();
        let _draining = crate::runtime::hold_until_woken(self.life.launch());
        let drained = on_executor(crate::runtime::counted(Some(self.life.launch()), async move {
            tokio::time::timeout(Duration::from_secs(10), leases.settled()).await.map_err(|_| DbError::Invalid { detail: format!("close timed out while leases were still held: {}", leases.held().join("; ")) })
        })).await;
        crate::runtime::release_held();
        let (db, life) = (self.db.clone(), self.life.clone());
        let _shutting = crate::runtime::hold_until_woken(self.life.launch());
        let ended = blocking(move || { life.shutdown(); Ok(db.close()?) }).await;
        crate::runtime::release_held();
        if let Some(lease) = self.lease.get() { lease.release(); }
        answered.and(drained).and(ended)
    }
}

impl Database {
    /// Runs the blocking part of a call. The owners barrier counts the job until the call wakes.
    pub(super) async fn offload<T: Send + 'static>(&self, work: impl FnOnce() -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
        let kept = crate::runtime::hold_until_woken(self.life.launch());
        let value = blocking(work).await;
        crate::runtime::release_held();
        drop(kept);
        value
    }

    /// Waits for the answer of an owner. An answer other than a value ends the call with the end of its epoch.
    pub(super) async fn await_life(&self, waiter: crate::runtime::Waiter<LifeReply>) -> Result<LifeReply, DbError> {
        match waiter.wait().await {
            crate::runtime::Outcome::Value(LifeReply::Closed) | crate::runtime::Outcome::Closing => Err(super::command::closed()),
            crate::runtime::Outcome::EpochEnded(_) => Err(super::command::ended()),
            crate::runtime::Outcome::Internal => Err(DbError::Database { detail: "the answer of the owner was lost".into() }),
            crate::runtime::Outcome::Value(reply) => Ok(reply),
        }
    }

    /// Server reads need a transport.
    pub(super) fn remote(&self) -> Result<(), DbError> {
        if self.remote { Ok(()) } else { Err(DbError::NoSession { detail: "transport is required for server operations".into() }) }
    }
    /// A call that always goes to the server fails at once while the network is away. It does not wait for the network and does not retry.
    pub(super) fn require_online(&self, what: &str) -> Result<(), DbError> {
        if self.db.connectivity().is_online() { return Ok(()); }
        Err(DbError::Network { detail: format!("{what} needs the network") })
    }

    pub(super) fn parsed_query(&self, json: &str) -> Result<crate::db::ParsedQuery, DbError> {
        serde_json::from_str::<Query>(json)?;
        Ok(self.db.parse_query(json)?)
    }
}


/// The failure of `run` as the platform sees it.
fn call_failure(error: crate::owners::calls::CallError) -> DbError {
    use crate::owners::calls::CallError;
    match error {
        CallError::NoSession => DbError::NoSession { detail: "the database has no session".into() },
        CallError::Invalid => DbError::Invalid { detail: "the operation is not registered".into() },
        CallError::Offline => DbError::Network { detail: NEEDS_NETWORK.into() },
        CallError::Network => DbError::Network { detail: "the request did not reach the server".into() },
        CallError::Graphql { messages, extensions } => DbError::Graphql { messages, extensions },
        CallError::Session { code, detail } => DbError::Session { code, detail },
        CallError::Http { status, detail } => DbError::Http { status, detail },
        CallError::Dropped => DbError::Network { detail: "the request left and the answer did not come".into() },
        CallError::InvalidResponse => DbError::Invalid { detail: "the response has no root of the operation".into() },
        CallError::Interrupted => super::command::ended(),
        CallError::Storage => DbError::Database { detail: "the database could not save its state".into() },
    }
}

/// The upload steps the models declare: every model with them carries the same block (the generator copies the one block of the project).
fn upload_steps_of(models: &[ModelSpec]) -> Result<Option<UploadOperations>, DbError> {
    let mut found: Option<&UploadOperations> = None;
    for steps in models.iter().filter_map(|model| model.upload_steps.as_ref()) {
        match found {
            Some(first) if first != steps => return Err(DbError::Invalid { detail: "models declare different upload steps".into() }),
            _ => found = Some(steps),
        }
    }
    Ok(found.cloned())
}
