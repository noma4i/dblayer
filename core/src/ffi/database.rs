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
use crate::owners::read::windows::{Goal, WindowKey, WindowOutcome};
use crate::sync::{LoadOptions, OperationDef, Sync, SyncConfig};
use crate::transport::{self as core, EncryptedClient};
use crate::upload::UploadOperations;
use super::open_slot::OpenLease;
use super::transport::ForeignTransport;
use super::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use super::wire::{WireModel, WireRow};
use super::{DbError, LoadMoreResult, OnlineSink, Transport};
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
    impl Drop for Cancel { fn drop(&mut self) { crate::db::lock::effect(|| self.0.abort()); } }
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
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64, pub retained_observers: u64 }

/// What a receipt waits on: the old stack (uploads) or the owner Outbox.
#[derive(Clone)]
pub(super) enum Ticket {
    Sync(Arc<crate::sync::OutcomeTicket>),
    Outbox(crate::owners::write::outbox::Receipt),
}

#[derive(uniffi::Object)]
pub struct WireReceiptTicket {
    pub(super) ticket: Option<Ticket>,
    pub(super) streams: Arc<crate::ffi::legacy::Leases>,
    pub(super) epoch: u64,
    pub(super) model: String,
    pub(super) operation: Option<i64>,
    pub(super) revision: i64,
    pub(super) optimistic_value: Option<WireRow>,
}

impl std::fmt::Debug for WireReceiptTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WireReceiptTicket").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireOperationReceipt {
    pub epoch: u64,
    pub model: String,
    pub operation: Option<i64>,
    pub revision: i64,
    pub optimistic_value: Option<WireRow>,
    pub ticket: Arc<WireReceiptTicket>,
}

pub(super) fn receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Arc<crate::sync::OutcomeTicket>>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    held_receipt(epoch, model, operation, revision, optimistic_value, ticket.map(Ticket::Sync), streams)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn receipt_of(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<crate::owners::write::outbox::Receipt>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    held_receipt(epoch, model, operation, revision, optimistic_value, ticket.map(Ticket::Outbox), streams)
}

fn held_receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Ticket>, streams: Arc<crate::ffi::legacy::Leases>) -> WireOperationReceipt {
    let held = WireReceiptTicket { ticket, streams, epoch, model: model.clone(), operation, revision, optimistic_value: optimistic_value.clone() };
    WireOperationReceipt { epoch, model, operation, revision, optimistic_value, ticket: Arc::new(held) }
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireOperationOutcome {
    Confirmed { value: Option<WireRow>, answer_json: Option<String> },
    Rejected { failure: super::wire::WireFailure },
    DeliveryUnknown,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, uniffi::Record)]
pub struct ItemId { pub value: String }

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireItemReceipt { pub item: ItemId, pub optimistic_value: WireRow }

#[derive(uniffi::Object)]
pub struct WireBatchTicket {
    receipt: crate::sync::OperationReceipt,
    model: String,
    items: Vec<WireItemReceipt>,
}

impl std::fmt::Debug for WireBatchTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WireBatchTicket").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireBatchReceipt {
    pub epoch: u64,
    pub operation: i64,
    pub revision: i64,
    pub items: Vec<WireItemReceipt>,
    pub ticket: Arc<WireBatchTicket>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireItemOutcome {
    Confirmed { item: ItemId, value: WireRow },
    Discarded { item: ItemId },
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireBatchResult {
    Confirmed,
    Rejected { failure: super::wire::WireFailure },
    DeliveryUnknown,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireBatchOutcome { pub result: WireBatchResult, pub items: Vec<WireItemOutcome> }

/// The interest of one caller in a load that `Database::begin_load` started.
#[derive(uniffi::Object)]
pub struct LoadRequest {
    outcome: tokio::sync::watch::Receiver<Option<Result<(), DbError>>>,
    held: Mutex<Option<tokio::task::AbortHandle>>,
}

fn load_cancelled() -> DbError { DbError::Interrupted { detail: "the load was cancelled".into() } }

#[uniffi::export]
impl LoadRequest {
    /// Waits for the end of the load. A request cancelled before its end fails with `Interrupted`.
    pub async fn result(&self) -> Result<(), DbError> {
        let mut outcome = self.outcome.clone();
        let seen = outcome.wait_for(Option::is_some).await.map_err(|_| load_cancelled())?.clone();
        seen.unwrap_or_else(|| Err(load_cancelled()))
    }

    /// Gives up this caller's interest. The request goes on while another caller waits for it.
    pub fn cancel(&self) {
        let held = crate::db::locked(&self.held).take();
        let Some(task) = held else { return };
        crate::db::lock::effect(|| task.abort());
    }
}

impl Drop for LoadRequest {
    fn drop(&mut self) { self.cancel(); }
}

struct LoadCall {
    epoch: u64,
    waiter: crate::runtime::Waiter<LifeReply>,
    leave: LeaveLoad,
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

#[derive(uniffi::Object)]
pub struct TestClock { clock: Arc<ManualClock> }

#[uniffi::export]
impl TestClock {
    #[uniffi::constructor]
    pub fn new(start_millis: u64) -> Arc<Self> { Arc::new(Self { clock: Arc::new(ManualClock::new(Duration::from_millis(start_millis))) }) }
    pub fn advance(&self, millis: u64) { self.clock.advance(Duration::from_millis(millis)) }
}

#[derive(Clone, uniffi::Object)]
pub struct Database { pub(super) lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) life: Arc<crate::owners::life::Life>, pub(super) streams: Arc<crate::ffi::legacy::Leases>, pub(super) db: Db, pub(super) sync: Option<Sync>, pub(super) session: Option<Arc<EncryptedClient>>, pub(super) stream_table: Arc<super::stream::StreamTable>, pub(super) reads: crate::owners::read::ReadOperations, pub(super) outcomes: Arc<Mutex<HashMap<(u64, u64, u64), WireOperationOutcome>>>, wires: Arc<HashMap<String, Arc<WireModel>>> }

pub(super) type Wiring = (Option<Arc<dyn core::Transport>>, Option<Arc<EncryptedClient>>);

impl Database {
    fn start(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<UploadOperations>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, uploads, clock, |db, _| {
            #[cfg(test)]
            if transport.is_some() { super::signed_session_tests::signed_session(db); }
            let _ = db;
            Ok((transport.map(|transport| Arc::new(ForeignTransport(transport)) as Arc<dyn core::Transport>), None))
        })
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
        let opening = db.admit()?;
        db.expire_blobs_before_use(&opening)?;
        drop(opening);
        let (transport, session) = wire(&db, &clock)?;
        if uploads.is_some() && transport.is_none() {
            return Err(DbError::Invalid { detail: "upload operations require a transport".into() });
        }
        let operations: Arc<OnceLock<crate::sync::WeakSync>> = Arc::default();
        let reads = crate::owners::read::ReadOperations::default();
        let life = Self::build_life(&db, transport.clone(), &session, operations.clone(), reads.clone())?;
        let sync = transport.map(|transport| Sync::new(db.clone(), transport, clock.clone(), SyncConfig { uploads, ..SyncConfig::default() }));
        if let Some(sync) = &sync { let _ = operations.set(sync.downgrade()); }
        let wires = keys.into_iter().map(|key| db.spec(&key).map(|spec| (key, Arc::new(WireModel::of(spec)))).map_err(DbError::from)).collect::<Result<_, _>>()?;
        let streams = db.shared.store.lifecycle.clone();
        Ok(Arc::new(Self { lease: Arc::new(OnceLock::new()), life, streams, db, sync, session, stream_table: Arc::default(), reads, outcomes: Arc::default(), wires: Arc::new(wires) }))
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
        Self::open_at(config, models_json, transport, encrypted, credentials, http, uploads, retries, Arc::new(SystemClock)).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_clock(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<UploadOperations>, clock: Arc<TestClock>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, retries: Option<Arc<super::RetryProgress>>) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, transport, encrypted, credentials, http, uploads, retries, clock.clock.clone()).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_at(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, encrypted: Option<super::EncryptedConfig>, credentials: Option<Arc<dyn super::CredentialStore>>, http: Option<Arc<dyn super::EncryptedHttp>>, uploads: Option<UploadOperations>, retries: Option<Arc<super::RetryProgress>>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
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
            Ok(database)
        }).await
    }

    /// F1: the owners of direction A on the threads of the core, started from the booted `Db`. It returns after the first `Publish`.
    fn build_life(db: &Db, transport: Option<Arc<dyn core::Transport>>, session: &Option<Arc<EncryptedClient>>, lookup: Arc<OnceLock<crate::sync::WeakSync>>, read_operations: crate::owners::read::ReadOperations) -> Result<Arc<crate::owners::life::Life>, DbError> {
        let executor = executor()?.handle().clone();
        let operations: crate::owners::life::OperationLookup = Arc::new(move |name| {
            let sync = lookup.get()?.upgrade()?;
            let definition = crate::db::locked(&sync.inner.operations).get(name).cloned()?;
            Some((definition.document.clone(), definition.root_path.clone()))
        });
        let ports = crate::owners::life::Ports {
            db: db.clone(),
            transport,
            executor,
            operations,
            credentials: session.as_ref().map(|client| client.credential_slot()),
            session: session.clone(),
            transfer: None,
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
    pub fn open_report(&self) -> OpenReport { self.db.open_report().clone() }
    pub fn restore_failed(&self) -> Result<Vec<crate::db::DroppedRecord>, DbError> { Ok(self.db.restore_failed()?) }
    pub fn discard_restore_failed(&self, table: String, key: String) -> Result<bool, DbError> { Ok(self.db.discard_restore_failed(&table, &key)?) }
    pub fn subscription_census(&self) -> SubscriptionCensus {
        let census = self.db.observer_census();
        SubscriptionCensus { observed_queries: census.observed_queries as u64, subscriptions: census.subscriptions as u64, retained_observers: census.retained as u64 }
    }
    pub async fn set_ephemeral(&self, epoch: u64, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        self.write_in_epoch(crate::ffi::legacy::Origin::Explicit, epoch, "set ephemeral", move |database, admitted| {
            Ok(database.db.set_ephemeral(admitted, &model, &id(&row_id)?, serde_json::from_str(&values_json)?)?)
        }).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(&self, epoch: u64, model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String) -> Result<String, DbError> {
        let row_id = id(&row_id)?;
        self.write_in_epoch(crate::ffi::legacy::Origin::Explicit, epoch, "attach", move |database, admitted| Ok(database.db.attach(admitted, &model, &row_id, &field, &bytes, &name, &mime)?.to_value().to_string())).await
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
        self.write_in_epoch(crate::ffi::legacy::Origin::Explicit, epoch, "detach", move |database, admitted| Ok(database.db.detach(admitted, &model, &row_id, &field)?)).await
    }
    pub fn register_operations(&self, defs_json: String) -> Result<(), DbError> {
        let defs: Vec<Value> = serde_json::from_str(&defs_json)?;
        let sync = self.sync()?;
        let mut names = Vec::new();
        let mut declared = Vec::new();
        for def in defs {
            let text = |key: &str| def[key].as_str().map(str::to_string).ok_or_else(|| DbError::Invalid { detail: format!("operation needs {key}") });
            let (name, document) = (text("name")?, text("document")?);
            let mut operation = match def["rootPath"].as_str() {
                Some(root) => OperationDef::new(&name, &document, root),
                None => OperationDef::from_document(&name, &document).ok_or_else(|| DbError::Invalid { detail: format!("operation {name} has no root field") })?,
            };
            if let Some(size) = def["pageSize"].as_u64() { operation = operation.paged(u32::try_from(size).map_err(|error| DbError::Invalid { detail: error.to_string() })?); }
            names.push((name.clone(), if document.trim_start().starts_with("mutation") { crate::owners::calls::CallKind::Mutate } else { crate::owners::calls::CallKind::Query }));
            declared.push(crate::owners::read::ReadOperation { name: operation.name.clone(), document: operation.document.clone(), root_path: operation.root_path.clone(), page_size: operation.page_size });
            sync.register(operation);
        }
        let registered: Vec<(String, String)> = self.db.shared.specs.values().flat_map(|spec| spec.actions.iter().filter_map(|action| action.mutation.as_ref().filter(|mutation| names.iter().any(|(name, _)| name == *mutation)).map(|_| (spec.key.to_string(), action.name.clone())))).collect();
        self.reads.declare(declared);
        self.life.declare(names);
        self.life.register_operations(registered);
        let _entered = executor()?.enter();
        sync.operations_ready();
        Ok(())
    }
    pub async fn run(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        self.sync()?;
        serde_json::from_str::<Value>(&variables_json)?;
        match self.await_life(self.life.run(epoch, operation, variables_json)).await? {
            LifeReply::Call(Ok(answer)) => Ok(answer),
            LifeReply::Call(Err(error)) => Err(call_failure(error)),
            _ => Err(super::command::ended()),
        }
    }
        /// Starts loading a relation and returns this caller's interest in the load at once. Callers of the same scope share one request: the request stops when every interest in it was cancelled.
    pub fn begin_load(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<Arc<LoadRequest>, DbError> {
        let call = self.start_load(epoch, "load", &model, &relation, &params_json, Goal::First, false)?;
        let (sender, outcome) = tokio::sync::watch::channel(None);
        let this = self.clone();
        let task = crate::task::spawn_on(crate::task::Carry::here(), executor()?.handle(), async move {
            let result = this.finish_load(call).await.map(drop);
            crate::db::lock::effect(|| sender.send_replace(Some(result)));
        });
        Ok(Arc::new(LoadRequest { outcome, held: Mutex::new(Some(task.abort_handle())) }))
    }
    pub async fn refresh(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.require_online(&format!("{model}.{relation} refresh"))?;
        let call = self.start_load(epoch, "refresh", &model, &relation, &params_json, Goal::First, true)?;
        self.finish_load(call).await.map(drop)
    }
    /// Starts reading every page of the relation and returns this caller's request at once. Cancelling it stops the pages that no other caller waits for.
    pub fn begin_load_all(&self, epoch: u64, model: String, relation: String, params_json: String, force: bool) -> Result<Arc<LoadRequest>, DbError> {
        let sync = self.sync()?.clone();
        drop(self.acquire(crate::ffi::legacy::Origin::Explicit, epoch, "load all")?);
        let (sender, outcome) = tokio::sync::watch::channel(None);
        let this = self.clone();
        let task = crate::task::spawn_on(crate::task::Carry::here(), executor()?.handle(), async move {
            let result = this.in_epoch_as(crate::ffi::legacy::Origin::Explicit, epoch, "load all", true, |admitted| async move {
                let params: Map<String, Value> = serde_json::from_str(&params_json)?;
                Ok::<_, DbError>(sync.load_all(&admitted, &model, &relation, &params, LoadOptions { force }).await?)
            }).await;
            crate::db::lock::effect(|| sender.send_replace(Some(result)));
        });
        Ok(Arc::new(LoadRequest { outcome, held: Mutex::new(Some(task.abort_handle())) }))
    }
    pub async fn load_more(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<LoadMoreResult, DbError> {
        self.require_online(&format!("{model}.{relation} load more"))?;
        let call = self.start_load(epoch, "load more", &model, &relation, &params_json, Goal::Next, false)?;
        self.finish_load(call).await
    }
    pub async fn node(&self, epoch: u64, model: String, relation: String, row_id: String, force: bool) -> Result<Option<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let row = self.serve_in_epoch(epoch, "node", move |admitted| async move { sync.node(&admitted, &model, &relation, &row_id, force).await }).await?;
        Ok(row.map(|row| wire.row(&row)))
    }
    pub async fn nodes(&self, epoch: u64, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let rows = self.serve_in_epoch(epoch, "nodes", move |admitted| async move { sync.nodes(&admitted, &model, &relation, &row_ids.iter().map(String::as_str).collect::<Vec<_>>()).await }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }
    pub async fn retry(&self, epoch: u64, model: String, row_id: String, action: Option<String>, source: Option<WireUploadRequest>) -> Result<WireOperationReceipt, DbError> {
        if let Some(source) = source {
            let sync = self.sync()?.clone();
            let wire = self.wire(&model)?.clone();
            let life = self.streams.clone();
            return self.in_epoch_as(crate::ffi::legacy::Origin::Session, epoch, "retry", false, |admitted| on_executor(async move {
                let file = source.into_file()?;
                let (operation, revision, row, ticket) = sync.retry_upload_source(&admitted, &model, &row_id, action.as_deref(), file).await?;
                Ok::<_, DbError>(receipt(epoch, model, Some(operation), revision, row.map(|row| wire.row(&row)), Some(ticket), life))
            })).await;
        }
        self.retry_operation(epoch, model, row_id).await
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }

    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync.as_ref().map(Sync::downgrade), online: self.db.connectivity(), life: Arc::downgrade(&self.life) })) }
    pub async fn upload(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<WireOperationReceipt, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        let life = self.streams.clone();
        let row = id(&row_id)?;
        self.in_epoch_as(crate::ffi::legacy::Origin::Session, epoch, "upload", false, |admitted| on_executor(async move {
            let file = request.into_file()?;
            let (operation, revision, row, ticket) = sync.upload(&admitted, &model, &row, &action, file).await?;
            Ok::<_, DbError>(receipt(epoch, model, Some(operation), revision, row.map(|row| wire.row(&row)), Some(ticket), life))
        })).await
    }
    pub async fn upload_create_batch(&self, epoch: u64, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<WireBatchReceipt, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        self.in_epoch_as(crate::ffi::legacy::Origin::Session, epoch, "upload batch", false, |admitted| on_executor(async move {
            let items = items.into_iter().map(WireUploadItem::into_item).collect::<Result<Vec<_>, _>>()?;
            let input = request.into_input()?;
            let receipt = sync.upload_create_batch(&admitted, &model, items, input).await?;
            let items: Vec<_> = receipt.items.iter().map(|item| WireItemReceipt { item: ItemId { value: item.item.0.clone() }, optimistic_value: wire.row(&item.optimistic_value) }).collect();
            Ok::<_, DbError>(WireBatchReceipt { epoch: receipt.epoch, operation: receipt.operation, revision: receipt.revision, items: items.clone(), ticket: Arc::new(WireBatchTicket { receipt, model, items }) })
        })).await
    }
    pub async fn batch_outcome(&self, receipt: WireBatchReceipt) -> Result<WireBatchOutcome, DbError> {
        let held = &receipt.ticket;
        if !Arc::ptr_eq(&held.receipt.life, &self.streams) || receipt.epoch != held.receipt.epoch || receipt.operation != held.receipt.operation || receipt.revision != held.receipt.revision || receipt.items != held.items {
            return Err(DbError::Invalid { detail: "the upload batch receipt was changed".into() });
        }
        if !self.admits(receipt.epoch) { return Ok(WireBatchOutcome { result: WireBatchResult::Interrupted, items: Vec::new() }); }
        let outcome = match held.receipt.outcome().await {
            Ok(outcome) => outcome,
            Err(_) if !self.admits(receipt.epoch) => return Ok(WireBatchOutcome { result: WireBatchResult::Interrupted, items: Vec::new() }),
            Err(error) => return Err(error.into()),
        };
        let wire = self.wire(&held.model)?;
        let items = outcome.items.into_iter().map(|item| match item {
            crate::sync::ItemOutcome::Confirmed { item, value } => WireItemOutcome::Confirmed { item: ItemId { value: item.0 }, value: wire.row(&value) },
            crate::sync::ItemOutcome::Discarded { item } => WireItemOutcome::Discarded { item: ItemId { value: item.0 } },
        }).collect();
        let result = match outcome.result {
            crate::sync::SendResult::Committed => WireBatchResult::Confirmed,
            crate::sync::SendResult::Rejected(failure) => WireBatchResult::Rejected { failure: super::wire::failure(&failure) },
            crate::sync::SendResult::DeliveryUnknown | crate::sync::SendResult::Quarantined => WireBatchResult::DeliveryUnknown,
            crate::sync::SendResult::Discarded => WireBatchResult::Interrupted,
            crate::sync::SendResult::Deferred | crate::sync::SendResult::Skipped(_) => return Err(DbError::Invalid { detail: "operation has not finished".into() }),
        };
        Ok(WireBatchOutcome { result, items })
    }
    pub async fn cancel_upload(&self, epoch: u64, model: String, row_id: String, action: String) -> Result<bool, DbError> {
        let sync = self.sync()?.clone();
        let row = id(&row_id)?;
        self.serve_in_epoch(epoch, "cancel upload", |admitted| async move { sync.cancel_upload(&admitted, &model, &row, &action).await }).await
    }
    /// Closes the database: every lease ends, every holder closes, every database thread lets go of the file, then the process slot opens for the next database. Closing again returns the same result. The name differs from `close` because Kotlin objects already carry `close()` for their native handle.
    pub async fn close_database(&self) -> Result<(), DbError> {
        if self.life.is_shut() { return Err(super::command::closed()); }
        let waiter = self.life.close();
        let answered = on_executor(async move {
            tokio::time::timeout(Duration::from_secs(10), async { waiter.wait().await }).await.map_err(|_| DbError::Invalid { detail: "close timed out while the owners were closing".into() })
        }).await.and_then(|outcome| match outcome {
            crate::runtime::Outcome::Value(LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Closed))) => Err(super::command::closed()),
            _ => Ok(()),
        });
        let leases = self.streams.clone();
        let drained = on_executor(async move {
            tokio::time::timeout(Duration::from_secs(10), leases.settled()).await.map_err(|_| DbError::Invalid { detail: format!("close timed out while leases were still held: {}", leases.held().join("; ")) })
        }).await;
        let (db, life) = (self.db.clone(), self.life.clone());
        let ended = blocking(move || { life.shutdown(); Ok(db.close()?) }).await;
        if let Some(lease) = self.lease.get() { lease.release(); }
        answered.and(drained).and(ended)
    }
}

impl Database {
    /// Waits for the answer of an owner. An answer other than a value ends the call with the end of its epoch.
    pub(super) async fn await_life(&self, waiter: crate::runtime::Waiter<LifeReply>) -> Result<LifeReply, DbError> {
        match waiter.wait().await {
            crate::runtime::Outcome::Value(LifeReply::Closed) | crate::runtime::Outcome::Closing => Err(super::command::closed()),
            crate::runtime::Outcome::EpochEnded(_) => Err(super::command::ended()),
            crate::runtime::Outcome::Internal => Err(DbError::Database { detail: "the answer of the owner was lost".into() }),
            crate::runtime::Outcome::Value(reply) => Ok(reply),
        }
    }

    #[cfg(test)]
    pub(crate) async fn load_all(&self, epoch: u64, model: String, relation: String, params_json: String, force: bool) -> Result<(), DbError> {
        self.begin_load_all(epoch, model, relation, params_json, force)?.result().await
    }

    /// Registers a call that waits for a load of the window in `Windows`. The window keeps the request while this call or another one waits for it.
    #[allow(clippy::too_many_arguments)]
    fn start_load(&self, epoch: u64, target: &'static str, model: &str, relation: &str, params_json: &str, goal: Goal, force: bool) -> Result<LoadCall, DbError> {
        self.sync()?;
        let params: Map<String, Value> = serde_json::from_str(params_json)?;
        self.db.window_record(model, relation, &params)?;
        drop(self.acquire(crate::ffi::legacy::Origin::Explicit, epoch, target)?);
        let published = self.life.published();
        if published.epoch != epoch { return Err(super::command::ended()); }
        if published.identity.is_none() { return Err(DbError::NoSession { detail: "the database has no session in this epoch".into() }); }
        let key = WindowKey::relation(model, relation, &params);
        let force = force || (goal == Goal::First && !self.db.window_is_fresh(model, relation, &params, self.sync()?.inner.now_ms())?);
        let (waiter, id) = self.life.load(epoch, key.clone(), goal, force, self.db.revision_seq()?);
        Ok(LoadCall { epoch, waiter, leave: LeaveLoad { life: self.life.clone(), key, id, settled: false } })
    }

    async fn finish_load(&self, mut call: LoadCall) -> Result<LoadMoreResult, DbError> {
        let reply = self.await_life(call.waiter).await;
        call.leave.settled = true;
        let reply = reply?;
        if !self.admits(call.epoch) { return Err(super::command::ended()); }
        match reply {
            LifeReply::Window(WindowOutcome::Loaded { has_more, fetched, .. }) => Ok(if fetched { LoadMoreResult::Loaded { has_more } } else { LoadMoreResult::NoMore }),
            LifeReply::Window(WindowOutcome::Failed { source: Some(source), .. }) => Err(crate::sync::SyncError::Transport(source).into()),
            LifeReply::Window(WindowOutcome::Failed { error, .. }) => Err(DbError::Database { detail: error }),
            LifeReply::Window(WindowOutcome::PageLimit) => Err(DbError::Invalid { detail: "the window reached its page limit".into() }),
            _ => Err(super::command::ended()),
        }
    }

    pub(super) fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::NoSession { detail: "transport is required for server operations".into() }) }
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
        CallError::Offline => DbError::Network { detail: "the operation needs the network".into() },
        CallError::Network => DbError::Network { detail: "the request did not reach the server".into() },
        CallError::Graphql(messages) => DbError::Graphql { messages, extensions: None },
        CallError::Http => DbError::Http { status: 0, detail: "the server refused the request".into() },
        CallError::Dropped => DbError::Network { detail: "the request left and the answer did not come".into() },
        CallError::InvalidResponse => DbError::Invalid { detail: "the response has no root of the operation".into() },
        CallError::Interrupted => super::command::ended(),
        CallError::Storage => DbError::Database { detail: "the database could not save its state".into() },
    }
}

