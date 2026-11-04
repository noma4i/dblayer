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

#[derive(uniffi::Object)]
pub struct WireReceiptTicket {
    ticket: Option<Arc<crate::sync::OutcomeTicket>>,
    life: Arc<Lifecycle>,
    epoch: u64,
    model: String,
    operation: Option<i64>,
    revision: i64,
    optimistic_value: Option<WireRow>,
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

pub(super) fn receipt(epoch: u64, model: String, operation: Option<i64>, revision: i64, optimistic_value: Option<WireRow>, ticket: Option<Arc<crate::sync::OutcomeTicket>>, life: Arc<Lifecycle>) -> WireOperationReceipt {
    let held = WireReceiptTicket { ticket, life, epoch, model: model.clone(), operation, revision, optimistic_value: optimistic_value.clone() };
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
        let opening = db.admit()?;
        db.expire_blobs_before_use(&opening)?;
        drop(opening);
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
    pub async fn create(&self, epoch: u64, model: String, values_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let declared = self.db.spec(&model)?.actions.iter().find(|action| action.name == "create").cloned();
        let remote = declared.as_ref().is_some_and(|action| action.mutation.is_some());
        if declared.as_ref().is_some_and(|action| action.durable) {
            return Err(DbError::Invalid { detail: format!("{model} sends its create as a durable command: use send") });
        }
        let origin = if remote { crate::lifecycle::Origin::Session } else { crate::lifecycle::Origin::Explicit };
        self.write_in_epoch(origin, epoch, "create", move |database, admitted| {
            let sync = database.sync.clone();
            let (row, operation, revision, ticket) = database.db.create_claiming(admitted, None, &model, serde_json::from_str(&values_json)?, serde_json::from_str(&args_json)?, |operation| operation.and_then(|operation| sync.as_ref().map(|sync| sync.claim_outcome(operation))))?;
            database.send_writes();
            Ok(receipt(epoch, model.clone(), operation, revision, Some(database.wire(&model)?.row(&row)), ticket, database.streams.clone()))
        }).await
    }
    pub async fn update(&self, epoch: u64, model: String, row_id: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let origin = self.write_origin(&model, "update")?;
        self.write_in_epoch(origin, epoch, "update", move |database, admitted| {
            let sync = database.sync.clone();
            let (row, operation, revision, ticket) = database.db.update_claiming(admitted, &model, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?, |operation| operation.and_then(|operation| sync.as_ref().map(|sync| sync.claim_outcome(operation))))?;
            database.send_writes();
            Ok(receipt(epoch, model.clone(), operation, revision, Some(database.wire(&model)?.row(&row)), ticket, database.streams.clone()))
        }).await
    }
    pub async fn set_local(&self, epoch: u64, model: String, row_id: String, path: String, value_json: String) -> Result<(), DbError> {
        self.write_in_epoch(crate::lifecycle::Origin::Explicit, epoch, "set local", move |database, admitted| {
            Ok(database.db.set_local(admitted, &model, &id(&row_id)?, &path, serde_json::from_str(&value_json)?)?)
        }).await
    }
    pub async fn act(&self, epoch: u64, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<WireOperationReceipt, DbError> {
        let origin = self.write_origin(&model, &action)?;
        self.write_in_epoch(origin, epoch, "act", move |database, admitted| {
            let sync = database.sync()?.clone();
            let (acted, ticket) = database.db.act_claiming(admitted, &model, &action, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?, |operation| sync.claim_outcome(operation))?;
            database.send_writes();
            Ok(receipt(epoch, model.clone(), Some(acted.operation), acted.revision, Some(database.wire(&model)?.row(&acted.row)), Some(ticket), database.streams.clone()))
        }).await
    }
    pub async fn destroy(&self, epoch: u64, model: String, row_id: String) -> Result<WireOperationReceipt, DbError> {
        let origin = self.write_origin(&model, "destroy")?;
        self.write_in_epoch(origin, epoch, "destroy", move |database, admitted| {
            let sync = database.sync.clone();
            let (_, operation, revision, ticket) = database.db.destroy_claiming(admitted, &model, &id(&row_id)?, |operation| operation.and_then(|operation| sync.as_ref().map(|sync| sync.claim_outcome(operation))))?;
            database.send_writes();
            Ok(receipt(epoch, model, operation, revision, None, ticket, database.streams.clone()))
        }).await
    }
    pub async fn outcome(&self, receipt: WireOperationReceipt) -> Result<WireOperationOutcome, DbError> {
        if !Arc::ptr_eq(&receipt.ticket.life, &self.streams) { return Err(DbError::Invalid { detail: "the receipt belongs to another database".into() }); }
        if receipt.epoch != receipt.ticket.epoch || receipt.model != receipt.ticket.model || receipt.operation != receipt.ticket.operation || receipt.revision != receipt.ticket.revision || receipt.optimistic_value != receipt.ticket.optimistic_value {
            return Err(DbError::Invalid { detail: "the receipt was changed".into() });
        }
        if !self.admits(receipt.epoch) { return Ok(WireOperationOutcome::Interrupted); }
        let Some(ticket) = receipt.ticket.ticket.clone() else {
            if receipt.operation.is_some() { return Err(DbError::Invalid { detail: "the operation has no sender".into() }); }
            return Ok(WireOperationOutcome::Confirmed { value: receipt.optimistic_value, answer_json: None });
        };
        let result = self.in_epoch(receipt.epoch, "operation outcome", async move { Ok::<_, DbError>(ticket.result().await) }).await;
        let result = match result {
            Ok(result) => result,
            Err(_) if !self.admits(receipt.epoch) => return Ok(WireOperationOutcome::Interrupted),
            Err(error) => return Err(error),
        };
        match result.result {
            crate::sync::SendResult::Committed => {
                let value = result.value.map(|row| self.wire(&receipt.model).map(|wire| wire.row(&row))).transpose()?;
                Ok(WireOperationOutcome::Confirmed { value, answer_json: result.answer.map(|answer| answer.to_string()) })
            }
            crate::sync::SendResult::Rejected(failure) => Ok(WireOperationOutcome::Rejected { failure: super::wire::failure(&failure) }),
            crate::sync::SendResult::DeliveryUnknown | crate::sync::SendResult::Quarantined => Ok(WireOperationOutcome::DeliveryUnknown),
            crate::sync::SendResult::Discarded => Ok(WireOperationOutcome::Interrupted),
            crate::sync::SendResult::Deferred | crate::sync::SendResult::Skipped(_) => Err(DbError::Invalid { detail: "operation has not finished".into() }),
        }
    }
    pub async fn set_ephemeral(&self, epoch: u64, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        self.write_in_epoch(crate::lifecycle::Origin::Explicit, epoch, "set ephemeral", move |database, admitted| {
            Ok(database.db.set_ephemeral(admitted, &model, &id(&row_id)?, serde_json::from_str(&values_json)?)?)
        }).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(&self, epoch: u64, model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String) -> Result<String, DbError> {
        let row_id = id(&row_id)?;
        self.write_in_epoch(crate::lifecycle::Origin::Explicit, epoch, "attach", move |database, admitted| Ok(database.db.attach(admitted, &model, &row_id, &field, &bytes, &name, &mime)?.to_value().to_string())).await
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
        self.write_in_epoch(crate::lifecycle::Origin::Explicit, epoch, "detach", move |database, admitted| Ok(database.db.detach(admitted, &model, &row_id, &field)?)).await
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
        sync.operations_ready();
        Ok(())
    }
    pub async fn run(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        let answer = self.serve_in_epoch(epoch, "run", |admitted| async move {
            let variables: Value = serde_json::from_str(&variables_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.run(&admitted, &operation, variables).await?)
        }).await?;
        Ok(answer.to_string())
    }
    pub async fn load(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        self.serve_shared_in_epoch(epoch, "load", |admitted| async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load(&admitted, &model, &relation, &params, LoadOptions::default()).await?)
        }).await.map(drop)
    }
    pub async fn refresh(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.require_online(&format!("{model}.{relation} refresh"))?;
        let sync = self.sync()?.clone();
        self.serve_shared_in_epoch(epoch, "refresh", |admitted| async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.refresh(&admitted, &model, &relation, &params).await?)
        }).await.map(drop)
    }
    pub async fn refresh_preloads(&self, epoch: u64) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        let failures = self.serve_in_epoch(epoch, "refresh preloads", |admitted| async move { Ok::<_, DbError>(sync.refresh_preloads(&admitted).await?) }).await?;
        if failures.is_empty() { return Ok(()); }
        let scopes: Vec<String> = failures.iter().map(|failure| format!("{}.{} {}: {}", failure.model, failure.relation, serde_json::Value::Object(failure.params.clone()), failure.error)).collect();
        Err(DbError::Sync { detail: format!("{} preload scopes failed to refresh: {}", scopes.len(), scopes.join("; ")) })
    }
    pub async fn load_all(&self, epoch: u64, model: String, relation: String, params_json: String, force: bool) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        self.serve_in_epoch(epoch, "load all", move |admitted| async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_all(&admitted, &model, &relation, &params, LoadOptions { force }).await?)
        }).await
    }
    pub async fn load_more(&self, epoch: u64, model: String, relation: String, params_json: String) -> Result<LoadMoreResult, DbError> {
        self.require_online(&format!("{model}.{relation} load more"))?;
        let sync = self.sync()?.clone();
        let outcome = self.serve_shared_in_epoch(epoch, "load more", |admitted| async move {
            let params = serde_json::from_str(&params_json).map_err(DbError::from)?;
            Ok::<_, DbError>(sync.load_more(&admitted, &model, &relation, &params).await?)
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
        let row = self.serve_shared_in_epoch(epoch, "node", |admitted| async move { sync.node(&admitted, &model, &relation, &row_id, force).await }).await?;
        Ok(row.map(|row| wire.row(&row)))
    }
    pub async fn nodes(&self, epoch: u64, model: String, relation: String, row_ids: Vec<String>) -> Result<Vec<WireRow>, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?;
        let rows = self.serve_shared_in_epoch(epoch, "nodes", |admitted| async move { sync.nodes(&admitted, &model, &relation, &row_ids.iter().map(String::as_str).collect::<Vec<_>>()).await }).await?;
        Ok(rows.iter().map(|row| wire.row(row)).collect())
    }
    pub async fn flush(&self) -> Result<(), DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(()) };
        let admitted = self.streams.admit(crate::lifecycle::Origin::Session, "flush").map_err(|_| DbError::NoSession { detail: "the database has no session".into() })?;
        on_executor(async move { sync.flush(&admitted).await }).await?;
        Ok(())
    }
    pub async fn retry(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<WireOperationReceipt, DbError> {
        if self.sync.is_none() {
            let row = self.db.find(&model, &id(&row_id)?)?.map(|row| self.wire(&model).map(|wire| wire.row(&row))).transpose()?;
            return Ok(receipt(epoch, model, None, self.db.revision_seq()?, row, None, self.streams.clone()));
        }
        self.write_in_epoch(crate::lifecycle::Origin::Session, epoch, "retry", move |database, admitted| {
            let sync = database.sync()?.clone();
            let id = id(&row_id)?;
            let (operation, revision, row, ticket) = database.db.retry_claiming(admitted, &model, &id, action.as_deref(), |operation| sync.claim_outcome(operation))?;
            let row = row.map(|row| database.wire(&model).map(|wire| wire.row(&row))).transpose()?;
            if operation.is_some() {
                sync.inner.start_uploads().map_err(DbError::from)?;
                database.send_writes();
            }
            Ok(receipt(epoch, model, operation, revision, row, ticket, database.streams.clone()))
        }).await
    }
    pub async fn retry_all(&self, epoch: u64) -> Result<u64, DbError> {
        let Some(sync) = self.sync.clone() else { return Ok(0) };
        let db = self.db.clone();
        self.in_epoch_as(crate::lifecycle::Origin::Session, epoch, "retry all", false, |admitted| async move {
            let count = db.retry_all(&admitted)?;
            sync.flush(&admitted).await?;
            Ok::<_, DbError>(count as u64)
        }).await
    }
    pub async fn discard(&self, epoch: u64, model: String, row_id: String, action: Option<String>) -> Result<(), DbError> {
        let sync = self.sync.clone();
        self.write_in_epoch(crate::lifecycle::Origin::Explicit, epoch, "discard", move |database, admitted| {
            let row = id(&row_id)?;
            database.db.discard(admitted, &model, &row, action.as_deref())?;
            if let Some(sync) = &sync {
                let _entered = executor()?.enter();
                sync.inner.start_uploads()?;
            }
            Ok(())
        }).await
    }
    pub async fn resume(&self, epoch: u64) -> Result<(), DbError> {
        let sync = self.sync.clone();
        self.in_epoch_as(crate::lifecycle::Origin::Explicit, epoch, "resume", false, |admitted| async move {
            let Some(sync) = sync else { return Ok(()) };
            on_executor(async move { sync.resume(&admitted).await.map(drop).map_err(DbError::from) }).await
        }).await
    }
    pub async fn resume_uploads(&self, epoch: u64) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        self.in_epoch_as(crate::lifecycle::Origin::Session, epoch, "resume uploads", false, |admitted| on_executor(async move { sync.resume_uploads(&admitted).await.map_err(DbError::from) })).await
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
    pub async fn upload(&self, epoch: u64, model: String, row_id: String, action: String, request: WireUploadRequest) -> Result<WireOperationReceipt, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        let life = self.streams.clone();
        let row = id(&row_id)?;
        self.in_epoch_as(crate::lifecycle::Origin::Session, epoch, "upload", false, |admitted| on_executor(async move {
            let file = request.into_file()?;
            let (operation, revision, row, ticket) = sync.upload(&admitted, &model, &row, &action, file).await?;
            Ok::<_, DbError>(receipt(epoch, model, Some(operation), revision, row.map(|row| wire.row(&row)), Some(ticket), life))
        })).await
    }
    pub async fn upload_create_batch(&self, epoch: u64, model: String, items: Vec<WireUploadItem>, request: WireUploadBatch) -> Result<WireBatchReceipt, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire(&model)?.clone();
        self.in_epoch_as(crate::lifecycle::Origin::Session, epoch, "upload batch", false, |admitted| on_executor(async move {
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
    pub async fn maintain(&self) -> Result<(), DbError> {
        let db = self.db.clone();
        let admitted = db.admit()?;
        blocking(move || Ok(db.maintain(&admitted, &[]).map(|_| ())?)).await
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

    pub(super) fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::NoSession { detail: "transport is required for server operations".into() }) }
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
    fn write_origin(&self, model: &str, action: &str) -> Result<crate::lifecycle::Origin, DbError> {
        let declared = self.db.spec(model)?.actions.iter().any(|declared| declared.name == action && declared.mutation.is_some());
        Ok(if declared { crate::lifecycle::Origin::Session } else { crate::lifecycle::Origin::Explicit })
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

