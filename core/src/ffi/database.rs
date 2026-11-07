use std::collections::HashMap;

use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use serde_json::Value;
use tokio::runtime::Runtime;
use crate::store::{OpenReport, Store};
use crate::identity::RowId;
use crate::runtime::{Clock, ManualClock, SystemClock};
use crate::schema::{FieldSpec, ModelSpec, Specs};
use crate::transport::{self as core, EncryptedTransport, Exchange};
use crate::owners::write::upload::UploadOperations;
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
    let runtime = build().map_err(|error| DbError::Runtime { detail: format!("the database executor did not start: {error}") })?;
    Ok(slot.runtime.get_or_init(|| runtime))
}

pub(super) async fn on_executor<T: Send + 'static, E: Into<DbError> + Send + 'static>(work: impl std::future::Future<Output = Result<T, E>> + Send + 'static) -> Result<T, DbError> {
    crate::task::spawn_on(executor()?.handle(), work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(Into::into)
}

pub(super) async fn cancellable<T: Send + 'static>(work: impl std::future::Future<Output = T> + Send + 'static) -> Result<T, DbError> {
    struct Cancel(tokio::task::AbortHandle);
    impl Drop for Cancel { fn drop(&mut self) { crate::lock::effect(|| self.0.abort()); } }
    let task = crate::task::spawn_on(executor()?.handle(), work);
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
    crate::task::spawn_blocking_on(executor()?.handle(), work).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
}

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct Config { pub path: Option<String>, pub data_version: Option<String> }

#[derive(uniffi::Record)]
pub struct SubscriptionCensus { pub observed_queries: u64, pub subscriptions: u64, pub retained_observers: u64, pub refused_events: u64 }

#[derive(uniffi::Object)]
pub struct TestClock { clock: Arc<ManualClock> }

#[uniffi::export]
impl TestClock {
    #[uniffi::constructor]
    pub fn new(start_millis: u64) -> Arc<Self> { Arc::new(Self { clock: Arc::new(ManualClock::new(Duration::from_millis(start_millis))) }) }
    pub fn advance(&self, millis: u64) { self.clock.advance(Duration::from_millis(millis)) }
    pub fn armed(&self) -> u64 { self.clock.armed_total() }
    pub async fn await_armed(&self, after: u64) { self.clock.wait_armed(after).await }
}

impl TestClock {
    pub(super) fn manual(&self) -> Arc<ManualClock> { self.clock.clone() }
}

type OutcomesByKey = Arc<Mutex<HashMap<(u64, u64, u64), WireOperationOutcome>>>;

#[derive(Clone, uniffi::Object)]
pub struct Database { pub(super) me: std::sync::Weak<Database>, pub(super) lease: Arc<OnceLock<Arc<OpenLease>>>, pub(super) life: Arc<crate::owners::life::Life>, pub(super) db: Arc<Store>, pub(super) remote: bool, pub(super) reads: crate::owners::read::ReadOperations, pub(super) returns: Arc<HashMap<String, crate::schema::FieldSpec>>, pub(super) uploads: Option<UploadOperations>, pub(super) jobs: Arc<super::upload_rounds::RoundJobsSlot>, pub(super) shows: Arc<super::transfer_status::Shows>, pub(super) outcomes: OutcomesByKey, pub(super) loads: Arc<super::load_order::LoadOrder>, wires: Arc<HashMap<String, Arc<WireModel>>> }

pub(super) struct Wiring {
    pub transport: Option<Arc<dyn core::Upstream>>,
    pub encrypted: Option<Encrypted>,
    pub cable: Option<Arc<dyn core::CableLines>>,
    pub seal: Option<Arc<crate::owners::session_seal::SealConfig>>,
}

/// What the encrypted session needs besides the transport: the exchange, the secret and the transport that joins the owners.
pub(super) struct Encrypted {
    pub exchange: Arc<Exchange>,
    pub secret: Arc<crate::transport::encrypted::credential::Secret>,
    pub transport: Arc<EncryptedTransport>,
}

impl Database {
    fn start(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, endpoint: Option<String>, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, clock, |db, _| {
            #[cfg(test)]
            if transport.is_some() { super::signed_session_tests::signed_session(db); }
            let _ = db;
            Ok(Wiring { transport: transport.map(|transport| ForeignTransport::plain(transport, endpoint).map(|plain| Arc::new(plain) as Arc<dyn core::Upstream>)).transpose()?, encrypted: None, cable: None, seal: None })
        })
    }

    pub(super) fn start_with(
        config: Config,
        models_json: String,
        clock: Arc<dyn Clock>,
        wire: impl FnOnce(&Arc<Store>, &Arc<dyn Clock>) -> Result<Wiring, DbError>,
    ) -> Result<Arc<Self>, DbError> {
        let Specs { models, operations } = Specs::parse(&models_json)?;
        let returns: Arc<HashMap<String, FieldSpec>> = Arc::new(operations.into_iter().map(|operation| (operation.name, operation.returns)).collect());
        let uploads = upload_steps_of(&models)?;
        let keys: Vec<String> = models.iter().map(|model| model.key.to_string()).collect();
        let db = Arc::new(Store::open_models(config.path.as_deref().map(Path::new), models, config.data_version.as_deref())?);
        db.set_clock(clock.clone());
        let Wiring { transport, encrypted, cable, seal } = wire(&db, &clock)?;
        let reads = crate::owners::read::ReadOperations::default();
        let jobs: Arc<super::upload_rounds::RoundJobsSlot> = Arc::default();
        let shows: Arc<super::transfer_status::Shows> = Arc::default();
        let life = Self::build_life(&db, transport.clone(), encrypted.as_ref(), cable, seal, reads.clone(), (uploads.clone(), jobs.clone(), shows.clone()))?;
        let committed_shows = shows.clone();
        life.on_committed(move |broadcast| committed_shows.touch(&broadcast.rows));
        let remote = transport.is_some();
        let wires = keys.into_iter().map(|key| db.spec(&key).map(|spec| (key, Arc::new(WireModel::of(spec)))).map_err(DbError::from)).collect::<Result<_, _>>()?;
        Ok(Arc::new_cyclic(|me| Self { me: me.clone(), lease: Arc::new(OnceLock::new()), life, db, remote, reads, returns, uploads, jobs, shows, outcomes: Arc::default(), loads: Arc::default(), wires: Arc::new(wires) }))
    }

    pub async fn open(config: Config, models_json: String, ports: super::OpenPorts) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, ports, Arc::new(SystemClock)).await
    }

    pub async fn open_with_clock(config: Config, models_json: String, ports: super::OpenPorts, clock: Arc<TestClock>) -> Result<Arc<Self>, DbError> {
        Self::open_at(config, models_json, ports, clock.clock.clone()).await
    }

    async fn open_at(config: Config, models_json: String, ports: super::OpenPorts, clock: Arc<dyn Clock>) -> Result<Arc<Self>, DbError> {
        let super::OpenPorts { transport, encrypted, credentials, http, endpoint, .. } = ports;
        blocking(move || {
            let database = match encrypted {
                Some(encrypted) => {
                    let credentials = credentials.ok_or_else(|| DbError::Invalid { detail: "encrypted requires credentials".into() })?;
                    let http = http.ok_or_else(|| DbError::Invalid { detail: "encrypted requires http".into() })?;
                    let transport = transport.ok_or_else(|| DbError::Invalid { detail: "encrypted requires transport".into() })?;
                    let _entered = executor()?.enter();
                    Self::start_with(config, models_json, clock, |db, clock| {
                        super::encrypted::wire(db, clock, encrypted, http, credentials, transport)
                    })?
                }
                None => {
                    if credentials.is_some() || http.is_some() {
                        return Err(DbError::Invalid { detail: "credentials and http require encrypted".into() });
                    }
                    Self::start(config, models_json, transport, endpoint, clock)?
                }
            };
            Ok(database)
        }).await
    }

    /// F1: the owners of direction A on the threads of the core, started from the booted `Store`. It returns after the first `Publish`.
    fn build_life(db: &Arc<Store>, transport: Option<Arc<dyn core::Upstream>>, encrypted: Option<&Encrypted>, cable: Option<Arc<dyn core::CableLines>>, seal: Option<Arc<crate::owners::session_seal::SealConfig>>, read_operations: crate::owners::read::ReadOperations, (uploads, jobs, shows): (Option<UploadOperations>, Arc<super::upload_rounds::RoundJobsSlot>, Arc<super::transfer_status::Shows>)) -> Result<Arc<crate::owners::life::Life>, DbError> {
        let executor = executor()?.handle().clone();
        let operations = operations_lookup(&read_operations);
        let config = crate::owners::write::upload::UploadConfig::default();
        let transfer = uploads.filter(|_| encrypted.is_some()).map(|operations| crate::owners::write::transfer_executor::TransferPort {
            put_timeout: config.put_timeout,
            codec: Arc::new(crate::owners::write::upload_codec::UploadCodec::new(operations, operations_lookup(&read_operations), config)),
            jobs,
            shows: Arc::new(move |key, shown| shows.put(key, shown)),
            slots: 1,
        });
        let ports = crate::owners::life::Ports {
            db: db.clone(),
            transport,
            executor,
            operations,
            secret: encrypted.map(|encrypted| encrypted.secret.clone()),
            exchange: encrypted.map(|encrypted| encrypted.exchange.clone()),
            seal,
            transfer,
            cable,
            read_operations: Some(read_operations),
        };
        let _entered = executor_enter()?;
        let life = Arc::new(crate::owners::life::Life::start(ports).map_err(|error| match error {
            crate::owners::life::LifeError::CorruptManifest(detail) => DbError::CorruptManifest { detail },
            other => DbError::Runtime { detail: format!("the owners did not start: {other:?}") },
        })?);
        if let Some(encrypted) = encrypted { encrypted.transport.attach(&life); }
        Ok(life)
    }

    pub(super) fn not_closed(&self) -> Result<(), DbError> {
        if self.life.published().closed { Err(super::command::closed()) } else { Ok(()) }
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

/// The edge fields of one row in a relation.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct EdgeRow {
    pub id: String,
    pub values: Vec<super::wire::CellEntry>,
}

pub(super) fn parse_json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, DbError> {
    Ok(serde_json::from_str(text)?)
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
    pub async fn edge_values(&self, epoch: u64, model: String, relation: Option<String>, params_json: String, row_ids: Vec<String>) -> Result<Vec<EdgeRow>, DbError> {
        let relation = super::loading::relation_named(&model, relation)?;
        self.read_in_epoch(epoch, "edge values", move |database| {
            let params = parse_json(&params_json)?;
            Ok(database.db.edge_values(&model, &relation, &params, &row_ids)?.into_iter().map(|(id, values)| EdgeRow {
                id,
                values: values.iter().map(|(key, value)| super::wire::CellEntry { key: key.clone(), value: super::wire::free_cell(value) }).collect(),
            }).collect())
        }).await
    }
    pub fn open_report(&self) -> Result<OpenReport, DbError> {
        self.not_closed()?;
        Ok(self.db.open_report().clone())
    }
    /// The epoch a call is admitted in: the epoch of the account scope the caller holds, else the epoch of the session now. `Closed` after the close.
    pub fn admit(&self, account_epoch: Option<u64>) -> Result<u64, DbError> {
        self.not_closed()?;
        Ok(account_epoch.unwrap_or_else(|| self.life.published().epoch))
    }
    pub async fn subscription_census(&self) -> Result<SubscriptionCensus, DbError> {
        self.not_closed()?;
        let (observed, subscriptions, retained, refused_events) = self.life.subscription_census().await.ok_or_else(|| DbError::Interrupted { detail: "the census was interrupted".into() })?;
        Ok(SubscriptionCensus { observed_queries: observed as u64, subscriptions: subscriptions as u64, retained_observers: retained as u64, refused_events })
    }
    pub async fn file(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<super::wire::WireFile>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "file", move |database| Ok(database.db.file(&model, &row_id, &field)?.map(Into::into))).await
    }
    pub async fn read(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<Vec<u8>>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "read file", move |database| Ok(database.db.read_file(&model, &row_id, &field)?)).await
    }
    pub async fn url(&self, epoch: u64, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        let row_id = id(&row_id)?;
        self.read_in_epoch(epoch, "file url", move |database| Ok(database.db.url(&model, &row_id, &field)?.map(|path| path.to_string_lossy().into_owned()))).await
    }
    pub async fn register_operations(&self, defs_json: String) -> Result<(), DbError> {
        if self.life.published().closed { return Err(super::command::closed()); }
        let defs: Vec<Value> = self.offload(move || parse_json(&defs_json)).await?;
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
            if let Some(kind) = crate::owners::calls::CallKind::declared_by(&document) {
                names.push((name.clone(), kind));
            }
            declared.push(operation);
        }
        let registered: Vec<(String, String)> = self.db.specs().values().flat_map(|spec| spec.actions.iter().filter_map(|action| action.mutation.as_ref().filter(|mutation| names.iter().any(|(name, _)| name == *mutation)).map(|_| (spec.key.to_string(), action.name.clone())))).collect();
        crate::owners::read::preload_keys(&self.db, false).map_err(|error| DbError::Invalid { detail: format!("the preload of the schema is invalid: {error}") })?;
        self.reads.declare(declared);
        self.life.declare(names);
        self.life.register_operations(registered);
        self.life.start_preload();
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
    pub async fn call(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        let Some(returns) = self.returns.get(&operation).cloned() else {
            return Err(if self.life.published().closed { super::command::ended() } else { DbError::Invalid { detail: format!("operation {operation} is not declared in the specs") } });
        };
        let root = self.run(epoch, operation, variables_json).await?;
        self.offload(move || super::call_result::normalize(&root, &returns).map_err(|detail| DbError::Invalid { detail })).await
    }
    pub async fn retry(&self, epoch: u64, model: String, row_id: String, action: Option<String>, source: Option<WireUploadRequest>) -> Result<WireOperationReceipt, DbError> {
        if source.is_some() || action.as_deref().is_some_and(|action| self.db.spec(&model).is_ok_and(|spec| spec.upload_action(action).is_some())) {
            return self.retry_transfer(epoch, model, row_id, action, source).await;
        }
        self.account_call(epoch)?;
        if let Some(receipt) = self.retry_declared_transfer(epoch, &model, &row_id).await? { return Ok(receipt); }
        self.retry_operation(epoch, model, row_id).await
    }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.apply_online(online) }

    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(OnlineSink::of_life(&self.life)) }
    /// Closes the database: every lease ends, every holder closes, every database thread lets go of the file, then the process slot opens for the next database. Closing again returns the same result. The name differs from `close` because Kotlin objects already carry `close()` for their native handle.
    pub async fn close_database(&self) -> Result<(), DbError> {
        let waiter = self.life.close();
        let _closing = crate::runtime::hold_until_woken(self.life.launch());
        let answered = on_executor(crate::runtime::counted(Some(self.life.launch()), async move { Ok::<_, DbError>(waiter.wait().await) })).await;
        crate::runtime::release_holds();
        let answered = answered.map(|_| ());
        let (life, _closing) = (self.life.clone(), crate::runtime::hold_until_woken(self.life.launch()));
        let ended = blocking(move || { life.shutdown(); Ok(()) }).await;
        crate::runtime::release_holds();
        if let Some(lease) = self.lease.get() { lease.release(); }
        answered.and(ended)
    }
}

impl Database {
    pub(crate) async fn run(&self, epoch: u64, operation: String, variables_json: String) -> Result<String, DbError> {
        self.remote()?;
        let variables_json = self.offload(move || Ok(parse_json::<Value>(&variables_json)?.to_string())).await?;
        match self.await_life(self.life.run(epoch, operation, variables_json)).await? {
            LifeReply::Call(Ok(answer)) => Ok(answer),
            LifeReply::Call(Err(error)) => Err(call_failure(error)),
            _ => Err(super::command::ended()),
        }
    }
    /// Runs the blocking part of a call. The owners barrier counts the job until the call wakes.
    pub(super) async fn offload<T: Send + 'static>(&self, work: impl FnOnce() -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
        let kept = crate::runtime::hold_until_woken(self.life.launch());
        let value = blocking(work).await;
        crate::runtime::release_holds();
        drop(kept);
        value
    }

    /// Waits for the answer of an owner. An answer other than a value ends the call with the end of its epoch.
    pub(super) async fn await_life(&self, waiter: crate::runtime::Waiter<LifeReply>) -> Result<LifeReply, DbError> {
        match waiter.wait().await {
            crate::runtime::Outcome::Value(LifeReply::Closed) | crate::runtime::Outcome::Closing => Err(super::command::closed()),
            crate::runtime::Outcome::EpochEnded(_) => Err(super::command::ended()),
            crate::runtime::Outcome::Internal => Err(DbError::Internal { detail: "the answer of the owner was lost".into() }),
            crate::runtime::Outcome::Value(reply) => Ok(reply),
        }
    }

    /// Server reads need a transport.
    pub(super) fn remote(&self) -> Result<(), DbError> {
        if self.remote { Ok(()) } else { Err(DbError::NoSession { detail: "transport is required for server operations".into() }) }
    }

    pub(super) fn parsed_query(&self, spec: &super::QuerySpec) -> Result<crate::store::ParsedQuery, DbError> {
        let relation = spec.relation.as_deref().map(|name| parse_json::<serde_json::Map<String, serde_json::Value>>(&spec.params_json).map(|params| (name, params))).transpose()?;
        Ok(self.db.select_query(spec.query()?, relation)?)
    }
}


/// The failure of `run` as the platform sees it.
fn call_failure(error: crate::owners::calls::CallError) -> DbError {
    use crate::owners::calls::CallError;
    match error {
        CallError::NoSession => DbError::NoSession { detail: "the database has no session".into() },
        CallError::Invalid => DbError::Invalid { detail: "the operation is not registered".into() },
        CallError::Offline => DbError::Offline { detail: "the operation needs the network".into() },
        CallError::Network => DbError::Network { detail: "the request did not reach the server".into() },
        CallError::Graphql { messages, extensions } => DbError::Graphql { messages, extensions },
        CallError::Session { code, detail } => DbError::Session { code, detail },
        CallError::Http { status, detail } => DbError::Http { status, detail },
        CallError::Unauthorized => DbError::Unauthorized { detail: "the server refused the credential".into() },
        CallError::Dropped => DbError::Dropped { detail: "the request left and the answer did not come".into() },
        CallError::InvalidResponse => DbError::Invalid { detail: "the response has no root of the operation".into() },
        CallError::Interrupted => super::command::ended(),
        CallError::Storage => DbError::Storage { detail: "the database could not save its state".into() },
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
