use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use crate::transport::{HttpRequest, RawOutcome};
use super::open_slot::OpenSlot;
use super::transport::ObserverStop;
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, PlatformObserver, TestClock, Transport};

/// A GraphQL operation the app declares: its name and its document.
#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OperationDefinition {
    pub name: String,
    pub document: String,
}

#[derive(Clone, Default, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub operations: Vec<OperationDefinition>,
}

/// The ports of the platform an open hands to the core: the network, the encrypted session and what it stores and reports.
#[derive(Default, uniffi::Record)]
pub struct OpenPorts {
    pub transport: Option<Arc<dyn Transport>>,
    pub encrypted: Option<EncryptedConfig>,
    pub credentials: Option<Arc<dyn CredentialStore>>,
    pub http: Option<Arc<dyn EncryptedHttp>>,
    /// The GraphQL address of a database without an encrypted session.
    #[uniffi(default = None)]
    pub endpoint: Option<String>,
    /// The HTTP and socket executor of the platform: the core takes the transport and the encrypted http from it.
    #[uniffi(default = None)]
    pub executor: Option<Arc<dyn Transport>>,
    /// Android below API 34 runs the upload round in a foreground service of type `dataSync`: `Some(declared)` says whether the manifest of the app declares its permission. `None` where the platform needs no declaration.
    #[uniffi(default = None)]
    pub data_sync_declared: Option<bool>,
}

struct ExecutorHttp(Arc<dyn Transport>);

impl Drop for ExecutorHttp {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ExecutorHttp::drop"); }
}

#[async_trait]
impl EncryptedHttp for ExecutorHttp {
    fn app_headers(&self) -> HashMap<String, String> { crate::lock::effect(|| self.0.app_headers()) }
    async fn send(&self, request: HttpRequest) -> RawOutcome { crate::lock::effect(|| self.0.send(request)).await }
}

impl OpenPorts {
    fn resolved(mut self) -> Self {
        if self.endpoint.is_none() {
            self.endpoint = self.encrypted.as_ref().map(|encrypted| encrypted.origin.clone());
        }
        if self.endpoint.is_some() && self.transport.is_none() {
            self.transport = self.executor.clone();
        }
        if self.encrypted.is_some() && self.http.is_none() {
            self.http = self.executor.clone().map(|executor| Arc::new(ExecutorHttp(executor)) as Arc<dyn EncryptedHttp>);
        }
        self
    }

    fn check_data_sync(&self) -> Result<(), DbError> {
        if self.data_sync_declared == Some(false) {
            return Err(DbError::Config { code: super::ConfigCode::InvalidRequest, detail: "the manifest of the app lacks android.permission.FOREGROUND_SERVICE_DATA_SYNC".into() });
        }
        Ok(())
    }
}

#[derive(uniffi::Object)]
pub struct OpenDatabase {
    slot: Arc<OpenSlot>,
    held: std::sync::Mutex<Option<Arc<Database>>>,
}

#[uniffi::export]
impl OpenDatabase {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { slot: OpenSlot::new(), held: std::sync::Mutex::new(None) }) }

    pub async fn open(&self, config: Config, models_json: String, options: OpenOptions, ports: OpenPorts, observers: Option<Arc<dyn PlatformObserver>>) -> Result<Arc<Database>, DbError> {
        ports.check_data_sync()?;
        let ports = ports.resolved();
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open(config, models_json, ports).await?;
        self.keep(Self::start(database, lease, remote, options, observers, true).await?)
    }

    /// The database this opener opened last, closed or not: a call on a closed database gets its `Closed` error. `NotOpen` before the first open.
    pub fn opened(&self) -> Result<Arc<Database>, DbError> {
        crate::lock::lock(&self.held).clone().ok_or_else(|| DbError::NotOpen { detail: "database is not open; call DBlayer.open once at app start".into() })
    }

    pub async fn open_with_clock(&self, config: Config, models_json: String, ports: OpenPorts, clock: Arc<TestClock>, options: OpenOptions, observers: Option<Arc<dyn PlatformObserver>>) -> Result<Arc<Database>, DbError> {
        ports.check_data_sync()?;
        let ports = ports.resolved();
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open_with_clock(config, models_json, ports, clock).await?;
        self.keep(Self::start(database, lease, remote, options, observers, false).await?)
    }
}

impl OpenDatabase {
    fn keep(&self, database: Arc<Database>) -> Result<Arc<Database>, DbError> {
        let previous = crate::lock::lock(&self.held).replace(database.clone());
        drop(previous);
        Ok(database)
    }

    async fn start(database: Arc<Database>, lease: Arc<super::open_slot::OpenLease>, remote: bool, options: OpenOptions, observers: Option<Arc<dyn PlatformObserver>>, verify_uploads: bool) -> Result<Arc<Database>, DbError> {
        database.hold(lease);
        let opened = match database.start_observers(observers).await {
            Ok(()) if remote => database.declare_operations(options.operations).await.and_then(|()| if verify_uploads { database.check_uploads() } else { Ok(()) }),
            other => other,
        };
        if let Err(error) = opened {
            let _ = database.close_database().await;
            return Err(error);
        }
        Ok(database)
    }
}

impl Database {
    async fn declare_operations(&self, operations: Vec<OperationDefinition>) -> Result<(), DbError> {
        let defs = serde_json::Value::Array(operations.into_iter().map(|operation| serde_json::json!({ "name": operation.name, "document": operation.document })).collect());
        self.register_operations(defs.to_string()).await
    }

    async fn start_observers(&self, observers: Option<Arc<dyn PlatformObserver>>) -> Result<(), DbError> {
        let Some(observers) = observers else { return Ok(()) };
        let sink = super::OnlineSink::of_life(&self.life);
        if self.db.observers().set(Arc::new(ObserverStop(observers.clone(), sink.registrations()))).is_err() {
            return Err(DbError::Invalid { detail: "the observers are already set".into() });
        }
        super::database::blocking(move || { observers.start(sink); Ok(()) }).await
    }
}

#[cfg(test)]
mod tests;
