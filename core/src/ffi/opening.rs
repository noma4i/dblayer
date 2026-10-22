use std::collections::HashMap;
use std::sync::Arc;
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, Transport};

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub endpoint: Option<String>,
    pub headers: HashMap<String, String>,
    pub operations: String,
    pub references: Vec<u64>,
}

#[derive(uniffi::Object)]
pub struct OpenDatabase {
    opened: tokio::sync::Mutex<Option<(OpenArguments, Arc<Database>)>>,
}

#[derive(PartialEq, Eq)]
struct OpenArguments {
    config: Config,
    models: String,
    endpoint: Option<String>,
    headers: HashMap<String, String>,
    operations: serde_json::Value,
    references: Vec<u64>,
    encrypted: Option<EncryptedConfig>,
}

#[uniffi::export]
impl OpenDatabase {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { opened: tokio::sync::Mutex::new(None) }) }

    pub async fn open(&self, config: Config, models_json: String, options: OpenOptions, transport: Option<Arc<dyn Transport>>, encrypted: Option<EncryptedConfig>, credentials: Option<Arc<dyn CredentialStore>>, http: Option<Arc<dyn EncryptedHttp>>) -> Result<Arc<Database>, DbError> {
        let arguments = OpenArguments { config: config.clone(), models: models_json.clone(), endpoint: options.endpoint.clone(), headers: options.headers.clone(), operations: serde_json::from_str(&options.operations)?, references: options.references.clone(), encrypted: encrypted.clone() };
        let mut opened = self.opened.lock().await;
        if let Some((previous, database)) = opened.as_ref() {
            return if previous == &arguments { Ok(database.clone()) } else { Err(DbError::Invalid { detail: "the database is already open with different parameters".into() }) };
        }
        let remote = transport.is_some();
        let database = Database::open(config, models_json, transport, encrypted, credentials, http).await?;
        if remote { database.register_operations(options.operations)?; }
        *opened = Some((arguments, database.clone()));
        Ok(database)
    }
}

#[cfg(test)]
#[path = "opening_tests.rs"]
mod tests;
