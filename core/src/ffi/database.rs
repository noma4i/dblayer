use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use serde_json::{Map, Value};
use tokio::runtime::Runtime;
use crate::db::{Db, DbConfig};
use crate::identity::RowId;
use crate::pacing::SystemClock;
use crate::query::{Query, Where};
use crate::schema::ModelSpec;
use crate::sync::{LoadOptions, OperationDef, Sync, SyncConfig};
use super::live::LiveHandles;
use super::transport::ForeignTransport;
use super::{row_json, DbError, LiveChange, LiveSubscription, OnlineSink, QueryListener, RelationStateChange, StateListener, StateSubscription, Transport};

pub(super) fn executor() -> &'static Runtime {
    static EXECUTOR: OnceLock<Runtime> = OnceLock::new();
    EXECUTOR.get_or_init(|| tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("database executor"))
}

#[derive(uniffi::Record)]
pub struct Config { pub path: Option<String>, pub data_version: Option<String>, pub cache_rows: u32 }

#[derive(uniffi::Object)]
pub struct Database { db: Db, sync: Option<Sync> }

fn id(value: &str) -> Result<RowId, DbError> {
    RowId::new(value).map_err(|error| DbError::Invalid { detail: error.to_string() })
}

#[uniffi::export]
impl Database {
    #[uniffi::constructor]
    pub fn open(config: Config, models_json: String, transport: Option<Arc<dyn Transport>>) -> Result<Arc<Self>, DbError> {
        let models: Vec<ModelSpec> = serde_json::from_str(&models_json)?;
        let db = Db::open_with(config.path.as_deref().map(Path::new), models, config.data_version.as_deref(), DbConfig { cache_rows: config.cache_rows as usize })?;
        let sync = transport.map(|transport| Sync::new(db.clone(), Arc::new(ForeignTransport(transport)), Arc::new(SystemClock), SyncConfig::default()));
        Ok(Arc::new(Self { db, sync }))
    }

    pub fn live_find(&self, model: String, row_id: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let row_id = id(&row_id)?;
        self.connect(Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))), None, false, listener)
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
        let task = executor().spawn(async move {
            loop {
                let change = RelationStateChange::of(&receiver.borrow_and_update());
                listener.changed(change);
                if receiver.changed().await.is_err() { return; }
            }
        });
        Ok(Arc::new(StateSubscription { task: Mutex::new(Some(task.abort_handle())) }))
    }
    pub fn live_belongs_to(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.related(model, row_id, name, listener)
    }
    pub fn live_has_many(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.related(model, row_id, name, listener)
    }
    pub fn live_has_one(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.related(model, row_id, name, listener)
    }
    pub fn live_references(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        self.related(model, row_id, name, listener)
    }
    pub fn create(&self, model: String, values_json: String) -> Result<String, DbError> {
        let row = self.db.create(&model, serde_json::from_str(&values_json)?)?;
        self.send_writes();
        Ok(row_json(&row))
    }
    pub fn update(&self, model: String, row_id: String, patch_json: String) -> Result<String, DbError> {
        let row = self.db.update(&model, &id(&row_id)?, serde_json::from_str(&patch_json)?)?;
        self.send_writes();
        Ok(row_json(&row))
    }
    pub fn act(&self, model: String, row_id: String, action: String, patch_json: String, args_json: String) -> Result<String, DbError> {
        let row = self.db.act(&model, &action, &id(&row_id)?, serde_json::from_str(&patch_json)?, serde_json::from_str(&args_json)?)?;
        self.send_writes();
        Ok(row_json(&row))
    }
    pub fn destroy(&self, model: String, row_id: String) -> Result<(), DbError> {
        self.db.destroy(&model, &id(&row_id)?)?;
        self.send_writes();
        Ok(())
    }
    pub fn set_ephemeral(&self, model: String, row_id: String, values_json: String) -> Result<(), DbError> {
        Ok(self.db.set_ephemeral(&model, &id(&row_id)?, serde_json::from_str(&values_json)?)?)
    }
    pub fn attach(&self, model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String) -> Result<String, DbError> {
        Ok(self.db.attach(&model, &id(&row_id)?, &field, &bytes, &name, &mime)?.to_value().to_string())
    }
    pub fn file(&self, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        Ok(self.db.file(&model, &id(&row_id)?, &field)?.map(|file| file.to_value().to_string()))
    }
    pub fn read(&self, model: String, row_id: String, field: String) -> Result<Option<Vec<u8>>, DbError> { Ok(self.db.read(&model, &id(&row_id)?, &field)?) }
    pub fn url(&self, model: String, row_id: String, field: String) -> Result<Option<String>, DbError> {
        Ok(self.db.url(&model, &id(&row_id)?, &field)?.map(|path| path.to_string_lossy().into_owned()))
    }
    pub fn detach(&self, model: String, row_id: String, field: String) -> Result<bool, DbError> { Ok(self.db.detach(&model, &id(&row_id)?, &field)?) }
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
    pub async fn refresh(&self, model: String, relation: String, params_json: String) -> Result<(), DbError> {
        self.sync()?.refresh(&model, &relation, &serde_json::from_str(&params_json)?).await?;
        Ok(())
    }
    pub async fn load_more(&self, model: String, relation: String, params_json: String) -> Result<bool, DbError> {
        Ok(self.sync()?.load_more(&model, &relation, &serde_json::from_str(&params_json)?).await?.has_next)
    }
    pub async fn flush(&self) -> Result<(), DbError> { self.sync()?.flush().await?; Ok(()) }
    pub async fn retry(&self, model: String, row_id: String) -> Result<(), DbError> { self.sync()?.retry(&model, &id(&row_id)?).await?; Ok(()) }
    pub async fn retry_all(&self) -> Result<(), DbError> { self.sync()?.retry_all().await?; Ok(()) }
    pub fn discard(&self, model: String, row_id: String) -> Result<(), DbError> { self.db.discard(&model, &id(&row_id)?)?; Ok(()) }
    pub async fn resume(&self) -> Result<(), DbError> { self.sync()?.resume().await?; Ok(()) }
    pub fn set_online(&self, online: bool) -> Result<(), DbError> { self.online_sink()?.set_online(online); Ok(()) }
    pub fn online_sink(&self) -> Result<Arc<OnlineSink>, DbError> { Ok(Arc::new(OnlineSink { sync: self.sync()?.clone() })) }
    pub async fn upload(&self, model: String, row_id: String, input_json: String) -> Result<(), DbError> {
        self.sync()?.upload(&model, &id(&row_id)?, serde_json::from_str(&input_json)?).await?;
        Ok(())
    }
    pub async fn cancel_upload(&self, model: String, row_id: String) -> Result<bool, DbError> { Ok(self.sync()?.cancel_upload(&model, &id(&row_id)?).await?) }
    pub fn maintain(&self) -> Result<(), DbError> { self.db.maintain(&[])?; Ok(()) }
    pub fn logout(&self) -> Result<(), DbError> { self.db.reset_all()?; Ok(()) }
    pub fn cache_stats(&self) -> String {
        let counts = self.db.counters();
        serde_json::json!({ "hits": counts.cache_hits, "misses": counts.cache_misses, "evictions": counts.cache_evictions }).to_string()
    }
    pub fn diagnostics(&self) -> String {
        let counts = self.db.counters();
        let census = self.db.observer_census();
        serde_json::json!({ "observationReads": counts.observation_reads, "observerCalls": counts.observer_calls,
            "queries": census.observed_queries, "subscriptions": census.subscriptions }).to_string()
    }
}

impl Database {
    fn sync(&self) -> Result<&Sync, DbError> { self.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "transport is required for server operations".into() }) }
    fn send_writes(&self) {
        if let Some(sync) = self.sync.clone() { executor().spawn(async move { let _ = sync.flush().await; }); }
    }
    fn connect_json(&self, json: &str, count: bool, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let description: Value = serde_json::from_str(json)?;
        let mut query: Query = serde_json::from_value(description.clone())?;
        let remote = description["relation"].as_str().map(|name| -> Result<_, DbError> {
            let params: Map<String, Value> = description.get("params").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or_default();
            let spec = self.db.spec(query.model.as_str())?;
            let relation = spec.relations.iter().find(|relation| relation.name == name).ok_or_else(|| DbError::Invalid { detail: format!("unknown relation {name}") })?;
            if relation.remote.is_some() {
                let strings = params.iter().map(|(key, value)| (key.clone(), value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string()))).collect();
                query.scope = Some(relation.scope_key(query.model.as_str(), &strings).map_err(|error| DbError::Invalid { detail: error.to_string() })?);
                Ok(Some((name.to_string(), params)))
            } else { Ok(None) }
        }).transpose()?.flatten();
        self.connect(query, remote, count, listener)
    }
    fn connect(&self, query: Query, remote: Option<(String, Map<String, Value>)>, count: bool, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let _entered = executor().enter();
        let (latest, _) = tokio::sync::watch::channel(None);
        let post = latest.clone();
        let callback = listener.clone();
        let observer = if count {
            self.db.observe_count(query.clone(), move |change| {
                let value = LiveChange { revision: change.revision, rows: vec![], count: Some(change.count), diff_json: "{}".into(), related_json: "{}".into(), error: None };
                post.send_replace(Some(value.clone())); callback.changed(value);
            })?
        } else {
            self.db.observe(query.clone(), move |change| { let value = LiveChange::rows(change); post.send_replace(Some(value.clone())); callback.changed(value); })?
        };
        let (watch, load) = match remote {
            Some((relation, params)) => {
                let sync = self.sync()?.clone();
                let watch = sync.watch(query.model.as_str(), &relation, &params)?;
                let model = query.model.to_string();
                let errors = latest.clone();
                let task = executor().spawn(async move {
                    if let Err(error) = sync.load(&model, &relation, &params, LoadOptions::default()).await {
                        let previous = errors.borrow().clone();
                        if let Some(mut change) = previous { change.error = Some(error.to_string()); errors.send_replace(Some(change.clone())); listener.changed(change); }
                    }
                });
                (Some(watch), Some(task.abort_handle()))
            }
            None => (None, None),
        };
        Ok(Arc::new(LiveSubscription { latest, handles: Mutex::new(Some(LiveHandles { _observer: observer, _watch: watch, load })) }))
    }
    fn related(&self, model: String, row_id: String, name: String, listener: Arc<dyn QueryListener>) -> Result<Arc<LiveSubscription>, DbError> {
        let row_id = id(&row_id)?;
        let (latest, _) = tokio::sync::watch::channel(None);
        let post = latest.clone();
        let query = Query::new(&model).filtered(Where::equal("id", serde_json::json!(row_id))).including(&[&name]);
        let observer = self.db.observe(query, move |change| {
            let rows = change.related.get(&name).and_then(|parents| parents.get(&row_id)).cloned().unwrap_or_default();
            let mut value = LiveChange::rows(change);
            value.rows = rows.iter().map(row_json).collect();
            post.send_replace(Some(value.clone())); listener.changed(value);
        })?;
        Ok(Arc::new(LiveSubscription { latest, handles: Mutex::new(Some(LiveHandles { _observer: observer, _watch: None, load: None })) }))
    }
}
