use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use super::model::Sut;
use super::value::{Input, Observation};
use crate::ffi::{CommandRequest, ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, PutReply, SubscriptionSink, Transport, TransportRequest};
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, OnFailure, RootShape};
use crate::transport::{PutFile, TransferIdentity};
use crate::value::ScalarKind;

/// The transport mock of the shared vocabulary: it answers no request, every call fails with a network error.
/// The database starts offline, so the vocabulary below sends nothing.
struct Quiet;

#[async_trait]
impl Transport for Quiet {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "quiet".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "quiet".into() }) }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Network { detail: "quiet".into() }) }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Network { detail: "quiet".into() }) }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { Err(DbError::Network { detail: "quiet".into() }) }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { Err(DbError::Network { detail: "quiet".into() }) }
}

const OPERATIONS: &str = r#"[{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"}]"#;

fn models_json() -> String {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("MakeMoment".to_string());
    create.durable = true;
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.on_failure = OnFailure::Keep;
    model.actions.push(create);
    serde_json::to_string(&vec![model]).unwrap_or_default()
}

fn error(kind: &str) -> Vec<Observation> {
    vec![Observation::new("error", json!({ "kind": kind }))]
}

/// The real core behind `Database` for the vocabulary of `specs/machines/corpus.md` (section "Inputs of the database runner").
/// `open`, `create`, `has`, `set_online`, `close` run. Every other input answers `unsupported`.
#[derive(Default)]
pub struct DatabaseSut {
    database: Option<Arc<Database>>,
    rows: HashMap<String, String>,
}

#[async_trait]
impl Sut for DatabaseSut {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        if input.name == "open" {
            let config = Config { path: None, data_version: None, cache_rows: 32 };
            return match Database::open(config, models_json(), Some(Arc::new(Quiet)), None, None, None, None, None).await {
                Ok(database) => {
                    if database.register_operations(OPERATIONS.into()).is_err() {
                        return error("operations");
                    }
                    let online = database.set_online(false);
                    self.database = Some(database);
                    match online {
                        Ok(()) => vec![Observation::bare("opened")],
                        Err(_) => error("online"),
                    }
                }
                Err(_) => error("open"),
            };
        }
        let Some(database) = self.database.clone() else { return error("not_open") };
        match input.name.as_str() {
            "set_online" => match database.set_online(input.flag("online").unwrap_or(false)) {
                Ok(()) => vec![Observation::new("online", json!(input.flag("online").unwrap_or(false)))],
                Err(_) => error("online"),
            },
            "create" => {
                let (Some(name), Some(text)) = (input.text("as"), input.text("text")) else { return error("arguments") };
                let values = json!({ "text": text }).to_string();
                match Database::send(&database, CommandRequest { epoch: database.session_epoch(), model: "Moment".into(), values_json: values, args_json: "{}".into(), upload: None }).await {
                    Ok(receipt) => match receipt.optimistic_value {
                        Some(row) => {
                            self.rows.insert(name.into(), row.id);
                            vec![Observation::new("created", json!({ "row": name }))]
                        }
                        None => error("no_row"),
                    },
                    Err(_) => error("create"),
                }
            }
            "has" => {
                let Some(name) = input.text("row") else { return error("arguments") };
                let id = self.rows.get(name).cloned().unwrap_or_else(|| name.to_string());
                match database.read_query(database.session_epoch(), r#"{"model":"Moment"}"#.into()).await {
                    Ok(found) => vec![Observation::new("row", json!({ "row": name, "present": found.iter().any(|row| row.id == id) }))],
                    Err(_) => error("has"),
                }
            }
            "close" => match database.close_database().await {
                Ok(()) => {
                    self.database = None;
                    vec![Observation::bare("closed")]
                }
                Err(_) => error("close"),
            },
            other => vec![Observation::new("unsupported", json!(other))],
        }
    }
}
