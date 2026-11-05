use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::task::JoinHandle;

use super::mock::{Credentials, Mock};
use super::model::Sut;
use super::value::{Input, Observation};
use crate::ffi::{
    CommandRequest, Config, Database, DbError, LoadMoreResult, OpenDatabase, ReadEnd, ReadFetch, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, TestClock,
    WireOperationOutcome, WireOperationReceipt, WireRow,
};
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};

type Cancel = Arc<dyn Fn() + Send + Sync>;

const MODELS: &str = include_str!("../../tests/corpus/models.json");
const EVENT_MODELS: &str = include_str!("../../tests/corpus/models_events.json");
const OPERATIONS: &str = include_str!("../../tests/corpus/operations.json");

#[derive(Default)]
struct Heard(AtomicU64);

/// A stream the runner reads: a task pulls it into a queue, `read` takes the next value.
struct Pulled {
    stream: Arc<ReadStream>,
    queue: Arc<std::sync::Mutex<std::collections::VecDeque<(u64, ReadValue)>>>,
    task: JoinHandle<()>,
}

impl Drop for Pulled {
    fn drop(&mut self) { self.task.abort(); }
}

/// What a finished call hands back to the runner: what to observe and what to remember under a name.
#[derive(Default)]
struct Done {
    observed: Vec<Observation>,
    row: Option<(String, String)>,
    receipt: Option<(String, WireOperationReceipt)>,
    load: Option<(String, Cancel)>,
}

fn error_kind(error: &DbError) -> &'static str {
    match error {
        DbError::NoSession { .. } => "no_session",
        DbError::Security { .. } => "security",
        DbError::Config { .. } => "config",
        DbError::Invalid { .. } => "invalid",
        DbError::NotFound { .. } => "not_found",
        DbError::Database { .. } => "database",
        DbError::Sync { .. } => "sync",
        DbError::Network { detail } if detail == crate::ffi::NEEDS_NETWORK => "offline",
        DbError::Network { .. } => "network",
        DbError::Interrupted { .. } => "interrupted",
        DbError::Http { status: 401, .. } => "unauthorized",
        DbError::Http { .. } => "http",
        DbError::RequestTooLarge { .. } => "request_too_large",
        DbError::Graphql { .. } => "graphql",
        DbError::Session { .. } => "session",
        DbError::Closed { .. } => "closed",
        DbError::CorruptManifest { .. } => "corrupt_manifest",
    }
}

fn error(kind: &str) -> Vec<Observation> {
    vec![Observation::new("error", json!({ "kind": kind }))]
}

fn failed(error: &DbError) -> Done {
    Done { observed: self::error(error_kind(error)), ..Done::default() }
}

fn phase_name(phase: ReadPhase) -> &'static str {
    match phase {
        ReadPhase::Idle => "idle",
        ReadPhase::Loading => "loading",
        ReadPhase::Loaded => "loaded",
        ReadPhase::Failed => "failed",
    }
}

fn cause_name(end: ReadEnd) -> &'static str {
    match end {
        ReadEnd::SessionEnded => "session_ended",
        ReadEnd::Closed => "closed",
        ReadEnd::Cancelled => "cancelled",
        ReadEnd::Invalid => "invalid",
    }
}

fn fetch_of(name: &str) -> Option<ReadFetch> {
    match name {
        "never" => Some(ReadFetch::Never),
        "stale" => Some(ReadFetch::Stale),
        "force" => Some(ReadFetch::Force),
        "if_missing" => Some(ReadFetch::IfMissing),
        _ => None,
    }
}

/// The real core behind `Database` for the vocabulary of the `corpus` machine.
/// A call that may wait for the network runs as a task. The runner settles after every input and observes what finished.
pub struct DatabaseSut {
    database: Option<Arc<Database>>,
    mock: Arc<Mock>,
    clock: Arc<TestClock>,
    heard: Arc<Heard>,
    rows: HashMap<String, String>,
    receipts: HashMap<String, WireOperationReceipt>,
    loads: HashMap<String, Cancel>,
    streams: HashMap<String, Pulled>,
    calls: Vec<JoinHandle<Done>>,
    started: Vec<tokio::sync::oneshot::Receiver<()>>,
}

impl Default for DatabaseSut {
    fn default() -> Self {
        Self {
            database: None,
            mock: Mock::new(),
            clock: TestClock::new(1_800_000_000_000),
            heard: Arc::default(),
            rows: HashMap::new(),
            receipts: HashMap::new(),
            loads: HashMap::new(),
            streams: HashMap::new(),
            calls: Vec::new(),
            started: Vec::new(),
        }
    }
}

impl DatabaseSut {
    /// Closes the database a trace left open, so the next trace can open its own.
    pub async fn finish(&mut self) {
        self.mock.release_all();
        if let Some(database) = self.database.take() { let _ = database.close_database().await; }
    }

    fn id_of(&self, name: &str) -> String {
        self.rows.get(name).cloned().unwrap_or_else(|| name.to_string())
    }

    fn name_of(&self, id: &str) -> String {
        self.rows.iter().find(|(_, value)| value.as_str() == id).map_or_else(|| id.to_string(), |(name, _)| name.clone())
    }

    fn names(&self, rows: &[WireRow]) -> Value {
        Value::Array(rows.iter().map(|row| Value::String(self.name_of(&row.id))).collect())
    }

    fn state(&self, stream: &str, state: &ReadState, into: &mut Map<String, Value>) {
        into.insert("stream".into(), stream.into());
        into.insert("phase".into(), phase_name(state.phase).into());
        if let Some(error) = &state.error { into.insert("error".into(), error.clone().into()); }
        if let Some(at) = state.loaded { into.insert("loaded".into(), at.into()); }
    }

    fn value(&self, stream: &str, value: &ReadValue) -> Observation {
        let mut out = Map::new();
        match value {
            ReadValue::Snapshot { revision, rows, state } => {
                self.state(stream, state, &mut out);
                out.insert("revision".into(), (*revision).into());
                out.insert("rows".into(), self.names(rows));
                Observation::new("snapshot", Value::Object(out))
            }
            ReadValue::Delta { base, revision, rows, removed, state, .. } => {
                self.state(stream, state, &mut out);
                out.insert("base".into(), (*base).into());
                out.insert("revision".into(), (*revision).into());
                out.insert("rows".into(), self.names(rows));
                out.insert("removed".into(), Value::Array(removed.iter().map(|id| Value::String(self.name_of(id))).collect()));
                Observation::new("delta", Value::Object(out))
            }
            ReadValue::Count { revision, count, state } => {
                self.state(stream, state, &mut out);
                out.insert("revision".into(), (*revision).into());
                out.insert("count".into(), (*count).into());
                Observation::new("count", Value::Object(out))
            }
            ReadValue::State { state, .. } => {
                self.state(stream, state, &mut out);
                Observation::new("state", Value::Object(out))
            }
            ReadValue::Event { position, payload } => {
                let event: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
                Observation::new("event", json!({ "stream": stream, "position": position, "area": event["area"], "kind": event["kind"], "row": self.name_of(event["id"].as_str().unwrap_or_default()) }))
            }
            ReadValue::Ready => Observation::new("ready", json!({ "stream": stream })),
            ReadValue::Gap => Observation::new("gap", json!({ "stream": stream })),
            ReadValue::Retry { .. } => Observation::new("unsupported", json!("retry")),
            ReadValue::Lost { first, last } => Observation::new("lost", json!({ "stream": stream, "first": first, "last": last })),
            ReadValue::Status { .. } => Observation::new("unsupported", json!("status")),
            ReadValue::Ended { cause } => Observation::new("ended", json!({ "stream": stream, "cause": cause_name(*cause) })),
        }
    }

    fn launch(&mut self, work: impl std::future::Future<Output = Done> + Send + 'static) {
        let (entered, started) = tokio::sync::oneshot::channel();
        self.started.push(started);
        self.calls.push(crate::task::spawn(crate::task::Carry::here(), async move {
            let mut work = Box::pin(work);
            let first = std::future::poll_fn(|cx| std::task::Poll::Ready(work.as_mut().poll(cx))).await;
            let _ = entered.send(());
            match first { std::task::Poll::Ready(done) => done, std::task::Poll::Pending => work.await }
        }));
    }

    /// Waits until requests, finished calls and stream notices stop changing for 80 ms, then observes everything that finished.
    async fn settle(&mut self) -> Vec<Observation> {
        for started in std::mem::take(&mut self.started) { let _ = started.await; }
        let mut quiet = 0;
        let mut last = (usize::MAX, usize::MAX, u64::MAX);
        if let Some(database) = self.database.clone() { let owners = database.life_handle(); let _ = tokio::task::spawn_blocking(move || owners.barrier(Duration::from_secs(2))).await; }
        for _ in 0..600 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = (self.mock.count(), self.calls.iter().filter(|call| call.is_finished()).count(), self.heard.0.load(Ordering::SeqCst));
            quiet = if now == last { quiet + 1 } else { 0 };
            last = now;
            if quiet >= 8 { break; }
        }
        let mut observed = Vec::new();
        let mut waiting = Vec::new();
        for call in std::mem::take(&mut self.calls) {
            if !call.is_finished() { waiting.push(call); continue; }
            let Ok(done) = call.await else { continue };
            observed.extend(done.observed);
            if let Some((name, id)) = done.row { self.rows.insert(name, id); }
            if let Some((name, receipt)) = done.receipt { self.receipts.insert(name, receipt); }
            if let Some((name, load)) = done.load { self.loads.insert(name, load); }
        }
        self.calls = waiting;
        observed.extend(self.mock.announce());
        observed
    }

    async fn open(&mut self, models: &str, encrypted: bool) -> Vec<Observation> {
        let config = Config { path: None, data_version: None, cache_rows: 32 };
        let transport: Arc<dyn crate::ffi::Transport> = self.mock.clone();
        let (encrypted, credentials, http): (Option<crate::ffi::EncryptedConfig>, Option<Arc<dyn crate::ffi::CredentialStore>>, Option<Arc<dyn crate::ffi::EncryptedHttp>>) = if encrypted {
            (Some(crate::transport::encrypted::protocol_tests::config()), Some(Arc::new(Credentials::default())), Some(self.mock.clone()))
        } else {
            (None, None, None)
        };
        let opened = OpenDatabase::new().open_with_clock(config, if models == "events" { EVENT_MODELS } else { MODELS }.into(), Some(transport), None, self.clock.clone(), encrypted, credentials, http, None).await;
        match opened {
            Ok(database) => {
                if database.register_operations(OPERATIONS.into()).is_err() { return error("operations"); }
                if database.set_online(true).is_err() { return error("online"); }
                self.database = Some(database);
                let _ = self.settle().await;
                vec![Observation::bare("opened")]
            }
            Err(_) => error("open"),
        }
    }

    fn receipt_done(name: String, row: String, op: &str, result: Result<WireOperationReceipt, DbError>) -> Done {
        match result {
            Ok(receipt) => Done { observed: vec![Observation::new("receipt", json!({ "row": row, "op": op }))], receipt: Some((name, receipt)), ..Done::default() },
            Err(error) => failed(&error),
        }
    }

    fn start(&mut self, database: Arc<Database>, input: &Input) -> Option<Vec<Observation>> {
        let epoch = database.session_epoch();
        let text = |key: &str| input.text(key).map(str::to_string);
        match input.name.as_str() {
            "create" => {
                let (Some(name), Some(text)) = (text("as"), text("text")) else { return Some(error("arguments")) };
                self.launch(async move {
                    match Database::send(&database, CommandRequest { epoch, model: "Moment".into(), values_json: json!({ "text": text }).to_string(), args_json: "{}".into(), upload: None }).await {
                        Ok(receipt) => {
                            let row = receipt.optimistic_value.as_ref().map(|row| row.id.clone());
                            let mut done = Self::receipt_done(name.clone(), name.clone(), "create", Ok(receipt));
                            done.row = row.map(|id| (name, id));
                            done
                        }
                        Err(error) => failed(&error),
                    }
                });
            }
            "update" => {
                let (Some(row), Some(text)) = (text("row"), text("text")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let name = text_or(input, "as", &format!("{row}:update"));
                self.launch(async move { Self::receipt_done(name, row, "update", database.update(epoch, "Moment".into(), id, json!({ "text": text }).to_string(), "{}".into()).await) });
            }
            "act" => {
                let (Some(row), Some(action)) = (text("row"), text("action")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let name = text_or(input, "as", &format!("{row}:{action}"));
                self.launch(async move { Self::receipt_done(name, row, "act", database.act(epoch, "Moment".into(), id, action, "{}".into(), "{}".into()).await) });
            }
            "destroy" => {
                let Some(row) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let name = text_or(input, "as", &format!("{row}:destroy"));
                self.launch(async move { Self::receipt_done(name, row, "destroy", database.destroy(epoch, "Moment".into(), id).await) });
            }
            "retry" => {
                let Some(row) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let action = text("action");
                let name = text_or(input, "as", &format!("{row}:retry"));
                self.launch(async move { Self::receipt_done(name, row, "retry", database.retry(epoch, "Moment".into(), id, action, None).await) });
            }
            "discard" => {
                let Some(row) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let action = text("action");
                self.launch(async move { database.discard(epoch, "Moment".into(), id, action).await.map_or_else(|e| failed(&e), |()| done("discarded", json!({ "row": row }))) });
            }
            "upload" => {
                let (Some(row), Some(action)) = (text("row"), text("action")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let name = text_or(input, "as", &format!("{row}:{action}"));
                let path = std::env::temp_dir().join(format!("corpus_{}.bin", std::process::id()));
                if std::fs::write(&path, b"corpus").is_err() { return Some(error("file")); }
                let request = WireUploadRequest { path: path.to_string_lossy().into_owned(), name: Some("clip.bin".into()), mime: Some("application/octet-stream".into()), variables_json: "{}".into() };
                self.launch(async move { Self::receipt_done(name, row, "upload", database.upload(epoch, "Moment".into(), id, action, request).await) });
            }
            "cancel_upload" => {
                let (Some(row), Some(action)) = (text("row"), text("action")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                self.launch(async move { database.cancel_upload(epoch, "Moment".into(), id, action).await.map_or_else(|e| failed(&e), |value| done("cancelled", json!({ "row": row, "value": value }))) });
            }
            "outcome" => {
                let Some(name) = text("receipt") else { return Some(error("arguments")) };
                let Some(receipt) = self.receipts.get(&name).cloned() else { return Some(error("unknown_receipt")) };
                self.launch(async move {
                    match database.outcome(receipt).await {
                        Ok(outcome) => done("outcome", json!({ "receipt": name, "result": outcome_name(&outcome) })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "begin_load" => {
                let (Some(name), Some(relation)) = (text("as"), text("relation")) else { return Some(error("arguments")) };
                match database.begin_load(epoch, "Moment".into(), relation, "{}".into()) {
                    Ok(load) => {
                        let handle = load.clone();
                        self.loads.insert(name.clone(), Arc::new(move || handle.cancel()));
                        self.launch(async move { load.result().await.map_or_else(|e| failed(&e), |()| done("loaded", json!({ "load": name }))) });
                    }
                    Err(error) => return Some(self::error(error_kind(&error))),
                }
            }
            "refresh" => {
                let Some(relation) = text("relation") else { return Some(error("arguments")) };
                self.launch(async move { database.refresh(epoch, "Moment".into(), relation.clone(), "{}".into()).await.map_or_else(|e| failed(&e), |()| done("refreshed", json!({ "relation": relation }))) });
            }
            "load_more" => {
                let Some(relation) = text("relation") else { return Some(error("arguments")) };
                self.launch(async move {
                    match database.load_more(epoch, "Moment".into(), relation.clone(), "{}".into()).await {
                        Ok(result) => done("more", json!({ "relation": relation, "result": more_name(result) })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "node" => {
                let (Some(relation), Some(row)) = (text("relation"), text("row")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                self.launch(async move {
                    match database.node(epoch, "Moment".into(), relation, id, false).await {
                        Ok(found) => done("node", json!({ "row": row, "present": found.is_some() })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "run" => {
                let Some(operation) = text("operation") else { return Some(error("arguments")) };
                let variables = input.args.get("variables").map_or_else(|| "{}".to_string(), Value::to_string);
                self.launch(async move {
                    match database.run(epoch, operation.clone(), variables).await {
                        Ok(answer) => done("ran", json!({ "operation": operation, "answer": serde_json::from_str::<Value>(&answer).unwrap_or(Value::Null) })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "nodes" => {
                let (Some(relation), Some(rows)) = (text("relation"), names_arg(input, "rows")) else { return Some(error("arguments")) };
                let ids: Vec<String> = rows.iter().map(|row| self.id_of(row)).collect();
                let by_id: HashMap<String, String> = rows.iter().map(|row| (self.id_of(row), row.clone())).collect();
                let model = text_or(input, "model", "Moment");
                self.launch(async move {
                    match database.nodes(epoch, model, relation.clone(), ids).await {
                        Ok(found) => done("nodes", json!({ "relation": relation, "rows": found.iter().map(|row| by_id.get(&row.id).cloned().unwrap_or_else(|| row.id.clone())).collect::<Vec<_>>() })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "begin_load_all" => {
                let (Some(name), Some(relation)) = (text("as"), text("relation")) else { return Some(error("arguments")) };
                match database.begin_load_all(epoch, "Moment".into(), relation, "{}".into(), input.flag("force").unwrap_or(false)) {
                    Ok(load) => {
                        let handle = load.clone();
                        self.loads.insert(name.clone(), Arc::new(move || handle.cancel()));
                        self.launch(async move { load.result().await.map_or_else(|e| failed(&e), |()| done("loaded_all", json!({ "load": name }))) });
                    }
                    Err(error) => return Some(self::error(error_kind(&error))),
                }
            }
            "peek_row" => {
                let Some(name) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&name);
                return Some(match database.peek_row(epoch, text_or(input, "model", "Moment"), id) {
                    Ok(found) => vec![Observation::new("peeked", json!({ "row": name, "present": found.is_some() }))],
                    Err(error) => self::error(error_kind(&error)),
                });
            }
            "edge_values" => {
                let (Some(relation), Some(rows)) = (text("relation"), names_arg(input, "rows")) else { return Some(error("arguments")) };
                let ids: Vec<String> = rows.iter().map(|row| self.id_of(row)).collect();
                let by_id: HashMap<String, String> = rows.iter().map(|row| (self.id_of(row), row.clone())).collect();
                let model = text_or(input, "model", "Moment");
                self.launch(async move {
                    match database.edge_values(epoch, model, relation.clone(), "{}".into(), ids).await {
                        Ok(values) => {
                            let parsed: Map<String, Value> = serde_json::from_str(&values).unwrap_or_default();
                            let named: Map<String, Value> = parsed.into_iter().map(|(id, value)| (by_id.get(&id).cloned().unwrap_or(id), value)).collect();
                            done("edges", json!({ "relation": relation, "values": named }))
                        }
                        Err(error) => failed(&error),
                    }
                });
            }
            "upload_batch" => {
                let (Some(name), Some(count), Some(operation)) = (text("as"), input.number("count"), text("operation")) else { return Some(error("arguments")) };
                let path = std::env::temp_dir().join(format!("corpus_batch_{}.bin", std::process::id()));
                if std::fs::write(&path, b"corpus").is_err() { return Some(error("file")); }
                let items: Vec<WireUploadItem> = (0..count).map(|index| WireUploadItem { values_json: json!({ "text": format!("{name}{}", index + 1) }).to_string(), path: path.to_string_lossy().into_owned(), name: Some("clip.bin".into()), mime: Some("application/octet-stream".into()), variables_json: "{}".into() }).collect();
                let request = WireUploadBatch { operation, variables_json: "{}".into() };
                self.launch(async move {
                    match database.upload_create_batch(epoch, "Moment".into(), items, request).await {
                        Ok(receipt) => Done { observed: (1..=receipt.items.len()).map(|index| Observation::new("receipt", json!({ "row": format!("{name}{index}"), "op": "upload" }))).collect(), ..Done::default() },
                        Err(error) => failed(&error),
                    }
                });
            }
            "session_call" => {
                let (Some(operation), Some(body)) = (text("operation"), input.args.get("body")) else { return Some(error("arguments")) };
                let body = body.to_string();
                self.launch(async move { database.session_call(operation.clone(), body).await.map_or_else(|e| failed(&e), |answer| done("session", json!({ "operation": operation, "answer": serde_json::from_str::<Value>(&answer).unwrap_or(Value::Null) }))) });
            }
            "session_reset" => {
                self.launch(async move { database.session_reset().await.map_or_else(|e| failed(&e), |()| done("reset", Value::Null)) });
            }
            "close" => {
                self.mock.release_all();
                self.launch(async move { database.close_database().await.map_or_else(|e| failed(&e), |()| Done { observed: vec![Observation::bare("closed")], ..Done::default() }) });
            }
            "has" => {
                let Some(name) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&name);
                self.launch(async move {
                    match database.read_query(epoch, r#"{"model":"Moment"}"#.into()).await {
                        Ok(found) => done("row", json!({ "row": name, "present": found.iter().any(|row| row.id == id) })),
                        Err(error) => failed(&error),
                    }
                });
            }
            _ => return None,
        }
        Some(Vec::new())
    }

    fn open_stream(&mut self, database: &Arc<Database>, input: &Input) -> Vec<Observation> {
        let Some(name) = input.text("as") else { return error("arguments") };
        let query = |relation: Option<&str>| match relation {
            Some(relation) => json!({ "model": "Moment", "relation": relation, "params": {} }).to_string(),
            None => json!({ "model": "Moment" }).to_string(),
        };
        let request = match input.name.as_str() {
            "open_list" => ReadRequest::List { query_json: query(input.text("relation")) },
            "open_count" => ReadRequest::Count { query_json: query(input.text("relation")) },
            "open_entity" => {
                let (Some(row), Some(fetch)) = (input.text("row"), input.text("fetch").and_then(fetch_of)) else { return error("arguments") };
                ReadRequest::Entity { model: "Moment".into(), id: self.id_of(row), fetch }
            }
            _ => {
                let Some(model) = input.text("model") else { return error("arguments") };
                ReadRequest::Events { model: model.into() }
            }
        };
        let stream = database.subscribe_values(database.session_epoch(), request);
        let queue: Arc<std::sync::Mutex<std::collections::VecDeque<(u64, ReadValue)>>> = Arc::default();
        let (pulling, kept, heard) = (stream.clone(), queue.clone(), self.heard.clone());
        let task = crate::task::spawn(crate::task::Carry::here(), async move {
            let mut seq = 0;
            while let Ok(Some(value)) = pulling.pull().await {
                seq += 1;
                kept.lock().unwrap().push_back((seq, value));
                heard.0.fetch_add(1, Ordering::SeqCst);
            }
        });
        self.streams.insert(name.into(), Pulled { stream, queue, task });
        Vec::new()
    }
}

fn names_arg(input: &Input, key: &str) -> Option<Vec<String>> {
    input.args.get(key)?.as_array()?.iter().map(|value| value.as_str().map(str::to_string)).collect()
}

fn text_or(input: &Input, key: &str, default: &str) -> String {
    input.text(key).unwrap_or(default).to_string()
}

fn done(kind: &str, value: Value) -> Done {
    let observed = if value.is_null() { Observation::bare(kind) } else { Observation::new(kind, value) };
    Done { observed: vec![observed], ..Done::default() }
}

fn outcome_name(outcome: &WireOperationOutcome) -> &'static str {
    match outcome {
        WireOperationOutcome::Confirmed { .. } => "confirmed",
        WireOperationOutcome::Rejected { .. } => "rejected",
        WireOperationOutcome::DeliveryUnknown => "delivery_unknown",
        WireOperationOutcome::Interrupted => "interrupted",
    }
}

fn more_name(result: LoadMoreResult) -> &'static str {
    match result {
        LoadMoreResult::Loaded { has_more: true } => "loaded_more",
        LoadMoreResult::Loaded { has_more: false } => "loaded_last",
        LoadMoreResult::NoMore => "no_more",
        LoadMoreResult::AlreadyLoading => "already_loading",
    }
}

#[async_trait]
impl Sut for DatabaseSut {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        match input.name.as_str() {
            "open" => return self.open(input.text("models").unwrap_or(""), input.flag("encrypted").unwrap_or(false)).await,
            "tick" => {
                self.clock.advance(input.number("ms").unwrap_or(0));
                return self.settle().await;
            }
            "answer" | "error" | "drop" | "ready" | "frame" => {
                let Some(id) = input.text("id") else { return error("arguments") };
                let reached = match input.name.as_str() {
                    "answer" => input.args.get("json").is_some_and(|body| self.mock.answer(id, &body.to_string())),
                    "error" => input.text("kind").is_some_and(|kind| self.mock.fail(id, kind)),
                    "drop" => self.mock.drop_connection(id),
                    "ready" => self.mock.ready(id),
                    _ => match input.args.get("event") {
                        Some(event) => match self.mock.frame(id, &event.to_string()) {
                            Ok(reached) => reached,
                            Err(_) => return error("frame"),
                        },
                        None => return error("arguments"),
                    },
                };
                if !reached { return error("no_request"); }
                return self.settle().await;
            }
            "lost" => return vec![Observation::new("unsupported", json!(input.name))],
            _ => {}
        }
        let Some(database) = self.database.clone() else { return error("not_open") };
        let mut observed = match input.name.as_str() {
            "online" => match database.online_sink() {
                Ok(sink) => { sink.set_online(input.flag("online").unwrap_or(false)); Vec::new() }
                Err(_) => error("online"),
            },
            "set_online" => match database.set_online(input.flag("online").unwrap_or(false)) {
                Ok(()) => vec![Observation::new("online", json!(input.flag("online").unwrap_or(false)))],
                Err(_) => error("online"),
            },
            "published" => vec![Observation::new("published", json!({ "epoch": database.session_epoch() }))],
            "open_list" | "open_count" | "open_entity" | "open_events" => self.open_stream(&database, input),
            "pull" => {
                let Some(name) = input.text("stream") else { return error("arguments") };
                let Some(pulled) = self.streams.get(name) else { return error("unknown_stream") };
                let next = pulled.queue.lock().unwrap().pop_front();
                match next {
                    Some((_, value)) => vec![self.value(name, &value)],
                    None => vec![Observation::new("empty", json!({ "stream": name }))],
                }
            }
            "cancel" => {
                let Some(target) = input.text("target") else { return error("arguments") };
                if let Some(cancel) = self.loads.get(target) { cancel(); } else if let Some(pulled) = self.streams.get(target) { let stream = pulled.stream.clone(); drop(crate::task::spawn(crate::task::Carry::here(), async move { stream.cancel().await })); } else { return error("unknown_target"); }
                Vec::new()
            }
            _ => match self.start(database, input) {
                Some(immediate) => immediate,
                None => return vec![Observation::new("unsupported", json!(input.name))],
            },
        };
        observed.extend(self.settle().await);
        if input.name == "close" && self.calls.is_empty() { self.database = None; }
        observed
    }
}
