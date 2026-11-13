use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use super::mock::{Credentials, Mock};
use super::model::Sut;
use super::value::{Input, Observation};
use crate::ffi::{
    CommandRequest, Config, Database, DbError, LoadMoreResult, OpenDatabase, EndCause, ReadFetch, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, QuerySpec, TestClock,
    WireOperationReceipt, WireRow,
};
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};

type Cancel = Arc<dyn Fn() + Send + Sync>;

const MODELS: &str = include_str!("../../../tests/corpus/models.json");
const EVENT_MODELS: &str = include_str!("../../../tests/corpus/models_events.json");
const OPERATIONS: &str = include_str!("../../../tests/corpus/operations.json");

type EncryptedParts = (Option<crate::ffi::EncryptedConfig>, Option<Arc<dyn crate::ffi::CredentialStore>>, Option<Arc<dyn crate::ffi::EncryptedHttp>>);

#[derive(Default)]
struct Heard(AtomicU64);

/// A stream the runner reads: a task counted by the owners barrier pulls it into a queue, `pull` of the corpus folds the unread values.
struct Pulled {
    stream: Arc<ReadStream>,
    queue: Arc<std::sync::Mutex<std::collections::VecDeque<(u64, ReadValue)>>>,
    task: tokio::task::JoinHandle<()>,
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

pub(super) fn error_kind(error: &DbError) -> &'static str {
    match error {
        DbError::NoSession { .. } => "no_session",
        DbError::Security { .. } => "security",
        DbError::Config { .. } => "config",
        DbError::Invalid { .. } => "invalid",
        DbError::NotFound { .. } => "not_found",
        DbError::AlreadyExists { .. } => "already_exists",
        DbError::Storage { .. } => "storage",
        DbError::Offline { .. } => "offline",
        DbError::Dropped { .. } => "dropped",
        DbError::Unauthorized { .. } => "unauthorized",
        DbError::InFlight { .. } => "in_flight",
        DbError::Runtime { .. } => "runtime",
        DbError::Internal { .. } => "internal",
        DbError::Copy { .. } => "copy",
        DbError::Network { .. } => "network",
        DbError::Interrupted { .. } => "interrupted",
        DbError::Http { .. } => "http",
        DbError::RequestTooLarge { .. } => "request_too_large",
        DbError::ResponseTooLarge { .. } => "response_too_large",
        DbError::Graphql { .. } => "graphql",
        DbError::Session { .. } => "session",
        DbError::Closed { .. } => "closed",
        DbError::Rejected { .. } => "rejected",
        DbError::CorruptManifest { .. } => "corrupt_manifest",
        DbError::NotOpen { .. } => "not_open",
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

fn cause_name(end: &EndCause) -> &'static str {
    match end {
        EndCause::SessionEnded => "session_ended",
        EndCause::Closed => "closed",
        EndCause::Cancelled => "cancelled",
        EndCause::Invalid { .. } => "invalid",
        EndCause::Discarded => "discarded",
        EndCause::Storage { .. } => "storage",
        EndCause::Internal { .. } => "internal",
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

/// A call of the corpus. The runner polls it itself: its waker only marks it woken and the runner clears the mark before each poll.
struct Call {
    work: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Done> + Send>>>,
    woken: Arc<std::sync::atomic::AtomicBool>,
    holds: Arc<crate::runtime::Holds>,
    done: Option<Done>,
}

/// A wake marks the call woken first and then lets go of the barrier counts of the work it waited for.
struct Wakes {
    woken: Arc<std::sync::atomic::AtomicBool>,
    holds: Arc<crate::runtime::Holds>,
}

impl std::task::Wake for Wakes {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        self.holds.release();
    }
}

impl Call {
    fn poll_woken(&mut self) {
        let Some(work) = self.work.as_mut() else { return };
        if !self.woken.swap(false, Ordering::SeqCst) { return; }
        let waker = std::task::Waker::from(Arc::new(Wakes { woken: self.woken.clone(), holds: self.holds.clone() }));
        let polled = crate::runtime::polling(&self.holds, || work.as_mut().poll(&mut std::task::Context::from_waker(&waker)));
        if let std::task::Poll::Ready(done) = polled {
            self.done = Some(done);
            self.work = None;
            self.holds.release();
        }
    }

    fn is_woken(&self) -> bool { self.work.is_some() && self.woken.load(Ordering::SeqCst) }
}

/// The real core behind `Database` for the vocabulary of the `corpus` machine.
/// A call that may wait for the network runs as a task. The runner settles after every input and observes what finished.
pub struct DatabaseSut {
    database: Option<Arc<Database>>,
    mock: Arc<Mock>,
    /// The secret slot of the app: one for the whole stand, so it survives `close` + `open` like the keychain.
    credentials: Arc<Credentials>,
    clock: Arc<TestClock>,
    /// The clock reading in ms and the ms the next `open` sets it back by (`clock_back`).
    clock_now: u64,
    back: u64,
    heard: Arc<Heard>,
    rows: HashMap<String, String>,
    receipts: HashMap<String, WireOperationReceipt>,
    loads: HashMap<String, Cancel>,
    streams: HashMap<String, Pulled>,
    calls: Vec<Call>,
    symbolic: Option<symbolic::Symbolic>,
}

impl Default for DatabaseSut {
    fn default() -> Self {
        Self {
            database: None,
            mock: Mock::new(),
            credentials: Arc::default(),
            clock: TestClock::new(1_800_000_000_000),
            clock_now: 1_800_000_000_000,
            back: 0,
            heard: Arc::default(),
            rows: HashMap::new(),
            receipts: HashMap::new(),
            loads: HashMap::new(),
            streams: HashMap::new(),
            calls: Vec::new(),
            symbolic: None,
        }
    }
}

impl DatabaseSut {
    /// Every request that reached the mock, in arrival order: the id and the decrypted body of an encrypted exchange (`Null` for a subscription or a put).
    pub fn requests(&self) -> Vec<(String, Value)> { self.mock.sent() }

    /// The protocol config of the shared tests. In symbolic mode the operation `social` is a `Login` one, so a second `login` meets `AlreadyAuthenticated` like the spec says.
    fn encrypted_config(&self) -> crate::ffi::EncryptedConfig {
        let mut config = crate::transport::encrypted::tests::protocol_tests::config();
        if self.symbolic.is_some() {
            if let Some(operation) = config.endpoints.session_operations.get_mut("social") { operation.class = crate::transport::EncryptedOperationClass::Login; }
        }
        config
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
        if let Some(error) = &state.error { into.insert("error".into(), error.to_string().into()); }
        if let Some(at) = state.loaded { into.insert("loaded".into(), at.into()); }
    }

    fn folded(&self, stream: &str, folded: &fold::Folded) -> Observation {
        match folded {
            fold::Folded::Delta { base, revision, rows, removed, state } => {
                let mut out = Map::new();
                self.state(stream, state, &mut out);
                out.insert("base".into(), (*base).into());
                out.insert("revision".into(), (*revision).into());
                out.insert("rows".into(), Value::Array(rows.iter().map(|id| Value::String(self.name_of(id))).collect()));
                out.insert("removed".into(), Value::Array(removed.iter().map(|id| Value::String(self.name_of(id))).collect()));
                Observation::new("delta", Value::Object(out))
            }
            fold::Folded::Value(value) => self.value(stream, value),
        }
    }

    fn value(&self, stream: &str, value: &ReadValue) -> Observation {
        let mut out = Map::new();
        match value {
            ReadValue::Snapshot { revision, rows, state, .. } => {
                self.state(stream, state, &mut out);
                out.insert("revision".into(), (*revision).into());
                out.insert("rows".into(), self.names(rows));
                Observation::new("snapshot", Value::Object(out))
            }
            ReadValue::Delta { .. } => unreachable!("a delta is shown folded"),
            ReadValue::Count { revision, count, state } => {
                self.state(stream, state, &mut out);
                out.insert("revision".into(), (*revision).into());
                out.insert("count".into(), (*count).into());
                Observation::new("count", Value::Object(out))
            }
            ReadValue::Operations { revision, items } => {
                out.insert("revision".into(), (*revision).into());
                out.insert("operations".into(), items.len().into());
                Observation::new("operations", Value::Object(out))
            }
            ReadValue::State { state, .. } => {
                self.state(stream, state, &mut out);
                Observation::new("state", Value::Object(out))
            }
            ReadValue::Event { position, area, kind, id, .. } => {
                Observation::new("event", json!({ "stream": stream, "position": position, "area": area, "kind": kind, "row": self.name_of(id) }))
            }
            ReadValue::CableClose { position, event } => Observation::new("cable_close", json!({ "stream": stream, "position": position, "connection": event.connection_id, "code": event.code, "intended": event.intended })),
            ReadValue::Ready => Observation::new("ready", json!({ "stream": stream })),
            ReadValue::Changes { revision, rows } => Observation::new("changes", json!({ "stream": stream, "revision": revision, "rows": rows.len() })),
            ReadValue::Gap => Observation::new("gap", json!({ "stream": stream })),
            ReadValue::Lost { first, last } => Observation::new("lost", json!({ "stream": stream, "first": first, "last": last })),
            ReadValue::Status { .. } => Observation::new("unsupported", json!("status")),
            ReadValue::Ended { cause } => Observation::new("ended", json!({ "stream": stream, "cause": cause_name(cause) })),
        }
    }

    /// A call whose observations carry its name (`call`) when the input asked for it with `named`.
    fn launch_named(&mut self, call: Option<String>, work: impl std::future::Future<Output = Done> + Send + 'static) {
        self.launch(async move {
            let mut done = work.await;
            done.observed = tagged(done.observed, &call);
            done
        });
    }

    fn launch(&mut self, work: impl std::future::Future<Output = Done> + Send + 'static) {
        self.calls.push(Call { work: Some(Box::pin(work)), woken: Arc::new(std::sync::atomic::AtomicBool::new(true)), holds: Arc::default(), done: None });
    }

    /// Polls the woken calls, waits for the barrier of the owners and repeats until no call is woken. Then observes everything that finished.
    async fn settle(&mut self) -> Vec<Observation> {
        loop {
            self.calls.iter_mut().for_each(Call::poll_woken);
            if let Some(database) = self.database.clone() {
                let owners = database.life_handle();
                tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap();
            }
            if !self.calls.iter().any(Call::is_woken) { break; }
        }
        let mut observed = Vec::new();
        for call in &mut self.calls {
            let Some(done) = call.done.take() else { continue };
            observed.extend(done.observed);
            if let Some((name, id)) = done.row { self.rows.insert(name, id); }
            if let Some((name, receipt)) = done.receipt { self.receipts.insert(name, receipt); }
            if let Some((name, load)) = done.load { self.loads.insert(name, load); }
        }
        self.calls.retain(|call| call.work.is_some());
        observed.extend(self.mock.announce());
        observed
    }

    async fn open(&mut self, models: &str, encrypted: bool, path: Option<String>) -> Vec<Observation> {
        if self.back > 0 {
            self.clock_now = self.clock_now.saturating_sub(self.back);
            self.clock = TestClock::new(self.clock_now);
            self.back = 0;
        }
        let config = Config { path };
        let transport: Arc<dyn crate::ffi::Transport> = self.mock.clone();
        let (encrypted, credentials, http): EncryptedParts = if encrypted {
            (Some(self.encrypted_config()), Some(self.credentials.clone()), Some(self.mock.clone()))
        } else {
            (None, None, None)
        };
        let opened = OpenDatabase::new().open_with_clock(config, if models == "events" { EVENT_MODELS } else { MODELS }.into(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(transport), encrypted, credentials, http, endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, self.clock.clone(), crate::ffi::OpenOptions::default(), None).await;
        match opened {
            Ok(database) => {
                if database.register_operations(OPERATIONS.into()).await.is_err() { return error("operations"); }
                if self.symbolic.is_none() && database.set_online(true).await.is_err() { return error("online"); }
                self.database = Some(database);
                let _ = self.settle().await;
                vec![Observation::bare("opened")]
            }
            Err(open_error) => error(error_kind(&open_error)),
        }
    }

    fn receipt_done(name: String, row: String, op: &str, result: Result<WireOperationReceipt, DbError>) -> Done {
        match result {
            Ok(receipt) => Done { observed: vec![Observation::new("receipt", json!({ "row": row, "op": op }))], receipt: Some((name, receipt)), ..Done::default() },
            Err(error) => failed(&error),
        }
    }

    async fn start(&mut self, database: Arc<Database>, input: &Input) -> Option<Vec<Observation>> {
        let epoch = database.session_epoch();
        let text = |key: &str| input.text(key).map(str::to_string);
        let params = input.args.get("params").map_or_else(|| "{}".to_string(), Value::to_string);
        let model = text_or(input, "model", "Moment");
        let call = if input.flag("named").unwrap_or(false) { text("as") } else { None };
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
                let request = WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: path.to_string_lossy().into_owned(), name: Some("clip.bin".into()), mime: Some("application/octet-stream".into()), prepare: None, max_bytes: None }, variables_json: "{}".into() };
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
                    match outcome_name(database.confirmed(receipt, false).await) {
                        Ok(result) => done("outcome", json!({ "receipt": name, "result": result })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "begin_load" => {
                let (Some(name), Some(relation)) = (text("as"), text("relation")) else { return Some(error("arguments")) };
                match database.begin_load(epoch, model, Some(relation), params) {
                    Ok(load) => {
                        let handle = load.clone();
                        self.loads.insert(name.clone(), Arc::new(move || handle.cancel()));
                        self.launch_named(call.clone(), async move { load.result().await.map_or_else(|e| failed(&e), |()| done("loaded", json!({ "load": name }))) });
                    }
                    Err(error) => return Some(tagged(self::error(error_kind(&error)), &call)),
                }
            }
            "refresh" => {
                let Some(relation) = text("relation") else { return Some(error("arguments")) };
                self.launch_named(call.clone(), async move { database.refresh(epoch, model, Some(relation.clone()), params).await.map_or_else(|e| failed(&e), |()| done("refreshed", json!({ "relation": relation }))) });
            }
            "load_more" => {
                let Some(relation) = text("relation") else { return Some(error("arguments")) };
                self.launch_named(call.clone(), async move {
                    match database.load_more(epoch, model, Some(relation.clone()), params).await {
                        Ok(result) => done("more", json!({ "relation": relation, "result": more_name(result) })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "node" => {
                let (Some(relation), Some(row)) = (text("relation"), text("row")) else { return Some(error("arguments")) };
                let id = self.id_of(&row);
                let force = input.flag("force").unwrap_or(false);
                self.launch_named(call.clone(), async move {
                    match database.node(epoch, model, relation, id, force).await {
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
                self.launch_named(call.clone(), async move {
                    match database.nodes(epoch, model, relation.clone(), ids).await {
                        Ok(found) => done("nodes", json!({ "relation": relation, "rows": found.iter().map(|row| by_id.get(&row.id).cloned().unwrap_or_else(|| row.id.clone())).collect::<Vec<_>>() })),
                        Err(error) => failed(&error),
                    }
                });
            }
            "begin_load_all" => {
                let (Some(name), Some(relation)) = (text("as"), text("relation")) else { return Some(error("arguments")) };
                match database.begin_load_all(epoch, model, Some(relation), params, input.flag("force").unwrap_or(false)) {
                    Ok(load) => {
                        let handle = load.clone();
                        self.loads.insert(name.clone(), Arc::new(move || handle.cancel()));
                        self.launch_named(call.clone(), async move { load.result().await.map_or_else(|e| failed(&e), |()| done("loaded_all", json!({ "load": name }))) });
                    }
                    Err(error) => return Some(tagged(self::error(error_kind(&error)), &call)),
                }
            }
            "peek_row" => {
                let Some(name) = text("row") else { return Some(error("arguments")) };
                let id = self.id_of(&name);
                return Some(match database.peek_row(epoch, text_or(input, "model", "Moment"), id).await {
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
                    match database.edge_values(epoch, model, Some(relation.clone()), "{}".into(), ids).await {
                        Ok(values) => {
                            let named: Map<String, Value> = values.into_iter().map(|edge| (by_id.get(&edge.id).cloned().unwrap_or(edge.id), Value::Object(edge.values.into_iter().map(|entry| (entry.key, entry.value.json())).collect()))).collect();
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
                let items: Vec<WireUploadItem> = (0..count).map(|index| WireUploadItem { values_json: json!({ "text": format!("{name}{}", index + 1) }).to_string(), source: crate::ffi::upload_wire::WireSource::File { path: path.to_string_lossy().into_owned(), name: Some("clip.bin".into()), mime: Some("application/octet-stream".into()), prepare: None, max_bytes: None }, variables_json: "{}".into() }).collect();
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
                    match database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Moment"}"#)).await {
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
        let params = input.args.get("params").map_or_else(|| "{}".to_string(), Value::to_string);
        let model = text_or(input, "model", "Moment");
        let on_connect = input.flag("refresh_on_connect").unwrap_or(false);
        let query = |relation: Option<&str>| QuerySpec { model: model.clone(), relation: relation.map(str::to_string), params_json: params.clone(), conditions: Vec::new(), order: Vec::new(), limit: None, offset: None, refresh_on_connect: on_connect };
        let request = match input.name.as_str() {
            "open_list" => ReadRequest::List { query: query(input.text("relation")) },
            "open_count" => ReadRequest::Count { query: query(input.text("relation")) },
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
        let launch = database.life_handle().launch();
        let task = crate::task::spawn(crate::runtime::counted(Some(launch), async move {
            let mut seq = 0;
            while let Some(value) = pulling.pull_next().await {
                seq += 1;
                kept.lock().unwrap().push_back((seq, value));
                heard.0.fetch_add(1, Ordering::SeqCst);
            }
        }));
        self.streams.insert(name.into(), Pulled { stream, queue, task });
        Vec::new()
    }
}

fn tagged(observed: Vec<Observation>, call: &Option<String>) -> Vec<Observation> {
    let Some(call) = call else { return observed };
    observed.into_iter().map(|mut item| {
        if let Value::Object(fields) = &mut item.value { fields.insert("call".into(), call.clone().into()); } else { item.value = json!({ "call": call }); }
        item
    }).collect()
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

fn outcome_name<Value>(result: Result<Value, DbError>) -> Result<String, DbError> {
    Ok(match result {
        Ok(_) => "confirmed".into(),
        Err(DbError::Rejected { .. }) => "rejected".into(),
        Err(DbError::NoSession { .. }) => "ended session_ended".into(),
        Err(DbError::Closed { .. }) => "ended closed".into(),
        Err(DbError::Invalid { .. }) => "ended invalid".into(),
        Err(DbError::Storage { .. }) => "ended storage".into(),
        Err(DbError::Internal { .. }) => "ended internal".into(),
        Err(DbError::Interrupted { detail }) => format!("ended {}", detail.to_lowercase()),
        Err(error) => return Err(error),
    })
}

fn more_name(result: LoadMoreResult) -> &'static str {
    match result {
        LoadMoreResult::Loaded { has_more: true } => "loaded_more",
        LoadMoreResult::Loaded { has_more: false } => "loaded_last",
        LoadMoreResult::NoMore => "no_more",
    }
}

mod database_sut;
mod fold;
mod symbolic;
