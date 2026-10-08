use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::{json, Value};

use crate::transport::{GraphQLRequest, MockTransport, PutReply, Reply, TransportError};

use super::dice::Dice;
use super::findings::Findings;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Committed,
    Refused,
    Unknown,
}

#[derive(Clone, Copy)]
enum Fate {
    Committed,
    Unreached,
    Refused,
    AppliedThenLost,
    LostBeforeApply,
    ServerErrorApplied,
    ServerErrorLost,
}

struct Delivery {
    outcome: Outcome,
    retries_seen: u64,
}

struct Moment {
    text: String,
    liked: bool,
    last_issue: HashMap<usize, u64>,
}

struct Backend {
    dice: Dice,
    moments: BTreeMap<String, Moment>,
    order: Vec<String>,
    next_id: u64,
    next_blob: u64,
    deliveries: HashMap<String, Vec<Delivery>>,
    offline: bool,
    calm: bool,
    retries: u64,
    active_retries: u64,
    received: u64,
}

#[derive(Clone)]
pub struct FakeServer {
    backend: Arc<Mutex<Backend>>,
    transport: MockTransport,
    findings: Findings,
}

pub struct RetryCall {
    backend: Arc<Mutex<Backend>>,
}

impl Drop for RetryCall {
    fn drop(&mut self) {
        self.backend.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).active_retries -= 1;
    }
}

pub fn issue_of(text: &str) -> Option<(usize, u64)> {
    let (worker, issue) = text.strip_prefix('w')?.split_once(':')?;
    Some((worker.parse().ok()?, issue.parse().ok()?))
}

fn fate_name(fate: Fate) -> &'static str {
    match fate {
        Fate::Committed => "committed",
        Fate::Unreached => "unreached",
        Fate::Refused => "refused",
        Fate::AppliedThenLost => "applied-then-lost",
        Fate::LostBeforeApply => "lost-before-apply",
        Fate::ServerErrorApplied => "503-applied",
        Fate::ServerErrorLost => "503-lost",
    }
}

fn node(id: &str, moment: &Moment) -> Value {
    json!({"id": id, "text": moment.text, "userId": null, "likedByMe": moment.liked})
}

fn page(ids: &[String], offset: usize, size: usize) -> (Vec<String>, bool, String) {
    let end = (offset + size).min(ids.len());
    (ids[offset.min(end)..end].to_vec(), end < ids.len(), format!("c{end}"))
}

fn offset_of(cursor: Option<&Value>) -> usize {
    cursor.and_then(Value::as_str).and_then(|text| text.strip_prefix('c')).and_then(|digits| digits.parse().ok()).unwrap_or(0)
}

impl FakeServer {
    pub fn install(transport: &MockTransport, seed: u64, findings: Findings) -> Self {
        let backend = Backend {
            dice: Dice::new(seed),
            moments: BTreeMap::new(),
            order: Vec::new(),
            next_id: 1,
            next_blob: 1,
            deliveries: HashMap::new(),
            offline: false,
            calm: false,
            retries: 0,
            active_retries: 0,
            received: 0,
        };
        let server = Self { backend: Arc::new(Mutex::new(backend)), transport: transport.clone(), findings };
        let responder = server.clone();
        transport.respond_with(move |request| responder.answer(request));
        server
    }

    fn lock(&self) -> MutexGuard<'_, Backend> {
        self.backend.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn seed_rows(&self, workers: usize, per_worker: usize, shared: usize) {
        let mut backend = self.lock();
        let owners = (0..workers).flat_map(|worker| (0..per_worker).map(move |_| format!("w{worker}:0")));
        for text in owners.chain((0..shared).map(|_| "n:0".to_string())) {
            let id = format!("s{}", backend.next_id);
            backend.next_id += 1;
            backend.moments.insert(id.clone(), Moment { text, liked: false, last_issue: HashMap::new() });
            backend.order.push(id);
        }
    }

    pub fn set_offline(&self, offline: bool) {
        self.lock().offline = offline;
    }

    pub fn set_calm(&self, calm: bool) {
        self.lock().calm = calm;
    }

    pub fn note_retry(&self) -> RetryCall {
        let mut backend = self.lock();
        backend.retries += 1;
        backend.active_retries += 1;
        RetryCall { backend: self.backend.clone() }
    }

    pub fn received(&self) -> u64 {
        self.lock().received
    }

    pub fn ids(&self) -> Vec<String> {
        self.lock().order.clone()
    }

    pub fn node_of(&self, id: &str) -> Option<Value> {
        self.lock().moments.get(id).map(|moment| node(id, moment))
    }

    pub fn state(&self) -> BTreeMap<String, (String, bool)> {
        self.lock().moments.iter().map(|(id, moment)| (id.clone(), (moment.text.clone(), moment.liked))).collect()
    }

    pub fn was_refused(&self, text: &str) -> bool {
        self.lock().deliveries.get(text).and_then(|history| history.last()).is_some_and(|last| last.outcome == Outcome::Refused)
    }

    pub fn was_ambiguous(&self, text: &str) -> bool {
        self.lock().deliveries.get(text).is_some_and(|history| history.iter().any(|delivery| delivery.outcome == Outcome::Unknown))
    }

    pub fn deliveries_of(&self, text: &str) -> usize {
        self.lock().deliveries.get(text).map_or(0, Vec::len)
    }

    fn answer(&self, request: &GraphQLRequest) -> Option<Reply> {
        let variables = &request.variables;
        let name = request.operation_name.as_str();
        match name {
            "CreateMoment" | "UpdateMoment" | "DeleteMoment" | "LikeMoment" | "CreateBlob" => Some(self.mutation(name, variables)),
            "MomentsFeed" | "MomentsHistory" | "MomentsByUser" => Some(self.read(name, variables)),
            _ => None,
        }
    }

    fn read(&self, name: &str, variables: &Value) -> Reply {
        let mut backend = self.lock();
        if backend.offline {
            return Reply::fail(TransportError::Network("offline".to_string()));
        }
        backend.received += 1;
        let roll = backend.dice.below(100);
        let calm = backend.calm;
        if !calm && roll < 6 {
            return Reply::fail(TransportError::Network("dropped".to_string()));
        }
        if !calm && roll < 8 {
            return Reply::fail(TransportError::Http { status: 503, body: String::new() });
        }
        let delay = Self::delay(&mut backend, 25);
        let newest_first: Vec<String> = backend.order.iter().rev().cloned().collect();
        let nodes_of = |ids: &[String]| -> Vec<Value> { ids.iter().filter_map(|id| backend.moments.get(id).map(|moment| node(id, moment))).collect() };
        let data = match name {
            "MomentsFeed" => {
                let size = variables["first"].as_u64().unwrap_or(2) as usize;
                let (ids, more, cursor) = page(&newest_first, offset_of(variables.get("after")), size);
                json!({"feed": {"nodes": nodes_of(&ids), "pageInfo": {"hasNextPage": more, "endCursor": cursor}}})
            }
            "MomentsHistory" => {
                let oldest_first: Vec<String> = backend.order.clone();
                let size = variables["last"].as_u64().unwrap_or(2) as usize;
                let (ids, more, cursor) = page(&oldest_first, offset_of(variables.get("before")), size);
                json!({"history": {"nodes": nodes_of(&ids), "pageInfo": {"hasNextPage": more, "endCursor": cursor}}})
            }
            _ => json!({"moments": nodes_of(&newest_first)}),
        };
        Reply::data(data).after(delay)
    }

    fn delay(backend: &mut Backend, percent: u64) -> Duration {
        if backend.dice.percent(percent) {
            Duration::from_millis(1 + backend.dice.below(12))
        } else {
            Duration::ZERO
        }
    }

    fn fate(backend: &mut Backend) -> Fate {
        if backend.calm {
            return Fate::Committed;
        }
        match backend.dice.below(100) {
            0..=57 => Fate::Committed,
            58..=67 => Fate::Unreached,
            68..=75 => Fate::Refused,
            76..=82 => Fate::AppliedThenLost,
            83..=87 => Fate::LostBeforeApply,
            88..=91 => Fate::ServerErrorApplied,
            92..=95 => Fate::ServerErrorLost,
            _ => Fate::Committed,
        }
    }

    fn mutation(&self, name: &str, variables: &Value) -> Reply {
        let mut backend = self.lock();
        if backend.offline {
            return Reply::fail(TransportError::Network("offline".to_string()));
        }
        let fate = Self::fate(&mut backend);
        if matches!(fate, Fate::Unreached) {
            return Reply::fail(TransportError::Network("unreachable".to_string()));
        }
        backend.received += 1;
        let applies = matches!(fate, Fate::Committed | Fate::AppliedThenLost | Fate::ServerErrorApplied);
        let delay = Self::delay(&mut backend, 30);
        let token = (name == "CreateMoment" || name == "UpdateMoment").then(|| variables["text"].as_str().unwrap_or_default().to_string());
        let keeps_failures = variables.get("signedId").is_some();
        let applied = if applies { self.apply(&mut backend, name, variables) } else { Err("not applied".to_string()) };
        let outcome = match (fate, &applied) {
            (Fate::Committed, Ok(_)) => Outcome::Committed,
            (Fate::AppliedThenLost | Fate::LostBeforeApply | Fate::ServerErrorApplied | Fate::ServerErrorLost, _) => Outcome::Unknown,
            _ => Outcome::Refused,
        };
        if let Some(token) = token {
            self.record_delivery(&mut backend, name, token, outcome, keeps_failures);
        }
        self.findings.trace(|| format!("server {name} {} fate {} -> {}", variables, fate_name(fate), if applied.is_ok() { "applied" } else { "not applied" }));
        let reply = match (fate, applied) {
            (Fate::Committed, Ok(data)) => Reply::data(data),
            (Fate::AppliedThenLost | Fate::LostBeforeApply, _) => Reply::fail(TransportError::Interrupted("connection lost".to_string())),
            (Fate::ServerErrorApplied | Fate::ServerErrorLost, _) => Reply::fail(TransportError::Http { status: 503, body: String::new() }),
            (_, Err(message)) => Reply::graphql_error(&message, "FORBIDDEN"),
            (_, Ok(_)) => Reply::graphql_error("refused", "FORBIDDEN"),
        };
        reply.after(delay)
    }

    fn record_delivery(&self, backend: &mut Backend, name: &str, token: String, outcome: Outcome, keeps_failures: bool) {
        let (retries, active_retries) = (backend.retries, backend.active_retries);
        let history = backend.deliveries.entry(token.clone()).or_default();
        if let Some(previous) = history.last() {
            let complaint = match previous.outcome {
                Outcome::Committed => Some("sent again after the server confirmed it"),
                Outcome::Refused if !keeps_failures => Some("sent again after the server refused it and the write was rolled back"),
                Outcome::Refused if retries <= previous.retries_seen && active_retries == 0 => Some("sent again after a refusal with no retry requested since"),
                Outcome::Unknown if retries <= previous.retries_seen && active_retries == 0 => Some("sent again with no retry requested since the unknown delivery"),
                _ => None,
            };
            if let Some(complaint) = complaint {
                self.findings.report("send-once", format!("{name} {token} {complaint} (delivery {})", history.len() + 1));
            }
        }
        history.push(Delivery { outcome, retries_seen: retries });
    }

    fn apply(&self, backend: &mut Backend, name: &str, variables: &Value) -> Result<Value, String> {
        match name {
            "CreateMoment" => {
                let text = variables["text"].as_str().ok_or("text is required")?.to_string();
                let id = format!("s{}", backend.next_id);
                backend.next_id += 1;
                let mut moment = Moment { text: text.clone(), liked: false, last_issue: HashMap::new() };
                if let Some((worker, issue)) = issue_of(&text) {
                    moment.last_issue.insert(worker, issue);
                }
                let data = json!({"createMoment": node(&id, &moment)});
                backend.moments.insert(id.clone(), moment);
                backend.order.push(id);
                Ok(data)
            }
            "UpdateMoment" => {
                let id = variables["id"].as_str().ok_or("id is required")?;
                let text = variables["text"].as_str().ok_or("text is required")?.to_string();
                let Backend { moments, .. } = backend;
                let moment = moments.get_mut(id).ok_or("not found")?;
                if let Some((worker, issue)) = issue_of(&text) {
                    if moment.last_issue.get(&worker).is_some_and(|last| *last > issue) {
                        self.findings.report("write-order", format!("UpdateMoment {id} applied {text} after {}", moment.last_issue[&worker]));
                    }
                    moment.last_issue.insert(worker, issue);
                }
                moment.text = text;
                Ok(json!({"updateMoment": node(id, moment)}))
            }
            "DeleteMoment" => {
                let id = variables["id"].as_str().ok_or("id is required")?.to_string();
                backend.moments.remove(&id);
                backend.order.retain(|other| *other != id);
                Ok(json!({"deleteMoment": {"id": id}}))
            }
            "LikeMoment" => {
                let id = variables["momentId"].as_str().ok_or("momentId is required")?;
                let moment = backend.moments.get_mut(id).ok_or("not found")?;
                moment.liked = true;
                Ok(json!({"likeMoment": node(id, moment)}))
            }
            _ => {
                let blob = backend.next_blob;
                backend.next_blob += 1;
                let url = format!("https://s3/b{blob}");
                let put = match backend.dice.below(100) {
                    0..=69 => PutReply::etag("e"),
                    70..=89 => PutReply::etag("e").after(Duration::from_millis(1 + backend.dice.below(10))),
                    90..=94 => PutReply::status(500),
                    _ => PutReply::fail(TransportError::Network("put dropped".to_string())),
                };
                if !backend.calm {
                    self.transport.enqueue_put(&url, put);
                }
                self.transport.enqueue_put(&url, PutReply::etag("e"));
                Ok(json!({"createBlob": {"url": url, "headers": {}, "signedId": format!("sid{blob}"), "blobId": format!("b{blob}")}}))
            }
        }
    }
}
