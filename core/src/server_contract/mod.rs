//! The one server contract that every test double of the server follows: a pair carries one body, an answer without a sequence spends no number, an unknown hash gets `PERSISTED_QUERY_NOT_FOUND`, a request nobody scripted is refused.

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};

#[cfg(test)]
mod tests;

pub(crate) struct Request {
    pub scope: String,
    pub request_id: String,
    pub sequence: Option<i64>,
    pub body: String,
    pub hash: Option<String>,
    pub document: bool,
}

pub(crate) enum Admission<A> {
    Stored(A),
    Refused(Value),
    TwoBodies,
    Fresh,
}

pub(crate) struct Contract<A> {
    known: HashMap<String, Option<String>>,
    everything_cached: bool,
    last: Option<i64>,
    settled: HashMap<(String, String, Option<i64>), (String, A)>,
    /// The number each request id of a scope went with: an id is one pair for good, so a second number under it is a reused id.
    numbers: HashMap<(String, String), Option<i64>>,
    script: HashMap<String, VecDeque<A>>,
    standing: Option<A>,
}

pub(crate) const DOCUMENT_MISSING: &str = "PERSISTED_QUERY_NOT_FOUND";
pub(crate) const SEQUENCE_CONFLICT: &str = "SEQUENCE_CONFLICT";

#[cfg(test)]
#[derive(Default)]
pub(crate) struct CableContract {
    pub frames: Vec<Value>,
    pub last_frame_ms: u64,
    pub closed: Vec<u16>,
    pub answer_ping: bool,
}

#[cfg(test)]
impl CableContract {
    pub fn receive(&mut self, now_ms: u64, body: &Value) -> Option<Value> {
        self.last_frame_ms = now_ms;
        self.frames.push(body.clone());
        (self.answer_ping && body["t"] == "ping").then(|| json!({"t":"pong", "ts":body["ts"]}))
    }

    pub fn expire(&mut self, now_ms: u64) -> bool {
        if now_ms.saturating_sub(self.last_frame_ms) < 60_000 || !self.closed.is_empty() { return false; }
        self.closed.push(4408);
        true
    }
}

pub(crate) fn refusal(code: &str) -> Value {
    json!({ "errors": [{ "message": code, "extensions": { "code": code } }] })
}

impl Request {
    pub(crate) fn from_wire(scope: &str, wire: &Value) -> Self {
        let body = &wire["body"];
        Request {
            scope: scope.to_string(),
            request_id: wire["requestId"].as_str().unwrap_or_default().to_string(),
            sequence: wire["sequence"].as_i64().filter(|sequence| *sequence > 0),
            body: body.to_string(),
            hash: body.pointer("/extensions/persistedQuery/sha256Hash").and_then(Value::as_str).map(str::to_string),
            document: body["query"].is_string(),
        }
    }

    fn pair(&self) -> (String, String, Option<i64>) { (self.scope.clone(), self.request_id.clone(), self.sequence) }
}

impl<A: Clone> Default for Contract<A> {
    fn default() -> Self { Self::new() }
}

impl<A: Clone> Contract<A> {
    pub(crate) fn new() -> Self {
        Contract { known: HashMap::new(), everything_cached: false, last: Some(0), settled: HashMap::new(), numbers: HashMap::new(), script: HashMap::new(), standing: None }
    }

    /// A server whose session number the double was never told: the first sequenced request sets it.
    pub(crate) fn adopting() -> Self { Contract { last: None, ..Self::new() } }

    pub(crate) fn register(&mut self, hash: &str) { self.known.entry(hash.to_string()).or_insert(None); }

    pub(crate) fn register_document(&mut self, hash: &str, document: &str) { self.known.insert(hash.to_string(), Some(document.to_string())); }

    pub(crate) fn document(&self, hash: &str) -> Option<&str> { self.known.get(hash).and_then(Option::as_deref) }

    /// The server cache holds every document.
    pub(crate) fn cache_everything(&mut self) { self.everything_cached = true; }

    pub(crate) fn forget(&mut self) {
        self.known.clear();
        self.everything_cached = false;
    }

    pub(crate) fn set_last(&mut self, sequence: i64) { self.last = Some(sequence); }

    pub(crate) fn last(&self) -> i64 { self.last.unwrap_or(0) }

    pub(crate) fn script(&mut self, operation: &str, answer: A) { self.script.entry(operation.to_string()).or_default().push_back(answer); }

    pub(crate) fn standing(&mut self, answer: A) { self.standing = Some(answer); }

    pub(crate) fn take(&mut self, operation: &str) -> Option<A> {
        self.script.get_mut(operation).and_then(VecDeque::pop_front).or_else(|| self.standing.clone())
    }

    pub(crate) fn admit(&mut self, request: &Request) -> Admission<A> {
        if !request.request_id.is_empty() && *self.numbers.entry((request.scope.clone(), request.request_id.clone())).or_insert(request.sequence) != request.sequence {
            return Admission::TwoBodies;
        }
        if let Some((body, answer)) = self.settled.get(&request.pair()) {
            return if *body == request.body { Admission::Stored(answer.clone()) } else { Admission::TwoBodies };
        }
        match (request.document, &request.hash) {
            (true, Some(hash)) => { self.register(hash); }
            (false, Some(hash)) if !self.everything_cached && !self.known.contains_key(hash) => return Admission::Refused(refusal(DOCUMENT_MISSING)),
            _ => {}
        }
        match request.sequence {
            Some(sequence) if self.last.is_some_and(|last| sequence != last + 1) => Admission::Refused(json!({ "error": SEQUENCE_CONFLICT })),
            _ => Admission::Fresh,
        }
    }

    pub(crate) fn settle(&mut self, request: &Request, answer: A) {
        if let Some(sequence) = request.sequence { self.last = Some(sequence); }
        self.settled.entry(request.pair()).or_insert_with(|| (request.body.clone(), answer));
    }
}
