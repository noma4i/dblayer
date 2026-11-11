use serde_json::{json, Value};

use crate::check::database::DatabaseSut;
use crate::check::model::Sut;
use crate::check::value::{Input, Observation};

/// A request the mock holds: its operation and variables, read from the decrypted body.
#[derive(Clone)]
struct Held {
    id: String,
    operation: String,
    variables: Value,
}

/// `DatabaseSut` with symbolic references: an answer or a refusal goes to the oldest request the mock still holds, `login` runs the session call and its answer.
/// After every `request` of an encrypted exchange it adds the observation `sent` with the operation and the variables of that request.
#[derive(Default)]
pub struct LibrarySut {
    inner: DatabaseSut,
    held: Vec<Held>,
    known: usize,
    live: bool,
}

fn operation_of(body: &Value) -> (String, Value) {
    let payload = &body["body"];
    match payload["operationName"].as_str() {
        Some(name) => (name.to_string(), payload["variables"].clone()),
        None => (format!("session{}", body["path"].as_str().unwrap_or("")), Value::Null),
    }
}

fn is_error(observed: &[Observation], kind: &str) -> bool {
    observed.iter().any(|item| item.kind == "error" && item.value["kind"] == kind)
}

fn feed_row(index: u64) -> Value {
    json!({ "__typename": "Moment", "createdAt": format!("2026-10-08T00:00:{:02}.000Z", index % 60), "id": format!("m{index}"), "likedByMe": false, "media": {}, "text": format!("t{index}"), "uuid": format!("u{index}") })
}

/// The body of a page of `rows` rows for the held request. A page after a cursor `c<k>` continues the ids at `k + 1`.
fn page_body(held: &Held, rows: u64, more: bool) -> Value {
    let after: u64 = held.variables["after"].as_str().and_then(|cursor| cursor.trim_start_matches('c').parse().ok()).unwrap_or(0);
    let ids = (after + 1)..=(after + rows);
    match held.operation.as_str() {
        "MomentsFeed" => json!({ "data": { "feed": { "nodes": ids.map(feed_row).collect::<Vec<_>>(), "pageInfo": { "endCursor": if more { json!(format!("c{}", after + rows)) } else { Value::Null }, "hasNextPage": more } } } }),
        "MomentTranscodeStatus" => json!({ "data": { "momentTranscodeStatus": if rows == 0 { Value::Null } else { feed_row(1) } } }),
        "ContainersByIds" => json!({ "data": { "containersByIds": ids.map(|index| json!({ "__typename": "Container", "id": format!("m{index}"), "title": "a" })).collect::<Vec<_>>() } }),
        "ContainerMembers" => json!({ "data": { "node": { "membersCount": rows, "members": { "nodes": ids.map(|index| json!({ "__typename": "User", "id": format!("u{index}") })).collect::<Vec<_>>(), "pageInfo": { "endCursor": if more { json!(format!("c{}", after + rows)) } else { Value::Null }, "hasNextPage": more } } } } }),
        _ => json!({ "nextStep": "authenticated" }),
    }
}

impl LibrarySut {
    pub async fn finish(&mut self) {
        self.inner.finish().await;
    }

    fn note_requests(&mut self) {
        let sent = self.inner.requests();
        for (id, body) in sent.iter().skip(self.known) {
            if body.is_null() { continue; }
            let (operation, variables) = operation_of(body);
            self.held.push(Held { id: id.clone(), operation, variables });
        }
        self.known = sent.len();
    }

    fn with_sent(&self, observed: Vec<Observation>) -> Vec<Observation> {
        let mut out = Vec::new();
        for item in observed {
            let id = (item.kind == "request" && item.value["kind"] == "http").then(|| item.value["id"].as_str().unwrap_or("").to_string());
            out.push(item);
            if let Some(held) = id.and_then(|id| self.held.iter().find(|held| held.id == id)) {
                out.push(Observation::new("sent", json!({ "id": held.id, "operation": held.operation, "variables": held.variables })));
            }
        }
        out
    }

    /// Sends the reply to the oldest held request the mock still knows. Returns what the core showed, or the refusal when none is left.
    async fn reply(&mut self, how: &str, make: impl Fn(&Held) -> Input) -> Vec<Observation> {
        while !self.held.is_empty() {
            let held = self.held.remove(0);
            let observed = self.inner.apply(&make(&held)).await;
            self.note_requests();
            if !is_error(&observed, "no_request") {
                let mut shown = vec![Observation::new("replied", json!({ "id": held.id, "how": how }))];
                shown.extend(self.with_sent(observed));
                return shown;
            }
        }
        vec![Observation::new("error", json!({ "kind": "no_request" }))]
    }
}

#[async_trait::async_trait]
impl Sut for LibrarySut {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        if input.name == "open" && self.live {
            return vec![Observation::new("error", json!({ "kind": "already_open" }))];
        }
        let observed = match input.name.as_str() {
            "login" => {
                let started = self.inner.apply(&Input::new("session_call").with("operation", "social").with("body", json!({ "token": "t" }))).await;
                self.note_requests();
                let mut observed = self.with_sent(started);
                if !is_error(&observed, "not_open") {
                    if let Some(index) = self.held.iter().rposition(|held| held.operation.starts_with("session")) {
                        let session = self.held.remove(index);
                        self.held.insert(0, session);
                    }
                    let answered = self.reply("page", |held| Input::new("answer").with("id", held.id.as_str()).with("json", page_body(held, 0, false))).await;
                    observed.extend(answered);
                }
                return observed;
            }
            "answer_page" => {
                let (rows, more) = (input.number("rows").unwrap_or(0), input.flag("more").unwrap_or(false));
                return self.reply("page", |held| Input::new("answer").with("id", held.id.as_str()).with("json", page_body(held, rows, more))).await;
            }
            "answer_error" => {
                let kind = input.text("kind").unwrap_or("temporary").to_string();
                return self.reply(&kind.clone(), |held| if kind == "drop" { Input::new("drop").with("id", held.id.as_str()) } else { Input::new("error").with("id", held.id.as_str()).with("kind", kind.as_str()) }).await;
            }
            _ => self.inner.apply(input).await,
        };
        self.note_requests();
        for item in &observed {
            match item.kind.as_str() {
                "opened" => self.live = true,
                "closed" => self.live = false,
                _ => {}
            }
        }
        let mut shown = self.with_sent(observed);
        if input.name == "close" && !self.held.is_empty() {
            shown.push(Observation::new("released", json!({ "ids": self.held.iter().map(|held| held.id.clone()).collect::<Vec<_>>() })));
            self.held.clear();
        }
        shown
    }
}
