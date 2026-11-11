use super::*;

/// A request the mock holds: its operation and variables, read from the decrypted body.
#[derive(Clone)]
struct Held {
    /// The id the observations show (`r1`, `r2`, ...), numbered in the canonical order of the requests of one input.
    id: String,
    /// The id the mock knows: an answer input names it.
    mock: String,
    operation: String,
    variables: Value,
}

/// The symbolic mode of the stand: an answer or a refusal goes to the oldest request the mock still holds, `login` runs the session call and its answer,
/// the database lives in a file of a temporary directory, so it survives `close` + `open`.
/// After every `request` of an encrypted exchange the stand adds the observation `sent` with the operation and the variables of that request.
#[derive(Default)]
pub(super) struct Symbolic {
    held: Vec<Held>,
    known: usize,
    shown: std::collections::HashMap<String, String>,
    live: bool,
    dir: Option<std::path::PathBuf>,
    seen: std::collections::HashMap<String, Seen>,
    /// The kinds of request the stand does not show and does not number (the slice L-read does not model the subscription).
    hidden: Vec<&'static str>,
}

/// What a stream showed in symbolic mode: its rows (or count) and the last value it showed. A state-only value and a delta are shown as the whole value.
#[derive(Default)]
struct Seen {
    rows: Vec<Value>,
    count: Option<Value>,
    last: Option<Observation>,
}

const VALUES: [&str; 4] = ["snapshot", "count", "delta", "state"];

static DIRS: AtomicU64 = AtomicU64::new(0);

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

impl DatabaseSut {
    /// The stand in symbolic mode.
    pub fn symbolic() -> Self {
        Self { symbolic: Some(Symbolic { hidden: vec!["subscription"], ..Symbolic::default() }), ..Self::default() }
    }

    /// Removes the temporary directory of the database file.
    pub(super) fn remove_file_dir(&mut self) {
        if let Some(dir) = self.symbolic.as_mut().and_then(|mode| mode.dir.take()) {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    fn mode(&mut self) -> &mut Symbolic {
        self.symbolic.as_mut().expect("the symbolic mode is on")
    }

    fn file(&mut self) -> String {
        let dir = self.mode().dir.get_or_insert_with(|| std::env::temp_dir().join(format!("dbl_library_{}_{}", std::process::id(), DIRS.fetch_add(1, Ordering::SeqCst)))).clone();
        let _ = std::fs::create_dir_all(&dir);
        dir.join("library.db").to_string_lossy().into_owned()
    }

    /// The requests that arrived since the last call get the shown ids in one canonical order: the ones with a body by (operation, variables as text), the others after them in arrival order.
    /// The requests of one input are independent, the library does not order them (H16).
    fn note_requests(&mut self, observed: &[Observation]) {
        let sent = self.requests();
        let mode = self.mode();
        let hidden: Vec<String> = observed.iter().filter(|item| item.kind == "request" && mode.hidden.iter().any(|kind| item.value["kind"] == *kind)).filter_map(|item| item.value["id"].as_str().map(str::to_string)).collect();
        let mut fresh: Vec<(bool, String, String, String, Value)> = sent
            .iter()
            .skip(mode.known)
            .filter(|(id, _)| !hidden.contains(id))
            .map(|(id, body)| {
                let (operation, variables) = if body.is_null() { (String::new(), String::new()) } else { let (operation, variables) = operation_of(body); (operation, variables.to_string()) };
                (body.is_null(), operation, variables, id.clone(), body.clone())
            })
            .collect();
        fresh.sort_by(|a, b| (a.0, &a.1, &a.2).cmp(&(b.0, &b.1, &b.2)));
        mode.known = sent.len();
        for (_, _, _, mock, body) in fresh {
            let shown = format!("r{}", mode.shown.len() + 1);
            mode.shown.insert(mock.clone(), shown.clone());
            if body.is_null() { continue; }
            let (operation, variables) = operation_of(&body);
            mode.held.push(Held { id: shown, mock, operation, variables });
        }
    }

    /// The error kinds of the values a `pull` will fold, in the order of the folded values.
    fn unread_errors(&self, input: &Input) -> Vec<Option<&'static str>> {
        let Some(pulled) = input.text("stream").and_then(|name| self.streams.get(name)) else { return Vec::new() };
        let unread: Vec<ReadValue> = pulled.queue.lock().unwrap().iter().map(|(_, value)| value.clone()).collect();
        fold::fold(unread)
            .iter()
            .map(|folded| {
                let state = match folded {
                    fold::Folded::Delta { state, .. } => Some(state),
                    fold::Folded::Value(ReadValue::Snapshot { state, .. } | ReadValue::Count { state, .. } | ReadValue::State { state, .. }) => Some(state),
                    fold::Folded::Value(_) => None,
                };
                state.and_then(|state| state.error.as_ref()).map(error_kind)
            })
            .collect()
    }

    /// The values of a `pull` as the spec names them (library.md section 1): rows, `phase`, the kind of the `error`, `loaded` present or not. No revisions, no texts.
    /// A delta and a state-only value are the whole value, the values of one pull are one (the last), a value equal to the one shown before is no new value.
    fn shape_pull(&mut self, stream: &str, observed: Vec<Observation>, errors: Vec<Option<&'static str>>) -> Vec<Observation> {
        let mut rest = observed;
        let tail = rest.split_off(errors.len().min(rest.len()));
        let mut values: Vec<Observation> = Vec::new();
        for (item, error) in rest.into_iter().zip(errors) {
            if !VALUES.contains(&item.kind.as_str()) {
                values.push(item);
                continue;
            }
            let stream = item.value["stream"].as_str().unwrap_or("").to_string();
            let seen = self.mode().seen.entry(stream).or_default();
            let mut fields = item.value.as_object().cloned().unwrap_or_default();
            match item.kind.as_str() {
                "snapshot" => seen.rows = fields.get("rows").and_then(Value::as_array).cloned().unwrap_or_default(),
                "count" => seen.count = fields.get("count").cloned(),
                "delta" => {
                    let removed = fields.get("removed").and_then(Value::as_array).cloned().unwrap_or_default();
                    seen.rows.retain(|row| !removed.contains(row));
                    for row in fields.get("rows").and_then(Value::as_array).cloned().unwrap_or_default() {
                        if !seen.rows.contains(&row) { seen.rows.push(row); }
                    }
                }
                _ => {}
            }
            ["revision", "base", "removed"].iter().for_each(|key| { fields.remove(*key); });
            if fields.contains_key("loaded") { fields.insert("loaded".into(), true.into()); }
            match error { Some(kind) => fields.insert("error".into(), kind.into()), None => fields.remove("error") };
            let kind = if seen.count.is_some() { "count" } else { "snapshot" };
            match &seen.count { Some(count) => fields.insert("count".into(), count.clone()), None => fields.insert("rows".into(), Value::Array(seen.rows.clone())) };
            let stream_name = fields.get("stream").cloned().unwrap_or_default();
            values.retain(|earlier| !(VALUES.contains(&earlier.kind.as_str()) && earlier.value["stream"] == stream_name));
            values.push(Observation::new(kind, Value::Object(fields)));
        }
        let mut shown = Vec::new();
        for item in values {
            if VALUES.contains(&item.kind.as_str()) {
                let seen = self.mode().seen.entry(item.value["stream"].as_str().unwrap_or("").to_string()).or_default();
                if seen.last.as_ref() == Some(&item) { continue; }
                seen.last = Some(item.clone());
            }
            shown.push(item);
        }
        if shown.is_empty() && !tail.iter().any(|item| item.kind == "empty" || item.kind == "error") {
            shown.push(Observation::new("empty", json!({ "stream": stream })));
        }
        shown.extend(tail);
        shown
    }

    /// The `request` observations of an input show the shown ids, in the order of those ids.
    fn relabel(&mut self, observed: Vec<Observation>) -> Vec<Observation> {
        let mode = self.mode();
        let (mut requests, mut out) = (Vec::new(), Vec::new());
        for mut item in observed {
            if item.kind != "request" { out.push(item); continue; }
            if mode.hidden.iter().any(|kind| item.value["kind"] == *kind) { continue; }
            let mock = item.value["id"].as_str().unwrap_or("").to_string();
            if let Some(shown) = mode.shown.get(&mock) { item.value["id"] = shown.clone().into(); }
            requests.push(item);
        }
        requests.sort_by_key(|item| item.value["id"].as_str().unwrap_or("").trim_start_matches('r').parse::<u64>().unwrap_or(0));
        out.extend(requests);
        out
    }

    fn with_sent(&mut self, observed: Vec<Observation>) -> Vec<Observation> {
        let mode = self.mode();
        let mut out = Vec::new();
        for item in observed {
            let id = (item.kind == "request" && item.value["kind"] == "http").then(|| item.value["id"].as_str().unwrap_or("").to_string());
            out.push(item);
            if let Some(held) = id.and_then(|id| mode.held.iter().find(|held| held.id == id)) {
                out.push(Observation::new("sent", json!({ "id": held.id, "operation": held.operation, "variables": held.variables })));
            }
        }
        out
    }

    /// Sends the reply to the oldest held request the mock still knows. Returns what the core showed, or the refusal when none is left.
    async fn reply(&mut self, how: &str, make: impl Fn(&Held) -> Input) -> Vec<Observation> {
        while !self.mode().held.is_empty() {
            let held = self.mode().held.remove(0);
            let observed = self.play(&make(&held)).await;
            self.note_requests(&observed);
            let observed = self.relabel(observed);
            if !is_error(&observed, "no_request") {
                let mut shown = vec![Observation::new("replied", json!({ "id": held.id, "how": how }))];
                shown.extend(self.with_sent(observed));
                return shown;
            }
        }
        error("no_request")
    }

    pub(super) async fn apply_symbolic(&mut self, input: &Input) -> Vec<Observation> {
        if input.name == "open" && self.mode().live {
            return error("already_open");
        }
        let placed;
        let input = if input.name == "open" {
            placed = input.clone().with("path", self.file());
            &placed
        } else {
            input
        };
        let observed = match input.name.as_str() {
            "login" => {
                let started = self.play(&Input::new("session_call").with("operation", "social").with("body", json!({ "token": "t" }))).await;
                self.note_requests(&started);
                let started = self.relabel(started);
                let mut observed = self.with_sent(started);
                if !is_error(&observed, "not_open") {
                    let mode = self.mode();
                    if let Some(index) = mode.held.iter().rposition(|held| held.operation.starts_with("session")) {
                        let session = mode.held.remove(index);
                        mode.held.insert(0, session);
                    }
                    let answered = self.reply("page", |held| Input::new("answer").with("id", held.mock.as_str()).with("json", page_body(held, 0, false))).await;
                    observed.extend(answered);
                }
                return observed;
            }
            "answer_page" => {
                let (rows, more) = (input.number("rows").unwrap_or(0), input.flag("more").unwrap_or(false));
                return self.reply("page", |held| Input::new("answer").with("id", held.mock.as_str()).with("json", page_body(held, rows, more))).await;
            }
            "answer_error" => {
                let kind = input.text("kind").unwrap_or("temporary").to_string();
                return self.reply(&kind.clone(), |held| if kind == "drop" { Input::new("drop").with("id", held.mock.as_str()) } else { Input::new("error").with("id", held.mock.as_str()).with("kind", kind.as_str()) }).await;
            }
            "pull" => {
                let errors = self.unread_errors(input);
                let observed = self.play(input).await;
                self.shape_pull(input.text("stream").unwrap_or(""), observed, errors)
            }
            _ => self.play(input).await,
        };
        if input.name.starts_with("open_") {
            if let Some(name) = input.text("as") { self.mode().seen.remove(name); }
        }
        self.note_requests(&observed);
        let observed = self.relabel(observed);
        for item in &observed {
            match item.kind.as_str() {
                "opened" => self.mode().live = true,
                "closed" => self.mode().live = false,
                _ => {}
            }
        }
        let mut shown = self.with_sent(observed);
        if input.name == "close" && !self.mode().held.is_empty() {
            let ids: Vec<String> = self.mode().held.drain(..).map(|held| held.id).collect();
            shown.push(Observation::new("released", json!({ "ids": ids })));
        }
        shown
    }
}
