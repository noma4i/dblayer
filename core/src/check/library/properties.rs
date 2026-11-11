use std::collections::HashMap;

use serde_json::Value;

use crate::check::model::Property;
use crate::check::value::{Observation, Step};

/// Fresh-window lifetime of the stand schema (`stale_time` 5 minutes, the same for an empty page).
const LIFETIME_MS: i64 = 300_000;

/// window.md win_loading_temp_error: a retryable failure waits for `Retry` while the attempts are below the limit (network 3, server 2).
const NETWORK_ATTEMPTS: u32 = 3;
const SERVER_ATTEMPTS: u32 = 2;

const LOADS: [&str; 6] = ["begin_load", "refresh", "load_more", "begin_load_all", "node", "nodes"];
const REFUSALS: [&str; 11] = ["already_open", "no_request", "not_open", "unknown_stream", "unknown_target", "arguments", "online", "open", "operations", "frame", "file"];
const READS: [&str; 5] = ["begin_load", "refresh", "load_more", "begin_load_all", "node"];

#[derive(Clone, Copy, PartialEq)]
enum Rule {
    /// I2: a fresh window without a Stale sign is not requested again.
    Fresh,
    /// I2b: a Stale sign allows 1 more first-page load.
    FreshStale,
    /// I7: a load has exactly one outcome.
    Outcomes,
    /// I9: inputs that ask for nothing send nothing.
    Quiet,
    /// I12: no identity, no request, NoSession at once.
    NoSession,
    /// I13: no network, no request; the return rereads the windows the rule names.
    Network,
}

#[derive(Default, Clone)]
struct Window {
    landed: Option<i64>,
    failed: bool,
    lost: bool,
    /// Stale signs seen, and the extra first-page loads they still allow.
    signs: u32,
    allowance: u32,
    /// A forced load (refresh, node with force, entity with force) is running: its request may be sent again after a network return.
    forced: bool,
}

struct Interest {
    operation: &'static str,
    on_connect: bool,
}

struct Request {
    operation: String,
    first_page: bool,
}

/// The state a reader of the trace can know: the clock moved by the ticks and `clock_back`, the network flag, the session, the requests the mock holds, the landed windows and the open streams.
#[derive(Default)]
struct Walk {
    now: i64,
    back: i64,
    online: bool,
    is_open: bool,
    logged: bool,
    subscriptions: u32,
    held: Vec<(String, String)>,
    pending: Vec<String>,
    streaks: HashMap<String, (u32, u32)>,
    windows: HashMap<String, Window>,
    streams: HashMap<String, Interest>,
}

fn kinds<'a>(observed: &'a [Observation], kind: &'a str) -> impl Iterator<Item = &'a Observation> {
    observed.iter().filter(move |item| item.kind == kind)
}

fn error_kind(item: &Observation) -> &str {
    item.value["kind"].as_str().unwrap_or("")
}

fn has_error(observed: &[Observation], kind: &str) -> bool {
    kinds(observed, "error").any(|item| error_kind(item) == kind)
}

fn requests(observed: &[Observation]) -> Vec<Request> {
    kinds(observed, "sent")
        .filter_map(|item| {
            let operation = item.value["operation"].as_str()?.to_string();
            (!operation.starts_with("session")).then(|| Request { first_page: item.value["variables"].get("after").is_none(), operation })
        })
        .collect()
}

fn operation_of(relation: &str) -> Option<&'static str> {
    match relation {
        "feed" => Some("MomentsFeed"),
        _ => None,
    }
}

fn describe(steps: &[Step], index: usize) -> String {
    format!("step {} {}", index + 1, steps[index].input)
}

fn fresh(window: &Window, now: i64) -> bool {
    let Some(landed) = window.landed else { return false };
    !window.failed && !window.lost && now >= landed && now - landed < LIFETIME_MS
}

fn sent_first_page(steps: &[Step], id: &str) -> bool {
    steps.iter().flat_map(|step| kinds(&step.observed, "sent")).any(|item| item.value["id"] == id && item.value["variables"].get("after").is_none())
}

/// After a network return: per watched window the number of first-page requests the rule asks for. A window is reread once when it is not fresh or a stream of it is declared to reload on connect.
fn reread_after_return(walk: &Walk, fresh_now: &HashMap<String, bool>) -> Vec<(&'static str, usize)> {
    let mut watched: Vec<&'static str> = walk.streams.values().map(|interest| interest.operation).collect();
    watched.sort_unstable();
    watched.dedup();
    watched
        .into_iter()
        .filter(|operation| !walk.windows.get(*operation).is_some_and(|window| window.lost))
        .map(|operation| {
            let declared = walk.streams.values().any(|interest| interest.operation == operation && interest.on_connect);
            let is_fresh = fresh_now.get(operation).copied().unwrap_or(false);
            (operation, usize::from(declared || !is_fresh))
        })
        .collect()
}

/// Folds the trace and checks the rule after every step.
fn analyze(steps: &[Step], rule: Rule) -> Result<(), String> {
    let mut walk = Walk::default();
    for (index, step) in steps.iter().enumerate() {
        let (input, observed) = (&step.input, &step.observed);
        let before_online = walk.online;
        let before_logged = walk.logged;
        let landed_before: HashMap<String, bool> = walk.windows.iter().map(|(operation, window)| (operation.clone(), window.landed.is_some())).collect();
        let was_held: Vec<String> = walk.held.iter().map(|(_, operation)| operation.clone()).collect();
        match input.name.as_str() {
            "tick" => walk.now += input.number("ms").unwrap_or(0) as i64,
            "clock_back" => walk.back += input.number("ms").unwrap_or(0) as i64,
            _ => {}
        }
        let refused = |kind: &str| has_error(observed, kind);
        if kinds(observed, "opened").next().is_some() {
            (walk.is_open, walk.online, walk.logged, walk.subscriptions) = (true, true, false, 0);
            (walk.now, walk.back) = (walk.now - walk.back, 0);
        }
        if let Some(online) = kinds(observed, "online").next() {
            walk.online = online.value.as_bool().unwrap_or(false);
        }
        let fresh_now: HashMap<String, bool> = walk.windows.iter().map(|(operation, window)| (operation.clone(), fresh(window, walk.now))).collect();

        let untagged_refusal = kinds(observed, "error").any(|item| item.value.get("call").is_none() && (error_kind(item) == "not_open" || error_kind(item) == "arguments"));
        if LOADS.contains(&input.name.as_str()) && input.flag("named") == Some(true) && !untagged_refusal {
            walk.pending.push(input.text("as").unwrap_or("").to_string());
        }
        let sent = requests(observed);
        let all_requests = kinds(observed, "request").count();
        let first_pages = |operation: &str| sent.iter().filter(|request| request.first_page && request.operation == operation).count();

        for _ in kinds(observed, "request").filter(|item| item.value["kind"] == "subscription") {
            walk.subscriptions += 1;
            if walk.subscriptions > 1 {
                let mut operations: Vec<String> = walk.windows.keys().cloned().chain(walk.held.iter().map(|(_, operation)| operation.clone())).chain(walk.streams.values().map(|interest| interest.operation.to_string())).collect();
                operations.sort_unstable();
                operations.dedup();
                for operation in operations {
                    let window = walk.windows.entry(operation).or_default();
                    window.signs += 1;
                    window.allowance += 1;
                }
            }
        }

        let mut replied_operation: Option<String> = None;
        for replied in kinds(observed, "replied") {
            let id = replied.value["id"].as_str().unwrap_or("");
            let Some(position) = walk.held.iter().position(|(held, _)| held == id) else { continue };
            let (_, operation) = walk.held.remove(position);
            replied_operation = Some(operation.clone());
            let first_page = sent_first_page(steps, id);
            let how = replied.value["how"].as_str().unwrap_or("");
            let streak = walk.streaks.entry(operation.clone()).or_default();
            match how {
                "temporary" | "drop" => streak.0 += 1,
                "server" => streak.1 += 1,
                _ => *streak = (0, 0),
            }
            if walk.streaks.get(&operation) == Some(&(0, 0)) {
                walk.streaks.remove(&operation);
            }
            if !matches!(how, "temporary" | "drop" | "server") {
                if let Some(window) = walk.windows.get_mut(&operation) {
                    window.forced = false;
                }
            }
            let landed_ok = how == "page" && first_page && !kinds(observed, "error").any(|item| !REFUSALS.contains(&error_kind(item)));
            if landed_ok {
                let window = walk.windows.entry(operation).or_default();
                (window.landed, window.failed, window.lost) = (Some(walk.now), false, false);
            } else if how != "page" {
                let (network, server) = walk.streaks.get(&operation).copied().unwrap_or((0, 0));
                let exhausted = network >= NETWORK_ATTEMPTS || server >= SERVER_ATTEMPTS || !matches!(how, "temporary" | "drop" | "server");
                if first_page || exhausted {
                    walk.windows.entry(operation).or_default().failed = true;
                }
            }
        }
        let forcing = input.name == "refresh" || (input.name == "node" && input.flag("force") == Some(true)) || (input.name == "open_entity" && input.text("fetch") == Some("force"));
        for item in kinds(observed, "sent") {
            let operation = item.value["operation"].as_str().unwrap_or("");
            if !operation.starts_with("session") {
                if forcing {
                    walk.windows.entry(operation.to_string()).or_default().forced = true;
                }
                walk.held.push((item.value["id"].as_str().unwrap_or("").to_string(), operation.to_string()));
            }
        }
        for released in kinds(observed, "released") {
            let ids: Vec<&str> = released.value["ids"].as_array().map(|ids| ids.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
            walk.held.retain(|(held, _)| !ids.contains(&held.as_str()));
        }
        if kinds(observed, "closed").next().is_some() {
            walk.is_open = false;
            walk.streams.clear();
        }
        if kinds(observed, "reset").next().is_some() {
            walk.logged = false;
            walk.streams.clear();
            walk.windows.values_mut().for_each(|window| window.lost = true);
        }
        if kinds(observed, "session").next().is_some() {
            walk.logged = true;
        }
        if LOADS.contains(&input.name.as_str()) && input.name != "node" && input.name != "nodes" && refused("offline") {
            if let Some(operation) = input.text("relation").and_then(operation_of) {
                walk.windows.entry(operation.to_string()).or_default().failed = true;
            }
        }
        if input.name == "open_list" || input.name == "open_count" {
            if let (Some(name), Some(operation)) = (input.text("as"), input.text("relation").and_then(operation_of)) {
                if !refused("not_open") && !refused("arguments") && walk.logged {
                    walk.streams.insert(name.to_string(), Interest { operation, on_connect: input.flag("refresh_on_connect").unwrap_or(false) });
                }
            }
        }
        if input.name == "cancel" {
            if let Some(target) = input.text("target") {
                walk.streams.remove(target);
            }
        }
        for ended in kinds(observed, "ended") {
            if let Some(name) = ended.value["stream"].as_str() {
                walk.streams.remove(name);
            }
        }

        for item in observed {
            let Some(call) = item.value.get("call").and_then(Value::as_str) else { continue };
            match walk.pending.iter().position(|pending| pending == call) {
                Some(position) => {
                    walk.pending.remove(position);
                    if item.kind == "error" {
                        if let Some(operation) = &replied_operation {
                            walk.streaks.remove(operation);
                        }
                    }
                }
                None if rule == Rule::Outcomes => return Err(format!("{}: a second outcome `{}` of the load `{call}`", describe(steps, index), item.kind)),
                None => {}
            }
        }
        let waiting_retry = walk.streaks.values().any(|(network, server)| (*network > 0 && *network < NETWORK_ATTEMPTS) || (*server > 0 && *server < SERVER_ATTEMPTS));

        match rule {
            Rule::Outcomes if !walk.pending.is_empty() && walk.held.is_empty() && walk.online && !waiting_retry => {
                return Err(format!("{}: the load(s) {:?} have no outcome, the mock holds no request, the network is on and no retry pause runs", describe(steps, index), walk.pending));
            }
            Rule::Quiet => {
                let quiet = matches!(input.name.as_str(), "pull" | "cancel" | "close")
                    || (input.name == "set_online" && input.flag("online") == Some(false))
                    || (input.name == "answer_error" && matches!(input.text("kind"), Some("permanent" | "auth")));
                if quiet && all_requests > 0 {
                    return Err(format!("{}: {} request(s) after an input that asks for none", describe(steps, index), all_requests));
                }
            }
            Rule::NoSession if walk.is_open && !walk.logged => {
                if !sent.is_empty() {
                    return Err(format!("{}: {} request(s) reached the mock without an identity", describe(steps, index), sent.len()));
                }
                if READS.contains(&input.name.as_str()) && walk.online && !refused("not_open") && !refused("arguments") && !has_error(observed, "no_session") {
                    return Err(format!("{}: a server read without an identity did not end with NoSession", describe(steps, index)));
                }
            }
            Rule::Network => {
                if !walk.online && all_requests > 0 {
                    return Err(format!("{}: {} request(s) while the network is off", describe(steps, index), all_requests));
                }
                let returned = !before_online && walk.online && input.name == "set_online";
                if returned && before_logged && walk.logged {
                    for (operation, expected) in reread_after_return(&walk, &fresh_now) {
                        let got = first_pages(operation);
                        if got != expected && !was_held.iter().any(|held| held == operation) {
                            return Err(format!("{}: network return sent {got} request(s) of {operation}, expected {expected}", describe(steps, index)));
                        }
                    }
                }
            }
            Rule::Fresh | Rule::FreshStale => {
                let connected = kinds(observed, "request").any(|item| item.value["kind"] == "subscription");
                let on_connect_return = (!before_online && walk.online) || connected;
                let streams_declared = |operation: &str| walk.streams.values().any(|interest| interest.operation == operation && interest.on_connect);
                let mut failure = None;
                for request in sent.iter().filter(|request| request.first_page && request.operation != "ContainersByIds") {
                    let was_fresh = fresh_now.get(&request.operation).copied().unwrap_or(false) && landed_before.get(&request.operation).copied().unwrap_or(false);
                    let forced = input.name == "refresh" || (input.name == "node" && input.flag("force") == Some(true)) || (input.name == "open_entity" && input.text("fetch") == Some("force"));
                    if !was_fresh || forced || (on_connect_return && streams_declared(&request.operation)) {
                        continue;
                    }
                    let Some(window) = walk.windows.get_mut(&request.operation) else { continue };
                    if window.forced {
                        continue;
                    }
                    if window.allowance > 0 {
                        window.allowance -= 1;
                        continue;
                    }
                    let message = format!("{}: a fresh window of {} was requested again", describe(steps, index), request.operation);
                    match (rule, window.signs == 0) {
                        (Rule::Fresh, true) => failure = Some(message),
                        (Rule::FreshStale, false) => failure = Some(format!("{message} beyond the loads its {} Stale sign(s) allow", window.signs)),
                        _ => {}
                    }
                }
                if let Some(message) = failure {
                    return Err(message);
                }
                let ready = walk.is_open && walk.online && walk.logged && before_logged && !refused("not_open") && !refused("no_session") && walk.streaks.is_empty();
                let exact = |operation: &str| ready && !was_held.iter().any(|held| held == operation) && walk.windows.get(operation).is_some_and(|window| !window.lost && window.landed.is_some());
                let forced_load = match (input.name.as_str(), input.text("relation"), input.flag("force")) {
                    ("refresh", Some("feed"), _) => Some("MomentsFeed"),
                    ("node", Some("transcode_status"), Some(true)) => Some("MomentTranscodeStatus"),
                    _ => None,
                };
                if let Some(operation) = forced_load.filter(|operation| rule == Rule::Fresh && exact(operation)) {
                    let got = first_pages(operation);
                    if got != 1 && !has_error(observed, "in_flight") {
                        return Err(format!("{}: {} sent {got} request(s) of {operation}, expected exactly 1", describe(steps, index), input.name));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn i2(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::Fresh)
}

pub fn i2b(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::FreshStale)
}

pub fn i7(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::Outcomes)
}

pub fn i9(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::Quiet)
}

pub fn i12(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::NoSession)
}

pub fn i13(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::Network)
}

/// I2, I2b, I9, I12, I13: the rules over the requests in the mock log.
pub fn request_rules() -> Vec<Property> {
    vec![
        Property { name: "I2", check: i2 },
        Property { name: "I2b", check: i2b },
        Property { name: "I9", check: i9 },
        Property { name: "I12", check: i12 },
        Property { name: "I13", check: i13 },
    ]
}

/// I7: every accepted load ends with exactly one outcome.
pub fn outcome_rules() -> Vec<Property> {
    vec![Property { name: "I7", check: i7 }]
}
