use std::collections::HashMap;

use serde_json::Value;

use crate::check::model::Property;
use crate::check::value::{Observation, Step};

/// Fresh-window lifetime of the stand schema (`stale_time` 5 minutes, the same for an empty page).
const LIFETIME_MS: u64 = 300_000;

/// window.md win_loading_temp_error: a retryable failure waits for `Retry` while the attempts are below the limit (network 3, server 2).
const NETWORK_ATTEMPTS: u32 = 3;
const SERVER_ATTEMPTS: u32 = 2;

const LOADS: [(&str, &str); 6] = [("begin_load", "loaded"), ("refresh", "refreshed"), ("load_more", "more"), ("begin_load_all", "loaded_all"), ("node", "node"), ("nodes", "nodes")];
const REFUSALS: [&str; 11] = ["already_open", "no_request", "not_open", "unknown_stream", "unknown_target", "arguments", "online", "open", "operations", "frame", "file"];
const READS: [&str; 5] = ["begin_load", "refresh", "load_more", "begin_load_all", "node"];

#[derive(Clone, Copy, PartialEq)]
enum Rule {
    Fresh,
    Outcomes,
    Quiet,
    NoSession,
    Network,
}

#[derive(Default, Clone)]
struct Window {
    landed: Option<u64>,
    failed: bool,
    lost: bool,
}

struct Interest {
    operation: &'static str,
    on_connect: bool,
}

struct Request {
    operation: String,
    first_page: bool,
}

/// The state a reader of the trace can know: the clock moved by the ticks, the network flag, the session, the requests the mock holds, the landed windows and the open streams.
#[derive(Default)]
struct Trace {
    now: u64,
    online: bool,
    is_open: bool,
    logged: bool,
    ever_logged: bool,
    held: Vec<(String, String)>,
    pending: Vec<&'static str>,
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

/// The load outcomes a request of the operation can end.
fn served_by(operation: &str) -> &'static [&'static str] {
    match operation {
        "MomentsFeed" | "ContainerMembers" => &["loaded", "refreshed", "more", "loaded_all"],
        "MomentTranscodeStatus" => &["node"],
        "ContainersByIds" => &["nodes"],
        _ => &[],
    }
}

fn describe(steps: &[Step], index: usize) -> String {
    format!("step {} {}", index + 1, steps[index].input)
}

/// Folds the trace and checks the rule after every step.
fn analyze(steps: &[Step], rule: Rule) -> Result<(), String> {
    let mut state = Trace::default();
    for (index, step) in steps.iter().enumerate() {
        let (input, observed) = (&step.input, &step.observed);
        let before_online = state.online;
        let before_fresh: HashMap<String, bool> = state.windows.iter().map(|(operation, window)| (operation.clone(), fresh(window, state.now))).collect();
        let before_logged = state.logged;
        let was_held: Vec<String> = state.held.iter().map(|(_, operation)| operation.clone()).collect();
        if input.name == "tick" {
            state.now += input.number("ms").unwrap_or(0);
        }
        let fresh_now: HashMap<String, bool> = state.windows.iter().map(|(operation, window)| (operation.clone(), fresh(window, state.now))).collect();
        let refused = |kind: &str| has_error(observed, kind);
        if kinds(observed, "opened").next().is_some() {
            (state.is_open, state.online, state.logged) = (true, true, false);
        }
        if let Some(online) = kinds(observed, "online").next() {
            state.online = online.value.as_bool().unwrap_or(false);
        }
        if input.name == "set_online" && refused("not_open") {
            state.online = before_online;
        }
        let load = LOADS.iter().find(|(name, _)| *name == input.name).map(|(_, outcome)| *outcome);
        let accepted = load.filter(|_| !refused("not_open") && !refused("arguments"));
        if let Some(outcome) = accepted {
            state.pending.push(outcome);
        }
        let sent = requests(observed);
        let all_requests = kinds(observed, "request").count();
        let first_pages = |operation: &str| sent.iter().filter(|request| request.first_page && request.operation == operation).count();

        let mut replied_operation: Option<String> = None;
        for replied in kinds(observed, "replied") {
            let id = replied.value["id"].as_str().unwrap_or("");
            let Some(position) = state.held.iter().position(|(held, _)| held == id) else { continue };
            let (_, operation) = state.held.remove(position);
            replied_operation = Some(operation.clone());
            let streak = state.streaks.entry(operation.clone()).or_default();
            match replied.value["how"].as_str().unwrap_or("") {
                "temporary" | "drop" => streak.0 += 1,
                "server" => streak.1 += 1,
                _ => *streak = (0, 0),
            }
            if state.streaks.get(&operation) == Some(&(0, 0)) {
                state.streaks.remove(&operation);
            }
            if replied.value["how"] == "page" && sent_first_page(steps, id) && !kinds(observed, "error").any(|item| !REFUSALS.contains(&error_kind(item))) {
                state.windows.insert(operation, Window { landed: Some(state.now), failed: false, lost: false });
            } else if sent_first_page(steps, id) && replied.value["how"] != "page" {
                state.windows.entry(operation).or_default().failed = true;
            }
        }
        for item in kinds(observed, "sent") {
            let operation = item.value["operation"].as_str().unwrap_or("");
            if !operation.starts_with("session") {
                state.held.push((item.value["id"].as_str().unwrap_or("").to_string(), operation.to_string()));
            }
        }
        for released in kinds(observed, "released") {
            let ids: Vec<&str> = released.value["ids"].as_array().map(|ids| ids.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
            state.held.retain(|(held, _)| !ids.contains(&held.as_str()));
            state.windows.values_mut().for_each(|window| window.lost = true);
            state.streams.clear();
        }
        if input.name == "close" || kinds(observed, "closed").next().is_some() {
            state.is_open = false;
            state.streams.clear();
            state.windows.values_mut().for_each(|window| window.lost = true);
        }
        if kinds(observed, "reset").next().is_some() {
            state.logged = false;
            state.windows.values_mut().for_each(|window| window.lost = true);
        }
        if kinds(observed, "session").next().is_some() {
            (state.logged, state.ever_logged) = (true, true);
        }
        if load.is_some() && input.name != "node" {
            if let (Some(operation), true) = (input.text("relation").and_then(operation_of), refused("offline")) {
                state.windows.entry(operation.to_string()).or_default().failed = true;
            }
        }
        if input.name == "open_list" || input.name == "open_count" {
            if let (Some(name), Some(operation)) = (input.text("as"), input.text("relation").and_then(operation_of)) {
                if !refused("not_open") && !refused("arguments") {
                    state.streams.insert(name.to_string(), Interest { operation, on_connect: input.flag("refresh_on_connect").unwrap_or(false) });
                }
            }
        }
        if input.name == "cancel" {
            if let Some(target) = input.text("target") {
                state.streams.remove(target);
            }
        }
        for ended in kinds(observed, "ended") {
            if let Some(name) = ended.value["stream"].as_str() {
                state.streams.remove(name);
            }
        }

        let mut own_ended = false;
        let attributable = !matches!(input.name.as_str(), "login" | "open" | "session_reset" | "pull");
        for item in observed {
            if let Some((_, outcome)) = LOADS.iter().find(|(_, outcome)| *outcome == item.kind) {
                match state.pending.iter().position(|pending| pending == outcome) {
                    Some(position) => { state.pending.remove(position); }
                    None if rule == Rule::Outcomes => return Err(format!("{}: outcome `{}` of a load that was not accepted or already ended", describe(steps, index), item.kind)),
                    None => {}
                }
            } else if item.kind == "error" && attributable && !REFUSALS.contains(&error_kind(item)) && !state.pending.is_empty() {
                let own = accepted.filter(|_| !own_ended).and_then(|outcome| state.pending.iter().rposition(|pending| *pending == outcome));
                own_ended |= own.is_some();
                let of_request = replied_operation.as_deref().and_then(|operation| state.pending.iter().position(|pending| served_by(operation).contains(pending)));
                state.pending.remove(own.or(of_request).unwrap_or(0));
                if let Some(operation) = &replied_operation {
                    state.streaks.remove(operation);
                }
            }
        }

        let waiting_retry = state.streaks.values().any(|(network, server)| (*network > 0 && *network < NETWORK_ATTEMPTS) || (*server > 0 && *server < SERVER_ATTEMPTS));
        match rule {
            Rule::Outcomes if !state.pending.is_empty() && state.held.is_empty() && state.online && !waiting_retry => {
                return Err(format!("{}: {} accepted load(s) have no outcome and the mock holds no request", describe(steps, index), state.pending.len()));
            }
            Rule::Quiet => {
                let quiet = matches!(input.name.as_str(), "pull" | "cancel" | "close") || (input.name == "set_online" && input.flag("online") == Some(false));
                if quiet && all_requests > 0 {
                    return Err(format!("{}: {} request(s) after an input that asks for none", describe(steps, index), all_requests));
                }
            }
            Rule::NoSession if !state.ever_logged => {
                if !sent.is_empty() {
                    return Err(format!("{}: {} request(s) reached the mock before any session", describe(steps, index), sent.len()));
                }
                if READS.contains(&input.name.as_str()) && state.is_open && state.online && !refused("not_open") && !refused("arguments") && !has_error(observed, "no_session") {
                    return Err(format!("{}: a server read without a session did not end with NoSession", describe(steps, index)));
                }
            }
            Rule::Network => {
                if !state.online && all_requests > 0 {
                    return Err(format!("{}: {} request(s) while the network is off", describe(steps, index), all_requests));
                }
                let returned = !before_online && state.online && input.name == "set_online";
                if returned && before_logged && state.ever_logged && state.logged {
                    for (operation, expected) in reread_after_return(&state, &fresh_now) {
                        let got = first_pages(operation);
                        if got != expected && !was_held.iter().any(|held| held == operation) {
                            return Err(format!("{}: network return sent {got} request(s) of {operation}, expected {expected}", describe(steps, index)));
                        }
                    }
                }
            }
            Rule::Fresh => {
                let connected = kinds(observed, "request").any(|item| item.value["kind"] == "subscription");
                let on_connect_return = (!before_online && state.online) || connected;
                for request in sent.iter().filter(|request| request.first_page && request.operation != "ContainersByIds") {
                    let Some(window) = state.windows.get(&request.operation) else { continue };
                    let was_fresh = before_fresh.get(&request.operation).copied().unwrap_or(false) && fresh_now.get(&request.operation).copied().unwrap_or(false) && !window.lost;
                    let declared = on_connect_return && state.streams.values().any(|interest| interest.operation == request.operation && interest.on_connect);
                    let forced = input.name == "refresh";
                    if was_fresh && !forced && !declared {
                        return Err(format!("{}: a fresh window of {} was requested again", describe(steps, index), request.operation));
                    }
                }
                let ready = state.is_open && state.online && state.logged && before_logged && !refused("not_open");
                if input.name == "refresh" && ready && input.text("relation") == Some("feed") && !was_held.iter().any(|held| held == "MomentsFeed") && !state.streaks.contains_key("MomentsFeed") && !has_error(observed, "no_session") && state.windows.get("MomentsFeed").is_some_and(|window| !window.lost) {
                    let got = first_pages("MomentsFeed");
                    if got != 1 && !has_error(observed, "in_flight") {
                        return Err(format!("{}: refresh sent {got} request(s) of MomentsFeed, expected exactly 1", describe(steps, index)));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn fresh(window: &Window, now: u64) -> bool {
    let Some(landed) = window.landed else { return false };
    !window.failed && !window.lost && now >= landed && now - landed < LIFETIME_MS
}

fn sent_first_page(steps: &[Step], id: &str) -> bool {
    steps.iter().flat_map(|step| kinds(&step.observed, "sent")).any(|item| item.value["id"] == id && item.value["variables"].get("after").is_none())
}

/// After a network return: per watched window the number of first-page requests the rule asks for. A window is reread once when it is not fresh or a stream of it is declared to reload on connect.
fn reread_after_return<'a>(state: &'a Trace, fresh_now: &HashMap<String, bool>) -> Vec<(&'a str, usize)> {
    let mut watched: Vec<&'static str> = state.streams.values().map(|interest| interest.operation).collect();
    watched.sort_unstable();
    watched.dedup();
    watched
        .into_iter()
        .filter(|operation| !state.windows.get(*operation).is_some_and(|window| window.lost))
        .map(|operation| {
            let declared = state.streams.values().any(|interest| interest.operation == operation && interest.on_connect);
            let fresh = fresh_now.get(operation).copied().unwrap_or(false);
            (operation, usize::from(declared || !fresh))
        })
        .collect()
}

pub fn i2(steps: &[Step]) -> Result<(), String> {
    analyze(steps, Rule::Fresh)
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

/// I2, I9, I12, I13: the rules over the requests in the mock log.
pub fn request_rules() -> Vec<Property> {
    vec![Property { name: "I2", check: i2 }, Property { name: "I9", check: i9 }, Property { name: "I12", check: i12 }, Property { name: "I13", check: i13 }]
}

/// I7: every accepted load ends with exactly one outcome.
pub fn outcome_rules() -> Vec<Property> {
    vec![Property { name: "I7", check: i7 }]
}
