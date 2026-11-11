use serde_json::json;

use crate::check::value::Input;

/// Page sizes the answers use: the ids are `m1..mN`, so a repeated id between pages is a repeated row.
pub const PAGE_EMPTY: u64 = 0;
pub const PAGE_ONE: u64 = 1;
pub const PAGE_MANY: u64 = 3;

/// The inputs every program starts with, kept by `minimize`. The symbolic stand leaves the network unknown after `open`, so the network goes on before `login`. `anonymous` stops after `open`, so reads meet a core without a session.
pub fn prelude(anonymous: bool) -> Vec<Input> {
    let open = Input::new("open").with("encrypted", true);
    if anonymous {
        return vec![open];
    }
    vec![open, Input::new("set_online").with("online", true), Input::new("login")]
}

fn stream(kind: &str, name: &str) -> Input {
    Input::new(kind).with("as", name)
}

fn on_feed(input: Input) -> Input {
    input.with("relation", "feed")
}

/// A load call that names itself, so its outcome (success or error) carries the name.
fn load(kind: &str, name: &str) -> Input {
    Input::new(kind).with("as", name).with("named", true)
}

fn page(rows: u64, more: bool) -> Input {
    Input::new("answer_page").with("rows", rows).with("more", more)
}

fn refuse(kind: &str) -> Input {
    Input::new("answer_error").with("kind", kind)
}

fn pull(name: &str) -> Input {
    Input::new("pull").with("stream", name)
}

fn cancel(name: &str) -> Input {
    Input::new("cancel").with("target", name)
}

/// The finite input list of the general run. References are symbolic: an answer goes to the oldest held request, a stream or a load is named.
/// An input that does not apply to the state is a legal program: the core shows a refusal observation.
pub fn alphabet() -> Vec<Input> {
    let members = json!({ "containerId": "c1" });
    vec![
        Input::new("open").with("encrypted", true),
        Input::new("login"),
        Input::new("set_online").with("online", true),
        Input::new("set_online").with("online", false),
        Input::new("tick").with("ms", 60_000u64),
        Input::new("tick").with("ms", 400_000u64),
        Input::new("close"),
        Input::new("session_reset"),
        on_feed(stream("open_list", "s1")),
        on_feed(stream("open_list", "s3")).with("refresh_on_connect", true),
        stream("open_list", "u1").with("model", "User").with("relation", "container_members").with("params", members),
        pull("s1"),
        stream("open_entity", "e1").with("row", "m1").with("fetch", "never"),
        stream("open_entity", "e2").with("row", "m1").with("fetch", "if_missing"),
        stream("open_entity", "e3").with("row", "m1").with("fetch", "force"),
        Input::new("clock_back").with("ms", 400_000u64),
        on_feed(load("begin_load", "l1")),
        on_feed(load("refresh", "rf")),
        on_feed(load("load_more", "lm")),
        on_feed(load("begin_load_all", "la")),
        load("node", "nd").with("relation", "transcode_status").with("row", "m1"),
        load("node", "nf").with("relation", "transcode_status").with("row", "m1").with("force", true),
        load("nodes", "ns").with("model", "Container").with("relation", "nodes").with("rows", json!(["m1"])),
        cancel("l1"),
        page(PAGE_EMPTY, false),
        page(PAGE_ONE, false),
        page(PAGE_MANY, true),
        refuse("temporary"),
        refuse("permanent"),
        refuse("server"),
    ]
}

/// The stream inputs of the family of programs with streams: a count, and a `pull` and a `cancel` of each stream of the feed and of the entity.
pub fn streams() -> Vec<Input> {
    vec![
        on_feed(stream("open_count", "c1")),
        pull("s3"),
        pull("c1"),
        pull("e1"),
        cancel("s1"),
        cancel("s3"),
        cancel("c1"),
    ]
}

/// True for an input that opens, reads or ends a stream.
pub fn is_stream(input: &Input) -> bool {
    input.name.starts_with("open_") || input.name == "pull" || input.name == "cancel" && input.text("target").is_some_and(|target| !target.starts_with('l'))
}
