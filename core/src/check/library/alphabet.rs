use serde_json::json;

use crate::check::value::Input;

/// Page sizes the answers use: the ids are `m1..mN`, so a repeated id between pages is a repeated row.
pub const PAGE_EMPTY: u64 = 0;
pub const PAGE_ONE: u64 = 1;
pub const PAGE_MANY: u64 = 3;

/// The inputs every program starts with, kept by `minimize`. `anonymous` stops after `open`, so reads meet a core without a session.
pub fn prelude(anonymous: bool) -> Vec<Input> {
    let open = Input::new("open").with("encrypted", true);
    if anonymous {
        return vec![open];
    }
    vec![open, Input::new("login"), Input::new("set_online").with("online", true)]
}

fn stream(kind: &str, name: &str) -> Input {
    Input::new(kind).with("as", name)
}

fn on_feed(input: Input) -> Input {
    input.with("relation", "feed")
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
        stream("open_list", "s2"),
        on_feed(stream("open_list", "s3")).with("refresh_on_connect", true),
        on_feed(stream("open_count", "c1")),
        stream("open_list", "u1").with("model", "User").with("relation", "container_members").with("params", members),
        pull("s1"),
        pull("c1"),
        pull("u1"),
        on_feed(stream("begin_load", "l1")),
        on_feed(Input::new("refresh")),
        on_feed(Input::new("load_more")),
        on_feed(stream("begin_load_all", "la")),
        Input::new("node").with("relation", "transcode_status").with("row", "m1"),
        Input::new("nodes").with("model", "Container").with("relation", "nodes").with("rows", json!(["m1"])),
        cancel("l1"),
        cancel("s1"),
        page(PAGE_EMPTY, false),
        page(PAGE_ONE, false),
        page(PAGE_MANY, true),
        refuse("temporary"),
        refuse("permanent"),
        refuse("server"),
    ]
}
