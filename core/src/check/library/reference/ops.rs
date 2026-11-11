use crate::check::value::Input;

/// The inputs of the alphabet, one variant per input. A load carries the name its outcome is reported under.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Op {
    Open,
    Login,
    SetOnline(bool),
    Tick(u64),
    ClockBack(u64),
    Close,
    SessionReset,
    BeginLoad(String),
    Refresh(String),
    LoadMore(String),
    BeginLoadAll { name: String, force: bool },
    Node { name: String, force: bool },
    Nodes(String),
    Cancel(String),
    AnswerPage { rows: u64, more: bool },
    AnswerError(String),
    OpenFeed { name: String, on_connect: bool },
    OpenCount(String),
    OpenMembers,
    OpenEntity { name: String, fetch: Fetch },
    OpenEvents,
    Pull(String),
}

/// The `fetch` values of an entity stream (library.md section 1: 4 values).
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum Fetch {
    Never,
    IfMissing,
    Force,
    Stale,
}

impl Op {
    /// False for the streams of the not-modeled column of library.md section 1 (`Events`).
    pub(super) fn modeled(&self) -> bool {
        !matches!(self, Op::OpenEvents)
    }
}

fn named(input: &Input) -> Result<String, String> {
    let wanted = |relation: &str| input.text("relation") == Some(relation);
    let relation = match input.name.as_str() {
        "begin_load" | "refresh" | "load_more" | "begin_load_all" => wanted("feed"),
        "node" => wanted("transcode_status") && input.text("row") == Some("m1"),
        _ => wanted("nodes") && input.text("model") == Some("Container"),
    };
    if !relation || input.flag("named") != Some(true) {
        return Err(format!("the load {input} is not a load of the alphabet"));
    }
    input.text("as").map(str::to_string).ok_or_else(|| format!("the load {input} has no name"))
}

/// An input outside the list is a parse error.
pub(super) fn parse(input: &Input) -> Result<Op, String> {
    Ok(match input.name.as_str() {
        "open" => Op::Open,
        "login" => Op::Login,
        "set_online" => Op::SetOnline(input.flag("online").ok_or("set_online without online")?),
        "tick" => Op::Tick(input.number("ms").ok_or("tick without ms")?),
        "clock_back" => Op::ClockBack(input.number("ms").ok_or("clock_back without ms")?),
        "close" => Op::Close,
        "session_reset" => Op::SessionReset,
        "begin_load" => Op::BeginLoad(named(input)?),
        "refresh" => Op::Refresh(named(input)?),
        "load_more" => Op::LoadMore(named(input)?),
        "begin_load_all" => Op::BeginLoadAll { name: named(input)?, force: input.flag("force").unwrap_or(false) },
        "node" => Op::Node { name: named(input)?, force: input.flag("force").unwrap_or(false) },
        "nodes" => Op::Nodes(named(input)?),
        "cancel" => Op::Cancel(input.text("target").ok_or("cancel without target")?.to_string()),
        "answer_page" => Op::AnswerPage { rows: input.number("rows").ok_or("answer_page without rows")?, more: input.flag("more").ok_or("answer_page without more")? },
        "answer_error" => Op::AnswerError(input.text("kind").ok_or("answer_error without kind")?.to_string()),
        "open_list" | "open_count" => {
            let name = input.text("as").ok_or("a stream without name")?.to_string();
            match (input.name.as_str(), input.text("relation")) {
                ("open_list", Some("feed")) => Op::OpenFeed { name, on_connect: input.flag("refresh_on_connect").unwrap_or(false) },
                ("open_count", Some("feed")) => Op::OpenCount(name),
                (_, Some("container_members")) => Op::OpenMembers,
                _ => return Err(format!("the stream {input} is not a stream of the alphabet")),
            }
        }
        "open_entity" => {
            let name = input.text("as").ok_or("a stream without name")?.to_string();
            let fetch = match input.text("fetch") {
                Some("never") => Fetch::Never,
                Some("if_missing") => Fetch::IfMissing,
                Some("force") => Fetch::Force,
                Some("stale") => Fetch::Stale,
                _ => return Err(format!("the entity {input} has no known fetch")),
            };
            Op::OpenEntity { name, fetch }
        }
        "open_events" => Op::OpenEvents,
        "pull" => Op::Pull(input.text("stream").ok_or("pull without stream")?.to_string()),
        other => return Err(format!("unknown input {other}")),
    })
}
