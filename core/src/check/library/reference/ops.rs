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
    OpenStream,
    OpenEntity,
    Pull,
}

impl Op {
    /// False for the streams: O1 does not model them (the "not modeled" column of library.md section 1).
    pub(super) fn modeled(&self) -> bool {
        match self {
            Op::OpenStream | Op::OpenEntity | Op::Pull => false,
            Op::Open | Op::Login | Op::SetOnline(_) | Op::Tick(_) | Op::ClockBack(_) | Op::Close | Op::SessionReset => true,
            Op::BeginLoad(_) | Op::Refresh(_) | Op::LoadMore(_) | Op::BeginLoadAll { .. } | Op::Node { .. } | Op::Nodes(_) => true,
            Op::Cancel(_) | Op::AnswerPage { .. } | Op::AnswerError(_) => true,
        }
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
        "open_list" | "open_count" | "open_events" => Op::OpenStream,
        "open_entity" => Op::OpenEntity,
        "pull" => Op::Pull,
        other => return Err(format!("unknown input {other}")),
    })
}
