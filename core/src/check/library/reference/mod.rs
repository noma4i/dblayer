use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Value};

use super::super::model::RefModel;
use super::super::value::{Input, Observation};
use ops::{parse, Op};
use window::{Key, Outcome, Phase, Run, Window};

mod env;
mod landing;
mod loads;
mod ops;
mod shapes;
mod streams;
mod window;

/// What a load call of the alphabet is.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Form {
    BeginLoad,
    Refresh,
    LoadMore,
    LoadAll,
    Node,
    Nodes,
}

/// How a call ends: the value of its outcome observation.
#[derive(Clone, Copy, Debug)]
enum Ending {
    Done,
    More(&'static str),
    Rows(u64),
    Failed(&'static str),
}

/// A call waiting for its outcome. `queued` is a `load_more` behind a running load, `target` the page it waits for.
struct Call {
    id: u64,
    name: String,
    form: Form,
    key: Option<Key>,
    target: Option<u32>,
    queued: bool,
}

/// The session call of `login`: its call number, whether its request is out, the refusals so far and the pause before the next send.
struct Login {
    call: u64,
    sent: bool,
    attempts: u32,
    pause: Option<u64>,
    /// Logins called while this one is out: they go after its end.
    queued: Vec<u64>,
}

/// Who a held request belongs to.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Owner {
    Window(Key),
    Session,
}

/// What a stream reads: a window (a list, or its count) or one row.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Reads {
    Window { key: Key, count: bool },
    Row { key: Option<Key> },
}

impl Reads {
    fn key(self) -> Option<Key> {
        match self {
            Reads::Window { key, .. } => Some(key),
            Reads::Row { key } => key,
        }
    }
}

/// What a stream asks of its window when it opens.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Load {
    None,
    Stale,
    Force,
}

/// A stream of the reader: its last value pulled and what waits for the next `pull` after its end.
struct Stream {
    reads: Reads,
    on_connect: bool,
    /// The stream asks its window to load (`Entity(never)` and an `Entity(if_missing)` of a stored row do not).
    wants: bool,
    live: bool,
    last: Option<Observation>,
    pending: Vec<Observation>,
}

/// A request the mock holds. A dead one was cancelled by the library: an answer to it reaches nobody.
struct Held {
    id: String,
    live: bool,
    owner: Owner,
}

struct Sent {
    id: String,
    operation: &'static str,
    variables: Value,
}

/// The executable form of library.md for the slices O1 and O2: environment, loads and streams. Written from the spec tables, the observations are the shapes the stand shows.
/// An input the spec does not decide marks a hole: `hole` names the first one the program met.
pub struct LibraryRef {
    live: bool,
    online: Option<bool>,
    identity: bool,
    login_call: Option<Login>,
    now: i64,
    back: i64,
    next_request: u64,
    next_provisional: u64,
    next_call: u64,
    held: Vec<Held>,
    handles: HashMap<String, u64>,
    windows: BTreeMap<Key, Window>,
    streams: BTreeMap<String, Stream>,
    row: bool,
    calls: Vec<Call>,
    outcomes: Vec<(u64, Observation)>,
    requests: Vec<Sent>,
    hole: Option<&'static str>,
}

impl Default for LibraryRef {
    fn default() -> Self {
        Self {
            live: false,
            online: None,
            identity: false,
            login_call: None,
            now: 0,
            back: 0,
            next_request: 1,
            next_provisional: 1,
            next_call: 1,
            held: Vec::new(),
            handles: HashMap::new(),
            windows: BTreeMap::new(),
            streams: BTreeMap::new(),
            row: false,
            calls: Vec::new(),
            outcomes: Vec::new(),
            requests: Vec::new(),
            hole: None,
        }
    }
}

/// True when O1 models the input.
pub fn modeled(input: &Input) -> bool {
    parse(input).is_ok_and(|op| op.modeled())
}

/// The first spec hole the inputs run into, if any.
pub fn first_hole(inputs: &[Input]) -> Option<&'static str> {
    let mut model = LibraryRef::default();
    inputs.iter().find_map(|input| {
        model.step(input);
        model.hole
    })
}

impl LibraryRef {
    fn mark(&mut self, hole: &'static str) {
        self.hole.get_or_insert(hole);
    }

    fn win(&mut self, key: Key) -> &mut Window {
        self.windows.entry(key).or_default()
    }

    /// The network is known to be on (library.md section 2: `online` is None, true or false).
    fn on(&self) -> bool {
        self.online == Some(true)
    }

    /// A new request waits under a provisional id; the ids are final when the input ends.
    fn provisional(&mut self) -> String {
        self.next_provisional += 1;
        format!("p{}", self.next_provisional - 1)
    }

    fn rename(&mut self, from: &str, to: &str) {
        self.held.iter_mut().filter(|held| held.id == from).for_each(|held| held.id = to.into());
        for window in self.windows.values_mut() {
            if let Some(Run { phase: Phase::Flight(id), .. }) = &mut window.load {
                if id == from {
                    *id = to.into();
                }
            }
        }
    }

    /// The requests of one input as the stand shows them (H16): in the canonical order of (operation, variables as text), numbered in that order, `request` then `sent`. The held list takes them in the same order.
    fn announce(&mut self) -> Vec<Observation> {
        let mut batch: Vec<Sent> = self.requests.drain(..).collect();
        batch.sort_by_key(|sent| (sent.operation, sent.variables.to_string()));
        let mut shown = Vec::new();
        let mut moved = Vec::new();
        for sent in batch {
            let id = format!("r{}", self.next_request);
            self.next_request += 1;
            self.rename(&sent.id, &id);
            if let Some(at) = self.held.iter().position(|held| held.id == id) {
                moved.push(self.held.remove(at));
            }
            shown.push(shapes::request(&id));
            shown.push(shapes::sent(&id, sent.operation, sent.variables));
        }
        self.held.extend(moved);
        shown
    }

    /// library.md section 3: the outcome belongs to the current interest, the last interest (streams and calls) taking it away.
    fn drop_outcomes_without_interest(&mut self) {
        for key in [Key::Feed, Key::Node, Key::Nodes, Key::Members] {
            if !self.interest(key) {
                self.win(key).outcome = Outcome::None;
            }
        }
    }

    fn apply(&mut self, op: Op) -> Vec<Observation> {
        match op {
            Op::Open => self.open(),
            Op::Login => self.login(),
            Op::SetOnline(online) => self.set_online(online),
            Op::Tick(ms) => self.tick(ms),
            Op::ClockBack(ms) => {
                self.back += ms as i64;
                Vec::new()
            }
            Op::Close => self.close(),
            Op::SessionReset => self.session_reset(),
            Op::BeginLoad(name) => self.load_call(Form::BeginLoad, name, false),
            Op::Refresh(name) => self.load_call(Form::Refresh, name, true),
            Op::LoadMore(name) => self.load_call(Form::LoadMore, name, false),
            Op::BeginLoadAll { name, force } => self.load_call(Form::LoadAll, name, force),
            Op::Node { name, force } => self.load_call(Form::Node, name, force),
            Op::Nodes(name) => self.load_call(Form::Nodes, name, true),
            Op::Cancel(target) => self.cancel(&target),
            Op::AnswerPage { rows, more } => self.answer(landing::Reply::Page { rows, more }),
            Op::AnswerError(kind) => self.answer(landing::Reply::Error(kind)),
            Op::OpenFeed { name, on_connect } => self.open_feed(name, Key::Feed, false, on_connect),
            Op::OpenCount(name) => self.open_feed(name, Key::Feed, true, false),
            Op::OpenEntity { name, fetch } => self.open_entity(name, fetch),
            Op::OpenMembers(name) => self.open_feed(name, Key::Members, false, false),
            Op::OpenEvents => {
                self.mark("unmodeled_events");
                Vec::new()
            }
            Op::Pull(name) => self.pull(&name),
        }
    }
}

impl RefModel for LibraryRef {
    fn step(&mut self, input: &Input) -> Vec<Observation> {
        let op = match parse(input) {
            Ok(op) => op,
            Err(message) => return vec![Observation::new("unparsed", json!(message))],
        };
        let mut shown = self.apply(op);
        self.drop_outcomes_without_interest();
        self.outcomes.sort_by_key(|(seq, _)| *seq);
        shown.extend(self.outcomes.drain(..).map(|(_, item)| item));
        shown.extend(self.announce());
        shown
    }
}
