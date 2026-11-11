use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Value};

use super::super::model::RefModel;
use super::super::value::{Input, Observation};
use ops::{parse, Op};
use window::{Key, Window};

mod env;
mod landing;
mod loads;
mod ops;
mod shapes;
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

/// A request the mock holds. A dead one was cancelled by the library: an answer to it reaches nobody.
struct Held {
    id: String,
    live: bool,
    key: Key,
}

struct Sent {
    id: String,
    operation: &'static str,
    variables: Value,
}

/// The executable form of library.md for the slice O1: environment and loads. Written from the spec tables, the observations are the shapes the stand shows.
/// An input the spec does not decide marks a hole: `hole` names the first one the program met.
pub struct LibraryRef {
    live: bool,
    online: bool,
    identity: bool,
    now: i64,
    back: i64,
    next_request: u64,
    next_call: u64,
    held: Vec<Held>,
    handles: HashMap<String, u64>,
    windows: BTreeMap<Key, Window>,
    calls: Vec<Call>,
    outcomes: Vec<(u64, Observation)>,
    requests: Vec<Sent>,
    hole: Option<&'static str>,
}

impl Default for LibraryRef {
    fn default() -> Self {
        Self {
            live: false,
            online: false,
            identity: false,
            now: 0,
            back: 0,
            next_request: 1,
            next_call: 1,
            held: Vec::new(),
            handles: HashMap::new(),
            windows: BTreeMap::new(),
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
            Op::OpenStream | Op::OpenEntity | Op::Pull => {
                self.mark("unmodeled");
                Vec::new()
            }
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
        self.outcomes.sort_by_key(|(seq, _)| *seq);
        shown.extend(self.outcomes.drain(..).map(|(_, item)| item));
        for sent in self.requests.drain(..) {
            shown.push(shapes::request(&sent.id));
            shown.push(shapes::sent(&sent.id, sent.operation, sent.variables));
        }
        shown
    }
}
