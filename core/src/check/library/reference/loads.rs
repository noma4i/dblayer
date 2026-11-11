use serde_json::{json, Value};

use super::shapes::{self, PAGE_SIZE};
use super::window::{Key, Phase, Run};
use super::{Call, Ending, Form, Held, LibraryRef, Owner, Sent};
use crate::check::value::Observation;

/// library.md section 4: the load calls, and the plumbing of one load (its request, its cancel, its outcome).
impl LibraryRef {
    pub(super) fn run_of(&mut self, key: Key) -> &mut Option<Run> {
        &mut self.windows.entry(key).or_default().load
    }

    /// The request goes back to the library: an answer to it reaches nobody.
    pub(super) fn kill(&mut self, run: &Run) {
        let Phase::Flight(id) = &run.phase else { return };
        self.held.iter_mut().filter(|held| held.id == *id).for_each(|held| held.live = false);
    }

    fn request_of(&mut self, key: Key, page: u32) -> (&'static str, Value) {
        match key {
            Key::Feed => {
                let mut variables = json!({ "first": PAGE_SIZE });
                if page > 1 {
                    variables["after"] = json!(self.win(Key::Feed).cursor);
                }
                ("MomentsFeed", variables)
            }
            Key::Node => ("MomentTranscodeStatus", json!({ "momentId": "m1" })),
            Key::Nodes(_) => ("ContainersByIds", json!({ "ids": ["m1"] })),
        }
    }

    /// The load sends its request now: the mock holds it until an answer input.
    pub(super) fn send(&mut self, key: Key) {
        let page = self.run_of(key).as_ref().map_or(1, |run| run.page);
        let (operation, variables) = self.request_of(key, page);
        let id = format!("r{}", self.next_request);
        self.next_request += 1;
        self.held.push(Held { id: id.clone(), live: true, owner: Owner::Window(key) });
        self.requests.push(Sent { id: id.clone(), operation, variables });
        if let Some(run) = self.run_of(key) {
            run.phase = Phase::Flight(id);
        }
    }

    /// A new load of `page`: with the network on it sends at once (the "loads" word of library.md section 4), otherwise it waits.
    pub(super) fn start_run(&mut self, key: Key, page: u32) {
        *self.run_of(key) = Some(Run { page, phase: Phase::Waiting, attempts: 0 });
        if self.on() {
            self.send(key);
        }
    }

    fn add_call(&mut self, form: Form, name: &str, key: Option<Key>) -> u64 {
        let id = self.next_call;
        self.next_call += 1;
        self.calls.push(Call { id, name: name.into(), form, key, target: None, queued: false });
        id
    }

    /// The outcome observation of a call, named by the call (library.md section 4, the column of what the call receives).
    pub(super) fn finish(&mut self, id: u64, ending: Ending) {
        let Some(at) = self.calls.iter().position(|call| call.id == id) else { return };
        let call = self.calls.remove(at);
        let present = self.windows.get(&Key::Node).is_some_and(|window| window.members > 0);
        let item = match (call.form, ending) {
            (_, Ending::Failed(kind)) => shapes::error(kind),
            (Form::BeginLoad, Ending::Done) => Observation::new("loaded", json!({ "load": call.name })),
            (Form::Refresh, Ending::Done) => Observation::new("refreshed", json!({ "relation": "feed" })),
            (Form::LoadMore, Ending::More(result)) => Observation::new("more", json!({ "relation": "feed", "result": result })),
            (Form::LoadAll, Ending::Done) => Observation::new("loaded_all", json!({ "load": call.name })),
            (Form::Node, Ending::Done) => Observation::new("node", json!({ "row": "m1", "present": present })),
            (Form::Nodes, Ending::Rows(rows)) => Observation::new("nodes", json!({ "relation": "nodes", "rows": if rows > 0 { json!(["m1"]) } else { json!([]) } })),
            (form, ending) => Observation::new("unparsed", json!(format!("{form:?} cannot end with {ending:?}"))),
        };
        if call.form == Form::Nodes {
            self.windows.remove(&Key::Nodes(id));
        }
        self.outcomes.push((id, shapes::tag(item, &call.name)));
    }

    /// One load call. Order of the checks: database open, identity (G4), then the row of library.md section 4.
    pub(super) fn load_call(&mut self, form: Form, name: String, force: bool) -> Vec<Observation> {
        if !self.live {
            return vec![shapes::error("not_open")];
        }
        if !self.identity {
            return vec![shapes::tag(shapes::error("no_session"), &name)];
        }
        match form {
            Form::Nodes => self.nodes_call(name),
            Form::LoadMore => self.load_more_call(name),
            Form::LoadAll => self.load_all_call(name, force),
            Form::BeginLoad | Form::Refresh | Form::Node => self.single_call(form, name, force),
        }
    }

    /// library.md section 4 `nodes(ids)`: 1 request always (K2, K4), without the network the call waits (H4). The stand schema must declare a `Container` relation `nodes`.
    fn nodes_call(&mut self, name: String) -> Vec<Observation> {
        let id = self.add_call(Form::Nodes, &name, None);
        self.start_run(Key::Nodes(id), 1);
        Vec::new()
    }

    /// `begin_load`, `refresh`, `node`, `node(force)`: the rows for page 1.
    fn single_call(&mut self, form: Form, name: String, force: bool) -> Vec<Observation> {
        let key = if form == Form::Node { Key::Node } else { Key::Feed };
        let forced = form == Form::Refresh || force;
        let now = self.now;
        let (fresh, running) = {
            let window = self.win(key);
            (window.fresh(now), window.load.as_ref().map(|run| run.page))
        };
        if forced && self.online == Some(false) {
            return vec![shapes::tag(shapes::error("offline"), &name)];
        }
        let id = self.add_call(form, &name, Some(key));
        if form == Form::BeginLoad {
            self.handles.insert(name, id);
        }
        match running {
            _ if !forced && fresh => self.finish(id, Ending::Done),
            None => self.start_run(key, 1),
            Some(page) if forced && page > 1 => {
                if let Some(run) = self.run_of(key).take() {
                    self.kill(&run);
                }
                self.start_run(key, 1);
            }
            Some(_) => {}
        }
        Vec::new()
    }

    /// library.md section 4 order (coordinator answer for H1, H2): network, then a running load (the call waits and decides again), then `cursor`.
    fn load_more_call(&mut self, name: String) -> Vec<Observation> {
        if self.online == Some(false) {
            return vec![shapes::tag(shapes::error("offline"), &name)];
        }
        let (cursor, pages, busy) = {
            let window = self.win(Key::Feed);
            (window.cursor.is_some(), window.pages, window.load.is_some())
        };
        let id = self.add_call(Form::LoadMore, &name, Some(Key::Feed));
        if busy {
            self.calls.iter_mut().filter(|call| call.id == id).for_each(|call| call.queued = true);
        } else if !cursor {
            self.finish(id, Ending::More("no_more"));
        } else {
            self.calls.iter_mut().filter(|call| call.id == id).for_each(|call| call.target = Some(pages + 1));
            self.start_run(Key::Feed, pages + 1);
        }
        Vec::new()
    }

    fn load_all_call(&mut self, name: String, force: bool) -> Vec<Observation> {
        let now = self.now;
        let (fresh, cursor, pages, busy) = {
            let window = self.win(Key::Feed);
            (window.fresh(now), window.cursor.is_some(), window.pages, window.load.is_some())
        };
        let id = self.add_call(Form::LoadAll, &name, Some(Key::Feed));
        self.handles.insert(name, id);
        if busy {
            return Vec::new();
        }
        if fresh && !force && !cursor {
            self.finish(id, Ending::Done);
        } else if fresh && !force {
            self.start_run(Key::Feed, pages + 1);
        } else {
            self.start_run(Key::Feed, 1);
        }
        Vec::new()
    }
}
