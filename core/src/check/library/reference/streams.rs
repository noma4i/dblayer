use serde_json::{json, Map, Value};

use super::ops::Fetch;
use super::shapes;
use super::window::{Key, Outcome, Window};
use super::{LibraryRef, Reads, Stream};
use crate::check::value::Observation;

/// library.md sections 3 and 4: the streams `List`, `Count`, `Entity`, their first value, `pull` and `cancel`. A stream shows one value: the rows, the state of section 3 and nothing else.
impl LibraryRef {
    /// A window has an interest while a call waits on it or a live stream reads it.
    pub(super) fn interest(&self, key: Key) -> bool {
        self.calls.iter().any(|call| call.key == Some(key)) || self.streams.values().any(|stream| stream.live && stream.reads.key() == Some(key))
    }

    /// The last interest went away (section 3 `phase`): the request is taken back, the window is not fresh any more and its phase follows the rows.
    pub(super) fn leave(&mut self, key: Key) {
        if self.interest(key) {
            return;
        }
        if let Some(run) = self.run_of(key).take() {
            self.kill(&run);
            self.win(key).unfresh = true;
        }
        self.win(key).outcome = Some(Outcome::Left);
    }

    pub(super) fn open_feed(&mut self, name: String, count: bool, on_connect: bool) -> Vec<Observation> {
        self.open_stream(name, Reads::Window { key: Key::Feed, count }, on_connect)
    }

    pub(super) fn open_entity(&mut self, name: String, fetch: Fetch) -> Vec<Observation> {
        if fetch != Fetch::Never {
            self.mark("H21_entity_stream_with_a_fetch_other_than_never");
            return Vec::new();
        }
        self.open_stream(name, Reads::Row, false)
    }

    /// Section 4 "open `List`, `Count`": no identity ends the stream at once, a fresh window loads nothing, a window that is not fresh loads when no load runs.
    fn open_stream(&mut self, name: String, reads: Reads, on_connect: bool) -> Vec<Observation> {
        if !self.live {
            return vec![shapes::error("not_open")];
        }
        if self.streams.get(&name).is_some_and(|stream| stream.live) {
            self.mark("H23_stream_opened_again_under_the_name_of_a_live_one");
            return Vec::new();
        }
        let mut stream = Stream { reads, on_connect, live: self.identity, last: None, pending: Vec::new() };
        if !self.identity {
            stream.pending.push(shapes::ended(&name, "session_ended"));
        }
        self.streams.insert(name, stream);
        let Some(key) = reads.key().filter(|_| self.identity) else { return Vec::new() };
        let now = self.now;
        let window = self.win(key);
        if !window.fresh(now) && window.load.is_none() {
            self.start_run(key, 1);
        }
        Vec::new()
    }

    /// Library.md section 3 as one function: the value of a stream now.
    fn stream_value(&self, name: &str) -> Observation {
        let Some(stream) = self.streams.get(name) else { return Observation::bare("none") };
        let mut fields = Map::new();
        fields.insert("stream".into(), name.into());
        match stream.reads {
            Reads::Row => {
                fields.insert("phase".into(), "loaded".into());
                fields.insert("rows".into(), json!(if self.row { vec!["m1"] } else { Vec::new() }));
                Observation::new("snapshot", Value::Object(fields))
            }
            Reads::Window { key, count } => {
                let window = self.windows.get(&key).cloned().unwrap_or_default();
                self.window_value(&window, count, &mut fields);
                Observation::new(if count { "count" } else { "snapshot" }, Value::Object(fields))
            }
        }
    }

    fn window_value(&self, window: &Window, count: bool, fields: &mut Map<String, Value>) {
        fields.insert("phase".into(), window.phase(self.now).into());
        if let Some(error) = window.error {
            fields.insert("error".into(), error.into());
        }
        if window.loaded(self.now) {
            fields.insert("loaded".into(), true.into());
        }
        if count {
            fields.insert("count".into(), window.members.into());
        } else {
            fields.insert("rows".into(), Value::Array((1..=window.members).map(|index| json!(format!("m{index}"))).collect()));
        }
    }

    /// The reader of the stream gets its last value and then `Ended(cause)` once.
    fn end_stream(&mut self, name: &str, cause: &'static str) {
        if !self.streams.get(name).is_some_and(|stream| stream.live) {
            return;
        }
        let value = self.stream_value(name);
        let Some(stream) = self.streams.get_mut(name) else { return };
        if stream.last.as_ref() != Some(&value) {
            stream.pending.push(value.clone());
            stream.last = Some(value);
        }
        stream.pending.push(shapes::ended(name, cause));
        stream.live = false;
    }

    pub(super) fn end_streams(&mut self, cause: &'static str) {
        let names: Vec<String> = self.streams.keys().cloned().collect();
        names.iter().for_each(|name| self.end_stream(name, cause));
    }

    /// Section 4 `cancel` of a stream: the reader gets `Ended(Cancelled)`, the last interest takes the request back.
    pub(super) fn cancel_stream(&mut self, name: &str) {
        let reads = self.streams.get(name).filter(|stream| stream.live).map(|stream| stream.reads);
        self.end_stream(name, "cancelled");
        if let Some(key) = reads.and_then(Reads::key) {
            self.leave(key);
        }
    }

    pub(super) fn pull(&mut self, name: &str) -> Vec<Observation> {
        if !self.live {
            return vec![shapes::error("not_open")];
        }
        let value = self.stream_value(name);
        let Some(stream) = self.streams.get_mut(name) else { return vec![shapes::error("unknown_stream")] };
        if !stream.pending.is_empty() {
            return std::mem::take(&mut stream.pending);
        }
        if !stream.live || stream.last.as_ref() == Some(&value) {
            return vec![shapes::empty(name)];
        }
        stream.last = Some(value.clone());
        vec![value]
    }

    /// Section 7 `Online(true)`: a window with a reader that is not fresh, or declared with a reread on connect, loads once.
    pub(super) fn reread_on_connect(&mut self) -> usize {
        let now = self.now;
        let mut starting = 0;
        for key in [Key::Feed] {
            let read = self.streams.values().any(|stream| stream.live && stream.reads.key() == Some(key));
            let wanted = self.streams.values().any(|stream| stream.live && stream.reads.key() == Some(key) && stream.on_connect);
            let window = self.win(key);
            if read && window.load.is_none() && (!window.fresh(now) || wanted) {
                starting += 1;
                self.start_run(key, 1);
            }
        }
        starting
    }
}
