use serde_json::{json, Map, Value};

use super::ops::Fetch;
use super::shapes;
use super::window::{Key, Outcome};
use super::{LibraryRef, Load, Reads, Stream};
use crate::check::value::Observation;

/// library.md sections 3 and 4: the streams `List`, `Count`, `Entity`, their first value, `pull` and `cancel`. A stream shows one value: the rows, the state of section 3 and nothing else.
impl LibraryRef {
    /// A window has an interest while a call waits on it or a live stream reads it.
    pub(super) fn interest(&self, key: Key) -> bool {
        self.calls.iter().any(|call| call.key == Some(key)) || self.streams.values().any(|stream| stream.live && stream.wants && stream.reads.key() == Some(key))
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

    pub(super) fn open_feed(&mut self, name: String, key: Key, count: bool, on_connect: bool) -> Vec<Observation> {
        self.open_stream(name, Reads::Window { key, count }, on_connect, Load::Stale)
    }

    /// Section 4 `Entity(*)`: the window of the stream is the key `node` of its row, the one of the call `node(id)` (coordinator answer H21).
    pub(super) fn open_entity(&mut self, name: String, fetch: Fetch) -> Vec<Observation> {
        match fetch {
            Fetch::Never => self.open_stream(name, Reads::Row { key: None }, false, Load::None),
            Fetch::Stale => self.open_stream(name, Reads::Row { key: Some(Key::Node) }, false, Load::Stale),
            Fetch::Force => self.open_stream(name, Reads::Row { key: Some(Key::Node) }, false, Load::Force),
            Fetch::IfMissing => {
                let load = if self.row { Load::None } else { Load::Stale };
                self.open_stream(name, Reads::Row { key: Some(Key::Node) }, false, load)
            }
        }
    }

    /// Section 4 "open `List`, `Count`, `Entity(stale)`": no identity ends the stream at once, a fresh window loads nothing, a window that is not fresh loads when no load runs. `Entity(force)` loads when no load runs.
    fn open_stream(&mut self, name: String, reads: Reads, on_connect: bool, load: Load) -> Vec<Observation> {
        if !self.live {
            return vec![shapes::error("not_open")];
        }
        if self.streams.get(&name).is_some_and(|stream| stream.live) {
            self.mark("H23_stream_opened_again_under_the_name_of_a_live_one");
            return Vec::new();
        }
        let ends = !self.identity && !matches!(reads, Reads::Row { key: None });
        let mut stream = Stream { reads, on_connect, wants: load != Load::None, live: !ends, last: None, pending: Vec::new() };
        if ends {
            stream.pending.push(shapes::ended(&name, "session_ended"));
        }
        self.streams.insert(name, stream);
        let Some(key) = reads.key().filter(|_| !ends && load != Load::None) else { return Vec::new() };
        let now = self.now;
        let window = self.win(key);
        let wanted = match load {
            Load::Force => true,
            Load::Stale | Load::None => !window.fresh(now),
        };
        if wanted && window.load.is_none() {
            self.start_run(key, 1);
        }
        Vec::new()
    }

    /// Library.md section 3 as one function: the value of a stream now.
    fn stream_value(&self, name: &str) -> Observation {
        let Some(stream) = self.streams.get(name) else { return Observation::bare("none") };
        let mut fields = Map::new();
        fields.insert("stream".into(), name.into());
        let count = matches!(stream.reads, Reads::Window { count: true, .. });
        let window = stream.reads.key().map(|key| self.windows.get(&key).cloned().unwrap_or_default());
        let rows = match stream.reads {
            Reads::Row { .. } => usize::from(self.row),
            Reads::Window { .. } => window.as_ref().map_or(0, |window| window.members as usize),
        };
        fields.insert("phase".into(), window.as_ref().map_or("loaded", |window| window.phase(self.now, rows > 0)).into());
        if let Some(window) = &window {
            if let Some(error) = window.error {
                fields.insert("error".into(), error.into());
            }
            if window.loaded(self.now) {
                fields.insert("loaded".into(), true.into());
            }
        }
        if count {
            fields.insert("count".into(), rows.into());
        } else {
            let prefix = stream.reads.key().map_or("m", Key::row_prefix);
            fields.insert("rows".into(), Value::Array((1..=rows).map(|index| json!(format!("{prefix}{index}"))).collect()));
        }
        Observation::new(if count { "count" } else { "snapshot" }, Value::Object(fields))
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
        for key in [Key::Feed, Key::Members, Key::Node] {
            let read = self.streams.values().any(|stream| stream.live && stream.wants && stream.reads.key() == Some(key));
            let wanted = self.streams.values().any(|stream| stream.live && stream.wants && stream.reads.key() == Some(key) && stream.on_connect);
            let window = self.win(key);
            if read && window.load.is_none() && (!window.fresh(now) || wanted) {
                starting += 1;
                self.start_run(key, 1);
            }
        }
        starting
    }
}
