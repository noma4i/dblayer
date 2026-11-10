
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::spec) enum In {
    WantPlus { model: String, id: u64, stream: bool, scoped: Vec<String> },
    WantMinus { model: String, id: u64 },
    Ready,
    ChannelConfirmed(String),
    Event { channel: String, model: String },
    Ended { channel: String, failed: bool },
    Resubscribe(String),
    Online(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::spec) enum Out {
    Connect,
    Joined(u64),
    Subscribe(String),
    Unsubscribe(String),
    ModelReady(u64),
    ModelGap(u64),
    StaleGap { model: String, online_seq: u64 },
    StaleLost { model: String },
    Commit,
    ModelInvalid(u64),
    ScheduleResubscribe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Off,
    Opening,
    Live,
}

pub(in crate::spec) struct Chan {
    mode: Mode,
    online: bool,
    online_seq: u64,
    channels: BTreeMap<String, Vec<String>>,
    pub(in crate::spec) want: BTreeMap<String, BTreeSet<u64>>,
    streams: BTreeSet<u64>,
    scoped: BTreeMap<u64, Vec<String>>,
    wanted: BTreeSet<String>,
    pub(in crate::spec) confirmed: BTreeSet<String>,
    ready: BTreeSet<u64>,
    gap: BTreeMap<String, u64>,
    pub(in crate::spec) position: BTreeMap<String, u64>,
    held: Vec<String>,
}

impl Chan {
    pub(in crate::spec) fn new(channels: BTreeMap<String, Vec<String>>) -> Self {
        Self { mode: Mode::Off, online: false, online_seq: 0, channels, want: BTreeMap::new(), streams: BTreeSet::new(), scoped: BTreeMap::new(), wanted: BTreeSet::new(), confirmed: BTreeSet::new(), ready: BTreeSet::new(), gap: BTreeMap::new(), position: BTreeMap::new(), held: Vec::new() }
    }

    fn channels_of(&self, model: &str) -> Vec<String> {
        let own = self.channels.get(model).cloned().unwrap_or_default();
        let windows = self.want.get(model).into_iter().flatten().filter_map(|id| self.scoped.get(id)).flatten().cloned();
        own.into_iter().chain(windows).collect::<BTreeSet<_>>().into_iter().collect()
    }

    fn streams_of(&self, ids: impl IntoIterator<Item = u64>) -> Vec<u64> { ids.into_iter().filter(|id| self.streams.contains(id)).collect() }

    fn complete(&self, model: &str, confirmed: &BTreeSet<String>) -> bool { self.channels_of(model).iter().all(|channel| confirmed.contains(channel)) }

    fn needed(&self) -> BTreeSet<String> { self.want.keys().flat_map(|model| self.channels_of(model)).collect() }

    fn models_of(&self, channel: &str) -> Vec<String> { self.want.keys().filter(|model| self.channels_of(model).iter().any(|own| own == channel)).cloned().collect() }

    pub(in crate::spec) fn step(&mut self, input: In) -> Option<Vec<Out>> {
        let mut out = Vec::new();
        match (self.mode, input) {
            (_, In::Online(seq)) => {
                self.online = true;
                self.online_seq = seq;
            }
            (mode, In::WantPlus { model, id, stream, scoped }) => {
                self.want.entry(model.clone()).or_default().insert(id);
                if !scoped.is_empty() {
                    self.scoped.insert(id, scoped);
                }
                let fresh: Vec<String> = self.channels_of(&model).into_iter().filter(|channel| !self.wanted.contains(channel)).collect();
                self.wanted.extend(fresh.iter().cloned());
                if stream {
                    self.streams.insert(id);
                    out.push(Out::Joined(id));
                }
                match mode {
                    Mode::Off if self.online => {
                        self.mode = Mode::Opening;
                        out.push(Out::Connect);
                    }
                    Mode::Off | Mode::Opening => {}
                    Mode::Live => {
                        out.extend(fresh.into_iter().map(Out::Subscribe));
                        if stream && self.complete(&model, &self.confirmed) {
                            self.ready.insert(id);
                            out.push(Out::ModelReady(id));
                        }
                    }
                }
            }
            (Mode::Off, In::WantMinus { .. }) => return None,
            (mode, In::WantMinus { model, id }) => {
                let Some(ids) = self.want.get_mut(&model) else { return Some(out) };
                if !ids.remove(&id) {
                    return Some(out);
                }
                self.ready.remove(&id);
                self.streams.remove(&id);
                self.scoped.remove(&id);
                if ids.is_empty() {
                    self.want.remove(&model);
                }
                if self.want.is_empty() {
                    return None;
                }
                let needed = self.needed();
                for channel in self.wanted.clone().difference(&needed) {
                    if mode == Mode::Live {
                        out.push(Out::Unsubscribe(channel.clone()));
                    }
                    self.confirmed.remove(channel);
                }
                self.wanted = needed;
            }
            (Mode::Opening, In::Ready) => {
                self.mode = Mode::Live;
                self.confirmed.clear();
                self.ready.clear();
                out.extend(self.wanted.iter().cloned().map(Out::Subscribe));
            }
            (Mode::Live, In::ChannelConfirmed(channel)) => {
                if !self.wanted.contains(&channel) || self.confirmed.contains(&channel) {
                    return Some(out);
                }
                let before = self.confirmed.clone();
                self.confirmed.insert(channel);
                for model in self.want.keys().cloned().collect::<Vec<_>>() {
                    if self.complete(&model, &before) || !self.complete(&model, &self.confirmed) {
                        continue;
                    }
                    let ids: Vec<u64> = self.streams_of(self.want[&model].iter().copied());
                    for id in &ids {
                        if self.ready.insert(*id) {
                            out.push(Out::ModelReady(*id));
                        }
                    }
                    if let Some(gap_seq) = self.gap.remove(&model) {
                        out.extend(ids.iter().map(|id| Out::ModelGap(*id)));
                        out.push(if self.online_seq > gap_seq { Out::StaleGap { model: model.clone(), online_seq: self.online_seq } } else { Out::StaleLost { model: model.clone() } });
                    }
                    let held = self.held.iter().filter(|held| **held == model).count();
                    self.held.retain(|held| *held != model);
                    out.extend((0..held).map(|_| Out::Commit));
                }
            }
            (Mode::Live | Mode::Opening, In::Event { channel, model }) => {
                if !self.want.contains_key(&model) || !self.wanted.contains(&channel) {
                    return Some(out);
                }
                *self.position.entry(model.clone()).or_default() += 1;
                if self.mode == Mode::Live && self.complete(&model, &self.confirmed) {
                    out.push(Out::Commit);
                } else {
                    self.held.push(model);
                }
            }
            (Mode::Live, In::Ended { channel, failed: false }) => {
                if self.wanted.contains(&channel) {
                    self.confirmed.remove(&channel);
                    let models = self.models_of(&channel);
                    for model in models { self.gap.insert(model, self.online_seq); }
                    out.push(Out::ScheduleResubscribe);
                }
            }
            (Mode::Live, In::Ended { channel, failed: true }) => {
                for model in self.models_of(&channel) {
                    let ids = self.want.remove(&model).unwrap_or_default();
                    out.extend(self.streams_of(ids.iter().copied()).into_iter().map(Out::ModelInvalid));
                    for id in &ids {
                        self.scoped.remove(id);
                        self.streams.remove(id);
                    }
                }
                let needed = self.needed();
                self.confirmed.retain(|channel| needed.contains(channel));
                self.wanted = needed;
                if self.want.is_empty() {
                    return None;
                }
            }
            (Mode::Live, In::Resubscribe(channel)) => {
                if self.wanted.contains(&channel) && !self.confirmed.contains(&channel) {
                    out.push(Out::Subscribe(channel));
                }
            }
            _ => return None,
        }
        Some(out)
    }
}
