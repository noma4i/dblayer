//! Owner-level tests of the channels of `Feed` (feed.md, D-69): the owner on `DetScheduler` with models that read several channels.
//! Names are the `Test` column of the spec. A model is ready once every channel of it is confirmed on the active connection.

use std::collections::{BTreeMap, BTreeSet};

use crate::test_wait::orders_of;

use crate::owners::contract::{Committed, Online};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::feed::{self as f, Feed, FeedEffect, FeedInput, FeedPeers, FramedEvent, Outgoing, ScopedChannel};
use crate::owners::read::windows::{StaleKeys, Windows};
use crate::owners::store::StoreWriter;
use crate::spec::support::{new_log, number_after, take, text_after, Discard, Emitted, Log, Sink, Spy, SpyState};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use super::ref_feed::ref_feed_channels::{Chan, In, Out};

const EPOCH: u64 = 1;
const STORE: usize = 0;
const WINDOWS: usize = 1;
const DELIVERIES: usize = 2;
const CHAT: u64 = 1;
const POST: u64 = 2;
const THIRD: u64 = 3;
const SCREEN: u64 = 4;
/// The first connection the owner opens.
const FIRST: u64 = 1;

fn channels() -> BTreeMap<String, Vec<String>> {
    [("Chat", vec!["A", "B"]), ("Post", vec!["B"]), ("Third", vec!["C"]), ("Screen", vec![])].into_iter().map(|(model, own)| (model.to_string(), own.into_iter().map(String::from).collect())).collect()
}

fn word(effect: Out) -> String {
    match effect {
        Out::Connect => "connect".into(),
        Out::Joined(stream) => format!("joined:{stream}"),
        Out::Subscribe(channel) => format!("subscribe:{channel}"),
        Out::Unsubscribe(channel) => format!("unsubscribe:{channel}"),
        Out::ModelReady(stream) => format!("modelready:{stream}"),
        Out::ModelGap(stream) => format!("modelgap:{stream}"),
        Out::StaleGap { model, online_seq } => format!("stale:{model}:gap{online_seq}"),
        Out::Commit => "commit".into(),
        Out::ModelInvalid(stream) => format!("modelinvalid:{stream}"),
        Out::ScheduleResubscribe => "schedule:channel".into(),
    }
}

struct Rig {
    det: DetScheduler,
    addr: Address<Spy<Feed>>,
    log: Log<Emitted<FeedEffect>>,
    batches: Vec<u64>,
    subscribes: Vec<Outgoing>,
    reference: Chan,
}

impl Rig {
    fn new() -> Self {
        let mut det = DetScheduler::new(4);
        let log = new_log();
        det.add::<Sink<StoreWriter>>("store", new_log(), Discard);
        det.add::<Sink<Windows>>("windows", new_log(), Discard);
        det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
        let peers = FeedPeers { store: Address::<StoreWriter>::new(STORE), windows: Address::<Windows>::new(WINDOWS), deliveries: Address::<Deliveries>::new(DELIVERIES) };
        let addr = det.add::<Spy<Feed>>("feed", SpyState::new(f::FeedState::new(EPOCH, peers, BTreeMap::new(), channels()), &log), Discard);
        det.start_epoch(EPOCH);
        let mut rig = Self { det, addr, log, batches: Vec::new(), subscribes: Vec::new(), reference: Chan::new(channels()) };
        rig.post(FeedInput::Online { online: Online(true), online_seq: 1, now: crate::runtime::Time(0) });
        rig
    }

    fn state(&self) -> &f::FeedState { &self.det.state(self.addr).expect("the owner state").inner }

    /// Posts one input and shows what the owner gave out, one word per effect.
    fn post(&mut self, input: FeedInput) -> Vec<String> {
        let row = self.row_of(&input);
        self.post_row(input, row)
    }

    fn row_of(&self, input: &FeedInput) -> Option<In> {
        match input {
            FeedInput::WantPlus { model, id, stream, .. } => Some(In::WantPlus { model: model.clone(), id: *id, stream: *stream, scoped: Vec::new() }),
            FeedInput::WantMinus { model, id, .. } => Some(In::WantMinus { model: model.clone(), id: *id }),
            FeedInput::HandshakeDone { .. } => Some(In::Ready),
            FeedInput::ChannelConfirmed { channel, .. } => Some(In::ChannelConfirmed(channel.clone())),
            FeedInput::Event { channel, event, .. } => Some(In::Event { channel: channel.clone(), model: event.model.clone() }),
            FeedInput::ChannelEnded { channel, error, .. } => Some(In::Ended { channel: channel.clone(), failed: error.is_some_and(|error| !error.retryable) }),
            FeedInput::ChannelResubscribe { channel, .. } => Some(In::Resubscribe(channel.clone())),
            FeedInput::Online { online_seq, .. } => Some(In::Online(*online_seq)),
            _ => None,
        }
    }

    fn post_row(&mut self, input: FeedInput, row: Option<In>) -> Vec<String> {
        let want = row.and_then(|row| self.reference.step(row)).map(|effects| effects.into_iter().map(word).collect::<Vec<_>>());
        let got = self.owner_post(input);
        if let Some(mut want) = want {
            let mut seen = got.clone();
            seen.sort();
            want.sort();
            assert_eq!(seen, want, "the owner and the reference give the same effects of the row");
            assert_eq!(self.state().confirmed(), self.reference.confirmed.iter().cloned().collect::<Vec<_>>(), "the same confirmed channels");
            for model in ["Chat", "Post", "Third"] {
                assert_eq!(self.state().position(model), self.reference.position.get(model).copied().unwrap_or(0), "the same position of {model}");
            }
        }
        got
    }

    fn owner_post(&mut self, input: FeedInput) -> Vec<String> {
        take(&self.log);
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        let emitted = take(&self.log);
        let mut words = Vec::new();
        for entry in emitted {
            match entry {
                Emitted::Run(FeedEffect::Connect { .. }) => words.push("connect".to_string()),
                Emitted::Run(FeedEffect::Seal { frame: Outgoing::Subscribe { channel, variables }, .. }) => {
                    words.push(format!("subscribe:{channel}"));
                    self.subscribes.push(Outgoing::Subscribe { channel, variables });
                }
                Emitted::Run(FeedEffect::Seal { frame: Outgoing::Unsubscribe { channel }, .. }) => words.push(format!("unsubscribe:{channel}")),
                Emitted::Run(FeedEffect::Close { .. }) => words.push("close".to_string()),
                Emitted::Schedule { input, .. } if input.starts_with("ChannelResubscribe") => words.push("schedule:channel".to_string()),
                Emitted::Send { to: STORE, input } => {
                    self.batches.push(number_after(&input, "id: ").expect("the batch id"));
                    words.push("commit".to_string());
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Stale {") => words.push(format!("stale:{}{}", text_after(&input, "model: ").expect("the model"), number_after(&input, "online_seq: ").map_or(String::new(), |seq| format!(":gap{seq}")))),
                Emitted::Send { to: DELIVERIES, input } => {
                    let stream = number_after(&input, "stream: ").expect("the stream");
                    let kind = input.split([' ', '{']).next().unwrap_or("");
                    words.push(format!("{}:{stream}", kind.to_lowercase()));
                }
                _ => {}
            }
        }
        words
    }

    fn want(&mut self, model: &str, id: u64) -> Vec<String> { self.post(FeedInput::WantPlus { model: model.into(), id, stream: true, scoped: Vec::new(), now: crate::runtime::Time(0) }) }

    fn want_window(&mut self, id: u64, slug: &str, channel: &str) -> Vec<String> {
        let variables = serde_json::Map::from_iter([("slug".to_string(), serde_json::json!(slug))]);
        let input = FeedInput::WantPlus { model: "Screen".into(), id, stream: false, scoped: vec![ScopedChannel { subscription: "S".into(), variables }], now: crate::runtime::Time(0) };
        self.post_row(input, Some(In::WantPlus { model: "Screen".into(), id, stream: false, scoped: vec![channel.to_string()] }))
    }

    fn unwant(&mut self, model: &str, id: u64) -> Vec<String> { self.post(FeedInput::WantMinus { model: model.into(), id, now: crate::runtime::Time(0) }) }

    fn handshake(&mut self) -> Vec<String> { self.post(FeedInput::HandshakeDone { connection_id: FIRST, key_id: 7, now: crate::runtime::Time(0) }) }

    fn confirm(&mut self, channel: &str) -> Vec<String> { self.post(FeedInput::ChannelConfirmed { connection_id: FIRST, channel: channel.into() }) }

    fn end(&mut self, channel: &str) -> Vec<String> { self.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: channel.into(), error: None, now: crate::runtime::Time(0) }) }

    fn event(&mut self, channel: &str, model: &str, payload: &str) -> Vec<String> {
        self.post(FeedInput::Event { connection_id: FIRST, channel: channel.into(), event: FramedEvent { model: model.into(), payload: payload.into(), reread: StaleKeys::none(), refused: false } })
    }

    fn refused_event(&mut self, channel: &str, model: &str) -> Vec<String> {
        self.owner_post(FeedInput::Event { connection_id: FIRST, channel: channel.into(), event: FramedEvent { model: model.into(), payload: "refused".into(), reread: StaleKeys::none(), refused: true } })
    }

    /// Chat and Post are wanted, the connection is open and every channel of them is confirmed.
    fn live() -> Self {
        let mut rig = Self::new();
        rig.want("Chat", CHAT);
        rig.want("Post", POST);
        rig.handshake();
        rig.confirm("A");
        rig.confirm("B");
        rig
    }
}

#[test]
fn feed_chan_ready_subscribes_all() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.want("Post", POST);
    assert_eq!(rig.handshake(), ["subscribe:A", "subscribe:B"], "one subscribe per channel, none per model, no ready before a confirmation");
    assert_eq!(rig.state().confirmed(), Vec::<String>::new());
}

#[test]
fn feed_chan_confirmed_ready() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.want("Post", POST);
    rig.handshake();
    assert_eq!(rig.confirm("B"), ["modelready:2"], "Post reads only B: ready, Chat still waits for A");
    assert_eq!(rig.confirm("A"), ["modelready:1"]);
}

#[test]
fn feed_chan_confirmed_partial() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.handshake();
    assert!(rig.confirm("A").is_empty(), "Chat waits for B");
    assert!(rig.confirm("A").is_empty(), "a repeated confirmation changes nothing");
}

#[test]
fn feed_chan_event_routed() {
    let mut rig = Rig::live();
    assert_eq!(rig.event("A", "Chat", "c1"), ["commit"]);
    assert_eq!(rig.event("B", "Chat", "c2"), ["commit"]);
    assert_eq!(rig.state().position("Chat"), 2, "events of two channels share the positions of the model");
}

#[test]
fn feed_live_rejected_invalid() {
    use crate::owners::contract::{RejectReason, Rejected};
    let mut rig = Rig::live();
    assert_eq!(rig.event("A", "Chat", "first"), ["commit"]);
    assert_eq!(rig.event("A", "Chat", "second"), ["commit"]);
    let first = rig.batches[0];
    let second = rig.batches[1];
    assert_eq!(rig.post(FeedInput::Rejected(Rejected { batch: second, reason: RejectReason::Invalid })), ["stale:Chat"]);
    let emitted = rig.post(FeedInput::Committed(Committed { batch: first, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert_eq!(emitted, ["modelevent:1", "modellost:1"]);
    assert_eq!(rig.state().mode(), f::Mode::Live);
}

#[test]
fn feed_chan_event_unwanted() {
    let mut rig = Rig::live();
    assert!(rig.event("A", "Third", "t1").is_empty(), "a model that is not wanted gets no position");
    assert!(rig.event("Z", "Chat", "c1").is_empty(), "a frame of a channel the connection never subscribed is not for Feed");
    assert_eq!((rig.state().position("Third"), rig.state().position("Chat")), (0, 0));
}

#[test]
fn feed_opening_event_unwatched() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    assert!(rig.event("A", "Post", "p1").is_empty());
    assert_eq!(rig.state().position("Post"), 0);
}

#[test]
fn feed_proto_handshake_deadline_after_done() {
    let mut rig = Rig::live();
    assert!(rig.post(FeedInput::HandshakeDeadline { connection_id: FIRST }).is_empty());
    assert_eq!(rig.state().active_connection(), Some(FIRST));
}

#[test]
fn feed_chan_event_before_confirm() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.handshake();
    assert!(rig.event("A", "Chat", "early").is_empty(), "the event waits until the model is ready");
    assert_eq!((rig.state().held(), rig.state().position("Chat")), (1, 1), "its position is fixed when it arrives");
    rig.confirm("A");
    assert_eq!(rig.confirm("B"), ["modelready:1", "commit"], "the held event lands after the model is ready");
    assert_eq!(rig.state().held(), 0);
}

#[test]
fn feed_changes_refused_held() {
    for handshaken in [false, true] {
        let mut rig = Rig::new();
        rig.owner_post(FeedInput::WantPlus { model: "Chat".into(), id: CHAT, stream: true, scoped: Vec::new(), now: crate::runtime::Time(0) });
        if handshaken {
            rig.owner_post(FeedInput::HandshakeDone { connection_id: FIRST, key_id: 7, now: crate::runtime::Time(0) });
        }
        assert!(rig.refused_event("A", "Chat").is_empty(), "handshaken {handshaken}: nothing before the model is ready");
        assert_eq!((rig.state().held(), rig.state().position("Chat"), rig.state().refused()), (1, 1, 0), "handshaken {handshaken}: held with its position, not counted yet");
        if !handshaken {
            rig.owner_post(FeedInput::HandshakeDone { connection_id: FIRST, key_id: 7, now: crate::runtime::Time(0) });
        }
        rig.owner_post(FeedInput::ChannelConfirmed { connection_id: FIRST, channel: "A".into() });
        let seat = rig.owner_post(FeedInput::ChannelConfirmed { connection_id: FIRST, channel: "B".into() });
        assert_eq!(seat, ["modelready:1", "modellost:1"], "handshaken {handshaken}: the seat gives Lost and no commit");
        assert_eq!((rig.state().held(), rig.state().landing(), rig.state().refused()), (0, 0, 1), "handshaken {handshaken}");
    }
}

#[test]
fn feed_chan_want_new_channel() {
    let mut rig = Rig::live();
    assert_eq!(rig.want("Third", THIRD), ["joined:3", "subscribe:C"], "no new connection, only the new channel");
    assert_eq!(rig.confirm("C"), ["modelready:3"]);
    assert_eq!(rig.state().subscribed(), ["A", "B", "C"]);
}

const HOME: &str = "S-1";
const ABOUT: &str = "S-2";

#[test]
fn feed_chan_scope_variables() {
    let mut rig = Rig::new();
    assert_eq!(rig.want_window(SCREEN, "home", HOME), ["connect"], "the first window opens the connection, the subscribe waits for it");
    assert_eq!(rig.state().wanted().into_iter().collect::<Vec<_>>(), [HOME], "the channel of the window is its subscription with a number of the owner for the variables");
    assert_eq!(rig.handshake(), [format!("subscribe:{HOME}")], "the frame subscribes the channel of the window");
    let variables = serde_json::Map::from_iter([("slug".to_string(), serde_json::json!("home"))]);
    assert_eq!(rig.subscribes, [Outgoing::Subscribe { channel: HOME.into(), variables: Some(variables) }], "the frame carries the variables of the window");
    assert!(rig.confirm(HOME).is_empty(), "a window is no stream: it gets no ready");
    assert_eq!(rig.event(HOME, "Screen", "e1"), ["commit"], "the event of the channel takes the position of its model");
    assert_eq!(rig.state().position("Screen"), 1);
}

#[test]
fn feed_chan_scope_variables_shared() {
    let mut rig = Rig::new();
    rig.want_window(SCREEN, "home", HOME);
    rig.handshake();
    rig.confirm(HOME);
    assert!(rig.want_window(SCREEN + 1, "home", HOME).is_empty(), "equal subscription and variables are one channel: no second subscribe");
    assert_eq!(rig.want_window(SCREEN + 2, "about", ABOUT), [format!("subscribe:{ABOUT}")], "other variables are another channel");
    assert_eq!(rig.state().subscribed(), [HOME, ABOUT]);
}

#[test]
fn feed_chan_scope_variables_want_minus() {
    let mut rig = Rig::new();
    rig.want_window(SCREEN, "home", HOME);
    rig.handshake();
    rig.confirm(HOME);
    rig.want_window(SCREEN + 1, "home", HOME);
    rig.want_window(SCREEN + 2, "about", ABOUT);
    rig.confirm(ABOUT);
    assert!(rig.unwant("Screen", SCREEN).is_empty(), "the other window of the channel still wants it");
    assert_eq!(rig.unwant("Screen", SCREEN + 1), [format!("unsubscribe:{HOME}")], "the last window of the channel takes only its channel away");
    assert_eq!((rig.state().subscribed(), rig.state().confirmed()), ([ABOUT].map(String::from).to_vec(), [ABOUT].map(String::from).to_vec()));
}

#[test]
fn feed_chan_want_confirmed() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.handshake();
    rig.confirm("A");
    rig.confirm("B");
    assert_eq!(rig.want("Post", POST), ["joined:2", "modelready:2"], "B is confirmed already: ready at once, no subscribe");
}

#[test]
fn feed_chan_want_pending_channels() {
    let mut rig = Rig::new();
    rig.want("Chat", CHAT);
    rig.handshake();
    rig.confirm("A");
    assert_eq!(rig.want("Post", POST), ["joined:2"]);
    let mut ready = rig.confirm("B");
    ready.sort();
    assert_eq!(ready, ["modelready:1", "modelready:2"]);
}

#[test]
fn feed_chan_confirmed_unwanted() {
    let mut rig = Rig::live();
    rig.want("Third", THIRD);
    assert_eq!(rig.unwant("Third", THIRD), ["unsubscribe:C"]);
    assert!(rig.confirm("C").is_empty());
    assert_eq!(rig.state().confirmed(), ["A", "B"]);
}

#[test]
fn feed_chan_unwant_last_user() {
    let mut rig = Rig::live();
    rig.want("Third", THIRD);
    rig.confirm("C");
    assert_eq!(rig.unwant("Third", THIRD), ["unsubscribe:C"]);
    assert_eq!((rig.state().subscribed(), rig.state().confirmed()), (["A", "B"].map(String::from).to_vec(), ["A", "B"].map(String::from).to_vec()));
    assert_eq!(rig.unwant("Post", POST), Vec::<String>::new(), "B is still read by Chat");
}

#[test]
fn feed_chan_ended_resubscribes() {
    let mut rig = Rig::live();
    assert_eq!(rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), error: None, now: crate::runtime::Time(0) }), ["schedule:channel"]);
    assert_eq!(rig.state().confirmed(), ["A"], "the channel left the confirmed channels");
    assert_eq!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into(), now: crate::runtime::Time(0) }), ["subscribe:B"]);
    let mut shown = rig.confirm("B");
    shown.sort();
    assert_eq!(shown, ["modelgap:1", "modelgap:2", "stale:Chat:gap1", "stale:Post:gap1"], "each model of the channel sees one gap after the new confirmation, its windows turn Stale with the cause Gap of the Online(true) that began the subscription, and no second ready");
}

#[test]
fn feed_chan_gap_cause_names_last_online() {
    let mut rig = Rig::live();
    rig.post(FeedInput::Online { online: Online(true), online_seq: 2, now: crate::runtime::Time(0) });
    rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), error: None, now: crate::runtime::Time(0) });
    rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into(), now: crate::runtime::Time(0) });
    let mut shown = rig.confirm("B");
    shown.sort();
    assert_eq!(shown, ["modelgap:1", "modelgap:2", "stale:Chat:gap2", "stale:Post:gap2"]);
}

#[test]
fn feed_chan_ended_error() {
    let mut rig = Rig::live();
    rig.want("Third", THIRD);
    rig.confirm("C");
    assert_eq!(rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "C".into(), error: Some(crate::owners::read::feed::ChannelError { retryable: false }), now: crate::runtime::Time(0) }), ["modelinvalid:3"]);
    assert_eq!(rig.state().wants(), ["Chat", "Post"], "the model of the refused channel left want");
    assert_eq!(rig.state().subscribed(), ["A", "B"]);
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "C".into(), now: crate::runtime::Time(0) }).is_empty(), "no resubscribe after an error of the request");
}

#[test]
fn feed_chan_resubscribe() {
    let mut rig = Rig::live();
    rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), error: None, now: crate::runtime::Time(0) });
    assert_eq!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into(), now: crate::runtime::Time(0) }), ["subscribe:B"]);
}

#[test]
fn feed_chan_resubscribe_confirmed() {
    let mut rig = Rig::live();
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into(), now: crate::runtime::Time(0) }).is_empty());
}

#[test]
fn feed_chan_resubscribe_unwanted() {
    let mut rig = Rig::live();
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "C".into(), now: crate::runtime::Time(0) }).is_empty());
}

#[test]
fn feed_live_graphql_error() {
    let mut rig = Rig::live();
    let mut ended = rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), error: Some(crate::owners::read::feed::ChannelError { retryable: false }), now: crate::runtime::Time(0) });
    ended.sort();
    assert_eq!(ended.iter().filter(|event| event.starts_with("modelinvalid:")).cloned().collect::<Vec<_>>(), ["modelinvalid:1", "modelinvalid:2"]);
    assert!(rig.state().wants().is_empty());
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into(), now: crate::runtime::Time(0) }).is_empty());
}

/// P-FEED-21: every channel of `wanted` gets exactly one subscribe on the connection, a channel outside it none, and without a wanted model no connection opens.
#[test]
fn feed_prop_chan_one_subscribe() {
    let quiet = Rig::new();
    assert!(quiet.state().wanted().is_empty() && quiet.state().active_connection().is_none() && quiet.state().opening_connection().is_none());
    for models in [vec![("Chat", CHAT)], vec![("Post", POST)], vec![("Chat", CHAT), ("Post", POST)], vec![("Chat", CHAT), ("Post", POST), ("Third", THIRD)]] {
        let mut rig = Rig::new();
        for (model, id) in &models {
            rig.want(model, *id);
        }
        let mut sent = rig.handshake();
        sent.sort();
        let wanted: Vec<String> = rig.state().wanted().into_iter().map(|channel| format!("subscribe:{channel}")).collect();
        assert_eq!(sent, wanted, "one subscribe per wanted channel");
    }
}

/// P-FEED-22: the events of every channel of a model take the positions of one ledger, whatever the order of the confirmations and of the landings.
#[test]
fn feed_prop_chan_one_ledger() {
    for order in [["A", "B"], ["B", "A"]] {
        let mut rig = Rig::new();
        rig.want("Chat", CHAT);
        rig.handshake();
        for channel in order {
            rig.confirm(channel);
        }
        for (channel, payload) in [("A", "e1"), ("B", "e2"), ("A", "e3")] {
            rig.event(channel, "Chat", payload);
        }
        assert_eq!(rig.state().position("Chat"), 3);
        let batches = std::mem::take(&mut rig.batches);
        assert_eq!(batches.len(), 3);
        let mut delivered = Vec::new();
        for batch in batches.iter().rev() {
            let words = rig.post(FeedInput::Committed(Committed { batch: *batch, revision: 1, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
            delivered.extend(words.into_iter().filter(|word| word.starts_with("modelevent")));
        }
        assert_eq!(delivered.len(), 3, "every event reaches the stream once, in one numbering");
    }
}

/// P-FEED-23: `ModelReady` of a model comes only after all its channels are confirmed on the active connection, exactly once per connection.
#[test]
fn feed_prop_chan_ready_after_all() {
    for order in [["A", "B"], ["B", "A"]] {
        let mut rig = Rig::new();
        rig.want("Chat", CHAT);
        rig.handshake();
        let first = rig.confirm(order[0]);
        let second = rig.confirm(order[1]);
        assert!(first.is_empty(), "not ready after one of two channels: {first:?}");
        assert_eq!(second, ["modelready:1"]);
        assert!(rig.confirm(order[1]).is_empty(), "a repeated confirmation is no second ready");
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Recovery {
    EndA,
    EndB,
    ConfirmA,
    ConfirmB,
    EventA,
    WantSecond,
}

/// P-FEED-23 (D-191): after the loss of channels the model is ready again only when all its channels are confirmed. Until then an event is held and a new reader gets no `ModelReady`. At that step the gap is given once, the held event commits and the new reader is ready once.
#[test]
fn feed_chan_want_during_recovery() {
    use Recovery::*;
    let orders = orders_of(&[EndA, EndB, ConfirmA, ConfirmB, EventA, WantSecond], &[(EndA, ConfirmA), (EndB, ConfirmB)]);
    assert_eq!(orders.len(), 180);
    for order in orders {
        let mut rig = Rig::new();
        rig.want("Chat", CHAT);
        rig.handshake();
        rig.confirm("A");
        rig.confirm("B");
        let (mut down, mut held, mut waiting, mut second) = (BTreeSet::new(), 0usize, false, 0usize);
        for step in &order {
            let before = down.is_empty();
            let words = match step {
                EndA => { down.insert("A"); rig.end("A") }
                EndB => { down.insert("B"); rig.end("B") }
                ConfirmA => { down.remove("A"); rig.confirm("A") }
                ConfirmB => { down.remove("B"); rig.confirm("B") }
                EventA => { if !before { held += 1; } rig.event("A", "Chat", "e") }
                WantSecond => { waiting = true; rig.want("Chat", POST) }
            };
            let after = down.is_empty();
            let recovered = !before && after;
            let commits = words.iter().filter(|word| *word == "commit").count();
            let gaps = words.iter().filter(|word| word.starts_with("modelgap")).count();
            let readied = words.iter().filter(|word| *word == &format!("modelready:{POST}")).count();
            let expected_commits = usize::from(*step == EventA && before) + if recovered { std::mem::take(&mut held) } else { 0 };
            assert_eq!(commits, expected_commits, "commits at {step:?} in {order:?}");
            assert_eq!(gaps > 0, recovered, "the gap comes once the last channel is confirmed, at {step:?} in {order:?}: {words:?}");
            if recovered { assert!(words.iter().position(|word| word.starts_with("modelgap")) < words.iter().position(|word| word == "commit") || commits == 0, "the gap comes before the held event in {order:?}: {words:?}"); }
            let expected_ready = usize::from(waiting && after);
            assert_eq!(readied, expected_ready, "the new reader is ready once and only when all channels are confirmed, at {step:?} in {order:?}");
            second += readied;
            if readied > 0 { waiting = false; }
            assert!(!words.contains(&format!("modelready:{CHAT}")), "the first reader is not ready twice in {order:?}");
        }
        assert_eq!(second, 1, "the new reader gets one ready in {order:?}");
    }
}
