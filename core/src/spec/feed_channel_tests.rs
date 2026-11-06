//! Owner-level tests of the channels of `Feed` (feed.md, D-69): the owner on `DetScheduler` with models that read several channels.
//! Names are the `Test` column of the spec. A model is ready once every channel of it is confirmed on the active connection.

use std::collections::BTreeMap;

use crate::owners::contract::{Committed, Online};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::feed::{self as f, Feed, FeedEffect, FeedInput, FeedPeers, FramedEvent, Outgoing};
use crate::owners::read::windows::{StaleKeys, Windows};
use crate::owners::store::StoreWriter;
use crate::owners::support::{new_log, number_after, take, text_after, Discard, Emitted, Log, Sink, Spy, SpyState};
use crate::runtime::{Address, DetScheduler, EpochStamp};

const EPOCH: u64 = 1;
const STORE: usize = 0;
const WINDOWS: usize = 1;
const DELIVERIES: usize = 2;
const CHAT: u64 = 1;
const POST: u64 = 2;
const THIRD: u64 = 3;
/// The first connection the owner opens.
const FIRST: u64 = 1;

fn channels() -> BTreeMap<String, Vec<String>> {
    [("Chat", vec!["A", "B"]), ("Post", vec!["B"]), ("Third", vec!["C"])].into_iter().map(|(model, own)| (model.to_string(), own.into_iter().map(String::from).collect())).collect()
}

struct Rig {
    det: DetScheduler,
    addr: Address<Spy<Feed>>,
    log: Log<Emitted<FeedEffect>>,
    batches: Vec<u64>,
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
        let mut rig = Self { det, addr, log, batches: Vec::new() };
        rig.post(FeedInput::Online(Online(true)));
        rig
    }

    fn state(&self) -> &f::FeedState { &self.det.state(self.addr).expect("the owner state").inner }

    /// Posts one input and shows what the owner gave out, one word per effect.
    fn post(&mut self, input: FeedInput) -> Vec<String> {
        take(&self.log);
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        let emitted = take(&self.log);
        let mut words = Vec::new();
        for entry in emitted {
            match entry {
                Emitted::Run(FeedEffect::Connect { .. }) => words.push("connect".to_string()),
                Emitted::Run(FeedEffect::Seal { frame: Outgoing::Subscribe { channel }, .. }) => words.push(format!("subscribe:{channel}")),
                Emitted::Run(FeedEffect::Seal { frame: Outgoing::Unsubscribe { channel }, .. }) => words.push(format!("unsubscribe:{channel}")),
                Emitted::Run(FeedEffect::Close { .. }) => words.push("close".to_string()),
                Emitted::Schedule { input, .. } if input.starts_with("ChannelResubscribe") => words.push("schedule:channel".to_string()),
                Emitted::Send { to: STORE, input } => {
                    self.batches.push(number_after(&input, "id: ").expect("the batch id"));
                    words.push("commit".to_string());
                }
                Emitted::Send { to: WINDOWS, input } if input.starts_with("Stale {") => words.push(format!("stale:{}", text_after(&input, "model: ").expect("the model"))),
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

    fn want(&mut self, model: &str, id: u64) -> Vec<String> { self.post(FeedInput::WantPlus { model: model.into(), id, stream: true }) }

    fn unwant(&mut self, model: &str, id: u64) -> Vec<String> { self.post(FeedInput::WantMinus { model: model.into(), id }) }

    fn handshake(&mut self) -> Vec<String> { self.post(FeedInput::HandshakeDone { connection_id: FIRST, key_id: 7 }) }

    fn confirm(&mut self, channel: &str) -> Vec<String> { self.post(FeedInput::ChannelConfirmed { connection_id: FIRST, channel: channel.into() }) }

    fn event(&mut self, channel: &str, model: &str, payload: &str) -> Vec<String> {
        self.post(FeedInput::Event { connection_id: FIRST, channel: channel.into(), event: FramedEvent { model: model.into(), payload: payload.into(), reread: StaleKeys::none() } })
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
fn feed_chan_want_new_channel() {
    let mut rig = Rig::live();
    assert_eq!(rig.want("Third", THIRD), ["joined:3", "subscribe:C"], "no new connection, only the new channel");
    assert_eq!(rig.confirm("C"), ["modelready:3"]);
    assert_eq!(rig.state().subscribed(), ["A", "B", "C"]);
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
    assert_eq!(rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), failed: false }), ["schedule:channel"]);
    assert_eq!(rig.state().confirmed(), ["A"], "the channel left the confirmed channels");
    assert_eq!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into() }), ["subscribe:B"]);
    let mut shown = rig.confirm("B");
    shown.sort();
    assert_eq!(shown, ["modelgap:1", "modelgap:2", "stale:Chat", "stale:Post"], "each model of the channel sees one gap after the new confirmation, and no second ready");
}

#[test]
fn feed_chan_ended_error() {
    let mut rig = Rig::live();
    rig.want("Third", THIRD);
    rig.confirm("C");
    assert_eq!(rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "C".into(), failed: true }), ["modelinvalid:3"]);
    assert_eq!(rig.state().wants(), ["Chat", "Post"], "the model of the refused channel left want");
    assert_eq!(rig.state().subscribed(), ["A", "B"]);
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "C".into() }).is_empty(), "no resubscribe after an error of the request");
}

#[test]
fn feed_chan_resubscribe() {
    let mut rig = Rig::live();
    rig.post(FeedInput::ChannelEnded { connection_id: FIRST, channel: "B".into(), failed: false });
    assert_eq!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into() }), ["subscribe:B"]);
}

#[test]
fn feed_chan_resubscribe_confirmed() {
    let mut rig = Rig::live();
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "B".into() }).is_empty());
}

#[test]
fn feed_chan_resubscribe_unwanted() {
    let mut rig = Rig::live();
    assert!(rig.post(FeedInput::ChannelResubscribe { connection_id: FIRST, channel: "C".into() }).is_empty());
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
            let words = rig.post(FeedInput::Committed(Committed { batch: *batch, revision: 1, tables: Vec::new(), models: Default::default() }));
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
