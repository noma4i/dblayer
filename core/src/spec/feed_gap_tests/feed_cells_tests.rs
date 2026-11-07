use super::*;


#[test]
fn feed_created() {
    let mut twin = Twin::with(None, true);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.state().online(), None, "online is unknown until the first Online");
    twin.post(FeedInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(7), generation: 4 }));
    let requests = take(&twin.log);
    assert!(requests.iter().any(|entry| matches!(entry, Emitted::Run(FeedEffect::SnapshotRequest { epoch: EPOCH }))), "the step of EpochStarted asks Network for the snapshot of Feed: {requests:?}");
    twin.post(FeedInput::Online { online: Online(true), online_seq: 1 });
    twin.reference.online = Some(true);
    twin.step(plus("Chat"));
    twin.step(Input::HandshakeDone);
    take(&twin.log);
    twin.step(chat("c1"));
    twin.step(Input::Tick);
    let sent = &twin.history;
    assert!(sent.iter().any(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.contains("basis: {\"Chat\": 4}"))), "the basis of an event is the generation of the epoch: {sent:?}");
}

#[test]
fn feed_off_want() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT), Effect::Connect]);
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_off_want_offline() {
    for online in [Some(false), None] {
        let mut twin = Twin::with(online, true);
        assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT)]);
        assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
        assert_eq!(twin.state().wants(), names(&["Chat"]));
    }
}

#[test]
fn feed_off_unwant() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert!(twin.step(minus("Chat")).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty(), "no reader is left, so coming online subscribes to nothing");
}

#[test]
fn feed_unwant_unknown() {
    for mut twin in [Twin::with(Some(false), true), opening(), live()] {
        twin.step(Input::WantMinus { model: "Chat".into(), id: 99 });
        twin.step(Input::WantMinus { model: "Post".into(), id: POST });
        assert_eq!(twin.state().wants().len(), usize::from(twin.state().mode() != f::Mode::Off { gap: false }));
    }
}

#[test]
fn feed_off_resume() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert_eq!(twin.step(Input::Online(true)), [Effect::Connect]);
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_off_online_no_want() {
    let mut twin = Twin::with(None, true);
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_offline() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    assert!(twin.step(Input::Online(false)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_off_stray() {
    let mut twin = Twin::with(Some(false), true);
    twin.step(plus("Chat"));
    for input in [event(To::Stray, "Chat", "x"), event(To::Active, "Chat", "x"), Input::HandshakeDone, Input::ChannelLost { to: To::Active, dropped: None }, Input::Dropped { to: To::Active }, Input::Ended { to: To::Opening }, Input::Tick, Input::Resubscribe { to: To::Opening }, Input::FrameOpened { to: To::Opening, counter: Counter::Expected, events: Vec::new() }] {
        assert!(twin.step(input).is_empty());
    }
}

#[test]
fn feed_off_models_reset() {
    let mut twin = Twin::with(Some(false), true);
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]), generation: 1 }).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_landing_after_drop() {
    let mut twin = live_immediate();
    assert_eq!(twin.step(chat("c1")), [Effect::Commit]);
    twin.step(Input::Dropped { to: To::Active });
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    twin.step(Input::Committed);
    assert_eq!(twin.reference.take_obs(), Vec::new());
    assert_eq!(twin.state().landing(), 0, "a batch that lands after the break is settled");
}

#[test]
fn feed_landing_rejected_after_drop() {
    let mut opening = live_immediate();
    opening.step(chat("c1"));
    opening.step(Input::Dropped { to: To::Active });
    assert!(matches!(opening.state().mode(), f::Mode::Opening { gap: true }));
    opening.step(Input::Rejected(Reject::NoSession));
    assert_eq!(opening.state().mode(), f::Mode::Ended, "a terminal refusal of the Store ends Feed in Opening");
    assert!(opening.state().opening_connection().is_none());
    let mut off = live_immediate();
    off.step(chat("c1"));
    off.step(Input::Online(false));
    assert_eq!(off.step(Input::Rejected(Reject::NoSession)), [Effect::Cancel, Effect::CancelDrain], "and in the drain of Off the connection is cancelled at once");
    assert_eq!(off.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_stray_batch() {
    for mut twin in [live(), Twin::with(Some(false), true)] {
        assert!(twin.step(Input::StrayBatch).is_empty());
    }
    let mut opening = opening();
    assert!(opening.step(Input::StrayBatch).is_empty());
}

#[test]
fn feed_opening_ready() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT)]);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    let mut held = opening();
    held.step(event(To::Opening, "Chat", "c1"));
    held.step(event(To::Opening, "Chat", "c2"));
    assert_eq!(held.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT), Effect::ScheduleTick], "the held frames sit by the debounce of the model");
    assert_eq!(held.state().position("Chat"), 2, "the positions were given when the frames arrived");
}

#[test]
fn feed_opening_ready_after_gap() {
    let mut twin = live();
    twin.step(Input::Dropped { to: To::Active });
    assert_eq!(twin.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::Stale { model: "Chat".into(), all: true }, Effect::ModelReady(CHAT)]);
    assert_eq!(twin.state().position("Chat"), 0, "a break of unknown length takes no position");
    assert_eq!(twin.reference.take_obs(), Vec::new());
}

#[test]
fn feed_opening_stray() {
    let mut twin = opening();
    for input in [event(To::Stray, "Chat", "x"), Input::ChannelLost { to: To::Stray, dropped: None }, Input::Dropped { to: To::Stray }, Input::Ended { to: To::Stray }, Input::Resubscribe { to: To::Stray }, Input::FrameOpened { to: To::Stray, counter: Counter::Expected, events: Vec::new() }] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_event() {
    let mut twin = opening();
    assert!(twin.step(event(To::Opening, "Chat", "c1")).is_empty());
    assert_eq!(twin.state().held(), 1);
    assert_eq!(twin.state().position("Chat"), 1);
    assert!(twin.step(event(To::Opening, "Post", "p1")).is_empty(), "a model with no reader takes no position");
}

#[test]
fn feed_opening_channel_lost() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let counts = Some(vec![("Chat".to_string(), 2)]);
    assert_eq!(twin.step(Input::ChannelLost { to: To::Opening, dropped: counts }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 3, "the lost positions follow the held one");
    assert_eq!(twin.step(Input::ChannelLost { to: To::Opening, dropped: None }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 3, "an unknown number takes no position");
}

#[test]
fn feed_opening_dropped() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let first = twin.state().opening_connection();
    assert_eq!(twin.step(Input::Dropped { to: To::Opening }), [Effect::ScheduleResubscribe, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_ne!(twin.state().opening_connection(), first, "the retry is a new connection");
    assert_eq!(twin.state().held(), 0);
    let mut ended = opening();
    assert_eq!(ended.step(Input::Ended { to: To::Opening }), [Effect::ScheduleResubscribe]);
}

#[test]
fn feed_opening_resubscribe() {
    let mut twin = opening();
    twin.step(Input::Dropped { to: To::Opening });
    assert_eq!(twin.step(Input::Resubscribe { to: To::Opening }), [Effect::Connect]);
    assert_eq!(twin.state().protocol(twin.connection(To::Opening)), Some(Protocol::Handshake));
}

#[test]
fn feed_opening_want() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    let first = twin.state().opening_connection();
    assert_eq!(twin.step(plus("Post")), [Effect::Joined(POST)]);
    assert_eq!(twin.state().opening_connection(), first, "the channels are subscribed once the connection is ready: the same connection serves the new model");
    assert_eq!(twin.state().held(), 1, "the held frame of Chat stays");
}

#[test]
fn feed_opening_want_same() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::WantPlus { model: "Chat".into(), id: 5, stream: true }), [Effect::Joined(5)], "a model that is wanted already joins without a new subscription");
    assert!(twin.step(window("Chat")).is_empty());
}

#[test]
fn feed_opening_unwant() {
    let mut twin = opening();
    twin.step(plus("Post"));
    assert!(twin.step(minus("Post")).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_last_unwant() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    assert_eq!(twin.step(minus("Chat")), [Effect::Close, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_opening_offline() {
    let mut twin = opening();
    twin.step(event(To::Opening, "Chat", "c1"));
    assert_eq!(twin.step(Input::Online(false)), [Effect::Close, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
    assert_eq!(twin.step(Input::Online(true)), [Effect::Connect], "the want is kept: coming online opens the subscription again");
}

#[test]
fn feed_opening_online() {
    let mut twin = opening();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert!(twin.step(Input::Tick).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Opening { gap: false });
}

#[test]
fn feed_opening_models_reset() {
    let mut twin = opening_with(Twin::new());
    twin.step(plus("Post"));
    twin.step(event(To::Opening, "Chat", "c1"));
    twin.step(event(To::Opening, "Post", "p1"));
    twin.generation += 1;
    let generation = twin.generation;
    assert_eq!(twin.step(Input::ModelsReset { models: names(&["Post"]), generation }), [Effect::Stale { model: "Post".into(), all: true }]);
    assert_eq!(twin.state().held(), 1, "the frames of the other model stay");
}

#[test]
fn feed_live_event() {
    let mut twin = live();
    assert_eq!(twin.step(chat("c1")), [Effect::ScheduleTick]);
    assert!(twin.step(chat("c2")).is_empty(), "the tick is scheduled once");
    assert_eq!(twin.state().pending(), 2);
}

#[test]
fn feed_live_event_now() {
    let mut twin = live_immediate();
    assert_eq!(twin.step(chat("c1")), [Effect::Commit]);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_event_unwatched() {
    let mut twin = live();
    assert!(twin.step(event(To::Active, "Post", "p1")).is_empty(), "an event of a model with no reader is not applied");
    assert_eq!(twin.state().pending(), 0);
}

#[test]
fn feed_live_stray() {
    let mut twin = live();
    for input in [event(To::Stray, "Chat", "x"), Input::ChannelLost { to: To::Stray, dropped: None }, Input::Dropped { to: To::Stray }, Input::Ended { to: To::Stray }, Input::Resubscribe { to: To::Stray }] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_flush() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Tick), [Effect::Commit]);
}

#[test]
fn feed_live_tick_empty() {
    let mut twin = live();
    assert!(twin.step(Input::Tick).is_empty());
}

#[test]
fn feed_live_committed() {
    let mut twin = live();
    twin.reread = Reread::Set(names(&["all"]));
    for payload in ["c1", "c2", "c3"] {
        twin.step(chat(payload));
    }
    twin.step(Input::Tick);
    assert_eq!(twin.step(Input::Committed), [Effect::StaleSet { model: "Chat".into(), keys: names(&["all"]) }]);
    assert_eq!(twin.state().position("Chat"), 3, "positions of a batch follow one another from 1");
    assert_eq!(twin.reference.take_obs(), Vec::new());
}

#[test]
fn data_sets_missing_parent() {
    let mut twin = live();
    twin.reread = Reread::Set(names(&["all"]));
    twin.step(chat("c1"));
    twin.step(Input::Tick);
    assert_eq!(twin.step(Input::CommittedStale(vec!["Profile".into()])), [Effect::StaleSet { model: "Chat".into(), keys: names(&["all"]) }, Effect::Stale { model: "Profile".into(), all: true }], "every parent model whose fields were not written turns Stale");
    twin.step(chat("c2"));
    twin.step(Input::Tick);
    assert_eq!(twin.step(Input::Committed), [Effect::StaleSet { model: "Chat".into(), keys: names(&["all"]) }], "a commit without stale parents adds nothing");
    assert!(twin.step(Input::CommittedStale(vec!["Profile".into()])).is_empty(), "a batch that is not in landing adds nothing");
}

#[test]
fn feed_live_rejected_generation() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::HandshakeDone);
    twin.step(chat("c1"));
    twin.step(event(To::Active, "Post", "p1"));
    twin.step(Input::Tick);
    let effects = twin.step(Input::Rejected(Reject::StaleGeneration(names(&["Chat"]))));
    assert_eq!(effects, [Effect::Stale { model: "Chat".into(), all: true }, Effect::Commit], "the events of the other models land in a new batch");
    twin.step(Input::Committed);
}

#[test]
fn feed_live_rejected_storage() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::Storage)), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 1, "the lost event holds its position");
}

#[test]
fn feed_live_rejected_session() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::NoSession)), [Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_live_rejected_invalid() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::Invalid)), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 1, "the lost event holds its position");
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_rejected_closed() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Rejected(Reject::Closed)), [Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_live_stray_batch() {
    let mut twin = live();
    assert!(twin.step(Input::StrayBatch).is_empty());
    assert_eq!(twin.state().position("Chat"), 0);
}

#[test]
fn feed_live_channel_lost() {
    let mut twin = live();
    twin.step(chat("c1"));
    let counts = Some(vec![("Chat".to_string(), 5)]);
    assert_eq!(twin.step(Input::ChannelLost { to: To::Active, dropped: counts }), [Effect::Commit, Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.reference.take_obs(), Vec::new());
    twin.step(Input::Committed);
    assert_eq!(twin.state().position("Chat"), 6, "the pending event and the 5 dropped hold 6 positions");
    assert_eq!(twin.step(Input::ChannelLost { to: To::Active, dropped: None }), [Effect::Stale { model: "Chat".into(), all: true }]);
    assert_eq!(twin.state().position("Chat"), 6, "an unknown number takes no position");
}

#[test]
fn feed_live_dropped() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Dropped { to: To::Active }), [Effect::Commit, Effect::Connect]);
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    let mut ended = live();
    assert_eq!(ended.step(Input::Ended { to: To::Active }), [Effect::Connect]);
}

#[test]
fn feed_live_offline() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Online(false)), [Effect::Commit, Effect::SealUnsubscribe, Effect::Close, Effect::ScheduleDrain]);
    assert!(matches!(twin.state().mode(), f::Mode::Draining { then: f::Resume::Off { gap: true }, .. }));
    assert_eq!(twin.step(Input::Drain { own: true }), [Effect::Cancel]);
    assert!(matches!(twin.state().mode(), f::Mode::Off { gap: true }));
}

#[test]
fn feed_live_online() {
    let mut twin = live();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_models_reset() {
    let mut twin = live();
    twin.step(chat("c1"));
    twin.generation += 1;
    let generation = twin.generation;
    assert!(twin.step(Input::ModelsReset { models: names(&["Chat"]), generation }).is_empty(), "the Store decides about the pending events");
    twin.step(Input::Tick);
    assert_eq!(twin.state().landing(), 1);
}

#[test]
fn feed_live_last_unwant() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(minus("Chat")), [Effect::Commit, Effect::SealUnsubscribe, Effect::Close, Effect::ScheduleDrain]);
    assert!(matches!(twin.state().mode(), f::Mode::Draining { then: f::Resume::Off { gap: false }, .. }));
    assert_eq!(twin.step(Input::Drain { own: true }), [Effect::Cancel]);
    assert_eq!(twin.state().mode(), f::Mode::Off { gap: false });
}

#[test]
fn feed_live_unwant() {
    let mut twin = live();
    twin.step(plus("Post"));
    twin.step(Input::HandshakeDone);
    assert!(twin.step(minus("Post")).is_empty(), "the subscription narrows at the next Subscribe");
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_live_want_more() {
    let mut twin = live();
    assert_eq!(twin.step(plus("Post")), [Effect::Joined(POST), Effect::ModelReady(POST)], "the channel of the new model is confirmed already: no new connection");
    assert!(twin.state().opening_connection().is_none());
}

#[test]
fn feed_live_want_same() {
    let mut twin = live();
    assert_eq!(twin.step(plus("Chat")), [Effect::Joined(CHAT), Effect::ModelReady(CHAT)]);
    assert!(twin.step(window("Chat")).is_empty(), "a window joins without an answer");
}

#[test]
fn feed_epoch_ended() {
    let mut twin = live();
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::EpochEnded), [Effect::SealUnsubscribe, Effect::Close]);
    assert_eq!(twin.state().pending(), 0, "the pending events of the ended epoch are dropped");
    for mut off in [Twin::new(), Twin::with(Some(false), true), opening()] {
        off.step(Input::EpochEnded);
        assert_eq!(off.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_closing() {
    for mut twin in [Twin::new(), opening()] {
        let effects = twin.step(Input::Closing);
        assert_eq!(effects.last(), Some(&Effect::Drained));
        assert_eq!(twin.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_close_drains() {
    let mut twin = live();
    assert_eq!(twin.step(Input::Closing), [Effect::SealUnsubscribe, Effect::Close, Effect::ScheduleDrain], "no Drained until the drain ends");
    assert_eq!(twin.state().mode(), f::Mode::Draining { connection: twin.connection(To::Draining), then: f::Resume::End });
}

#[test]
fn feed_drain_elapsed() {
    let mut twin = live();
    twin.step(Input::Closing);
    assert_eq!(twin.step(Input::Drain { own: true }), [Effect::Cancel, Effect::Drained]);
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_drain_pre_empted() {
    for end in [Input::Dropped { to: To::Draining }, Input::Ended { to: To::Draining }, Input::ChannelLost { to: To::Draining, dropped: None }] {
        let mut twin = live();
        twin.step(Input::Closing);
        assert_eq!(twin.step(end), [Effect::Cancel, Effect::CancelDrain, Effect::Drained]);
        assert_eq!(twin.state().mode(), f::Mode::Ended);
    }
}

#[test]
fn feed_draining_other() {
    let mut twin = live();
    twin.step(Input::Closing);
    for other in [Input::Online(false), Input::Tick, chat("c1"), minus("Chat"), plus("Post"), Input::Drain { own: false }, Input::Dropped { to: To::Stray }, Input::Dropped { to: To::Active }, Input::Closing] {
        assert!(twin.step(other.clone()).is_empty(), "{other:?} changes nothing while draining");
        assert_eq!(twin.state().mode(), f::Mode::Draining { connection: twin.connection(To::Draining), then: f::Resume::End });
    }
}

#[test]
fn feed_draining_resumes() {
    let mut twin = live();
    twin.step(Input::Online(false));
    assert!(twin.step(plus("Post")).len() == 1, "a want during the drain joins at once and opens nothing");
    assert!(twin.step(Input::Online(true)).is_empty(), "the network returns during the drain: no connection yet");
    assert_eq!(twin.step(Input::Drain { own: true }), [Effect::Cancel, Effect::Connect], "the drain over: the feed opens for the wants it has now");
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
    let mut closing = live();
    closing.step(Input::Online(false));
    assert!(closing.step(Input::Closing).is_empty());
    assert_eq!(closing.step(Input::Drain { own: true }), [Effect::Cancel, Effect::Drained], "Closing during a drain that resumes makes the drain end the feed");
    assert_eq!(closing.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_draining_epoch_ended() {
    let mut twin = live();
    twin.step(Input::Closing);
    assert_eq!(twin.step(Input::EpochEnded), [Effect::Cancel, Effect::CancelDrain, Effect::Drained], "the epoch end purges the drain timer: the connection is cancelled at once");
    assert_eq!(twin.state().mode(), f::Mode::Ended);
}

#[test]
fn feed_ended_closing() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    assert_eq!(twin.step(Input::Closing), [Effect::Drained]);
    assert!(twin.step(Input::Closing).is_empty(), "Drained goes out once");
}

#[test]
fn feed_ended() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    for input in [plus("Post"), Input::HandshakeDone, Input::Tick, Input::Online(true), Input::Committed, chat("c9"), Input::Dropped { to: To::Active }] {
        assert!(twin.step(input).is_empty());
    }
}

// The connection protocol.

#[test]
fn feed_proto_handshake_done() {
    let mut twin = opening();
    let connection = twin.connection(To::Opening);
    assert_eq!(twin.state().protocol(connection), Some(Protocol::Handshake));
    twin.step(Input::HandshakeDone);
    assert_eq!(twin.state().protocol(connection), Some(Protocol::Established { key_id: 7, send_counter: 1, recv_counter: f::FIRST_RECV_COUNTER }), "the subscribe frame reserved the first send counter");
    assert!(twin.step(Input::HandshakeDone).is_empty(), "the handshake of an established connection is done once");
}

#[test]
fn feed_proto_frame_before_handshake() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::FrameOpened { to: To::Opening, counter: Counter::Expected, events: vec![("Chat".into(), "c1".into())] }), [Effect::Close, Effect::ScheduleResubscribe]);
    assert_eq!(twin.state().position("Chat"), 0, "the frame does not sit");
}

#[test]
fn feed_proto_ping_pong() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    assert_eq!(twin.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::Ping }), [Effect::SealPong]);
    assert!(matches!(twin.state().protocol(connection), Some(Protocol::Established { send_counter: 2, recv_counter, .. }) if recv_counter == f::FIRST_RECV_COUNTER + 1), "the pong reserved its own send counter");
}

#[test]
fn feed_proto_frame_kinds() {
    let mut other = live();
    assert!(other.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::OtherId }).is_empty(), "a frame of another subscription id is not for Feed");
    let mut quiet = live();
    assert!(quiet.step(Input::Frame { to: To::Active, counter: Counter::Expected, kind: Kind::Quiet }).is_empty());
    let mut replayed = live();
    assert_eq!(replayed.step(Input::Frame { to: To::Active, counter: Counter::Replayed, kind: Kind::Complete }), [Effect::Close, Effect::ScheduleDrain], "the counter is checked before the meaning of the frame");
}

#[test]
fn feed_proto_frame_ok() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    assert_eq!(twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Expected, events: vec![("Chat".into(), "c1".into()), ("Chat".into(), "c2".into())] }), [Effect::ScheduleTick]);
    assert!(matches!(twin.state().protocol(connection), Some(Protocol::Established { recv_counter, .. }) if recv_counter == f::FIRST_RECV_COUNTER + 1));
    assert_eq!(twin.state().pending(), 2);
}

#[test]
fn feed_proto_frame_counter_mismatch() {
    let mut twin = live();
    assert_eq!(twin.step(Input::FrameOpened { to: To::Active, counter: Counter::Replayed, events: vec![("Chat".into(), "c1".into())] }), [Effect::Close, Effect::ScheduleDrain]);
    assert_eq!(twin.state().pending(), 0, "a replayed frame does not sit");
    assert!(matches!(twin.state().mode(), f::Mode::Draining { then: f::Resume::Reopen, .. }));
    assert_eq!(twin.step(Input::Drain { own: true }), [Effect::Cancel, Effect::Connect], "the new connection opens after the drain");
    assert!(matches!(twin.state().mode(), f::Mode::Opening { gap: true }));
}

#[test]
fn feed_proto_error() {
    let mut twin = live();
    assert_eq!(twin.step(Input::ProtocolError { to: To::Active }), [Effect::Connect]);
    let mut handshake = opening();
    assert_eq!(handshake.step(Input::ProtocolError { to: To::Opening }), [Effect::ScheduleResubscribe]);
    assert!(handshake.step(Input::ProtocolError { to: To::Stray }).is_empty());
}

#[test]
fn feed_proto_handshake_timeout() {
    let mut twin = opening();
    assert_eq!(twin.step(Input::HandshakeDeadline { to: To::Opening }), [Effect::Close, Effect::ScheduleResubscribe]);
    let mut established = live();
    assert!(established.step(Input::HandshakeDeadline { to: To::Active }).is_empty(), "an established connection has no deadline");
    assert!(established.step(Input::HandshakeDeadline { to: To::Stray }).is_empty());
}

#[test]
fn feed_proto_send_reserves() {
    let mut twin = live();
    twin.step(Input::EpochEnded);
    let counters: Vec<u64> = twin.sealed.iter().map(|(_, counter)| *counter).collect();
    assert_eq!(counters, [0, 1], "the subscribe frame and the unsubscribe frame take the next send counters, each one once");
}

#[test]
fn feed_wait_ready() {
    let mut opening = Twin::new();
    assert_eq!(opening.step(plus("Chat")), [Effect::Joined(CHAT), Effect::Connect]);
    assert_eq!(opening.step(Input::HandshakeDone), [Effect::SealSubscribe, Effect::ModelReady(CHAT)], "Ready comes with the handshake of the connection");
    assert_eq!(opening.step(plus("Chat")), [Effect::Joined(CHAT), Effect::ModelReady(CHAT)], "a live subscription answers at once");
}

/// The channels the models of `want` read on the active connection, asked of the owner.
pub(super) fn census_of(twin: &mut Twin) -> usize {
    twin.post(FeedInput::Census { reply: 7 });
    take(&twin.log).into_iter().find_map(|entry| match entry {
        Emitted::Run(FeedEffect::Census { reply: 7, subscriptions, .. }) => Some(subscriptions),
        _ => None,
    }).expect("the owner answers the census")
}

#[test]
fn feed_wait_subscription_census() {
    let mut twin = Twin::new();
    assert_eq!(census_of(&mut twin), 0, "no model wanted, no channel");
    twin.step(plus("Chat"));
    assert_eq!(census_of(&mut twin), 0, "a connection in its handshake is not active");
    twin.step(Input::HandshakeDone);
    twin.step(plus("Post"));
    assert_eq!(census_of(&mut twin), 1, "two models on one channel count it once");
    twin.step(minus("Chat"));
    twin.step(minus("Post"));
    assert_eq!(census_of(&mut twin), 0, "after the last Want- no channel stays wanted");
}


#[test]
fn feed_changed_by_forms() {
    let forms: [(&str, Reread, Effect); 5] = [
        ("feed_changed_by_node", Reread::Set(names(&["node"])), Effect::StaleSet { model: "Chat".into(), keys: names(&["node"]) }),
        ("feed_changed_by_named", Reread::Set(names(&["byChat"])), Effect::StaleSet { model: "Chat".into(), keys: names(&["byChat"]) }),
        ("feed_changed_by_named_default_param", Reread::Set(names(&["byChat", "byAuthor"])), Effect::StaleSet { model: "Chat".into(), keys: names(&["byAuthor", "byChat"]) }),
        ("feed_changed_by_observed", Reread::All, Effect::Stale { model: "Chat".into(), all: true }),
        ("feed_changed_by_current", Reread::Set(names(&["current"])), Effect::StaleSet { model: "Chat".into(), keys: names(&["current"]) }),
    ];
    for (name, reread, stale) in forms {
        let mut twin = live_immediate();
        twin.reread = reread;
        assert_eq!(twin.step(chat("c1")), [Effect::Commit], "{name}");
        assert_eq!(twin.step(Input::Committed), [stale], "{name}");
    }
}

#[test]
fn feed_changed_by_two_forms() {
    let mut twin = live_immediate();
    twin.reread = Reread::All;
    twin.step(chat("c1"));
    assert_eq!(twin.step(Input::Committed), [Effect::Stale { model: "Chat".into(), all: true }]);
}

#[test]
fn feed_changes_not_list() {
    let mut twin = live_immediate();
    twin.step(chat("c1"));
    twin.step(Input::Refused { model: "Chat".into() });
    assert_eq!(census_refused(&mut twin), twin.reference.refused);
    assert_eq!(twin.reference.refused, 1);
    assert_eq!(twin.shown, [], "the refused position waits behind the pending position of c1");
    twin.step(Input::Committed);
    assert_eq!(twin.shown, [Obs::Event { stream: CHAT, position: 1, payload: "c1".into() }, Obs::Lost { stream: CHAT, first: 2, last: 2 }]);
    twin.step(Input::Refused { model: "Chat".into() });
    twin.step(Input::Refused { model: "Chat".into() });
    assert_eq!((census_refused(&mut twin), twin.reference.refused), (3, 3), "one count per refused event, never reset in the epoch");
}

#[test]
fn feed_changes_entry_incomplete() {
    let mut twin = live_immediate();
    let connection = twin.connection(To::Active);
    let counter = twin.counter(connection, Counter::Expected);
    let events = vec![framed("", "refused", &Reread::None, true), framed("Chat", "c1", &Reread::None, false)];
    take(&twin.log);
    twin.post(FeedInput::FrameOpened { connection_id: connection, counter, frame: Frame::Events { id: CHANNEL.into(), events } });
    let emitted = take(&twin.log);
    let (got, seen) = twin.split(emitted);
    assert_eq!((got, seen), (vec![Effect::Commit], vec![]), "the entry takes no position, the next entry of the frame goes to the Store");
    twin.owner_batches.clear();
    assert_eq!(twin.reference.step(chat("c1")), [Effect::Commit], "the reference sees the entries that go");
    twin.reference.take_marks();
    assert_eq!(census_refused(&mut twin), 1, "the entry without area, kind or id counts once");
    assert_eq!(twin.reference.refused, 0, "the reference has no input for a refused entry without a model");
    twin.step(Input::Committed);
    assert_eq!(twin.shown, [Obs::Event { stream: CHAT, position: 1, payload: "c1".into() }]);
}

#[test]
fn feed_live_verdict_by_batch() {
    let mut twin = live_immediate();
    for payload in ["c1", "c2", "c3"] {
        twin.step(chat(payload));
    }
    assert!(twin.step(Input::StrayBatch).is_empty(), "an id that never was a batch of the Feed");
    twin.step(Input::CommittedAt(2));
    assert_eq!(twin.shown, [], "the third position waits for the first two");
    twin.step(Input::RejectedAt(1, Reject::Storage));
    assert!(twin.step(Input::StrayRepeat).is_empty(), "the verdict of a batch that landed changes nothing");
    assert_eq!(twin.state().landing(), 1);
    twin.step(Input::Committed);
    assert_eq!(twin.shown, [Obs::Event { stream: CHAT, position: 1, payload: "c1".into() }, Obs::Lost { stream: CHAT, first: 2, last: 2 }, Obs::Event { stream: CHAT, position: 3, payload: "c3".into() }]);
}

#[test]
fn feed_commit_observe_stream() {
    let mut twin = Twin::immediate();
    twin.step(plus("Chat"));
    twin.step(window("Post"));
    twin.step(Input::HandshakeDone);
    twin.step(chat("{\"id\":\"c1\"}"));
    twin.step(event(To::Active, "Post", "{\"id\":\"p1\"}"));
    assert_eq!(twin.state().landing(), 2);
}

fn census_refused(twin: &mut Twin) -> u64 {
    twin.post(FeedInput::Census { reply: 7 });
    take(&twin.log).into_iter().find_map(|entry| match entry {
        Emitted::Run(FeedEffect::Census { reply: 7, refused, .. }) => Some(refused),
        _ => None,
    }).expect("the owner answers the census")
}
