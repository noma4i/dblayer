//! Matrix S for the owner Feed: every start state of the harness x every input of the alphabet x every second input. The oracle is the reference `ref_feed` written from the table: after each step the owner and the reference must give the same effects, the same observable and the same state (the assertions of `Twin::step`).
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::*;
use crate::test_matrix::Matrix;

#[test]
fn feed_proto_error_established() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    take(&twin.log);
    twin.post(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Decrypt });
    let emitted = take(&twin.log);
    assert!(matches!(twin.state().mode(), f::Mode::Draining { connection: found, then: f::Resume::Reopen } if found == connection));
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Close { connection: found, .. }) if *found == connection)));
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Schedule { after, input, .. } if *after == crate::owners::read::CLOSE_DRAIN && input.starts_with("Drain"))));
}

#[test]
fn feed_keepalive_stale() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    let counter = match twin.state().protocol(connection).unwrap() { Protocol::Established { send_counter, .. } => send_counter, _ => panic!("established") };
    for input in [
        FeedInput::Keepalive { connection_id: connection + 100, counter },
        FeedInput::Keepalive { connection_id: connection, counter: counter + 100 },
        FeedInput::PongDeadline { connection_id: connection + 100, counter },
        FeedInput::PongDeadline { connection_id: connection, counter },
    ] {
        take(&twin.log);
        twin.post(input);
        assert!(take(&twin.log).is_empty());
        assert_eq!(twin.state().mode(), f::Mode::Live);
    }
}

fn ping_due() -> (Twin, ConnectionId, u64) {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    let counter = match twin.state().protocol(connection).unwrap() { Protocol::Established { send_counter, .. } => send_counter, _ => panic!("established") };
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::Tick);
    take(&twin.log);
    (twin, connection, counter)
}

#[test]
fn feed_keepalive_ping() {
    let (mut twin, connection, counter) = ping_due();
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::Keepalive { connection_id: connection, counter });
    let emitted = take(&twin.log);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Seal { connection: found, frame: Outgoing::Ping { ts: 25_000 }, .. }) if *found == connection)));
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Schedule { after, input, .. } if *after == std::time::Duration::from_secs(10) && input.starts_with("PongDeadline"))));
}

#[test]
fn feed_keepalive_pong() {
    let (mut twin, connection, counter) = ping_due();
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::Keepalive { connection_id: connection, counter });
    take(&twin.log);
    let recv_counter = twin.counter(connection, Counter::Expected);
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::FrameOpened { connection_id: connection, counter: recv_counter, frame: Frame::Pong });
    take(&twin.log);
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::PongDeadline { connection_id: connection, counter });
    assert!(take(&twin.log).is_empty());
    assert_eq!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_keepalive_half_open() {
    let (mut twin, connection, counter) = ping_due();
    twin.det.tick(Time(25_000));
    twin.post(FeedInput::Keepalive { connection_id: connection, counter });
    take(&twin.log);
    twin.det.tick(Time(35_000));
    twin.post(FeedInput::Tick);
    take(&twin.log);
    twin.det.tick(Time(35_000));
    twin.post(FeedInput::PongDeadline { connection_id: connection, counter });
    let emitted = take(&twin.log);
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Cancel { connection: found }) if *found == connection)));
    assert_ne!(twin.state().mode(), f::Mode::Live);
}

#[test]
fn feed_keepalive_rearm() {
    let mut twin = live();
    let connection = twin.connection(To::Active);
    let recv_counter = twin.counter(connection, Counter::Expected);
    twin.det.tick(Time(4_000));
    twin.post(FeedInput::Tick);
    take(&twin.log);
    twin.det.tick(Time(4_000));
    twin.post(FeedInput::FrameOpened { connection_id: connection, counter: recv_counter, frame: Frame::Ping { ts: 4.into() } });
    let emitted = take(&twin.log);
    assert_eq!(twin.state().mode(), f::Mode::Live);
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Seal { connection: found, frame: Outgoing::Pong { .. }, .. }) if *found == connection)));
    assert!(emitted.iter().any(|effect| matches!(effect, Emitted::Schedule { after, input, .. } if *after == std::time::Duration::from_secs(25) && input.starts_with("Keepalive"))));
}

#[test]
fn feed_frame_protocol_error() {
    for reason in [ProtocolReason::Decrypt, ProtocolReason::Frame] {
        let mut twin = live();
        let connection = twin.connection(To::Active);
        take(&twin.log);
        twin.post(FeedInput::ProtocolError { connection_id: connection, reason });
        let effects = take(&twin.log);
        assert!(matches!(twin.state().mode(), f::Mode::Draining { connection: found, then: f::Resume::Reopen } if found == connection));
        assert!(effects.iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Close { connection: found, .. }) if *found == connection)));
        assert!(effects.iter().any(|effect| matches!(effect, Emitted::Schedule { input, .. } if input.starts_with("Drain"))));
    }
    let mut twin = live();
    let connection = twin.connection(To::Active);
    let counter = twin.counter(connection, Counter::Expected) + 1;
    take(&twin.log);
    twin.post(FeedInput::FrameOpened { connection_id: connection, counter, frame: Frame::Pong });
    assert!(matches!(twin.state().mode(), f::Mode::Draining { then: f::Resume::Reopen, .. }));
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, Emitted::Run(FeedEffect::Close { .. }))));
    let mut twin = opening();
    let connection = twin.connection(To::Opening);
    take(&twin.log);
    twin.post(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Handshake });
    assert!(take(&twin.log).iter().any(|effect| matches!(effect, Emitted::Schedule { input, .. } if input.starts_with("Resubscribe"))));
}

fn alphabet() -> Vec<(&'static str, Input)> {
    vec![
        ("WantPlus Chat", plus("Chat")),
        ("WantPlus Post", plus("Post")),
        ("WantMinus Chat", minus("Chat")),
        ("WantMinus Post", minus("Post")),
        ("HandshakeDone", Input::HandshakeDone),
        ("Dropped active", Input::Dropped { to: To::Active }),
        ("Dropped opening", Input::Dropped { to: To::Opening }),
        ("Ended active", Input::Ended { to: To::Active }),
        ("Resubscribe active", Input::Resubscribe { to: To::Active }),
        ("ProtocolError active", Input::ProtocolError { to: To::Active }),
        ("ProtocolError opening", Input::ProtocolError { to: To::Opening }),
        ("ProtocolError stray", Input::ProtocolError { to: To::Stray }),
        ("HandshakeDeadline opening", Input::HandshakeDeadline { to: To::Opening }),
        ("Event Chat", chat("payload")),
        ("Event stray", event(To::Stray, "Chat", "payload")),
        ("Committed", Input::Committed),
        ("Rejected storage", Input::Rejected(Reject::Storage)),
        ("StrayBatch", Input::StrayBatch),
        ("Tick", Input::Tick),
        ("Online false", Input::Online(false)),
        ("Online true", Input::Online(true)),
        ("Drain own", Input::Drain { own: true }),
        ("Drain stray", Input::Drain { own: false }),
        ("EpochEnded", Input::EpochEnded),
    ]
}

type Start = (&'static str, fn() -> Twin);

fn starts() -> Vec<Start> {
    vec![("off", Twin::new), ("opening", opening), ("live", live), ("live without debounce", live_immediate)]
}

fn first_line(payload: &Box<dyn std::any::Any + Send>) -> String {
    let text = payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default();
    text.lines().next().unwrap_or("").chars().take(120).collect()
}

#[test]
fn matrix_owner_state_input_feed() {
    let mut matrix = Matrix::new("S-feed");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (start_name, start) in starts() {
        for (first_name, first) in alphabet() {
            for (second_name, second) in std::iter::once(("-", None)).chain(alphabet().into_iter().map(|(name, input)| (name, Some(input)))) {
                let sequence = format!("{first_name} then {second_name}");
                let first = first.clone();
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    let mut twin = start();
                    twin.step(first);
                    if let Some(second) = second { twin.step(second); }
                }));
                let observed = match &outcome { Ok(()) => "owner and reference agree".to_string(), Err(payload) => first_line(payload) };
                matrix.check(start_name, "reference ref_feed", &sequence, "owner and reference agree", &observed);
            }
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
