use super::*;

const KEEPALIVE_MS: u64 = 25_000;
const PONG_MS: u64 = 10_000;
const HANDSHAKE_AT: u64 = 1_000;

#[derive(Clone, Copy, Debug)]
enum Turn {
    Want,
    Handshake,
    Keepalive,
    PongDeadline,
    Event,
    /// An incoming frame that carries no event.
    Heard,
    /// A pong that echoes the given ping `ts`.
    Pong(u64),
    /// Outgoing traffic: a second model is wanted.
    WantOther,
}

type Plan = Vec<(Turn, u64)>;

#[derive(Debug, PartialEq, Eq)]
struct Out {
    pings: usize,
    pongs: usize,
    live: bool,
}

struct Drive {
    twin: Twin,
    ping_counter: u64,
}

fn send_counter(twin: &Twin) -> u64 {
    match twin.state().active_connection().and_then(|connection| twin.state().protocol(connection)) {
        Some(Protocol::Established { send_counter, .. }) => send_counter,
        _ => 0,
    }
}

fn input_of(turn: Turn, twin: &Twin, ping_counter: u64) -> Vec<FeedInput> {
    let active = twin.connection(To::Active);
    match turn {
        Turn::Want => vec![FeedInput::WantPlus { model: "Chat".into(), id: CHAT, stream: true, scoped: Vec::new() }],
        Turn::Handshake => {
            let opening = twin.connection(To::Opening);
            vec![FeedInput::HandshakeDone { connection_id: opening, key_id: 7 }, FeedInput::ChannelConfirmed { connection_id: opening, channel: CHANNEL.into() }]
        }
        Turn::Keepalive => vec![FeedInput::Keepalive { connection_id: active }],
        Turn::PongDeadline => vec![FeedInput::PongDeadline { connection_id: active, counter: ping_counter }],
        Turn::Heard => vec![FeedInput::FrameOpened { connection_id: active, counter: twin.counter(active, Counter::Expected), frame: Frame::Quiet }],
        Turn::Pong(ts) => vec![FeedInput::FrameOpened { connection_id: active, counter: twin.counter(active, Counter::Expected), frame: Frame::Pong { ts: ts.into() } }],
        Turn::WantOther => vec![FeedInput::WantPlus { model: "Other".into(), id: CHAT + 1, stream: true, scoped: Vec::new() }],
        Turn::Event => vec![FeedInput::Event { connection_id: active, channel: CHANNEL.into(), event: framed("Chat", "p", &Reread::None, false) }],
    }
}

impl Drive {
    fn new() -> Self { Self { twin: Twin::new(), ping_counter: 0 } }

    fn turn(&mut self, turn: Turn, at: u64) -> Out {
        let twin = &mut self.twin;
        twin.det.tick(Time(at));
        take(&twin.log);
        let before = send_counter(twin);
        for input in input_of(turn, twin, self.ping_counter) {
            twin.post(input);
        }
        let emitted = take(&twin.log);
        let pings = emitted.iter().filter(|effect| matches!(effect, Emitted::Run(FeedEffect::Seal { frame: Outgoing::Ping { .. }, .. }))).count();
        let pongs = emitted.iter().filter(|effect| matches!(effect, Emitted::Schedule { input: FeedInput::PongDeadline { .. }, .. })).count();
        if pings > 0 {
            self.ping_counter = before;
        }
        Out { pings, pongs, live: twin.state().mode() == f::Mode::Live }
    }

    fn run(&mut self, plan: &Plan) -> Vec<Out> { plan.iter().map(|(turn, at)| self.turn(*turn, *at)).collect() }
}

fn opened() -> Plan { vec![(Turn::Want, 0), (Turn::Handshake, HANDSHAKE_AT)] }

fn after(mut plan: Plan, turn: Turn, at: u64) -> Plan {
    plan.push((turn, at));
    plan
}

#[test]
fn feed_prop_timers_for_any_input_times() {
    let due = HANDSHAKE_AT + KEEPALIVE_MS;
    let pinged = || after(opened(), Turn::Keepalive, due);
    let pong_due = due + PONG_MS;
    let cases: [(&str, Plan, Out); 5] = [
        ("a keepalive 1 ms before the term", after(opened(), Turn::Keepalive, due - 1), Out { pings: 0, pongs: 0, live: true }),
        ("a keepalive at the term", after(opened(), Turn::Keepalive, due), Out { pings: 1, pongs: 1, live: true }),
        ("a pong deadline 1 ms before the term", after(pinged(), Turn::PongDeadline, pong_due - 1), Out { pings: 0, pongs: 0, live: true }),
        ("a pong deadline at the term", after(pinged(), Turn::PongDeadline, pong_due), Out { pings: 0, pongs: 0, live: false }),
        ("a keepalive twice at the term, the second is stale", after(pinged(), Turn::Keepalive, due), Out { pings: 0, pongs: 0, live: true }),
    ];
    debounce_from_step_time();
    for (name, plan, last) in cases {
        let outs = Drive::new().run(&plan);
        assert_eq!(outs.last(), Some(&last), "{name}");
    }
}

fn debounce_from_step_time() {
    let mut drive = Drive::new();
    drive.run(&opened());
    let at = 2_000;
    take(&drive.twin.log);
    drive.twin.det.tick(Time(at));
    for input in input_of(Turn::Event, &drive.twin, 0) {
        drive.twin.post(input);
    }
    let scheduled = take(&drive.twin.log);
    let debounce = std::time::Duration::from_millis(DEBOUNCE_MS);
    assert_eq!(scheduled.iter().filter(|effect| matches!(effect, Emitted::Schedule { after, input: FeedInput::Tick, .. } if *after == debounce)).count(), 1, "the landing of the batch is ordered after the debounce: {scheduled:?}");
    let landed = |log: &Spied| take(log).iter().filter(|effect| matches!(effect, Emitted::Send { to: STORE, .. })).count();
    drive.twin.det.tick(Time(at + DEBOUNCE_MS - 1));
    drive.twin.run();
    assert_eq!(landed(&drive.twin.log), 0, "one ms before the term of the step time the batch waits");
    drive.twin.det.tick(Time(at + DEBOUNCE_MS));
    drive.twin.run();
    assert_eq!(landed(&drive.twin.log), 1, "at the term of the step time the batch lands");
}

const FIRST_PING_AT: u64 = HANDSHAKE_AT + KEEPALIVE_MS;

fn ping_count(plan: &Plan) -> Vec<usize> { Drive::new().run(plan).iter().map(|out| out.pings).collect() }

#[test]
fn feed_ping_after_incoming_silence() {
    let heard_at = HANDSHAKE_AT + 10_000;
    let plan = vec![(Turn::Want, 0), (Turn::Handshake, HANDSHAKE_AT), (Turn::Heard, heard_at), (Turn::Keepalive, FIRST_PING_AT), (Turn::Keepalive, heard_at + KEEPALIVE_MS - 1), (Turn::Keepalive, heard_at + KEEPALIVE_MS)];
    assert_eq!(ping_count(&plan), [0, 0, 0, 0, 0, 1], "an incoming frame restarts the 25 s silence; the ping goes 25 s after it");
}

#[test]
fn feed_outgoing_traffic_does_not_delay_ping() {
    let plan = vec![(Turn::Want, 0), (Turn::Handshake, HANDSHAKE_AT), (Turn::WantOther, HANDSHAKE_AT + 10_000), (Turn::Keepalive, FIRST_PING_AT)];
    assert_eq!(ping_count(&plan), [0, 0, 0, 1], "an outgoing frame does not restart the silence");
}

#[test]
fn feed_stale_pong_ignored() {
    let pong_due = FIRST_PING_AT + PONG_MS;
    let pinged = || after(opened(), Turn::Keepalive, FIRST_PING_AT);
    let stale = Drive::new().run(&after(after(pinged(), Turn::Pong(FIRST_PING_AT - 1), FIRST_PING_AT + 1), Turn::PongDeadline, pong_due));
    assert_eq!(stale.last().map(|out| out.live), Some(false), "a pong of another ping does not clear the wait");
    let fresh = Drive::new().run(&after(after(pinged(), Turn::Pong(FIRST_PING_AT), FIRST_PING_AT + 1), Turn::PongDeadline, pong_due));
    assert_eq!(fresh.last().map(|out| out.live), Some(true), "the pong of the outstanding ping clears the wait");
}
