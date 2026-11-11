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
        Turn::Keepalive => vec![FeedInput::Keepalive { connection_id: active, counter: send_counter(twin) }],
        Turn::PongDeadline => vec![FeedInput::PongDeadline { connection_id: active, counter: ping_counter }],
        Turn::Event => vec![FeedInput::Event { connection_id: active, channel: CHANNEL.into(), event: framed("Chat", "p", &Reread::None, false) }],
    }
}

fn ref_input(turn: Turn) -> Input {
    match turn {
        Turn::Want => plus("Chat"),
        Turn::Handshake => Input::HandshakeDone,
        Turn::Keepalive => Input::Keepalive,
        Turn::PongDeadline => Input::PongDeadline,
        Turn::Event => chat("p"),
    }
}

impl Drive {
    fn new() -> Self { Self { twin: Twin::new(), ping_counter: 0 } }

    fn turn(&mut self, turn: Turn, at: u64) -> (Out, Out) {
        let twin = &mut self.twin;
        twin.det.tick(Time(at));
        take(&twin.log);
        let before = send_counter(twin);
        for input in input_of(turn, twin, self.ping_counter) {
            twin.post(input);
        }
        let emitted = take(&twin.log);
        twin.reference.now = at;
        let mut want = twin.reference.fire_due();
        want.extend(twin.reference.step(ref_input(turn)));
        let pings = emitted.iter().filter(|effect| matches!(effect, Emitted::Run(FeedEffect::Seal { frame: Outgoing::Ping { .. }, .. }))).count();
        let pongs = emitted.iter().filter(|effect| matches!(effect, Emitted::Schedule { input, .. } if input.starts_with("PongDeadline"))).count();
        if pings > 0 {
            self.ping_counter = before;
        }
        let got = Out { pings, pongs, live: twin.state().mode() == f::Mode::Live };
        let expected = Out { pings: want.iter().filter(|effect| **effect == Effect::SealPing).count(), pongs: want.iter().filter(|effect| **effect == Effect::SchedulePong).count(), live: twin.reference.mode == Mode::Live };
        (got, expected)
    }

    fn run(&mut self, plan: &Plan, failures: &mut Vec<String>) -> Vec<Out> {
        let mut outs = Vec::new();
        for (index, (turn, at)) in plan.iter().enumerate() {
            let (got, expected) = self.turn(*turn, *at);
            if got != expected {
                failures.push(format!("turn {index} {turn:?} at {at}: the owner gave {got:?}, the time of the step gives {expected:?}"));
            }
            outs.push(got);
        }
        outs
    }
}

fn opened() -> Plan { vec![(Turn::Want, 0), (Turn::Handshake, HANDSHAKE_AT)] }

fn after(mut plan: Plan, turn: Turn, at: u64) -> Plan {
    plan.push((turn, at));
    plan
}

fn seeded_plan(seed: u64) -> Plan {
    const DELTAS: [u64; 7] = [1, 9_999, 10_000, 12_345, 24_999, 25_000, 25_001];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut roll = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut plan = opened();
    let mut at = HANDSHAKE_AT;
    for _ in 0..10 {
        let turn = [Turn::Keepalive, Turn::PongDeadline][roll(2) as usize];
        at += DELTAS[roll(DELTAS.len() as u64) as usize];
        plan.push((turn, at));
    }
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
    let mut failures = Vec::new();
    for (name, plan, last) in cases {
        let outs = Drive::new().run(&plan, &mut failures);
        if outs.last() != Some(&last) {
            failures.push(format!("{name}: the last turn gave {:?}, the rule gives {last:?}", outs.last()));
        }
    }
    for seed in 1..=8u64 {
        let plan = seeded_plan(seed);
        Drive::new().run(&plan, &mut failures);
    }
    assert!(failures.is_empty(), "{} lines of the rule fail:\n{}", failures.len(), failures.join("\n"));
}

fn debounce_from_step_time() {
    let mut drive = Drive::new();
    drive.run(&opened(), &mut Vec::new());
    let at = 2_000;
    take(&drive.twin.log);
    drive.twin.det.tick(Time(at));
    for input in input_of(Turn::Event, &drive.twin, 0) {
        drive.twin.post(input);
    }
    let scheduled = take(&drive.twin.log);
    let debounce = std::time::Duration::from_millis(DEBOUNCE_MS);
    assert_eq!(scheduled.iter().filter(|effect| matches!(effect, Emitted::Schedule { after, input, .. } if *after == debounce && input.starts_with("Tick"))).count(), 1, "the landing of the batch is ordered after the debounce: {scheduled:?}");
    let landed = |log: &Spied| take(log).iter().filter(|effect| matches!(effect, Emitted::Send { to: STORE, .. })).count();
    drive.twin.det.tick(Time(at + DEBOUNCE_MS - 1));
    drive.twin.run();
    assert_eq!(landed(&drive.twin.log), 0, "one ms before the term of the step time the batch waits");
    drive.twin.det.tick(Time(at + DEBOUNCE_MS));
    drive.twin.run();
    assert_eq!(landed(&drive.twin.log), 1, "at the term of the step time the batch lands");
}
