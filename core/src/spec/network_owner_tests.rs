//! Owner-level tests of `specs/machines/network.md`: the owner `Network` on `DetScheduler` against `ref_network.rs`. Names are the `Test` column.

use super::ref_network::{Effect as RefEffect, Input, Network as Ref, Reply as RefReply, State as RefState};
use crate::owners::network::{Network, NetworkEffect, NetworkInput, NetworkMode, NetworkState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, DetScheduler, EpochStamp};

struct Twin {
    det: DetScheduler,
    addr: Address<Network>,
    log: Log<NetworkEffect>,
    reference: Ref,
    history: Vec<NetworkEffect>,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let addr = det.add::<Network>("network", NetworkState::new(), Recorder::<Network>::new(&log));
        Self { det, addr, log, reference: Ref::new(), history: Vec::new() }
    }

    fn feed(&mut self, input: Input) -> Vec<RefEffect> {
        let owner_input = match input {
            Input::SetOnline(online) => NetworkInput::SetOnline { online, reply: 1 },
            Input::EpochStarted => NetworkInput::EpochStarted,
            Input::Closing => NetworkInput::Closing,
        };
        self.det.post(self.addr, EpochStamp::Control, owner_input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(input);
        let effects: Vec<RefEffect> = raw.iter().filter_map(|effect| match effect {
            NetworkEffect::Broadcast(value) => Some(RefEffect::Broadcast(*value)),
            NetworkEffect::EpochOnline(value) => Some(RefEffect::EpochOnline(*value)),
            NetworkEffect::Drained(_) => Some(RefEffect::Drained),
            NetworkEffect::Reply { .. } => None,
        }).collect();
        let reply = raw.iter().find_map(|effect| match effect {
            NetworkEffect::Reply { closed, .. } => Some(if *closed { RefReply::Closed } else { RefReply::Ok }),
            _ => None,
        });
        assert_eq!(effects, want.effects, "the same effects");
        assert_eq!(reply, want.reply, "the same answer to set_online");
        let mode = match self.det.state(self.addr).expect("state").mode() {
            NetworkMode::Open { online } => RefState::Open { online },
            NetworkMode::Closed => RefState::Closed,
        };
        assert_eq!(mode, self.reference.state, "the same state");
        want.effects
    }
}

#[test]
fn net_set_online_changed() { assert_eq!(Twin::new().feed(Input::SetOnline(false)), vec![RefEffect::Broadcast(false)]); }

#[test]
fn net_set_online_same() {
    let mut twin = Twin::new();
    assert!(twin.feed(Input::SetOnline(true)).is_empty(), "online is true at open");
    twin.feed(Input::SetOnline(false));
    assert!(twin.feed(Input::SetOnline(false)).is_empty());
}

#[test]
fn net_epoch_started() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(Input::EpochStarted), vec![RefEffect::EpochOnline(true)]);
    twin.feed(Input::SetOnline(false));
    assert_eq!(twin.feed(Input::EpochStarted), vec![RefEffect::EpochOnline(false)]);
}

#[test]
fn net_closing() { assert_eq!(Twin::new().feed(Input::Closing), vec![RefEffect::Drained]); }

#[test]
fn net_closed() {
    let mut twin = Twin::new();
    twin.feed(Input::Closing);
    twin.feed(Input::SetOnline(true));
    twin.feed(Input::EpochStarted);
    twin.feed(Input::Closing);
}

#[test]
fn net_wait_set_online() {
    let mut twin = Twin::new();
    twin.feed(Input::SetOnline(true));
    assert!(matches!(twin.history.first(), Some(NetworkEffect::Reply { closed: false, .. })), "the answer comes with the step");
    twin.feed(Input::Closing);
    twin.feed(Input::SetOnline(false));
    assert!(matches!(twin.history.last(), Some(NetworkEffect::Reply { closed: true, .. })), "Closing is answered Err(Closed)");
}

fn random_inputs(seed: u64, len: usize) -> Vec<Input> {
    let mut state = seed;
    (0..len).map(|_| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        match (state >> 33) % 5 {
            0 => Input::SetOnline(true),
            1 | 2 => Input::SetOnline(false),
            3 => Input::EpochStarted,
            _ => if (state >> 40) % 9 == 0 { Input::Closing } else { Input::SetOnline(true) },
        }
    }).collect()
}

#[test]
fn net_prop_broadcast_per_change() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let (mut online, mut changes, mut epochs, mut closed) = (true, 0, 0, false);
        for input in random_inputs(seed, 30) {
            if closed { break; }
            match input {
                Input::SetOnline(value) => { if value != online { changes += 1; online = value; } }
                Input::EpochStarted => epochs += 1,
                Input::Closing => closed = true,
            }
            twin.feed(input);
        }
        let broadcasts = twin.history.iter().filter(|effect| matches!(effect, NetworkEffect::Broadcast(_) | NetworkEffect::EpochOnline(_))).count();
        assert_eq!(broadcasts, changes + epochs, "one Online per change and per EpochStarted");
    }
}

#[test]
fn net_prop_offline_silent() {
    let mut twin = Twin::new();
    twin.feed(Input::SetOnline(false));
    twin.feed(Input::EpochStarted);
    twin.feed(Input::SetOnline(false));
    assert!(twin.history.iter().all(|effect| matches!(effect, NetworkEffect::Broadcast(false) | NetworkEffect::EpochOnline(false) | NetworkEffect::Reply { .. })), "no Online(true) goes out before SetOnline(true)");
}

#[test]
fn net_prop_single_source() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let mut last = true;
        for input in random_inputs(seed, 30) {
            twin.feed(input);
            for effect in &twin.history {
                match effect {
                    NetworkEffect::Broadcast(value) | NetworkEffect::EpochOnline(value) => last = *value,
                    _ => {}
                }
            }
            if let Some(online) = twin.reference.online() {
                assert_eq!(online, last, "an owner holds the last Online it got");
            }
        }
    }
}
