//! Owner-level tests of the Network table: the owner `Network` on `DetScheduler` against `ref_network.rs`. Names are the `Test` column.

use super::ref_network::{Effect as RefEffect, Input, Network as Ref, Owner, Reply as RefReply, State as RefState};
use crate::owners::lifecycle::Peer;
use crate::owners::network::{Network, NetworkEffect, NetworkInput, NetworkMode, NetworkState};
use crate::spec::support::{new_log, take, Log, Recorder};
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
            Input::Foreground => NetworkInput::Foreground { reply: 1 },
            Input::PowerSaving(saving) => NetworkInput::PowerSaving { saving, reply: 1 },
            Input::SnapshotRequest(owner, epoch) => NetworkInput::SnapshotRequest { owner: match owner { Owner::Transfer => Peer::Transfer, Owner::Outbox => Peer::Outbox, Owner::Calls => Peer::Calls, Owner::Session => Peer::Session, Owner::Window => Peer::Windows, Owner::Feed => Peer::Feed, Owner::Delivery => Peer::Deliveries, Owner::Network => Peer::Network, Owner::LocalWrites => Peer::LocalWrites, Owner::Rounds => Peer::Rounds }, epoch },
            Input::Closing => NetworkInput::Closing,
        };
        self.det.post(self.addr, EpochStamp::Control, owner_input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(input);
        let effects: Vec<RefEffect> = raw.iter().filter_map(|effect| match effect {
            NetworkEffect::Broadcast { online, online_seq } => Some(RefEffect::Broadcast(*online, *online_seq)),
            NetworkEffect::Snapshot { owner, epoch, online, online_seq, saving } => Some(RefEffect::Snapshot { owner: match owner { Peer::Transfer => Owner::Transfer, Peer::Outbox => Owner::Outbox, Peer::Calls => Owner::Calls, Peer::Session => Owner::Session, Peer::Windows => Owner::Window, Peer::Feed => Owner::Feed, Peer::Deliveries => Owner::Delivery, Peer::Network => Owner::Network, Peer::LocalWrites => Owner::LocalWrites, Peer::Rounds => Owner::Rounds }, epoch: *epoch, online: *online, online_seq: *online_seq, saving: *saving }),
            NetworkEffect::Foreground(seq) => Some(RefEffect::Foreground(*seq)),
            NetworkEffect::PowerSaving(value) => Some(RefEffect::PowerSaving(*value)),
            NetworkEffect::Drained(_) => Some(RefEffect::Drained),
            NetworkEffect::Reply { .. } => None,
        }).collect();
        let reply = raw.iter().find_map(|effect| match effect {
            NetworkEffect::Reply { closed, .. } => Some(if *closed { RefReply::Closed } else { RefReply::Ok }),
            _ => None,
        });
        assert_eq!(effects, want.effects, "the same effects");
        assert_eq!(reply, want.reply, "the same answer");
        let mode = match self.det.state(self.addr).expect("state").mode() {
            NetworkMode::Open { online, online_seq, foreground_seq, saving } => RefState::Open { online, online_seq, foreground_seq, saving },
            NetworkMode::Closed => RefState::Closed,
        };
        assert_eq!(mode, self.reference.state, "the same state");
        want.effects
    }

    fn replies(&self) -> Vec<bool> { self.history.iter().filter_map(|effect| match effect { NetworkEffect::Reply { closed, .. } => Some(*closed), _ => None }).collect() }
}

/// Every step feeds the owner and expects these effects.
fn echoes(steps: Vec<(Input, Vec<RefEffect>)>) {
    let mut twin = Twin::new();
    for (input, want) in steps {
        let label = format!("{input:?}");
        assert_eq!(twin.feed(input), want, "{label}");
    }
}

#[test]
fn net_set_online_changed() { echoes(vec![(Input::SetOnline(true), vec![RefEffect::Broadcast(true, 1)])]); }

#[test]
fn net_set_online_same() { echoes(vec![(Input::SetOnline(false), vec![]), (Input::SetOnline(true), vec![RefEffect::Broadcast(true, 1)]), (Input::SetOnline(true), vec![])]); }

#[test]
fn net_foreground() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(Input::Foreground), vec![RefEffect::Foreground(1)]);
    assert_eq!(twin.feed(Input::Foreground), vec![RefEffect::Foreground(2)]);
}

#[test]
fn net_power_saving_changed() { echoes(vec![(Input::PowerSaving(true), vec![RefEffect::PowerSaving(true)])]); }

#[test]
fn net_power_saving_same() { echoes(vec![(Input::PowerSaving(false), vec![]), (Input::PowerSaving(true), vec![RefEffect::PowerSaving(true)]), (Input::PowerSaving(true), vec![])]); }

#[test]
fn net_snapshot_request() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(snapshot()), vec![RefEffect::Snapshot { owner: Owner::Outbox, epoch: 1, online: false, online_seq: 0, saving: None }]);
    twin.feed(Input::SetOnline(true));
    twin.feed(Input::PowerSaving(true));
    assert_eq!(twin.feed(Input::SnapshotRequest(Owner::Transfer, 2)), vec![RefEffect::Snapshot { owner: Owner::Transfer, epoch: 2, online: true, online_seq: 1, saving: Some(true) }]);
}

fn snapshot() -> Input { Input::SnapshotRequest(Owner::Outbox, 1) }

#[test]
fn net_snapshot_windows() {
    let mut det = DetScheduler::new(3);
    let log = new_log();
    let addr = det.add::<Network>("network", NetworkState::new(), Recorder::<Network>::new(&log));
    det.post(addr, EpochStamp::Control, NetworkInput::PowerSaving { saving: true, reply: 1 });
    det.post(addr, EpochStamp::Control, NetworkInput::SnapshotRequest { owner: Peer::Windows, epoch: 2 });
    det.run_until_quiet(100).expect("the owner is quiet");
    assert!(take(&log).contains(&NetworkEffect::Snapshot { owner: Peer::Windows, epoch: 2, online: false, online_seq: 0, saving: Some(true) }));
}

#[test]
fn net_closing() { echoes(vec![(Input::Closing, vec![RefEffect::Drained])]); }

#[test]
fn net_closed() {
    let mut twin = Twin::new();
    twin.feed(Input::Closing);
    for input in [Input::SetOnline(true), Input::Foreground, Input::PowerSaving(true), snapshot(), Input::Closing] {
        assert!(twin.feed(input).is_empty(), "a closed Network gives no effect");
    }
    assert_eq!(twin.replies(), vec![true, true, true], "the three calls are answered Err(Closed)");
}

fn wait_answers(input: fn() -> Input) {
    let mut twin = Twin::new();
    twin.feed(input());
    assert_eq!(twin.replies(), vec![false], "the answer comes with the step, also for a repeated value");
    twin.feed(input());
    twin.feed(Input::Closing);
    twin.feed(input());
    assert_eq!(twin.replies(), vec![false, false, true], "after Closing the call is answered Err(Closed)");
}

#[test]
fn net_wait_set_online() { wait_answers(|| Input::SetOnline(true)); }

#[test]
fn net_wait_foreground() { wait_answers(|| Input::Foreground); }

#[test]
fn net_wait_power_saving() { wait_answers(|| Input::PowerSaving(true)); }

fn random_inputs(seed: u64, len: usize) -> Vec<Input> {
    let mut state = seed;
    (0..len).map(|_| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        match (state >> 33) % 7 {
            0 => Input::SetOnline(true),
            1 | 2 => Input::SetOnline(false),
            3 => snapshot(),
            4 => Input::Foreground,
            5 => Input::PowerSaving((state >> 41).is_multiple_of(2)),
            _ => if (state >> 40).is_multiple_of(9) { Input::Closing } else { Input::SetOnline(true) },
        }
    }).collect()
}

#[test]
fn net_prop_broadcast_per_change() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let (mut online, mut changes, mut epochs, mut closed) = (false, 0, 0, false);
        for input in random_inputs(seed, 30) {
            if closed { break; }
            match input {
                Input::SetOnline(value) => { if value != online { changes += 1; online = value; } }
                Input::SnapshotRequest(..) => epochs += 1,
                Input::Closing => closed = true,
                Input::Foreground | Input::PowerSaving(_) => {}
            }
            twin.feed(input);
        }
        let broadcasts = twin.history.iter().filter(|effect| matches!(effect, NetworkEffect::Broadcast { .. } | NetworkEffect::Snapshot { .. })).count();
        assert_eq!(broadcasts, changes + epochs, "one Online per change and per EpochStarted");
    }
}

#[test]
fn net_prop_offline_silent() {
    let mut twin = Twin::new();
    twin.feed(Input::SetOnline(false));
    twin.feed(snapshot());
    twin.feed(Input::Foreground);
    twin.feed(Input::SetOnline(false));
    assert!(twin.history.iter().all(|effect| !matches!(effect, NetworkEffect::Broadcast { online: true, .. } | NetworkEffect::Snapshot { online: true, .. })), "no Online(true) goes out before SetOnline(true)");
}

#[test]
fn net_prop_single_source() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let mut last = false;
        for input in random_inputs(seed, 30) {
            twin.feed(input);
            for effect in &twin.history {
                match effect {
                    NetworkEffect::Broadcast { online: value, .. } | NetworkEffect::Snapshot { online: value, .. } => last = *value,
                    _ => {}
                }
            }
            if let Some(online) = twin.reference.online() {
                assert_eq!(online, last, "an owner holds the last Online it got");
            }
        }
    }
}

#[test]
fn net_prop_foreground_once() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let mut calls = 0;
        for input in random_inputs(seed, 30) {
            if input == Input::Foreground && twin.reference.online().is_some() { calls += 1; }
            twin.feed(input);
        }
        let sequence: Vec<u64> = twin.history.iter().filter_map(|effect| match effect { NetworkEffect::Foreground(seq) => Some(*seq), _ => None }).collect();
        assert_eq!(sequence, (1..=calls).collect::<Vec<u64>>(), "each Foreground broadcasts once with the next number");
    }
}

fn orders(items: &[Input]) -> Vec<Vec<Input>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|at| {
        let mut rest = items.to_vec();
        let head = rest.remove(at);
        orders(&rest).into_iter().map(move |mut tail| { tail.insert(0, head); tail })
    }).collect()
}

#[test]
fn net_prop_snapshot_after_create() {
    let inputs = [Input::SetOnline(true), Input::SnapshotRequest(Owner::Calls, 1), Input::SnapshotRequest(Owner::Outbox, 1), Input::SetOnline(false), Input::SnapshotRequest(Owner::Feed, 2)];
    for order in orders(&inputs) {
        let mut twin = Twin::new();
        let (mut online, mut asked, mut answered) = (false, 0, Vec::new());
        for input in order {
            match &input {
                Input::SetOnline(value) => online = *value,
                Input::SnapshotRequest(owner, epoch) => answered.push((*owner, *epoch, online)),
                _ => {}
            }
            asked += usize::from(matches!(input, Input::SnapshotRequest(..)));
            twin.feed(input);
        }
        let got: Vec<(Owner, u64, bool)> = twin.history.iter().filter_map(|effect| match effect {
            NetworkEffect::Snapshot { owner, epoch, online, .. } => Some((match owner { Peer::Calls => Owner::Calls, Peer::Outbox => Owner::Outbox, _ => Owner::Feed }, *epoch, *online)),
            _ => None,
        }).collect();
        assert_eq!(got.len(), asked, "each owner of each epoch gets exactly one first snapshot");
        assert_eq!(got, answered, "the snapshot carries the value at the request, in any delivery order");
    }
}
