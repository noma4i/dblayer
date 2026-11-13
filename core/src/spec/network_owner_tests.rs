//! Owner-level tests of the `Network` owner on `DetScheduler`: input -> effects and answers.

use crate::owners::lifecycle::Peer;
use crate::owners::network::{Network, NetworkEffect, NetworkInput, NetworkState};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, DetScheduler, EpochStamp};

struct Twin {
    det: DetScheduler,
    addr: Address<Network>,
    log: Log<NetworkEffect>,
    history: Vec<NetworkEffect>,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let addr = det.add::<Network>("network", NetworkState::new(), Recorder::<Network>::new(&log));
        Self { det, addr, log, history: Vec::new() }
    }

    /// Posts one input, runs the owner quiet and returns the effects of that step without the answers.
    fn feed(&mut self, input: NetworkInput) -> Vec<NetworkEffect> {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().copied());
        raw.into_iter().filter(|effect| !matches!(effect, NetworkEffect::Reply { .. })).collect()
    }

    fn replies(&self) -> Vec<bool> { self.history.iter().filter_map(|effect| match effect { NetworkEffect::Reply { closed, .. } => Some(*closed), _ => None }).collect() }
}

fn online(online: bool) -> NetworkInput {
    static STAMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NetworkInput::SetOnline { online, stamp: STAMP.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1, reply: 1 }
}
fn saving(saving: bool) -> NetworkInput { NetworkInput::PowerSaving { saving, reply: 1 } }
fn foreground() -> NetworkInput { NetworkInput::Foreground { reply: 1 } }
fn snapshot(owner: Peer, epoch: u64) -> NetworkInput { NetworkInput::SnapshotRequest { owner, epoch } }
fn broadcast(online: bool, online_seq: u64) -> NetworkEffect { NetworkEffect::Broadcast { online, online_seq } }

#[test]
fn net_online_broadcasts_only_a_change() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(online(false)), vec![broadcast(false, 0)], "the first value is a value");
    assert_eq!(twin.feed(online(true)), vec![broadcast(true, 1)]);
    assert!(twin.feed(online(true)).is_empty(), "the same value gives no broadcast");
}

#[test]
fn net_open_before_first_value() {
    for first in [true, false] {
        let mut twin = Twin::new();
        assert!(twin.feed(snapshot(Peer::Calls, 1)).is_empty());
        assert!(twin.feed(snapshot(Peer::Session, 1)).is_empty());
        let seq = u64::from(first);
        let answered = twin.feed(online(first));
        assert_eq!(answered, vec![
            broadcast(first, seq),
            NetworkEffect::Snapshot { owner: Peer::Calls, epoch: 1, online: first, online_seq: seq, saving: None },
            NetworkEffect::Snapshot { owner: Peer::Session, epoch: 1, online: first, online_seq: seq, saving: None },
        ]);
        assert!(twin.feed(online(first)).is_empty(), "each waiting request is answered once");
    }
}

#[test]
fn net_foreground_numbers_every_call() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(foreground()), vec![NetworkEffect::Foreground(1)]);
    assert_eq!(twin.feed(foreground()), vec![NetworkEffect::Foreground(2)]);
}

#[test]
fn net_power_saving_broadcasts_only_a_change() {
    let mut twin = Twin::new();
    assert!(twin.feed(saving(false)).is_empty());
    assert_eq!(twin.feed(saving(true)), vec![NetworkEffect::PowerSaving(true)]);
    assert!(twin.feed(saving(true)).is_empty());
}

#[test]
fn net_snapshot_request_carries_the_values_at_the_request() {
    let mut twin = Twin::new();
    assert!(twin.feed(snapshot(Peer::Outbox, 1)).is_empty(), "no value yet: the request waits");
    assert_eq!(twin.feed(online(true)), vec![broadcast(true, 1), NetworkEffect::Snapshot { owner: Peer::Outbox, epoch: 1, online: true, online_seq: 1, saving: None }]);
    twin.feed(saving(true));
    assert_eq!(twin.feed(snapshot(Peer::Transfer, 2)), vec![NetworkEffect::Snapshot { owner: Peer::Transfer, epoch: 2, online: true, online_seq: 1, saving: Some(true) }]);
    assert_eq!(twin.feed(snapshot(Peer::Windows, 2)), vec![NetworkEffect::Snapshot { owner: Peer::Windows, epoch: 2, online: true, online_seq: 1, saving: Some(true) }]);
}

#[test]
fn net_closing_drains_and_a_closed_network_answers_closed() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(NetworkInput::Closing).len(), 1, "Closing drains");
    assert!(matches!(twin.history.last(), Some(NetworkEffect::Drained(_))));
    for input in [online(true), foreground(), saving(true), snapshot(Peer::Outbox, 1), NetworkInput::Closing] {
        assert!(twin.feed(input).is_empty(), "a closed Network gives no effect");
    }
    assert_eq!(twin.replies(), vec![true, true, true], "the three calls are answered Err(Closed)");
}

#[test]
fn net_calls_are_answered_with_the_step_and_closed_after_closing() {
    for input in [(|| online(true)) as fn() -> NetworkInput, foreground, || saving(true)] {
        let mut twin = Twin::new();
        twin.feed(input());
        assert_eq!(twin.replies(), vec![false], "the answer comes with the step, also for a repeated value");
        twin.feed(input());
        twin.feed(NetworkInput::Closing);
        twin.feed(input());
        assert_eq!(twin.replies(), vec![false, false, true], "after Closing the call is answered Err(Closed)");
    }
}

fn random_inputs(seed: u64, len: usize) -> Vec<NetworkInput> {
    let mut state = seed;
    (0..len).map(|_| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        match (state >> 33) % 7 {
            0 => online(true),
            1 | 2 => online(false),
            3 => snapshot(Peer::Outbox, 1),
            4 => foreground(),
            5 => saving((state >> 41).is_multiple_of(2)),
            _ => if (state >> 40).is_multiple_of(9) { NetworkInput::Closing } else { online(true) },
        }
    }).collect()
}

#[test]
fn net_prop_broadcast_per_change() {
    for seed in 1..=40 {
        let mut twin = Twin::new();
        let (mut value, mut changes, mut epochs, mut waiting) = (None, 0, 0, 0);
        for input in random_inputs(seed, 30) {
            match input {
                NetworkInput::SetOnline { online, .. } => { if Some(online) != value { changes += 1; epochs += std::mem::take(&mut waiting); value = Some(online); } }
                NetworkInput::SnapshotRequest { .. } => if value.is_some() { epochs += 1 } else { waiting += 1 },
                NetworkInput::Closing => { twin.feed(input); break; }
                _ => {}
            }
            twin.feed(input);
        }
        let broadcasts = twin.history.iter().filter(|effect| matches!(effect, NetworkEffect::Broadcast { .. } | NetworkEffect::Snapshot { .. })).count();
        assert_eq!(broadcasts, changes + epochs, "one Online per change and per EpochStarted answered after the first value");
    }
}

fn orders(items: &[NetworkInput]) -> Vec<Vec<NetworkInput>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|at| {
        let mut rest = items.to_vec();
        let head = rest.remove(at);
        orders(&rest).into_iter().map(move |mut tail| { tail.insert(0, head); tail })
    }).collect()
}

#[test]
fn net_prop_snapshot_after_create() {
    let inputs = [online(true), snapshot(Peer::Calls, 1), snapshot(Peer::Outbox, 1), online(false), snapshot(Peer::Feed, 2)];
    for order in orders(&inputs) {
        let mut twin = Twin::new();
        let (mut value, mut applied, mut answered, mut waiting) = (None, 0, Vec::new(), Vec::new());
        for input in order {
            match input {
                NetworkInput::SetOnline { online, stamp, .. } => { if stamp > applied { if value.is_none() { answered.extend(waiting.drain(..).map(|(owner, epoch)| (owner, epoch, online))); } value = Some(online); applied = stamp; } }
                NetworkInput::SnapshotRequest { owner, epoch } => match value { Some(known) => answered.push((owner, epoch, known)), None => waiting.push((owner, epoch)) },
                _ => {}
            }
            twin.feed(input);
        }
        let got: Vec<(Peer, u64, bool)> = twin.history.iter().filter_map(|effect| match effect {
            NetworkEffect::Snapshot { owner, epoch, online, .. } => Some((*owner, *epoch, *online)),
            _ => None,
        }).collect();
        assert_eq!(got, answered, "each owner of each epoch gets exactly one first snapshot carrying the value known at its answer, a request before the first value is answered by it, in any delivery order");
    }
}

#[test]
fn net_set_online_stale_stamp() {
    let mut twin = Twin::new();
    let stamped = |online: bool, stamp: u64| NetworkInput::SetOnline { online, stamp, reply: 1 };
    assert_eq!(twin.feed(stamped(true, 2)), vec![broadcast(true, 1)]);
    assert!(twin.feed(stamped(false, 1)).is_empty(), "a value older than the applied one changes nothing");
    assert!(twin.feed(stamped(false, 2)).is_empty(), "the applied stamp again changes nothing");
    assert_eq!(twin.feed(snapshot(Peer::Outbox, 1)), vec![NetworkEffect::Snapshot { owner: Peer::Outbox, epoch: 1, online: true, online_seq: 1, saving: None }]);
    assert_eq!(twin.replies(), vec![false, false, false], "each call is answered with its step");
}
