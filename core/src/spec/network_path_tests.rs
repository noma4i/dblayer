//! D-491 and D-492 at the `Network` owner: the path of the phone, the bounce pair and the fresh connections.

use super::network_owner_tests::{broadcast, foreground, rotate, snapshot, Twin};
use crate::ffi::NetworkKind::{self, Cellular, Wifi};
use crate::owners::lifecycle::Peer;
use crate::owners::network::{NetworkEffect, NetworkInput};

fn path(online: bool, network: u64, kind: NetworkKind, stamp: u64) -> NetworkInput { NetworkInput::SetPath { online, network, kind, stamp, reply: 1 } }

fn direct(online: bool, stamp: u64) -> NetworkInput { NetworkInput::SetOnline { online, stamp, reply: 1 } }

/// The pair of one bounce at `seq`: `Online(false, seq)`, `Online(true, seq + 1)`, fresh connections before both.
fn bounce(seq: u64, cable_value: bool) -> Vec<NetworkEffect> {
    let wire = |online| Some(online).filter(|_| cable_value);
    vec![rotate(), NetworkEffect::Broadcast { online: false, online_seq: seq, cable: wire(false) }, NetworkEffect::Broadcast { online: true, online_seq: seq + 1, cable: wire(true) }]
}

#[test]
fn net_path_stale_stamp() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(path(true, 1, Wifi, 5)), vec![rotate(), broadcast(true, 1)]);
    assert!(twin.feed(path(false, 2, Cellular, 4)).is_empty(), "an older stamp changes nothing");
    assert!(twin.feed(path(true, 3, Cellular, 5)).is_empty(), "the applied stamp again changes nothing, the route stays");
    assert!(twin.feed(path(true, 1, Wifi, 6)).is_empty(), "the route of the stale input was not kept: the same path as the first is silent");
    assert_eq!(twin.replies(), vec![false; 4], "each input is answered");
}

#[test]
fn net_path_online_change() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(path(false, 1, NetworkKind::None, 1)), vec![broadcast(false, 0)], "no fresh connections while offline");
    assert_eq!(twin.feed(path(true, 2, Wifi, 2)), vec![rotate(), broadcast(true, 1)], "offline to online: the row of SetOnline plus fresh connections");
    assert_eq!(twin.feed(path(false, 3, NetworkKind::None, 3)), vec![broadcast(false, 1)]);
}

#[test]
fn net_path_first_route() {
    let mut twin = Twin::new();
    twin.feed(direct(true, 1));
    assert!(twin.feed(path(true, 7, Cellular, 100)).is_empty(), "the first path of an online owner only records the route");
    assert_eq!(twin.feed(path(true, 8, Wifi, 101)), bounce(1, true), "the next change is a change against the recorded route");
}

#[test]
fn net_path_change_bounces_owners() {
    let mut twin = Twin::new();
    twin.feed(path(true, 1, Wifi, 1));
    assert_eq!(twin.feed(path(true, 2, Cellular, 2)), bounce(1, true), "foreground: Feed hears false then true");
    assert_eq!(twin.feed(path(true, 2, Wifi, 3)), bounce(2, true), "a changed kind alone is a change");
    twin.feed(NetworkInput::Background { reply: 1 });
    assert_eq!(twin.feed(path(true, 3, Wifi, 4)), bounce(3, false), "background: Feed hears nothing");
}

#[test]
fn net_path_change_offline_silent() {
    let mut twin = Twin::new();
    twin.feed(path(false, 1, NetworkKind::None, 1));
    assert!(twin.feed(path(false, 2, NetworkKind::None, 2)).is_empty());
    assert!(twin.feed(path(false, 3, Wifi, 3)).is_empty(), "a route change while offline gives no effect");
    assert_eq!(twin.feed(path(true, 3, Wifi, 4)), vec![rotate(), broadcast(true, 1)], "the return is the online row, not a bounce");
}

#[test]
fn net_path_same_silent() {
    let mut twin = Twin::new();
    twin.feed(path(true, 4, Wifi, 1));
    assert!(twin.feed(path(true, 4, Wifi, 2)).is_empty());
    assert!(twin.feed(path(true, 4, Wifi, 3)).is_empty());
    assert!(twin.feed(direct(true, 9)).is_empty(), "the direct API with the same value is silent too");
}

#[test]
fn net_path_rotates_connections() {
    let mut twin = Twin::new();
    let rotations = |twin: &Twin| twin.history.iter().filter(|effect| matches!(effect, NetworkEffect::RotateConnections)).count();
    twin.feed(path(false, 1, NetworkKind::None, 1));
    assert_eq!(rotations(&twin), 0, "offline does not rotate");
    twin.feed(path(true, 1, Wifi, 2));
    assert_eq!(rotations(&twin), 1, "offline to online rotates once");
    twin.feed(path(true, 2, Cellular, 3));
    assert_eq!(rotations(&twin), 2, "a path change while online rotates once");
    twin.feed(path(true, 2, Cellular, 4));
    assert_eq!(rotations(&twin), 2, "the same path does not rotate");
    twin.feed(direct(false, 10));
    twin.feed(direct(true, 11));
    assert_eq!(rotations(&twin), 3, "the return by the direct API rotates too");
    let first = twin.history.iter().position(|effect| matches!(effect, NetworkEffect::RotateConnections)).unwrap();
    assert!(matches!(twin.history[first + 1], NetworkEffect::Broadcast { .. }), "fresh connections come before the owners hear Online");
}

/// P-NET-14: each change of `(network, kind)` while online is exactly 1 pair in 1 step and 1 rotation, the repeat is silent, `online_seq` grows by 1.
#[test]
fn net_prop_path_bounce_once() {
    for seed in 1..=30u64 {
        let mut twin = Twin::new();
        twin.feed(path(true, 0, Wifi, 1));
        let (mut route, mut seq, mut stamp, mut state) = ((0, Wifi), 1, 1, seed);
        for _ in 0..40 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let next = ((state >> 33) % 3, if (state >> 40).is_multiple_of(2) { Wifi } else { Cellular });
            stamp += 1;
            let effects = twin.feed(path(true, next.0, next.1, stamp));
            if next == route {
                assert!(effects.is_empty(), "seed {seed}: the same path is silent");
            } else {
                assert_eq!(effects, bounce(seq, true), "seed {seed}: one pair and one rotation");
                seq += 1;
                route = next;
            }
        }
    }
}

/// P-NET-17: `Online(false)` comes only from a platform value or from the pair of a bounce; snapshots, the foreground and the power saving never lower it.
#[test]
fn net_prop_offline_only_from_phone() {
    for seed in 1..=30u64 {
        let mut twin = Twin::new();
        let (mut state, mut stamp) = (seed, 0);
        for _ in 0..60 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            stamp += 1;
            let effects = match (state >> 33) % 5 {
                0 => twin.feed(path(true, (state >> 40) % 3, Wifi, stamp)),
                1 => twin.feed(snapshot(Peer::Outbox, 1)),
                2 => twin.feed(foreground()),
                3 => twin.feed(NetworkInput::Background { reply: 1 }),
                _ => twin.feed(NetworkInput::PowerSaving { saving: state & 1 == 0, reply: 1 }),
            };
            let lowered = effects.iter().filter(|effect| matches!(effect, NetworkEffect::Broadcast { online: false, .. })).count();
            let raised = effects.iter().filter(|effect| matches!(effect, NetworkEffect::Broadcast { online: true, .. })).count();
            assert!(lowered == 0 || (lowered == 1 && raised == 1), "seed {seed}: a platform that never says offline gives `false` only inside the pair: {effects:?}");
        }
    }
}
