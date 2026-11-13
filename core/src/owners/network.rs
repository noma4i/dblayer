//! Owner `Network`: the only source of `online`, `foreground_seq` and `saving`.

use super::contract::{Drained, ReplyId};
use super::lifecycle::Peer;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    Open { online: Option<bool>, online_seq: u64, foreground_seq: u64, foreground: bool, saving: bool },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkEffect {
    /// `Online(value, online_seq)` to every owner.
    Broadcast { online: bool, online_seq: u64, cable: Option<bool> },
    /// `Online(cable, online_seq)` to Feed after a change of `foreground`.
    Cable { cable: bool, online_seq: u64 },
    /// `Online(online, online_seq, epoch)` and, for Transfer and Windows, `PowerSaving(saving, epoch)`.
    Snapshot { owner: Peer, epoch: Epoch, online: bool, online_seq: u64, saving: Option<bool> },
    /// `Foreground(seq)` to Rounds, Session and Windows, with the `online_seq` of the return.
    Foreground { seq: u64, online_seq: u64 },
    /// `PowerSaving(value)` to Transfer and Windows.
    PowerSaving(bool),
    Drained(Drained),
    /// The answer to `set_online`, `foreground`, `background`, `set_power_saving`.
    Reply { id: ReplyId, closed: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkInput {
    SetOnline { online: bool, stamp: u64, reply: ReplyId },
    Foreground { reply: ReplyId },
    Background { reply: ReplyId },
    PowerSaving { saving: bool, reply: ReplyId },
    SnapshotRequest { owner: Peer, epoch: Epoch },
    Closing,
    Idle,
}

pub struct NetworkState {
    mode: NetworkMode,
    online_stamp: u64,
    pending_snapshots: Vec<(Peer, Epoch)>,
}

impl Default for NetworkState {
    fn default() -> Self { Self::new() }
}

impl NetworkState {
    /// `online = None` until the first `SetOnline`, `saving = false` until the first input.
    pub fn new() -> Self { Self { mode: NetworkMode::Open { online: None, online_seq: 0, foreground_seq: 0, foreground: true, saving: false }, online_stamp: 0, pending_snapshots: Vec::new() } }

    pub fn mode(&self) -> NetworkMode { self.mode }
}

fn snapshot(owner: Peer, epoch: Epoch, online: bool, online_seq: u64, saving: bool, foreground: bool) -> NetworkEffect {
    let online = online && (foreground || owner != Peer::Feed);
    NetworkEffect::Snapshot { owner, epoch, online, online_seq, saving: matches!(owner, Peer::Transfer | Peer::Windows).then_some(saving) }
}

pub struct Network;

impl StateOwner for Network {
    const EPOCHED: bool = false;

    type State = NetworkState;
    type Input = NetworkInput;
    type Effect = NetworkEffect;

    fn step(state: &mut NetworkState, input: NetworkInput, _now: Time) -> Effects<Self> {
        let run = |effects: Vec<NetworkEffect>| effects.into_iter().map(Effect::Run).collect();
        let reply = |id| NetworkEffect::Reply { id, closed: false };
        match (state.mode, input) {
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { stamp, reply: id, .. }) if stamp <= state.online_stamp => run(vec![reply(id)]),
            (NetworkMode::Open { online, online_seq, foreground_seq, foreground, saving }, NetworkInput::SetOnline { online: value, stamp, reply: id }) if Some(value) != online => {
                state.online_stamp = stamp;
                let online_seq = online_seq + u64::from(value);
                state.mode = NetworkMode::Open { online: Some(value), online_seq, foreground_seq, foreground, saving };
                let cable = Some(value && foreground);
                let cable_changed = cable != online.map(|before| before && foreground);
                let mut effects = vec![reply(id), NetworkEffect::Broadcast { online: value, online_seq, cable: cable.filter(|_| cable_changed) }];
                effects.extend(std::mem::take(&mut state.pending_snapshots).into_iter().map(|(owner, epoch)| snapshot(owner, epoch, value, online_seq, saving, foreground)));
                run(effects)
            }
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { stamp, reply: id, .. }) => {
                state.online_stamp = stamp;
                run(vec![reply(id)])
            }
            (NetworkMode::Open { online, online_seq, foreground_seq, foreground, saving }, NetworkInput::Foreground { reply: id }) => {
                let reopens = !foreground && online == Some(true);
                let online_seq = online_seq + u64::from(reopens);
                state.mode = NetworkMode::Open { online, online_seq, foreground_seq: foreground_seq + 1, foreground: true, saving };
                let mut effects = vec![reply(id), NetworkEffect::Foreground { seq: foreground_seq + 1, online_seq }];
                if reopens { effects.push(NetworkEffect::Cable { cable: true, online_seq }); }
                run(effects)
            }
            (NetworkMode::Open { online, online_seq, foreground_seq, foreground, saving }, NetworkInput::Background { reply: id }) => {
                state.mode = NetworkMode::Open { online, online_seq, foreground_seq, foreground: false, saving };
                let mut effects = vec![reply(id)];
                if foreground && online == Some(true) { effects.push(NetworkEffect::Cable { cable: false, online_seq }); }
                run(effects)
            }
            (NetworkMode::Open { online, online_seq, foreground_seq, foreground, saving }, NetworkInput::PowerSaving { saving: value, reply: id }) if value != saving => {
                state.mode = NetworkMode::Open { online, online_seq, foreground_seq, foreground, saving: value };
                run(vec![reply(id), NetworkEffect::PowerSaving(value)])
            }
            (NetworkMode::Open { .. }, NetworkInput::PowerSaving { reply: id, .. }) => run(vec![reply(id)]),
            (NetworkMode::Open { online: None, .. }, NetworkInput::SnapshotRequest { owner, epoch }) => {
                state.pending_snapshots.push((owner, epoch));
                Vec::new()
            }
            (NetworkMode::Open { online: Some(online), online_seq, foreground, saving, .. }, NetworkInput::SnapshotRequest { owner, epoch }) => run(vec![snapshot(owner, epoch, online, online_seq, saving, foreground)]),
            (NetworkMode::Open { .. }, NetworkInput::Closing) => {
                state.mode = NetworkMode::Closed;
                state.pending_snapshots.clear();
                run(vec![NetworkEffect::Drained(Drained { owner: "network" })])
            }
            (NetworkMode::Closed, NetworkInput::SetOnline { reply: id, .. } | NetworkInput::Foreground { reply: id } | NetworkInput::Background { reply: id } | NetworkInput::PowerSaving { reply: id, .. }) => run(vec![NetworkEffect::Reply { id, closed: true }]),
            (_, NetworkInput::Idle) | (NetworkMode::Closed, _) => Vec::new(),
        }
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> NetworkInput { NetworkInput::Idle }

    fn closing() -> NetworkInput { NetworkInput::Closing }
}
