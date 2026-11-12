//! Owner `Network`: the only source of `online`, `foreground_seq` and `saving`.

use super::contract::{Drained, ReplyId};
use super::lifecycle::Peer;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    Open { online: bool, online_seq: u64, foreground_seq: u64, saving: bool },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkEffect {
    /// `Online(value, online_seq)` to every owner.
    Broadcast { online: bool, online_seq: u64 },
    /// `Online(online, online_seq, epoch)` and, for Transfer and Windows, `PowerSaving(saving, epoch)`.
    Snapshot { owner: Peer, epoch: Epoch, online: bool, online_seq: u64, saving: Option<bool> },
    /// `Foreground(seq)` to Rounds, Session and Windows.
    Foreground(u64),
    /// `PowerSaving(value)` to Transfer and Windows.
    PowerSaving(bool),
    Drained(Drained),
    /// The answer to `set_online`, `foreground`, `set_power_saving`.
    Reply { id: ReplyId, closed: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkInput {
    SetOnline { online: bool, stamp: u64, reply: ReplyId },
    Foreground { reply: ReplyId },
    PowerSaving { saving: bool, reply: ReplyId },
    SnapshotRequest { owner: Peer, epoch: Epoch },
    Closing,
    Idle,
}

pub struct NetworkState {
    mode: NetworkMode,
    online_stamp: u64,
}

impl Default for NetworkState {
    fn default() -> Self { Self::new() }
}

impl NetworkState {
    /// `online = false` and `saving = false` until the first input.
    pub fn new() -> Self { Self { mode: NetworkMode::Open { online: false, online_seq: 0, foreground_seq: 0, saving: false }, online_stamp: 0 } }

    pub fn mode(&self) -> NetworkMode { self.mode }
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
            (NetworkMode::Open { online, online_seq, foreground_seq, saving }, NetworkInput::SetOnline { online: value, stamp, reply: id }) if value != online => {
                state.online_stamp = stamp;
                let online_seq = online_seq + u64::from(value);
                state.mode = NetworkMode::Open { online: value, online_seq, foreground_seq, saving };
                run(vec![reply(id), NetworkEffect::Broadcast { online: value, online_seq }])
            }
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { stamp, reply: id, .. }) => {
                state.online_stamp = stamp;
                run(vec![reply(id)])
            }
            (NetworkMode::Open { online, online_seq, foreground_seq, saving }, NetworkInput::Foreground { reply: id }) => {
                state.mode = NetworkMode::Open { online, online_seq, foreground_seq: foreground_seq + 1, saving };
                run(vec![reply(id), NetworkEffect::Foreground(foreground_seq + 1)])
            }
            (NetworkMode::Open { online, online_seq, foreground_seq, saving }, NetworkInput::PowerSaving { saving: value, reply: id }) if value != saving => {
                state.mode = NetworkMode::Open { online, online_seq, foreground_seq, saving: value };
                run(vec![reply(id), NetworkEffect::PowerSaving(value)])
            }
            (NetworkMode::Open { .. }, NetworkInput::PowerSaving { reply: id, .. }) => run(vec![reply(id)]),
            (NetworkMode::Open { online, online_seq, saving, .. }, NetworkInput::SnapshotRequest { owner, epoch }) => run(vec![NetworkEffect::Snapshot { owner, epoch, online, online_seq, saving: matches!(owner, Peer::Transfer | Peer::Windows).then_some(saving) }]),
            (NetworkMode::Open { .. }, NetworkInput::Closing) => {
                state.mode = NetworkMode::Closed;
                run(vec![NetworkEffect::Drained(Drained { owner: "network" })])
            }
            (NetworkMode::Closed, NetworkInput::SetOnline { reply: id, .. } | NetworkInput::Foreground { reply: id } | NetworkInput::PowerSaving { reply: id, .. }) => run(vec![NetworkEffect::Reply { id, closed: true }]),
            (_, NetworkInput::Idle) | (NetworkMode::Closed, _) => Vec::new(),
        }
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> NetworkInput { NetworkInput::Idle }

    fn closing() -> NetworkInput { NetworkInput::Closing }
}
