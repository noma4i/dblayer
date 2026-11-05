//! Owner `Network` (`specs/machines/network.md`): the only source of `online`, `foreground_seq` and `saving`.

use super::contract::{Drained, ReplyId};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    Open { online: bool, foreground_seq: u64, saving: bool },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotOwner {
    Windows,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkEffect {
    /// `Online(value)` to every owner.
    Broadcast(bool),
    /// `Online(online)` to the owners of the new epoch.
    EpochOnline(bool),
    Snapshot { owner: SnapshotOwner, epoch: Epoch, online: bool },
    /// `Foreground(seq)` to Transfer and Rounds.
    Foreground(u64),
    /// `PowerSaving(value)` to Transfer.
    PowerSaving(bool),
    /// `PowerSaving(saving)` to Transfer at `EpochStarted`.
    EpochPowerSaving(bool),
    Drained(Drained),
    /// The answer to `set_online`, `foreground`, `set_power_saving`.
    Reply { id: ReplyId, closed: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkInput {
    SetOnline { online: bool, reply: ReplyId },
    Foreground { reply: ReplyId },
    PowerSaving { saving: bool, reply: ReplyId },
    EpochStarted,
    SnapshotRequest { owner: SnapshotOwner, epoch: Epoch },
    Closing,
    Idle,
}

pub struct NetworkState {
    mode: NetworkMode,
}

impl NetworkState {
    /// `online = false` and `saving = false` until the first input.
    pub fn new() -> Self { Self { mode: NetworkMode::Open { online: false, foreground_seq: 0, saving: false } } }

    pub fn mode(&self) -> NetworkMode { self.mode }
}

pub struct Network;

impl StateOwner for Network {
    const EPOCHED: bool = false;

    type State = NetworkState;
    type Input = NetworkInput;
    type Effect = NetworkEffect;

    fn step(state: &mut NetworkState, input: NetworkInput) -> Effects<Self> {
        let run = |effects: Vec<NetworkEffect>| effects.into_iter().map(Effect::Run).collect();
        let reply = |id| NetworkEffect::Reply { id, closed: false };
        match (state.mode, input) {
            (NetworkMode::Open { online, foreground_seq, saving }, NetworkInput::SetOnline { online: value, reply: id }) if value != online => {
                state.mode = NetworkMode::Open { online: value, foreground_seq, saving };
                run(vec![reply(id), NetworkEffect::Broadcast(value)])
            }
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { reply, .. }) => run(NetworkEffect::Reply { id: reply, closed: false }),
            (NetworkMode::Open { online }, NetworkInput::EpochStarted) => run(NetworkEffect::EpochOnline(online)),
            (NetworkMode::Open { online, .. }, NetworkInput::SnapshotRequest { owner, epoch }) => run(NetworkEffect::Snapshot { owner, epoch, online }),
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { reply: id, .. }) => run(vec![reply(id)]),
            (NetworkMode::Open { online, foreground_seq, saving }, NetworkInput::Foreground { reply: id }) => {
                state.mode = NetworkMode::Open { online, foreground_seq: foreground_seq + 1, saving };
                run(vec![reply(id), NetworkEffect::Foreground(foreground_seq + 1)])
            }
            (NetworkMode::Open { online, foreground_seq, saving }, NetworkInput::PowerSaving { saving: value, reply: id }) if value != saving => {
                state.mode = NetworkMode::Open { online, foreground_seq, saving: value };
                run(vec![reply(id), NetworkEffect::PowerSaving(value)])
            }
            (NetworkMode::Open { .. }, NetworkInput::PowerSaving { reply: id, .. }) => run(vec![reply(id)]),
            (NetworkMode::Open { online, saving, .. }, NetworkInput::EpochStarted) => run(vec![NetworkEffect::EpochOnline(online), NetworkEffect::EpochPowerSaving(saving)]),
            (NetworkMode::Open { .. }, NetworkInput::Closing) => {
                state.mode = NetworkMode::Closed;
                run(vec![NetworkEffect::Drained(Drained { owner: "network" })])
            }
            (NetworkMode::Closed, NetworkInput::SetOnline { reply: id, .. } | NetworkInput::Foreground { reply: id } | NetworkInput::PowerSaving { reply: id, .. }) => run(vec![NetworkEffect::Reply { id, closed: true }]),
            (_, NetworkInput::Idle) | (NetworkMode::Closed, _) => Vec::new(),
        }
    }

    fn tick(_now: Time) -> NetworkInput { NetworkInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> NetworkInput { NetworkInput::Idle }

    fn closing() -> NetworkInput { NetworkInput::Closing }
}
