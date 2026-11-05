//! Owner `Network`: the only source of `online`.

use super::contract::Drained;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

use super::contract::ReplyId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    Open { online: bool },
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
    Drained(Drained),
    /// The answer to `set_online`.
    Reply { id: ReplyId, closed: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkInput {
    SetOnline { online: bool, reply: ReplyId },
    EpochStarted,
    SnapshotRequest { owner: SnapshotOwner, epoch: Epoch },
    Closing,
    Idle,
}

pub struct NetworkState {
    mode: NetworkMode,
}

impl NetworkState {
    /// `online = true` until the first `SetOnline` (`network.md`).
    pub fn new() -> Self { Self { mode: NetworkMode::Open { online: true } } }

    pub fn mode(&self) -> NetworkMode { self.mode }
}

pub struct Network;

impl StateOwner for Network {
    const EPOCHED: bool = false;

    type State = NetworkState;
    type Input = NetworkInput;
    type Effect = NetworkEffect;

    fn step(state: &mut NetworkState, input: NetworkInput) -> Effects<Self> {
        let run = |effect| vec![Effect::Run(effect)];
        match (state.mode, input) {
            (NetworkMode::Open { online }, NetworkInput::SetOnline { online: value, reply }) if value != online => {
                state.mode = NetworkMode::Open { online: value };
                vec![Effect::Run(NetworkEffect::Reply { id: reply, closed: false }), Effect::Run(NetworkEffect::Broadcast(value))]
            }
            (NetworkMode::Open { .. }, NetworkInput::SetOnline { reply, .. }) => run(NetworkEffect::Reply { id: reply, closed: false }),
            (NetworkMode::Open { online }, NetworkInput::EpochStarted) => run(NetworkEffect::EpochOnline(online)),
            (NetworkMode::Open { online }, NetworkInput::SnapshotRequest { owner, epoch }) => run(NetworkEffect::Snapshot { owner, epoch, online }),
            (NetworkMode::Open { .. }, NetworkInput::Closing) => {
                state.mode = NetworkMode::Closed;
                run(NetworkEffect::Drained(Drained { owner: "network" }))
            }
            (NetworkMode::Closed, NetworkInput::SetOnline { reply, .. }) => run(NetworkEffect::Reply { id: reply, closed: true }),
            (_, NetworkInput::Idle) | (NetworkMode::Closed, _) => Vec::new(),
        }
    }

    fn tick(_now: Time) -> NetworkInput { NetworkInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> NetworkInput { NetworkInput::Idle }

    fn closing() -> NetworkInput { NetworkInput::Closing }
}
