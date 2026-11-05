//! Executable table of the Network owner, one match arm per row.

pub use super::ref_lifecycle::Owner;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    SetOnline(bool),
    Foreground,
    PowerSaving(bool),
    SnapshotRequest(Owner, u64),
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// `Broadcast Online(value)` to every owner.
    Broadcast(bool),
    /// `Send owner Online(online, epoch)`, for Transfer also `PowerSaving(saving, epoch)`.
    Snapshot { owner: Owner, epoch: u64, online: bool, saving: Option<bool> },
    /// `Broadcast Foreground(foreground_seq)` to Rounds and Session.
    Foreground(u64),
    /// `Broadcast PowerSaving(value)` to Transfer.
    PowerSaving(bool),
    Drained,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Open { online: bool, foreground_seq: u64, saving: bool },
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Out {
    pub effects: Vec<Effect>,
    /// The answer to `set_online`, `foreground`, `set_power_saving`; `None` for the inputs that have no caller.
    pub reply: Option<Reply>,
}

pub struct Network {
    pub state: State,
}

impl Network {
    /// `online = false` and `saving = false` until the first input (network.md, values).
    pub fn new() -> Self { Self { state: State::Open { online: false, foreground_seq: 0, saving: false } } }

    pub fn online(&self) -> Option<bool> {
        match self.state {
            State::Open { online, .. } => Some(online),
            State::Closed => None,
        }
    }

    pub fn step(&mut self, input: Input) -> Out {
        let ok = |effects| Out { effects, reply: Some(Reply::Ok) };
        match (self.state, input) {
            (State::Open { online, foreground_seq, saving }, Input::SetOnline(value)) if value != online => {
                self.state = State::Open { online: value, foreground_seq, saving };
                ok(vec![Effect::Broadcast(value)])
            }
            (State::Open { .. }, Input::SetOnline(_)) => ok(vec![]),
            (State::Open { online, foreground_seq, saving }, Input::Foreground) => {
                self.state = State::Open { online, foreground_seq: foreground_seq + 1, saving };
                ok(vec![Effect::Foreground(foreground_seq + 1)])
            }
            (State::Open { online, foreground_seq, saving }, Input::PowerSaving(value)) if value != saving => {
                self.state = State::Open { online, foreground_seq, saving: value };
                ok(vec![Effect::PowerSaving(value)])
            }
            (State::Open { .. }, Input::PowerSaving(_)) => ok(vec![]),
            (State::Open { online, saving, .. }, Input::SnapshotRequest(owner, epoch)) => Out { effects: vec![Effect::Snapshot { owner, epoch, online, saving: (owner == Owner::Transfer).then_some(saving) }], reply: None },
            (State::Open { .. }, Input::Closing) => {
                self.state = State::Closed;
                Out { effects: vec![Effect::Drained], reply: None }
            }
            (State::Closed, Input::SetOnline(_) | Input::Foreground | Input::PowerSaving(_)) => Out { effects: vec![], reply: Some(Reply::Closed) },
            (State::Closed, _) => Out { effects: vec![], reply: None },
        }
    }
}
