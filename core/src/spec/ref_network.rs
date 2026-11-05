//! Executable table of `specs/machines/network.md`, one match arm per row.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    SetOnline(bool),
    Foreground,
    PowerSaving(bool),
    EpochStarted,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// `Broadcast Online(value)` to every owner.
    Broadcast(bool),
    /// `Online(online)` to the owners of the epoch.
    EpochOnline(bool),
    /// `Broadcast Foreground(foreground_seq)` to Transfer and Rounds.
    Foreground(u64),
    /// `Broadcast PowerSaving(value)` to Transfer.
    PowerSaving(bool),
    /// `PowerSaving(saving)` to Transfer at `EpochStarted`.
    EpochPowerSaving(bool),
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
            (State::Open { online, saving, .. }, Input::EpochStarted) => Out { effects: vec![Effect::EpochOnline(online), Effect::EpochPowerSaving(saving)], reply: None },
            (State::Open { .. }, Input::Closing) => {
                self.state = State::Closed;
                Out { effects: vec![Effect::Drained], reply: None }
            }
            (State::Closed, Input::SetOnline(_) | Input::Foreground | Input::PowerSaving(_)) => Out { effects: vec![], reply: Some(Reply::Closed) },
            (State::Closed, _) => Out { effects: vec![], reply: None },
        }
    }
}
