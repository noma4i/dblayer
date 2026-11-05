//! Executable table of `specs/machines/network.md`.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    SetOnline(bool),
    EpochStarted,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// `Broadcast Online(value)` to every owner.
    Broadcast(bool),
    /// `Online(online)` to the owners of the epoch.
    EpochOnline(bool),
    Drained,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Open { online: bool },
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Out {
    pub effects: Vec<Effect>,
    /// The answer to `set_online`, `None` for the inputs that have no caller.
    pub reply: Option<Reply>,
}

pub struct Network {
    pub state: State,
}

impl Network {
    pub fn new() -> Self { Self { state: State::Open { online: true } } }

    pub fn online(&self) -> Option<bool> {
        match self.state {
            State::Open { online } => Some(online),
            State::Closed => None,
        }
    }

    /// One row of the table per match arm.
    pub fn step(&mut self, input: Input) -> Out {
        match (self.state, input) {
            (State::Open { online }, Input::SetOnline(value)) if value != online => {
                self.state = State::Open { online: value };
                Out { effects: vec![Effect::Broadcast(value)], reply: Some(Reply::Ok) }
            }
            (State::Open { .. }, Input::SetOnline(_)) => Out { effects: vec![], reply: Some(Reply::Ok) },
            (State::Open { online }, Input::EpochStarted) => Out { effects: vec![Effect::EpochOnline(online)], reply: None },
            (State::Open { .. }, Input::Closing) => {
                self.state = State::Closed;
                Out { effects: vec![Effect::Drained], reply: None }
            }
            (State::Closed, Input::SetOnline(_)) => Out { effects: vec![], reply: Some(Reply::Closed) },
            (State::Closed, _) => Out { effects: vec![], reply: None },
        }
    }
}
