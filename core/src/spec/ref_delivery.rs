//! `specs/machines/delivery.md` as code: one match arm per table row. No library code.

use std::collections::VecDeque;

use super::harness::{Cause, Val, WState};

pub(super) const JOURNAL_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Rows,
    Events,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    Open { kind: Kind, epoch_current: bool },
    Committed { revision: i64, touches: bool },
    ReadDone { revision: i64, rows: Vec<String> },
    ReadFailed { revision: i64 },
    WindowState(WState),
    ModelEvent { position: u64, id: String },
    ModelLost { first: u64, last: u64 },
    Cancel,
    EpochEnded { own: bool },
    ModelsReset { own_model: bool },
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Read(i64),
    DropOldRead,
    InterestPlus,
    InterestMinus,
    WantPlus,
    WantMinus,
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pulled {
    Value(Val),
    Waits,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum State {
    Nothing,
    Opening { kind: Kind, window: WState, read: i64 },
    Live { kind: Kind, revision: i64, rows: Vec<String>, window: WState, next: u64 },
    Ended,
}

pub(super) struct Delivery {
    pub(super) state: State,
    pub(super) journal: VecDeque<Val>,
    ended_given: bool,
}

impl Delivery {
    pub(super) fn new() -> Self { Self { state: State::Nothing, journal: VecDeque::new(), ended_given: false } }

    fn push(&mut self, value: Val) {
        if matches!(value, Val::Snapshot { .. }) { self.journal.retain(|kept| !matches!(kept, Val::Delta { .. } | Val::Snapshot { .. })); }
        self.journal.push_back(value);
        if self.journal.len() <= JOURNAL_LIMIT { return; }
        let mut first = u64::MAX;
        let mut last = 0u64;
        let mut at = None;
        while self.journal.len() > JOURNAL_LIMIT - 1 {
            let Some(index) = self.journal.iter().position(|kept| matches!(kept, Val::Event { .. } | Val::Lost { .. })) else { break };
            at.get_or_insert(index);
            match self.journal.remove(index).unwrap() {
                Val::Event { position, .. } => { first = first.min(position); last = last.max(position); }
                Val::Lost { first: a, last: b } => { first = first.min(a); last = last.max(b); }
                _ => unreachable!(),
            }
        }
        if let Some(index) = at { self.journal.insert(index, Val::Lost { first, last }); }
    }

    /// The row `Pull`: the first value of the journal, or the reader waits, or the stream is over.
    pub(super) fn pull(&mut self) -> Pulled {
        if let Some(value) = self.journal.pop_front() {
            if matches!(value, Val::Ended(_)) { self.ended_given = true; }
            return Pulled::Value(value);
        }
        if self.ended_given { Pulled::Done } else { Pulled::Waits }
    }

    pub(super) fn drain(&mut self) -> Vec<Val> {
        let mut all = Vec::new();
        while let Pulled::Value(value) = self.pull() { all.push(value); }
        all
    }

    fn end(&mut self, cause: Cause) {
        self.state = State::Ended;
        self.push(Val::Ended(cause));
    }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        let state = std::mem::replace(&mut self.state, State::Ended);
        match (state, input) {
            (State::Nothing, Input::Open { kind, epoch_current: true }) => match kind {
                Kind::Rows => { self.state = State::Opening { kind, window: WState::IDLE, read: 0 }; vec![Effect::Read(0), Effect::InterestPlus] }
                Kind::Events => { self.state = State::Live { kind, revision: 0, rows: Vec::new(), window: WState::IDLE, next: 1 }; vec![Effect::WantPlus] }
            },
            (State::Nothing, Input::Open { epoch_current: false, .. }) => { self.end(Cause::SessionEnded); Vec::new() }
            (State::Opening { kind, window, .. }, Input::ReadDone { revision, rows }) => {
                self.push(Val::Snapshot { rows: rows.clone(), state: window });
                self.state = State::Live { kind, revision, rows, window, next: 1 };
                Vec::new()
            }
            (State::Opening { kind, .. }, Input::ReadFailed { revision }) => {
                let failed = WState { phase: super::harness::Phase::Failed, error: true, loaded: false };
                self.push(Val::Snapshot { rows: Vec::new(), state: failed });
                self.state = State::Live { kind, revision, rows: Vec::new(), window: failed, next: 1 };
                Vec::new()
            }
            (State::Opening { kind, window, read }, Input::Committed { revision, touches }) => {
                self.state = State::Opening { kind, window, read: if touches { revision } else { read } };
                if touches { vec![Effect::Read(revision), Effect::DropOldRead] } else { Vec::new() }
            }
            (State::Opening { kind, read, .. }, Input::WindowState(window)) => { self.state = State::Opening { kind, window, read }; Vec::new() }
            (State::Opening { kind, window, read }, Input::ModelsReset { own_model }) => {
                self.state = State::Opening { kind, window, read };
                if own_model { vec![Effect::Read(read), Effect::DropOldRead] } else { Vec::new() }
            }
            (state @ State::Opening { .. }, Input::ModelEvent { .. } | Input::ModelLost { .. }) => { self.state = state; Vec::new() }
            (State::Opening { kind, .. } | State::Live { kind, .. }, Input::Cancel) => {
                self.end(Cause::Cancelled);
                vec![if kind == Kind::Rows { Effect::InterestMinus } else { Effect::WantMinus }]
            }
            (State::Opening { kind, .. } | State::Live { kind, .. }, Input::EpochEnded { own: true }) => {
                self.end(Cause::SessionEnded);
                vec![if kind == Kind::Rows { Effect::InterestMinus } else { Effect::WantMinus }]
            }
            (state @ (State::Opening { .. } | State::Live { .. }), Input::EpochEnded { own: false }) => { self.state = state; Vec::new() }
            (State::Opening { .. } | State::Live { .. }, Input::Closing) => { self.end(Cause::Closed); vec![Effect::Drained] }
            (State::Live { kind, revision, rows, window, next }, Input::Committed { revision: at, touches }) => {
                self.state = State::Live { kind, revision, rows, window, next };
                if touches { vec![Effect::Read(at)] } else { Vec::new() }
            }
            (State::Live { kind, revision, rows, window, next }, Input::ReadDone { revision: at, rows: now }) => {
                if at <= revision {
                    self.state = State::Live { kind, revision, rows, window, next };
                } else if now != rows {
                    let upserted = now.iter().filter(|id| !rows.contains(id)).cloned().collect();
                    let removed = rows.iter().filter(|id| !now.contains(id)).cloned().collect();
                    self.push(Val::Delta { upserted, removed });
                    self.state = State::Live { kind, revision: at, rows: now, window, next };
                } else {
                    self.state = State::Live { kind, revision, rows, window, next };
                }
                Vec::new()
            }
            (State::Live { kind, revision, rows, window, next }, Input::ReadFailed { .. }) => {
                let state = WState { phase: if rows.is_empty() { super::harness::Phase::Failed } else { super::harness::Phase::Loaded }, error: true, loaded: window.loaded };
                self.push(Val::Snapshot { rows: rows.clone(), state });
                self.state = State::Live { kind, revision, rows, window: state, next };
                Vec::new()
            }
            (State::Live { kind, revision, rows, window, next }, Input::WindowState(now)) => {
                if now != window { self.push(Val::Snapshot { rows: rows.clone(), state: now }); }
                self.state = State::Live { kind, revision, rows, window: now, next };
                Vec::new()
            }
            (State::Live { kind, revision, rows, window, next }, Input::ModelEvent { position, id }) => {
                let mut next = next;
                if kind == Kind::Events {
                    if position > next { self.push(Val::Lost { first: next, last: position - 1 }); }
                    if position >= next {
                        self.push(Val::Event { position, id });
                        next = position + 1;
                    }
                }
                self.state = State::Live { kind, revision, rows, window, next };
                Vec::new()
            }
            (State::Live { kind, revision, rows, window, next }, Input::ModelLost { first, last }) => {
                let mut next = next;
                if kind == Kind::Events {
                    self.push(Val::Lost { first, last });
                    next = last + 1;
                }
                self.state = State::Live { kind, revision, rows, window, next };
                Vec::new()
            }
            (State::Live { kind, revision, rows, window, next }, Input::ModelsReset { own_model }) => {
                self.state = State::Live { kind, revision, rows, window, next };
                if own_model { vec![Effect::Read(revision)] } else { Vec::new() }
            }
            (State::Ended, _) => { self.state = State::Ended; Vec::new() }
            (state, _) => {
                // The remaining pairs are cells that change nothing: `Open` on an open stream, a read input after `Ended`.
                self.state = state;
                Vec::new()
            }
        }
    }
}
