use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{Map, Value};

use crate::identity::RowId;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::TransportError;

use super::super::error::SyncError;
use super::report::SendResult;

pub(super) const UNREAD_LIMIT: usize = 2048;

struct Outcome {
    model: String,
    action_key: String,
    rows: Vec<RowId>,
    result: SendResult,
    answer: Option<Value>,
}

#[derive(Default)]
pub(crate) struct Outcomes {
    unread: BTreeMap<i64, Outcome>,
    lost: usize,
}

impl Outcomes {
    #[cfg(test)]
    pub(crate) fn lost(&self) -> usize {
        self.lost
    }

    fn forget_answer(&mut self, sequence: i64) {
        if self.unread.get(&sequence).is_some_and(|outcome| outcome.answer.is_some()) {
            self.unread.remove(&sequence);
        }
    }

    fn forget_refusal(&mut self, sequence: i64) {
        if self.unread.get(&sequence).is_some_and(|outcome| outcome.answer.is_none()) {
            self.unread.remove(&sequence);
        }
    }

    pub(crate) fn answered(&mut self, sequence: i64, model: &str, action_key: &str, rows: Vec<RowId>, answer: Value) {
        self.insert(sequence, Outcome { model: model.to_string(), action_key: action_key.to_string(), rows, result: SendResult::Committed, answer: Some(answer) });
    }

    pub(crate) fn keep(&mut self, sequence: i64, model: &str, action_key: &str, rows: Vec<RowId>, result: &SendResult) {
        if matches!(result, SendResult::Deferred | SendResult::Skipped(_)) {
            return;
        }
        if *result == SendResult::Committed {
            if self.unread.get(&sequence).is_some_and(|outcome| outcome.answer.is_none()) {
                self.unread.remove(&sequence);
            }
            return;
        }
        self.insert(sequence, Outcome { model: model.to_string(), action_key: action_key.to_string(), rows, result: result.clone(), answer: None });
    }

    fn insert(&mut self, sequence: i64, outcome: Outcome) {
        self.unread.remove(&sequence);
        if self.unread.len() >= UNREAD_LIMIT && self.unread.pop_first().is_some() {
            self.lost += 1;
        }
        self.unread.insert(sequence, outcome);
    }

    pub(crate) fn failure_of(&mut self, model: &str, action_key: Option<&str>, row: &RowId, waiting: &[i64]) -> Option<SyncError> {
        let sequence = self
            .unread
            .iter()
            .find(|(sequence, outcome)| outcome.answer.is_none() && outcome.model == model && action_key.is_none_or(|key| outcome.action_key == key) && outcome.rows.contains(row) && !waiting.contains(sequence))
            .map(|(sequence, _)| *sequence)?;
        let outcome = self.unread.remove(&sequence)?;
        Some(error_of(model, &outcome.result, &row.to_string()))
    }

    pub(crate) fn refusal_of(&mut self, model: &str, sequence: i64) -> Option<SyncError> {
        if !self.unread.get(&sequence).is_some_and(|outcome| outcome.answer.is_none() && outcome.model == model) { return None; }
        let outcome = self.unread.remove(&sequence)?;
        let subject = outcome.rows.first().map_or_else(|| sequence.to_string(), ToString::to_string);
        Some(error_of(model, &outcome.result, &subject))
    }

    pub(crate) fn take(&mut self, model: &str, sequence: i64) -> Result<Value, SyncError> {
        let outcome = match self.unread.get(&sequence) {
            Some(outcome) if outcome.model == model => self.unread.remove(&sequence),
            _ => None,
        };
        match outcome {
            Some(Outcome { result: SendResult::Committed, answer, .. }) => Ok(answer.unwrap_or_else(|| Value::Object(Map::new()))),
            Some(outcome) => Err(error_of(model, &outcome.result, &sequence.to_string())),
            None => Err(SyncError::Store(format!("{model} operation {sequence}: the answer is not kept"))),
        }
    }
}

pub(super) struct KeptAnswer<'a> {
    outcomes: &'a Mutex<Outcomes>,
    sequence: i64,
    committed: bool,
}

impl<'a> KeptAnswer<'a> {
    pub(super) fn new(outcomes: &'a Mutex<Outcomes>, sequence: i64) -> Self {
        Self { outcomes, sequence, committed: false }
    }

    pub(super) fn committed(&mut self) {
        self.committed = true;
    }
}

impl Drop for KeptAnswer<'_> {
    fn drop(&mut self) {
        if !self.committed {
            super::super::locked(self.outcomes).forget_answer(self.sequence);
        }
    }
}

pub(super) struct KeptRefusal<'a> {
    outcomes: &'a Mutex<Outcomes>,
    sequence: i64,
    landed: bool,
}

impl<'a> KeptRefusal<'a> {
    pub(super) fn new(outcomes: &'a Mutex<Outcomes>, sequence: i64) -> Self {
        Self { outcomes, sequence, landed: false }
    }

    pub(super) fn landed(&mut self) {
        self.landed = true;
    }
}

impl Drop for KeptRefusal<'_> {
    fn drop(&mut self) {
        if !self.landed {
            super::super::locked(self.outcomes).forget_refusal(self.sequence);
        }
    }
}

pub(super) fn error_of(model: &str, result: &SendResult, subject: &str) -> SyncError {
    match result {
        SendResult::Rejected(OperationFailure { kind: FailureKind::Session(code), message, .. }) => SyncError::Transport(TransportError::Session { code: *code, detail: message.clone() }),
        SendResult::Rejected(OperationFailure { kind: FailureKind::Network, message, .. }) => SyncError::Transport(TransportError::Network(message.clone())),
        SendResult::Rejected(failure) => SyncError::Graphql { messages: vec![failure.message.clone()], extensions: failure.extensions.clone() },
        SendResult::Discarded => SyncError::Cancelled,
        _ => SyncError::Store(format!("{model} {subject}: the answer is unknown")),
    }
}
