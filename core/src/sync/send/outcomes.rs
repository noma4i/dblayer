use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::identity::RowId;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::TransportError;

use super::super::error::SyncError;
use super::report::SendResult;

pub(super) const UNREAD_LIMIT: usize = 2048;

struct Outcome {
    model: String,
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

    pub(crate) fn answered(&mut self, sequence: i64, model: &str, rows: Vec<RowId>, answer: Value) {
        self.insert(sequence, Outcome { model: model.to_string(), rows, result: SendResult::Committed, answer: Some(answer) });
    }

    pub(crate) fn keep(&mut self, sequence: i64, model: &str, rows: Vec<RowId>, result: &SendResult) {
        if matches!(result, SendResult::Deferred | SendResult::Skipped(_)) {
            return;
        }
        if *result == SendResult::Committed {
            if self.unread.get(&sequence).is_some_and(|outcome| outcome.answer.is_none()) {
                self.unread.remove(&sequence);
            }
            return;
        }
        self.insert(sequence, Outcome { model: model.to_string(), rows, result: result.clone(), answer: None });
    }

    fn insert(&mut self, sequence: i64, outcome: Outcome) {
        self.unread.remove(&sequence);
        if self.unread.len() >= UNREAD_LIMIT && self.unread.pop_first().is_some() {
            self.lost += 1;
        }
        self.unread.insert(sequence, outcome);
    }

    pub(crate) fn failure_of(&mut self, model: &str, row: &RowId, waiting: &[i64]) -> Option<SyncError> {
        let sequence = self
            .unread
            .iter()
            .find(|(sequence, outcome)| outcome.answer.is_none() && outcome.model == model && outcome.rows.contains(row) && !waiting.contains(sequence))
            .map(|(sequence, _)| *sequence)?;
        let outcome = self.unread.remove(&sequence)?;
        Some(error_of(model, &outcome.result, &row.to_string()))
    }

    pub(crate) fn take(&mut self, model: &str, sequence: i64) -> Result<Value, SyncError> {
        let outcome = self.unread.remove(&sequence).filter(|outcome| outcome.model == model);
        match outcome {
            Some(Outcome { result: SendResult::Committed, answer, .. }) => Ok(answer.unwrap_or_else(|| Value::Object(Map::new()))),
            Some(outcome) => Err(error_of(model, &outcome.result, &sequence.to_string())),
            None => Err(SyncError::Store(format!("{model} operation {sequence}: the answer is not kept"))),
        }
    }
}

fn error_of(model: &str, result: &SendResult, subject: &str) -> SyncError {
    match result {
        SendResult::Rejected(OperationFailure { kind: FailureKind::Session(code), message }) => SyncError::Transport(TransportError::Session { code: *code, detail: message.clone() }),
        SendResult::Rejected(failure) => SyncError::Graphql(vec![failure.message.clone()]),
        SendResult::Discarded => SyncError::Cancelled,
        _ => SyncError::Store(format!("{model} {subject}: the answer is unknown")),
    }
}
