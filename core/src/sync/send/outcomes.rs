use std::collections::BTreeMap;

use crate::identity::RowId;

use super::super::error::SyncError;
use super::report::SendResult;

pub(super) const UNREAD_LIMIT: usize = 2048;

struct Outcome {
    model: String,
    rows: Vec<RowId>,
    result: SendResult,
}

#[derive(Default)]
pub(crate) struct Outcomes {
    unread: BTreeMap<i64, Outcome>,
    lost: usize,
}

impl Outcomes {
    pub(crate) fn lost(&self) -> usize {
        self.lost
    }

    pub(crate) fn keep(&mut self, sequence: i64, model: &str, rows: Vec<RowId>, result: &SendResult) {
        if matches!(result, SendResult::Deferred | SendResult::Skipped(_)) {
            return;
        }
        self.unread.remove(&sequence);
        if *result == SendResult::Committed {
            return;
        }
        if self.unread.len() >= UNREAD_LIMIT && self.unread.pop_first().is_some() {
            self.lost += 1;
        }
        self.unread.insert(sequence, Outcome { model: model.to_string(), rows, result: result.clone() });
    }

    pub(crate) fn failure_of(&mut self, model: &str, row: &RowId, waiting: &[i64]) -> Option<SyncError> {
        let sequence = self.unread.iter().find(|(sequence, outcome)| outcome.model == model && outcome.rows.contains(row) && !waiting.contains(sequence)).map(|(sequence, _)| *sequence)?;
        let outcome = self.unread.remove(&sequence)?;
        Some(match outcome.result {
            SendResult::Rejected(message) => SyncError::Graphql(message),
            SendResult::Discarded => SyncError::Cancelled,
            _ => SyncError::Store(format!("{model} {row}: the answer is unknown")),
        })
    }
}
