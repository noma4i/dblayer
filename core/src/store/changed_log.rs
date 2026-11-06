use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;

use crate::lock::lock as held;

use super::CommitResult;

const MAX_LOGGED_ROWS: usize = 8192;

pub(crate) type ChangedRow = (String, String);

fn changed_rows(result: &CommitResult) -> Vec<ChangedRow> {
    let written = result.rows.iter().map(|change| &change.row).chain(result.members.iter()).chain(result.renewed.iter());
    let replaced = result.replaced.iter().flat_map(|(from, to)| [(from.model.to_string(), from.id.to_string()), (from.model.to_string(), to.to_string())]);
    let mut rows: Vec<ChangedRow> = written.map(|row| (row.model.to_string(), row.id.to_string())).chain(replaced).collect();
    rows.sort();
    rows.dedup();
    rows
}

/// The changed rows of a commit as `(table, row_id)`: what `Committed.rows` carries.
pub(crate) fn table_rows(result: &CommitResult) -> BTreeSet<(String, String)> {
    changed_rows(result).into_iter().map(|(model, id)| (crate::ddl::table_name(&model), id)).collect()
}

#[derive(Default)]
pub(crate) struct ChangedLog(Mutex<Entries>);

#[derive(Default)]
struct Entries {
    items: VecDeque<Entry>,
    rows: usize,
    start_revision: i64,
    dropped_through: (i64, u64),
    logged_through: i64,
}

struct Entry {
    at: (i64, u64),
    rows: Option<Vec<ChangedRow>>,
}

impl ChangedLog {
    pub(crate) fn start(&self, revision: i64) {
        let mut log = held(&self.0);
        log.start_revision = revision;
        log.logged_through = revision;
    }

    pub(crate) fn log_commit(&self, result: &CommitResult) {
        let Some(revision) = result.revision else { return };
        self.push(Entry { at: (revision, result.position), rows: Some(changed_rows(result)) });
    }

    pub(crate) fn log_unknown(&self, revision: i64) { self.push(Entry { at: (revision, 0), rows: None }); }

    fn push(&self, entry: Entry) {
        let mut log = held(&self.0);
        log.logged_through = log.logged_through.max(entry.at.0);
        log.rows += entry.rows.as_ref().map_or(1, Vec::len);
        log.items.push_back(entry);
        while log.rows > MAX_LOGGED_ROWS {
            let Some(dropped) = log.items.pop_front() else { break };
            log.rows -= dropped.rows.as_ref().map_or(1, Vec::len);
            log.dropped_through = log.dropped_through.max(dropped.at);
        }
    }

    pub(crate) fn changed_between(&self, from: (i64, u64), upto: i64, limit: usize) -> Option<BTreeSet<ChangedRow>> {
        let log = held(&self.0);
        if upto > log.logged_through || from.0 < log.start_revision || from < log.dropped_through {
            return None;
        }
        let mut changed = BTreeSet::new();
        for entry in log.items.iter().filter(|entry| entry.at > from && entry.at.0 <= upto) {
            changed.extend(entry.rows.iter().flatten().cloned());
            if entry.rows.is_none() || changed.len() > limit {
                return None;
            }
        }
        Some(changed)
    }
}
