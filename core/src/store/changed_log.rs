use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;

use crate::lock::lock as held;

use super::CommitResult;
use crate::owners::contract::CommittedBroadcast;
use crate::owners::read::deliveries::{touches, Touching, View};

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
    changed_rows(result).into_iter().map(|(model, id)| (crate::store::ddl::table_name(&model), id)).collect()
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
    pending: Option<Entry>,
}

struct Entry {
    at: (i64, u64),
    notice: Option<CommittedBroadcast>,
}

pub(crate) struct Prepared<'a> {
    log: &'a ChangedLog,
    finished: bool,
}

impl Prepared<'_> {
    pub(crate) fn finish(mut self) {
        let mut log = held(&self.log.0);
        if let Some(entry) = log.pending.take() { push(&mut log, entry); }
        self.finished = true;
    }
}

impl Drop for Prepared<'_> {
    fn drop(&mut self) {
        if !self.finished { held(&self.log.0).pending = None; }
    }
}

fn push(log: &mut Entries, entry: Entry) {
    log.logged_through = log.logged_through.max(entry.at.0);
    log.rows += entry.notice.as_ref().and_then(|notice| notice.rows.as_ref()).map_or(1, |rows| rows.len().max(1));
    log.items.push_back(entry);
    while log.rows > MAX_LOGGED_ROWS {
        let Some(dropped) = log.items.pop_front() else { break };
        log.rows -= dropped.notice.as_ref().and_then(|notice| notice.rows.as_ref()).map_or(1, |rows| rows.len().max(1));
        log.dropped_through = log.dropped_through.max(dropped.at);
    }
}

impl ChangedLog {
    pub(crate) fn start(&self, revision: i64) {
        let mut log = held(&self.0);
        log.start_revision = revision;
        log.logged_through = revision;
    }

    pub(crate) fn prepare_commit(&self, result: &CommitResult) -> Option<Prepared<'_>> {
        let revision = result.revision?;
        Some(self.prepare(Entry { at: (revision, result.position), notice: Some(CommittedBroadcast::of_commit(result)) }))
    }

    pub(crate) fn prepare_reset(&self, revision: i64) -> Prepared<'_> { self.prepare(Entry { at: (revision, 0), notice: None }) }

    fn prepare(&self, entry: Entry) -> Prepared<'_> {
        let mut log = held(&self.0);
        assert!(log.pending.is_none());
        log.pending = Some(entry);
        Prepared { log: self, finished: false }
    }

    pub(crate) fn changed_between(&self, from: (i64, u64), upto: (i64, u64), limit: usize) -> Option<BTreeSet<ChangedRow>> {
        let log = held(&self.0);
        let logged = log.pending.as_ref().map_or(log.logged_through, |entry| log.logged_through.max(entry.at.0));
        if upto.0 > logged || from.0 < log.start_revision || from < log.dropped_through {
            return None;
        }
        let mut changed = BTreeSet::new();
        for entry in log.items.iter().chain(log.pending.iter()).filter(|entry| entry.at > from && entry.at <= upto) {
            let Some(rows) = entry.notice.as_ref().and_then(|notice| notice.rows.as_ref()) else { return None };
            changed.extend(rows.iter().cloned());
            if changed.len() > limit {
                return None;
            }
        }
        Some(changed)
    }

    fn scan(&self, view: &View, from: (i64, u64), upto: (i64, u64), mut touching: impl FnMut(&Entry)) -> bool {
        let log = held(&self.0);
        let logged = log.pending.as_ref().map_or(log.logged_through, |entry| log.logged_through.max(entry.at.0));
        let mut covered = upto.0 <= logged && from.0 >= log.start_revision && from >= log.dropped_through;
        for entry in log.items.iter().chain(log.pending.iter()).filter(|entry| entry.at > from && entry.at <= upto) {
            match &entry.notice {
                Some(notice) if touches(view, &notice.models, &notice.tables, &notice.rows) => touching(entry),
                None => covered = false,
                _ => {}
            }
        }
        covered
    }

    pub(crate) fn relevant(&self, view: &View, from: (i64, u64), upto: (i64, u64)) -> Option<(i64, u64)> {
        let mut latest = None;
        let covered = self.scan(view, from, upto, |entry| latest = Some(entry.at));
        latest.or_else(|| (!covered).then_some(upto))
    }

    pub(crate) fn touching(&self, view: &View, from: (i64, u64), upto: (i64, u64)) -> Touching {
        let mut entries = Vec::new();
        let covered = self.scan(view, from, upto, |entry| entries.push((entry.at, entry.notice.as_ref().and_then(|notice| notice.rows.clone()))));
        if covered { entries } else { Vec::new() }
    }
}
