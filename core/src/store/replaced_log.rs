use std::collections::VecDeque;
use std::sync::Mutex;

use crate::identity::RowId;

use crate::lock::lock as held;

const MAX_REPLACED_LOG: usize = 4096;

/// The temporary-to-real id pairs of recent commits, so a reader can name the pairs between two revisions.
#[derive(Default)]
pub(crate) struct ReplacedLog(Mutex<Pairs>, tokio::sync::watch::Sender<u64>);

#[derive(Default)]
struct Pairs {
    entries: VecDeque<(i64, RowId, RowId)>,
    dropped_through: i64,
}

impl ReplacedLog {
    pub(crate) fn log_replaced(&self, revision: i64, pairs: impl Iterator<Item = (RowId, RowId)>) {
        let mut log = held(&self.0);
        let before = log.entries.len();
        log.entries.extend(pairs.map(|(from, to)| (revision, from, to)));
        let swapped = log.entries.len() > before;
        while log.entries.len() > MAX_REPLACED_LOG {
            if let Some((revision, _, _)) = log.entries.pop_front() {
                log.dropped_through = log.dropped_through.max(revision);
            }
        }
        drop(log);
        if swapped {
            crate::lock::effect(|| self.1.send_modify(|count| *count += 1));
        }
    }

    /// Changes each time a commit swaps temporary ids.
    pub(crate) fn swaps(&self) -> tokio::sync::watch::Receiver<u64> { self.1.subscribe() }

    pub(crate) fn replaced_between(&self, after: i64, upto: i64) -> Vec<(RowId, RowId)> {
        let log = held(&self.0);
        if log.dropped_through > after {
            return Vec::new();
        }
        log.entries.iter().filter(|(revision, _, _)| *revision > after && *revision <= upto).map(|(_, from, to)| (from.clone(), to.clone())).collect()
    }
}
