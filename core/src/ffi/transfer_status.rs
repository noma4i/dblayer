//! What `uploads[action]` shows: the owner Transfer pushes a `Shown` for each key, the stream `Blob` of a key follows it.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::watch;

use crate::owners::write::transfer::Shown;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireTransferState {
    Copying,
    Queued,
    Uploading,
    Failed,
    Gone,
}

/// The status of a transfer: its key, state, progress 0..1 and the error of a failed one.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireTransferStatus {
    pub key: String,
    pub state: WireTransferState,
    pub progress: f64,
    pub error: Option<String>,
}

impl WireTransferStatus {
    pub(super) fn of(key: &str, shown: Option<&Shown>) -> Self {
        let (state, progress, error) = match shown {
            None | Some(Shown::Gone) => (WireTransferState::Gone, 0.0, None),
            Some(Shown::Copying) => (WireTransferState::Copying, 0.0, None),
            Some(Shown::Queued) => (WireTransferState::Queued, 0.0, None),
            Some(Shown::Uploading { progress }) => (WireTransferState::Uploading, f64::from(*progress) / 100.0, None),
            Some(Shown::Failed { error }) => (WireTransferState::Failed, 0.0, Some(error.clone())),
        };
        Self { key: key.to_string(), state, progress, error }
    }
}

impl WireTransferStatus {
    /// The value of the stream `Blob`: the state by name, the progress in percent.
    pub(super) fn blob_status(&self) -> crate::owners::read::deliveries::BlobStatus {
        let state = match self.state {
            WireTransferState::Copying => "copying",
            WireTransferState::Queued => "queued",
            WireTransferState::Uploading => "uploading",
            WireTransferState::Failed => "failed",
            WireTransferState::Gone => "gone",
        };
        crate::owners::read::deliveries::BlobStatus { state: state.into(), progress: (self.progress * 100.0).round() as u64, error: self.error.clone(), local_url: None, processing: None }
    }
}

/// The last `Shown` of every key, with a counter that moves at each change.
pub(super) struct Shows {
    map: Mutex<HashMap<String, Shown>>,
    changed: watch::Sender<u64>,
    /// One counter per open `Blob` stream row: `(table, row id)` to the counter its commits move.
    rows: Mutex<HashMap<(String, String), watch::Sender<u64>>>,
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
}

impl Default for Shows {
    fn default() -> Self { Self { map: Mutex::default(), changed: watch::channel(0).0, rows: Mutex::default(), #[cfg(test)] reads: std::sync::atomic::AtomicUsize::new(0) } }
}

impl Shows {
    pub(super) fn put(&self, key: &str, shown: Shown) {
        crate::lock::lock(&self.map).insert(key.to_string(), shown);
        crate::lock::effect(|| self.changed.send_modify(|count| *count += 1));
    }

    pub(super) fn status(&self, key: &str) -> WireTransferStatus { WireTransferStatus::of(key, crate::lock::lock(&self.map).get(key)) }

    /// Moves the counter of each watched row the commit changed: `None` = every row of the tables.
    pub(super) fn touch(&self, changed: &crate::owners::contract::ChangedRows) {
        let hit: Vec<watch::Sender<u64>> = {
            let mut rows = crate::lock::lock(&self.rows);
            rows.retain(|_, sender| sender.receiver_count() > 0);
            match changed {
                None => rows.values().cloned().collect(),
                Some(changed) => changed.iter().filter_map(|key| rows.get(key).cloned()).collect(),
            }
        };
        for sender in hit {
            crate::lock::effect(|| sender.send_modify(|count| *count += 1));
        }
    }

    pub(super) fn watch_row(&self, table: String, row: String) -> watch::Receiver<u64> {
        crate::lock::lock(&self.rows).entry((table, row)).or_insert_with(|| watch::channel(0).0).subscribe()
    }

    #[cfg(test)]
    pub(super) fn note_read(&self) { self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }

    #[cfg(test)]
    pub(crate) fn row_reads(&self) -> usize { self.reads.load(std::sync::atomic::Ordering::SeqCst) }

    pub(super) fn watch(&self) -> watch::Receiver<u64> { self.changed.subscribe() }
}
