use std::sync::Arc;

use crate::db::{AcceptedUpload, RoundStep};

use super::{Database, DbError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireAcceptedUpload {
    pub seq: i64,
    pub position: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadAccepted {
    pub model: String,
    pub items: Vec<WireAcceptedUpload>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireRoundStep {
    More,
    Stale,
    Done,
}

impl From<RoundStep> for WireRoundStep {
    fn from(step: RoundStep) -> Self {
        match step {
            RoundStep::More => Self::More,
            RoundStep::Stale => Self::Stale,
            RoundStep::Done => Self::Done,
        }
    }
}

impl From<AcceptedUpload> for WireAcceptedUpload {
    fn from(item: AcceptedUpload) -> Self {
        Self { seq: item.seq, position: item.position }
    }
}

#[uniffi::export(with_foreign)]
pub trait UploadAcceptedSink: Send + Sync {
    fn accepted(&self, event: WireUploadAccepted);
}

#[uniffi::export]
impl Database {
    pub fn set_upload_accepted_sink(&self, sink: Arc<dyn UploadAcceptedSink>) {
        self.db.set_upload_accepted_sink(Arc::new(move |event| {
            sink.accepted(WireUploadAccepted { model: event.model, items: event.items.into_iter().map(Into::into).collect() });
        }));
    }

    pub fn background_refused(&self, items: Vec<WireAcceptedUpload>, detail: String) -> Result<(), DbError> {
        let items: Vec<_> = items.iter().map(|item| AcceptedUpload { seq: item.seq, position: item.position }).collect();
        self.db.note_background_refused(&items, &detail)?;
        Ok(())
    }

    pub fn round_begin(&self) -> Option<u32> {
        self.db.round_begin()
    }

    pub fn round_next(&self, owner: u32) -> WireRoundStep {
        self.db.round_next(owner).into()
    }

    pub fn round_leave(&self, owner: u32) -> WireRoundStep {
        self.db.round_leave(owner).into()
    }

    pub fn round_end(&self, owner: u32) -> Option<Vec<WireAcceptedUpload>> {
        self.db.round_end(owner).map(|late| late.into_iter().map(Into::into).collect())
    }

    pub fn round_abandon(&self, owner: u32) {
        self.db.round_abandon(owner);
    }
}
