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
    pub epoch: u64,
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
            crate::db::lock::assert_no_library_lock("UploadAcceptedSink::accepted");
            sink.accepted(WireUploadAccepted { epoch: event.epoch, model: event.model, items: event.items.into_iter().map(Into::into).collect() });
        }));
    }

    pub fn background_refused(&self, epoch: u64, items: Vec<WireAcceptedUpload>, detail: String) -> Result<(), DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Session, epoch, "background upload refusal")?;
        let items: Vec<_> = items.iter().map(|item| AcceptedUpload { seq: item.seq, position: item.position }).collect();
        self.db.note_background_refused(&admitted, &items, &detail)?;
        Ok(())
    }

    pub fn round_begin(&self, epoch: u64) -> Result<Option<u32>, DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Explicit, epoch, "begin upload round")?;
        Ok(self.db.round_begin(&admitted)?)
    }

    pub fn round_next(&self, epoch: u64, owner: u32) -> Result<WireRoundStep, DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Explicit, epoch, "advance upload round")?;
        Ok(self.db.round_next(&admitted, owner)?.into())
    }

    pub fn round_leave(&self, epoch: u64, owner: u32) -> Result<WireRoundStep, DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Explicit, epoch, "leave upload round")?;
        Ok(self.db.round_leave(&admitted, owner)?.into())
    }

    pub fn round_end(&self, epoch: u64, owner: u32) -> Result<Option<Vec<WireUploadAccepted>>, DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Explicit, epoch, "end upload round")?;
        Ok(self.db.round_end(&admitted, owner)?.map(|late| late.into_iter().map(|group| WireUploadAccepted { epoch: group.epoch, model: group.model, items: group.items.into_iter().map(Into::into).collect() }).collect()))
    }

    pub fn round_abandon(&self, epoch: u64, owner: u32) -> Result<(), DbError> {
        let admitted = self.acquire(crate::lifecycle::Origin::Explicit, epoch, "abandon upload round")?;
        Ok(self.db.round_abandon(&admitted, owner)?)
    }
}
