use std::sync::Arc;

use crate::commit::{CommitEnvelope, OperationTransition, WriteOrigin};
use crate::identity::RowId;

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::{locked, Db};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedUpload {
    pub seq: i64,
    pub position: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadAccepted {
    pub epoch: u64,
    pub model: String,
    pub items: Vec<AcceptedUpload>,
}

pub type UploadAcceptedCallback = Arc<dyn Fn(UploadAccepted) + Send + Sync>;

impl Db {
    pub fn set_upload_accepted_sink(&self, sink: UploadAcceptedCallback) {
        *locked(&self.shared.upload_accepted) = Some(sink);
    }

    pub fn note_background_refused(&self, admitted: &crate::ffi::legacy::Admitted, items: &[AcceptedUpload], detail: &str) -> Result<usize, DbError> {
        let generation = self.generation();
        let mut noted = 0;
        self.commit_following(&admitted.authority(), generation, |reader| {
            noted = 0;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            for item in items {
                let open: i64 = reader.connection().query_row("SELECT count(*) FROM op_item WHERE seq = ?1 AND position = ?2 AND phase NOT IN ('failed', 'cancelled')", [item.seq, item.position], |row| row.get(0))?;
                if open == 0 {
                    continue;
                }
                envelope.operation_transitions.push(OperationTransition::BackgroundRefused { seq: item.seq, item_position: item.position, detail: detail.to_string() });
                noted += 1;
            }
            Ok::<_, DbError>(Some(envelope))
        }).map_err(plan_fault)?;
        Ok(noted)
    }
}
