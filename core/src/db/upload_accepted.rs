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

    pub(crate) fn report_upload_accepted(&self, admitted: &crate::lifecycle::Admitted, model: &str, items: Vec<AcceptedUpload>) {
        let Some(sink) = locked(&self.shared.upload_accepted).clone() else { return };
        let group = UploadAccepted { epoch: admitted.value(), model: model.to_string(), items };
        let accepted = {
            let life = &self.shared.store.lifecycle;
            let _line = life.commit_line();
            std::sync::Arc::ptr_eq(life, admitted.life()) && admitted.is_current() && !group.items.is_empty() && self.round_accept(group.clone())
        };
        if !accepted {
            return;
        }
        sink(group);
    }

    pub(crate) fn accepted_items(&self, ids: &[RowId]) -> Result<Vec<AcceptedUpload>, DbError> {
        self.shared.read(&[], |reader| {
            let mut statement = reader.connection().prepare("SELECT seq, position FROM op_item WHERE row_id = ?1 AND transferred = 0 AND phase NOT IN ('failed', 'cancelled') ORDER BY seq, position")?;
            let mut found = Vec::new();
            for id in ids {
                for item in statement.query_map([id.as_str()], |row| Ok(AcceptedUpload { seq: row.get(0)?, position: row.get(1)? }))? {
                    found.push(item?);
                }
            }
            Ok(found)
        })
    }

    pub fn note_background_refused(&self, admitted: &crate::lifecycle::Admitted, items: &[AcceptedUpload], detail: &str) -> Result<usize, DbError> {
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
