use std::sync::Mutex;

use crate::sync::{LoadPhase, RelationState};

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RelationStateChange {
    pub phase: String,
    pub has_more: bool,
    pub error_kind: Option<String>,
    pub error: Option<String>,
    pub loaded_at: Option<i64>,
}

impl RelationStateChange {
    pub(super) fn of(state: &RelationState) -> Self {
        let phase = match state.phase {
            LoadPhase::Idle => "idle",
            LoadPhase::Loading => "loading",
            LoadPhase::Failed => "failed",
        };
        Self {
            phase: phase.into(),
            has_more: state.has_more,
            error_kind: state.error.as_ref().map(|error| error.kind.to_string()),
            error: state.error.as_ref().map(|error| error.message.clone()),
            loaded_at: state.loaded_at,
        }
    }
}

#[uniffi::export(rust, foreign)]
pub trait StateListener: Send + Sync {
    fn changed(&self, state: RelationStateChange);
}

#[derive(uniffi::Object)]
pub struct StateSubscription {
    pub(super) task: Mutex<Option<tokio::task::AbortHandle>>,
}

#[uniffi::export]
impl StateSubscription {
    pub fn cancel(&self) {
        if let Some(task) = self.task.lock().expect("state task").take() { task.abort(); }
    }
}

impl Drop for StateSubscription {
    fn drop(&mut self) { self.cancel(); }
}
