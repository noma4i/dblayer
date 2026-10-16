// TEMPORARY: Remove after the list return measurement.
use std::sync::{Arc, OnceLock};

#[uniffi::export(rust, foreign)]
pub trait GapLogger: Send + Sync {
    fn record(&self, message: String);
}

static LOGGER: OnceLock<Arc<dyn GapLogger>> = OnceLock::new();

#[uniffi::export]
pub fn install_gap_logger(logger: Arc<dyn GapLogger>) {
    let _ = LOGGER.set(logger);
}

pub(crate) fn record(message: impl FnOnce() -> String) {
    if let Some(logger) = LOGGER.get() {
        logger.record(message());
    }
}

pub(crate) fn tracks(model: &str) -> bool {
    matches!(model, "Chat" | "Message" | "User")
}
