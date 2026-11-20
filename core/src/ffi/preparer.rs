//! The port of the platform library that prepares a file: it owns the descriptor and the codecs, the core owns the queue, the attempt and the limits.

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

#[uniffi::export(with_foreign)]
#[async_trait]
pub trait Preparer: Send + Sync {
    async fn probe(&self, path: String, descriptor: Option<Vec<u8>>, mime: Option<String>) -> ProbeOutcome;
    async fn run(&self, job: PrepareJob, progress: Arc<PrepareProgress>) -> PrepareOutcome;
    fn cancel(&self, attempt_id: u64);
    /// Pure and synchronous: the core calls it outside every lock and transaction.
    fn import_descriptor(&self, saved: SavedPreparation) -> PrepareImportOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum ProbeOutcome {
    File { width: Option<u32>, height: Option<u32>, mime: Option<String> },
    Failed { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PrepareJob {
    pub attempt_id: u64,
    pub source: String,
    pub destination: String,
    pub descriptor: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum PrepareOutcome {
    Done { name: Option<String>, mime: Option<String> },
    Unchanged { name: Option<String>, mime: Option<String> },
    CannotDecode { message: String },
    Failed { message: String },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SavedPreparation {
    pub columns: HashMap<String, Option<String>>,
    pub children: Vec<HashMap<String, Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum PrepareImportOutcome {
    Done { descriptor: Vec<u8> },
    Failed { message: String },
}

#[derive(uniffi::Object)]
pub struct PrepareProgress { report: Box<dyn Fn(f64) + Send + Sync> }

impl PrepareProgress {
    pub fn new(report: impl Fn(f64) + Send + Sync + 'static) -> Self { Self { report: Box::new(report) } }
}

#[uniffi::export]
impl PrepareProgress {
    pub fn report(&self, fraction: f64) {
        crate::lock::assert_no_library_lock("PrepareProgress::report");
        (self.report)(fraction);
    }
}
