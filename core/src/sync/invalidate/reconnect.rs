use super::super::error::SyncError;
use super::super::resume::ResumeReport;
use super::super::send::FlushReport;
use super::super::Sync;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconnectReport {
    pub flush: FlushReport,
    pub resume: ResumeReport,
}

impl Sync {
    pub async fn reconnect(&self) -> Result<ReconnectReport, SyncError> {
        let flush = self.flush().await?;
        let resume = self.resume().await?;
        Ok(ReconnectReport { flush, resume })
    }
}
