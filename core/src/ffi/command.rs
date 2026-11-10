use std::future::Future;

use super::receipt::{WireOperationReceipt};
use super::upload_wire::WireUploadRequest;
use super::{Database, DbError};

/// The media file of a command and the declared upload action that sends it.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandUpload {
    pub action: String,
    pub request: WireUploadRequest,
}

/// A command: the row is written locally at once and the library queues and sends it, also after the network returned or the app restarted.
/// A model sends through its `create` operation, and a media upload goes through a declared upload action.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandRequest {
    /// The session epoch the caller was admitted in. A command for an epoch that has gone is refused.
    pub epoch: u64,
    pub model: String,
    pub values_json: String,
    pub args_json: String,
    /// Set for a media upload.
    pub upload: Option<CommandUpload>,
}

/// The error of a call that finds the database closed.
pub(super) fn closed() -> DbError { DbError::Closed { detail: "the database is closed".into() } }

fn refused(model: &str) -> DbError { DbError::Invalid { detail: format!("{model} declares no create operation and no declared upload action for this command") } }

impl Database {
    /// Runs work admitted in the caller's epoch.
    pub(super) async fn in_epoch<T>(&self, epoch: u64, _target: &'static str, work: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
        self.account_call(epoch)?;
        let result = work.await;
        self.close_after(epoch)?;
        result
    }

    /// Reads the local database after admission in the caller's epoch.
    pub(super) async fn read_in_epoch<T: Send + 'static>(&self, epoch: u64, target: &'static str, read: impl FnOnce(&Database) -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
        let kept = crate::runtime::hold_until_woken(self.life.launch());
        let this = self.clone();
        let answer = this.clone();
        let work = async move { super::database::blocking(move || read(&this)).await };
        let value = self.in_epoch(epoch, target, work).await;
        crate::runtime::release_holds();
        drop(kept);
        answer.close_after(epoch)?;
        value
    }

    fn admit_command(&self, request: &CommandRequest) -> Result<(), DbError> {
        let spec = self.db.spec(&request.model)?;
        let allowed = match &request.upload {
            Some(upload) => spec.upload_action(&upload.action).is_some(),
            None => spec.actions.iter().any(|action| action.name == "create"),
        };
        if allowed { Ok(()) } else { Err(refused(&request.model)) }
    }

    /// Whether the epoch is still the one the receipt belongs to.
    pub(super) fn admission_end(&self, epoch: u64) -> Option<super::EndCause> {
        use crate::owners::life::CallVerdict;
        match self.life.verdict(epoch) {
            CallVerdict::Pass => None,
            CallVerdict::Closed => Some(super::EndCause::Closed),
            CallVerdict::Refuse(crate::owners::calls::CallError::Storage) => Some(super::EndCause::Storage { error: "the database could not save its state".into() }),
            CallVerdict::Refuse(_) => Some(super::EndCause::SessionEnded),
        }
    }

    pub(super) fn close_after(&self, epoch: u64) -> Result<(), DbError> {
        if matches!(self.life.verdict(epoch), crate::owners::life::CallVerdict::Closed) { return Err(closed()); }
        Ok(())
    }

    /// A call that holds no future: refused when the epoch of the caller is not current.
    pub(super) fn admit_now(&self, epoch: u64) -> Result<(), DbError> {
        self.account_call(epoch)
    }

    /// The rule for a call of account data: `Closed` after the close, `NoSession` when the epoch of the caller is not the current one or a database with a session has none.
    pub(super) fn account_call(&self, epoch: u64) -> Result<(), DbError> {
        use crate::owners::life::CallVerdict;
        match self.life.verdict(epoch) {
            CallVerdict::Pass => Ok(()),
            CallVerdict::Closed => Err(closed()),
            CallVerdict::Refuse(crate::owners::calls::CallError::Storage) => Err(DbError::Storage { detail: "the database could not save its state".into() }),
            CallVerdict::Refuse(_) => Err(DbError::NoSession { detail: "the database has no session in this epoch".into() }),
        }
    }

}

#[uniffi::export]
impl Database {
    /// Writes the row locally in the epoch of the caller and queues it for the server. The SQL commit is refused when the epoch moved or the database closed before it.
    pub async fn send(&self, request: CommandRequest) -> Result<WireOperationReceipt, DbError> {
        self.account_call(request.epoch)?;
        self.admit_command(&request)?;
        self.mutation_needs_transport(&request.model, request.upload.as_ref().map_or("create", |upload| upload.action.as_str()))?;
        if request.upload.is_none() {
            return self.create_row(request.epoch, request.model, request.values_json, request.args_json).await;
        }
        self.send_upload(request).await
    }
}
