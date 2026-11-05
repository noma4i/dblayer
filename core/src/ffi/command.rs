use std::future::Future;

use crate::ffi::admission::Origin;
use super::receipt::{WireOperationReceipt};
use super::upload_wire::WireUploadRequest;
use super::{Database, DbError};

/// The media file of a command and the declared upload action that sends it.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandUpload {
    pub action: String,
    pub request: WireUploadRequest,
}

/// A durable command: the row is written locally at once and the library sends it, also after the network returned or the app restarted.
/// Only a message send and a media upload are durable: a model sends a message through its `durable` create action, and a media upload goes through a declared upload action.
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

pub(super) fn ended() -> DbError { DbError::Interrupted { detail: "the session of this command ended".into() } }

/// The error of a call that finds the database closed.
pub(super) fn closed() -> DbError { DbError::Closed { detail: "the database is closed".into() } }

fn refused(model: &str) -> DbError { DbError::Invalid { detail: format!("{model} declares no durable command: only sending a message and a media upload work without a network") } }

impl Database {
    pub(super) fn acquire(&self, origin: Origin, epoch: u64, target: &'static str) -> Result<crate::ffi::admission::Admitted, DbError> {
        self.account_call(epoch)?;
        let admitted = self.streams.admit(origin, target).map_err(|_| {
            let state = self.streams.snapshot();
            if state.closed { closed() }
            else if origin == Origin::Session && !state.ending && !state.signed_in() {
                DbError::NoSession { detail: "the database has no session".into() }
            } else { ended() }
        })?;
        if admitted.value() != epoch { return Err(ended()); }
        Ok(admitted)
    }

    /// Runs the work in the epoch of the caller: it starts only while that epoch is current and ends with it. `Session` work also needs a signed-in session.
    pub(super) async fn in_epoch_as<T, F>(&self, origin: Origin, epoch: u64, target: &'static str, restore: bool, work: impl FnOnce(crate::ffi::admission::Admitted) -> F) -> Result<T, DbError>
    where F: Future<Output = Result<T, DbError>> {
        let admitted = self.acquire(origin, epoch, target)?;
        let result = admitted.run(work(admitted.clone())).await.map_err(|_| ended())??;
        let _ = restore;
        admitted.run(async { result }).await.map_err(|_| ended())
    }

    pub(super) async fn in_epoch<T>(&self, epoch: u64, target: &'static str, work: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
        self.in_epoch_as(Origin::Explicit, epoch, target, false, |_| work).await
    }

    /// A read of the local database that belongs to the epoch of the caller: a result read after the epoch moved is not returned.
    pub(super) async fn read_in_epoch<T: Send + 'static>(&self, epoch: u64, target: &'static str, read: impl FnOnce(&Database) -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
        let this = self.clone();
        let answer = this.clone();
        let work = async move { super::database::blocking(move || read(&this)).await };
        let value = self.in_epoch(epoch, target, work).await?;
        if !answer.admits(epoch) { return Err(ended()); }
        Ok(value)
    }

    fn admit_command(&self, request: &CommandRequest) -> Result<(), DbError> {
        let spec = self.db.spec(&request.model)?;
        let allowed = match &request.upload {
            Some(upload) => spec.upload_action(&upload.action).is_some(),
            None => spec.actions.iter().any(|action| action.name == "create" && action.durable),
        };
        if allowed { Ok(()) } else { Err(refused(&request.model)) }
    }

    /// Whether the epoch is still the one the receipt belongs to.
    pub(super) fn admits(&self, epoch: u64) -> bool { self.streams.admits_epoch(epoch) }

    /// A call that holds no future: refused when the epoch of the caller is not current.
    pub(super) fn admit_now(&self, epoch: u64) -> Result<(), DbError> {
        self.account_call(epoch)
    }

    /// The rule for a call of account data: `Closed` after the close, `NoSession` when the epoch of the caller is not the current one or a database with a session has none.
    pub(super) fn account_call(&self, epoch: u64) -> Result<(), DbError> {
        let state = self.streams.snapshot();
        if state.closed { return Err(closed()); }
        if !self.streams.admits_epoch(epoch) || (self.session.is_some() && !state.signed_in()) {
            return Err(DbError::NoSession { detail: "the database has no session in this epoch".into() });
        }
        Ok(())
    }

}

#[uniffi::export]
impl Database {
    /// Writes the row locally in the epoch of the caller and queues it for the server. The SQL commit is refused when the epoch moved or the database closed before it.
    pub async fn send(&self, request: CommandRequest) -> Result<WireOperationReceipt, DbError> {
        self.admit_command(&request)?;
        if request.upload.is_none() {
            return self.create_row(request.epoch, request.model, serde_json::from_str(&request.values_json)?, serde_json::from_str(&request.args_json)?).await;
        }
        self.send_upload(request).await
    }
}
