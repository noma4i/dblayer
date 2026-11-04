use std::future::Future;

use crate::lifecycle::Origin;
use super::database::{blocking, on_executor};
use super::database::{receipt, WireOperationReceipt};
use super::upload_wire::WireUploadRequest;
use super::{Database, DbError};

/// The media file of a command and the declared upload action that sends it.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandUpload {
    pub action: String,
    pub request: WireUploadRequest,
}

/// A durable command: the row is written locally at once and the library sends it, also after the network returned or the app restarted.
/// Only the commands of `specs/architecture.md` section D are durable: a model sends a message through its `durable` create action, and a media upload goes through a declared upload action.
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

#[cfg(test)]
struct WriteProbe(std::sync::Arc<crate::lifecycle::Lifecycle>);

#[cfg(test)]
impl Drop for WriteProbe {
    fn drop(&mut self) { self.0.writes_entered.fetch_sub(1, std::sync::atomic::Ordering::SeqCst); }
}

fn refused(model: &str) -> DbError { DbError::Invalid { detail: format!("{model} declares no durable command: only sending a message and a media upload work without a network") } }

impl Database {
    pub(super) fn acquire(&self, origin: Origin, epoch: u64, target: &'static str) -> Result<crate::lifecycle::Admitted, DbError> {
        if !self.streams.admits_epoch(epoch) { return Err(ended()); }
        let admitted = self.streams.admit(origin, target).map_err(|_| {
            let state = self.streams.snapshot();
            if origin == Origin::Session && !state.closed && state.transition.is_none() && !state.signed_in() {
                DbError::NoSession { detail: "the database has no session".into() }
            } else { ended() }
        })?;
        if admitted.value() != epoch { return Err(ended()); }
        Ok(admitted)
    }

    /// Runs the work in the epoch of the caller: it starts only while that epoch is current and ends with it. `Session` work also needs a signed-in session.
    pub(super) async fn in_epoch_as<T, F>(&self, origin: Origin, epoch: u64, target: &'static str, restore: bool, work: impl FnOnce(crate::lifecycle::Admitted) -> F) -> Result<T, DbError>
    where F: Future<Output = Result<T, DbError>> {
        let admitted = self.acquire(origin, epoch, target)?;
        let result = admitted.run(work(admitted.clone())).await.map_err(|_| ended())??;
        let admitted = if restore {
            if let Some(sync) = &self.sync {
                let session = sync.inner.transport.session().map_err(|error| DbError::from(crate::sync::SyncError::Transport(error)))?;
                if session.is_some() { self.streams.session_from(session, admitted).map_err(|_| ended())? } else { admitted }
            } else { admitted }
        } else { admitted };
        admitted.run(async { result }).await.map_err(|_| ended())
    }

    pub(super) async fn in_epoch<T>(&self, epoch: u64, target: &'static str, work: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
        self.in_epoch_as(Origin::Explicit, epoch, target, false, |_| work).await
    }

    /// A write to the local database that belongs to the epoch of the caller. The SQL commit is refused when the epoch moved or the database closed before it.
    pub(super) async fn write_in_epoch<T: Send + 'static>(&self, origin: Origin, epoch: u64, target: &'static str, write: impl FnOnce(&Database, &crate::lifecycle::Admitted) -> Result<T, DbError> + Send + 'static) -> Result<T, DbError> {
        let admitted = self.acquire(origin, epoch, target)?;
        if origin == Origin::Session { self.require_online(target)?; }
        let this = self.clone();
        let write_admitted = admitted.clone();
        let work = async move {
            super::database::blocking(move || {
                #[cfg(test)]
                let _probe = {
                    let life = this.db.shared.store.lifecycle.clone();
                    life.writes_entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    WriteProbe(life)
                };
                let result = write(&this, &write_admitted);
                result.map_err(|error| if this.admits(epoch) { error } else { ended() })
            }).await
        };
        admitted.run(work).await.map_err(|_| ended())?
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

    /// Server work of the epoch of the caller: the task is aborted when the epoch ends.
    pub(super) async fn serve_in_epoch<T: Send + 'static, E: Into<DbError> + Send + 'static, F: Future<Output = Result<T, E>> + Send + 'static>(&self, epoch: u64, target: &'static str, work: impl FnOnce(crate::lifecycle::Admitted) -> F + Send + 'static) -> Result<T, DbError> {
        self.in_epoch_as(Origin::Explicit, epoch, target, true, |admitted| async move {
            super::database::cancellable(async move { work(admitted).await.map_err(Into::into) }).await?
        }).await
    }

    /// One request whose flight other callers may join: a caller that goes away does not stop it. A chain of requests (`load_all`, `refresh_preloads`) goes through `serve_in_epoch`, which stops it with its caller. The epoch still bounds the lease, the origin and the SQL commit.
    pub(super) async fn serve_shared_in_epoch<T: Send + 'static, E: Into<DbError> + Send + 'static, F: Future<Output = Result<T, E>> + Send + 'static>(&self, epoch: u64, target: &'static str, work: impl FnOnce(crate::lifecycle::Admitted) -> F) -> Result<T, DbError> {
        self.in_epoch_as(Origin::Explicit, epoch, target, true, |admitted| super::database::on_executor(work(admitted))).await
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
        if self.admits(epoch) { Ok(()) } else { Err(ended()) }
    }

}

#[uniffi::export]
impl Database {
    /// Writes the row locally in the epoch of the caller and queues it for the server. The SQL commit is refused when the epoch moved or the database closed before it.
    pub async fn send(&self, request: CommandRequest) -> Result<WireOperationReceipt, DbError> {
        self.admit_command(&request)?;
        let this = self.clone();
        let epoch = request.epoch;
        let model = request.model.clone();
        let work = move |admitted: crate::lifecycle::Admitted| async move {
            let values: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&request.values_json)?;
            let (row, operation, revision, ticket) = match request.upload {
                None => {
                    let args = serde_json::from_str(&request.args_json)?;
                    let (database, model) = (this.clone(), request.model.clone());
                    blocking(move || {
                        #[cfg(test)]
                        let _probe = {
                            let life = database.db.shared.store.lifecycle.clone();
                            life.writes_entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            WriteProbe(life)
                        };
                        let sync = database.sync()?.clone();
                        let result = database.db.create_claiming(&admitted, None, &model, values, args, |operation| operation.map(|operation| sync.claim_outcome(operation)));
                        let (row, operation, revision, ticket) = result.map_err(|error| if database.admits(epoch) { DbError::from(error) } else { ended() })?;
                        database.send_writes();
                        Ok((database.wire(&model)?.row(&row), operation, revision, ticket))
                    }).await?
                }
                Some(upload) => {
                    let sync = this.sync()?.clone();
                    let (wire, model) = (this.wire(&request.model)?.clone(), request.model.clone());
                    on_executor(async move {
                        let file = upload.request.into_file()?;
                        let (row, operation, revision, ticket) = sync.upload_create_claiming(&admitted, &model, &upload.action, values, file).await?;
                        Ok::<_, DbError>((wire.row(&row), Some(operation), revision, Some(ticket)))
                    }).await?
                }
            };
            Ok((row, operation, revision, ticket))
        };
        let (row, operation, revision, ticket) = self.in_epoch_as(Origin::Session, epoch, "send", false, work).await?;
        Ok(receipt(epoch, model, operation, revision, Some(row), ticket, self.streams.clone()))
    }
}
