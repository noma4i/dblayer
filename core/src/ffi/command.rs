use std::future::Future;

use crate::identity::RowId;
use crate::lifecycle::Origin;
use crate::syncstate::SyncState;
use crate::upload::UploadStatus;
use super::database::{blocking, on_executor};
use super::upload_wire::WireUploadRequest;
use super::wire::WireRow;
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

#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandReceipt {
    pub epoch: u64,
    pub model: String,
    /// The local row. Its id is a temporary id until the server confirms the command.
    pub row: WireRow,
    /// The upload action of a media command.
    pub upload_action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CommandStatus {
    Queued,
    /// The file of a media command is on its way.
    Uploading { sent_bytes: u64, total_bytes: u64 },
    Sending,
    /// The server confirmed. `id` is the id of the row on the server.
    Delivered { id: String },
    /// The server refused, or the attempts ran out. The row stays until the caller retries or discards it.
    Failed { message: String },
    /// The request left and the answer did not come back.
    Unknown,
    /// The session of the command ended first. Its pending work ended with it.
    Ended,
}

impl CommandStatus {
    fn settled(&self) -> bool { !matches!(self, Self::Queued | Self::Uploading { .. } | Self::Sending) }
}

fn ended() -> DbError { DbError::Interrupted { detail: "the session of this command ended".into() } }

fn refused(model: &str) -> DbError { DbError::Invalid { detail: format!("{model} declares no durable command: only sending a message and a media upload work without a network") } }

impl Database {
    /// Runs the work in the epoch of the caller: it starts only while that epoch is current and ends with it.
    async fn in_epoch<T>(&self, epoch: u64, target: &'static str, work: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
        let lease = self.streams.lease(Origin::Session, target).map_err(|_| ended())?;
        if lease.epoch() != epoch { return Err(ended()); }
        lease.run(work).await.map_err(|_| ended())?
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
    fn admits(&self, epoch: u64) -> bool { self.streams.admits_epoch(epoch) }
}

#[uniffi::export]
impl Database {
    /// Writes the row locally in the epoch of the caller and queues it for the server. The SQL commit is refused when the epoch moved or the database closed before it.
    pub async fn send(&self, request: CommandRequest) -> Result<CommandReceipt, DbError> {
        self.admit_command(&request)?;
        let this = self.clone();
        let epoch = request.epoch;
        let (model, upload_action) = (request.model.clone(), request.upload.as_ref().map(|upload| upload.action.clone()));
        let work = async move {
            let values: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&request.values_json)?;
            let row = match request.upload {
                None => {
                    let args = serde_json::from_str(&request.args_json)?;
                    let (database, model) = (this.clone(), request.model.clone());
                    blocking(move || {
                        let life = database.db.shared.store.lifecycle.clone();
                        let row = crate::store::pinned_to_epoch(&life, epoch, || database.db.create_in(None, &model, values, args));
                        let row = row.map_err(|error| if database.admits(epoch) { DbError::from(error) } else { ended() })?;
                        database.send_writes();
                        Ok(database.wire(&model)?.row(&row))
                    }).await?
                }
                Some(upload) => {
                    let sync = this.sync()?.clone();
                    let (db, wire, model) = (this.db.clone(), this.wire(&request.model)?.clone(), request.model.clone());
                    on_executor(async move {
                        let file = upload.request.into_file()?;
                        let id = sync.upload_create_in(&model, &upload.action, values, file, Some(epoch)).await?;
                        let row = db.find(&model, &id)?.ok_or_else(|| DbError::Invalid { detail: format!("{model} {id} was not created") })?;
                        Ok::<_, DbError>(wire.row(&row))
                    }).await?
                }
            };
            Ok(row)
        };
        let row = self.in_epoch(epoch, "send", work).await?;
        Ok(CommandReceipt { epoch, model, row, upload_action })
    }

    /// The status of the command now. A receipt of an epoch that has gone is `Ended`, also when the epoch moves while the row is read.
    pub fn command_status(&self, receipt: CommandReceipt) -> Result<CommandStatus, DbError> {
        if !self.admits(receipt.epoch) { return Ok(CommandStatus::Ended); }
        let status = self.status_of(&receipt)?;
        Ok(if self.admits(receipt.epoch) { status } else { CommandStatus::Ended })
    }

    /// Waits for the one outcome of the command: delivered, refused, or ended with its session. It waits also while the network is away and the command is queued.
    pub async fn command_outcome(&self, receipt: CommandReceipt) -> Result<CommandStatus, DbError> {
        let mut outbox = self.db.outbox_changes();
        let mut transitions = self.streams.watch();
        loop {
            outbox.borrow_and_update();
            let status = self.command_status(receipt.clone())?;
            if status.settled() { return Ok(status); }
            tokio::select! {
                _ = outbox.changed() => {}
                _ = transitions.changed() => {}
            }
        }
    }
}

impl Database {
    fn status_of(&self, receipt: &CommandReceipt) -> Result<CommandStatus, DbError> {
        let key = format!("{}\0{}", receipt.model, receipt.row.id);
        if let Some(real) = crate::db::locked(&self.db.shared.replaced_ids).get(&key) { return Ok(CommandStatus::Delivered { id: real.clone() }); }
        let id = RowId::new(&receipt.row.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let Some(row) = self.db.find(&receipt.model, &id)? else { return Ok(CommandStatus::Failed { message: "the command was discarded".into() }) };
        if let Some(info) = receipt.upload_action.as_ref().and_then(|action| row.sync.uploads.get(action)) {
            match info.status {
                UploadStatus::Failed | UploadStatus::Cancelled => return Ok(CommandStatus::Failed { message: info.error.as_ref().map_or_else(|| "the upload failed".into(), |error| error.message.clone()) }),
                UploadStatus::Waiting | UploadStatus::Preparing | UploadStatus::Queued | UploadStatus::Uploading => return Ok(CommandStatus::Uploading { sent_bytes: info.sent_bytes, total_bytes: info.total_bytes }),
                UploadStatus::Sending | UploadStatus::Done | UploadStatus::DeliveryUnknown => {}
            }
        }
        Ok(match row.sync.state {
            SyncState::Pending => CommandStatus::Queued,
            SyncState::Sending => CommandStatus::Sending,
            SyncState::Synced => CommandStatus::Delivered { id: row.id.to_string() },
            SyncState::DeliveryUnknown => CommandStatus::Unknown,
            SyncState::Failed => CommandStatus::Failed { message: row.sync.error.as_ref().map_or_else(|| "failed".into(), |error| error.message.clone()) },
        })
    }
}
