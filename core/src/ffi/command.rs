use crate::identity::RowId;
use crate::lifecycle::Origin;
use crate::syncstate::SyncState;
use super::database::blocking;
use super::wire::WireRow;
use super::{Database, DbError};

/// A durable command: the row is written locally at once and the library sends it, also after the network returned or the app restarted.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandRequest {
    /// The session epoch the caller was admitted in. A command for an epoch that has gone is refused.
    pub epoch: u64,
    pub model: String,
    pub values_json: String,
    pub args_json: String,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct CommandReceipt {
    pub epoch: u64,
    pub model: String,
    /// The local row. Its id is a temporary id until the server confirms the command.
    pub row: WireRow,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CommandStatus {
    Queued,
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

fn ended() -> DbError { DbError::Interrupted { detail: "the session of this command ended".into() } }

#[uniffi::export]
impl Database {
    /// Writes the row locally in the epoch of the caller and queues it for the server.
    pub async fn send(&self, request: CommandRequest) -> Result<CommandReceipt, DbError> {
        let lease = self.streams.lease(Origin::Session, "send").map_err(|_| ended())?;
        if lease.epoch() != request.epoch { return Err(ended()); }
        let pinned = lease.generation();
        let (database, model, values, args) = (self.clone(), request.model.clone(), request.values_json, request.args_json);
        let work = blocking(move || {
            let row = database.db.create_in(pinned, &model, serde_json::from_str(&values)?, serde_json::from_str(&args)?)?;
            database.send_writes();
            Ok(database.wire(&model)?.row(&row))
        });
        let row = lease.run(work).await.map_err(|_| ended())??;
        Ok(CommandReceipt { epoch: request.epoch, model: request.model, row })
    }

    /// The status of the command now.
    pub fn command_status(&self, receipt: CommandReceipt) -> Result<CommandStatus, DbError> {
        if self.streams.snapshot().session_epoch != receipt.epoch { return Ok(CommandStatus::Ended); }
        let key = format!("{}\0{}", receipt.model, receipt.row.id);
        if let Some(real) = crate::db::locked(&self.db.shared.replaced_ids).get(&key) { return Ok(CommandStatus::Delivered { id: real.clone() }); }
        let id = RowId::new(&receipt.row.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let Some(row) = self.db.find(&receipt.model, &id)? else { return Ok(CommandStatus::Failed { message: "the command was discarded".into() }) };
        Ok(match row.sync.state {
            SyncState::Pending => CommandStatus::Queued,
            SyncState::Sending => CommandStatus::Sending,
            SyncState::Synced => CommandStatus::Delivered { id: row.id.to_string() },
            SyncState::DeliveryUnknown => CommandStatus::Unknown,
            SyncState::Failed => CommandStatus::Failed { message: row.sync.error.as_ref().map_or_else(|| "failed".into(), |error| error.message.clone()) },
        })
    }

    /// Waits for the one outcome of the command: delivered, refused, or ended with its session.
    pub async fn command_outcome(&self, receipt: CommandReceipt) -> Result<CommandStatus, DbError> {
        let sync = self.sync()?.clone();
        let id = RowId::new(&receipt.row.id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let model = receipt.model.clone();
        let settled = super::database::on_executor(async move { sync.confirmed(&model, &id).await }).await;
        match settled {
            Ok(real) => Ok(CommandStatus::Delivered { id: real.to_string() }),
            Err(DbError::Interrupted { .. }) => Ok(CommandStatus::Ended),
            Err(error @ (DbError::Network { .. } | DbError::Invalid { .. } | DbError::Database { .. })) => Err(error),
            Err(error) => Ok(CommandStatus::Failed { message: error.to_string() }),
        }
    }
}
