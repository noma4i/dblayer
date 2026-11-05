//! Owner `StoreWriter` (`specs/machines/store.md`): the only writer of SQLite. SQLite runs as the effects `Write` and `Erase`;
//! the result comes back as the input `Written` or `Erased` (R2), then `revision` grows and the author gets its answer.

use std::collections::{BTreeMap, BTreeSet};

use super::contract::{BatchAuthor, BatchId, EpochEnded, Published, RejectReason};
use super::session::PendingCall;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

use super::lifecycle::Identity;

/// An optimistic row of an operation: the values the caller sees before the server answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectRow {
    pub model: String,
    pub row_id: String,
    /// The values as JSON text.
    pub values: String,
}

/// Membership of a row in a list with the revision of the operation (`outbox.md`, membership by revision).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectMember {
    pub list: String,
    pub row_id: String,
    pub revision: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectTombstone {
    pub model: String,
    pub row_id: String,
}

/// What an operation changes before the server answers; `RollBack` takes it back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntentEffects {
    pub rows: Vec<EffectRow>,
    pub members: Vec<EffectMember>,
    pub tombstones: Vec<EffectTombstone>,
}

/// A row of the server answer or of a local write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowValue {
    pub row_id: String,
    /// The values as JSON text.
    pub values: String,
}

/// The columns of a row of `sync_upload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRow {
    pub key: String,
    pub seq: u64,
    pub source: String,
    pub parts: u32,
    pub batch: bool,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub completed: bool,
    pub failed: Option<String>,
    pub created_at: u64,
    pub inline: bool,
    pub size: u64,
    pub sha256: String,
    /// The final mutation: `path` and `body` of the request.
    pub path: String,
    pub body: String,
}

/// One write of a batch, by value (R3). The writer executor turns it into SQL (`store.md`, the batch).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// `encrypted_pending` = this row. `credential_digest` is the full digest of the credential, `response_key` seals the answer.
    Pending { pending: PendingCall, credential_digest: String, response_key: String },
    /// Remove the row `encrypted_pending`.
    RemovePending,
    /// `encrypted_session`: `digest = None` keeps the stored digest, `sequence = None` keeps the stored sequence.
    Session { digest: Option<String>, sequence: Option<u64> },
    /// The intent of an operation with its optimistic `effects`.
    Intent { op_id: u64, model: String, row_id: String, action: String, payload: String, effects: IntentEffects },
    /// Take back the `effects` of the operation.
    RollBack { op_id: u64 },
    /// The operation leaves the queue (its answer landed or it ended).
    RemoveIntent { op_id: u64 },
    /// Set or clear the error shown on the row of the operation (`on_reject: keep`).
    MarkError { op_id: u64, error: Option<String> },
    /// Rows of the server answer of `model`.
    Rows { model: String, rows: Vec<RowValue> },
    /// Rows of a local model (`set_local`, `set_ephemeral`, `attach`, `detach`).
    LocalRows { model: String, rows: Vec<RowValue> },
    RemoveRows { model: String, ids: Vec<String> },
    Upload { row: UploadRow },
    RemoveUpload { key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreBatch {
    pub author: BatchAuthor,
    pub id: BatchId,
    pub epoch: Epoch,
    pub basis: BTreeMap<String, u64>,
    pub rows: Vec<(String, String)>,
    /// Number of intents the batch writes (operations of the queue, `encrypted_*`, transfers).
    pub intents: u64,
    /// The batch writes `encrypted_session` with this identity.
    pub credential: Option<Identity>,
    /// The writes to `encrypted_*`, in one transaction with `rows`.
    pub ops: Vec<StoreOp>,
}

/// What `Boot` found in the database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootFound {
    Fresh,
    Same { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity> },
    ChangedModels { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, changed_models: BTreeSet<String> },
    FormatChange { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity> },
    SqliteError(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenMessage {
    pub saved_epoch: Epoch,
    pub saved_generation: u64,
    pub saved_identity: Option<Identity>,
    pub changed_models: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreReply {
    Committed { revision: u64 },
    Rejected(RejectReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreEffect {
    /// Create the tables (boot of a new database) or rebuild the cache tables (format change).
    Prepare { format_change: bool },
    OpenLifecycle(OpenMessage),
    OpenFailed(String),
    /// One transaction: `rows`, `intents`, `files`.
    Write { batch: StoreBatch },
    /// One transaction: erase the account (`erase_encrypted` for `SessionReset` and `ServerEnded`).
    EraseAccount { generation_new: u64, erase_encrypted: bool },
    EraseModels { generation_new: u64, models: BTreeSet<String> },
    Reply { author: BatchAuthor, batch: BatchId, reply: StoreReply },
    Cleared { epoch_old: Epoch },
    ModelsCleared { generation_new: u64 },
    ClearFailed(String),
    /// Broadcast `Committed{revision, tables, models}`; `None` = all tables and models.
    BroadcastCommitted { revision: u64, models: Option<Vec<String>> },
    SavePublished { epoch: Epoch, generation: u64 },
    CloseConnections,
}

#[derive(Clone, Debug)]
pub enum StoreInput {
    Boot(BootFound),
    Commit(StoreBatch),
    Written { author: BatchAuthor, batch: BatchId, result: Result<(), String> },
    ClearAccount { epoch_old: Epoch, cause: Cause, generation_new: u64 },
    ClearModels { epoch: Epoch, generation_new: u64, models: BTreeSet<String> },
    Erased { result: Result<(), String> },
    Published(Published),
    EpochEnded(EpochEnded),
    Close,
    Idle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMode {
    Open,
    Closed,
}

enum Erasing {
    Account { epoch_old: Epoch, generation_new: u64 },
    Models { generation_new: u64, models: BTreeSet<String> },
}

pub struct StoreState {
    mode: StoreMode,
    revision: u64,
    epoch: Epoch,
    saved_epoch: Epoch,
    saved_generation: u64,
    generations: BTreeMap<String, u64>,
    /// `model_generation[*]` of the models without an entry.
    base_generation: u64,
    writing: BTreeMap<(BatchAuthor, BatchId), StoreBatch>,
    erasing: Option<Erasing>,
}

impl StoreState {
    pub fn new() -> Self {
        Self { mode: StoreMode::Open, revision: 0, epoch: 0, saved_epoch: 0, saved_generation: 0, generations: BTreeMap::new(), base_generation: 0, writing: BTreeMap::new(), erasing: None }
    }

    pub fn mode(&self) -> StoreMode { self.mode }

    pub fn revision(&self) -> u64 { self.revision }

    pub fn epoch(&self) -> Epoch { self.epoch }

    pub fn generation(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    fn stale_models(&self, batch: &StoreBatch) -> Vec<String> {
        batch.basis.iter().filter(|(model, basis)| **basis < self.generation(model)).map(|(model, _)| model.clone()).collect()
    }
}

pub struct StoreWriter;

impl StateOwner for StoreWriter {
    const EPOCHED: bool = false;

    type State = StoreState;
    type Input = StoreInput;
    type Effect = StoreEffect;

    fn step(state: &mut StoreState, input: StoreInput) -> Effects<Self> {
        let mut fx = Vec::new();
        state.run(input, &mut fx);
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(_now: Time) -> StoreInput { StoreInput::Idle }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> StoreInput { StoreInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> StoreInput { StoreInput::Idle }
}

impl StoreState {
    /// One row of `store.md` per match arm, in table order.
    fn run(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if let StoreInput::Boot(found) = input {
            self.boot(found, fx);
            return;
        }
        if self.mode == StoreMode::Closed {
            if let StoreInput::Commit(batch) = input {
                fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, reply: StoreReply::Rejected(RejectReason::Closed) });
            }
            return;
        }
        match input {
            StoreInput::Commit(batch) if batch.epoch != self.epoch => fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, reply: StoreReply::Rejected(RejectReason::NoSession) }),
            StoreInput::Commit(batch) if !self.stale_models(&batch).is_empty() => {
                let models = self.stale_models(&batch);
                fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, reply: StoreReply::Rejected(RejectReason::StaleGeneration { models }) });
            }
            StoreInput::Commit(batch) => {
                self.writing.insert((batch.author, batch.id), batch.clone());
                fx.push(StoreEffect::Write { batch });
            }
            StoreInput::Written { author, batch, result } => {
                let Some(written) = self.writing.remove(&(author, batch)) else { return };
                match result {
                    Err(error) => fx.push(StoreEffect::Reply { author, batch, reply: StoreReply::Rejected(RejectReason::Storage(error)) }),
                    Ok(()) => {
                        self.revision += 1;
                        let models: Vec<String> = written.rows.iter().map(|(model, _)| model.clone()).collect::<BTreeSet<_>>().into_iter().collect();
                        fx.push(StoreEffect::Reply { author, batch, reply: StoreReply::Committed { revision: self.revision } });
                        fx.push(StoreEffect::BroadcastCommitted { revision: self.revision, models: Some(models) });
                    }
                }
            }
            StoreInput::ClearAccount { epoch_old, cause, generation_new } => {
                self.erasing = Some(Erasing::Account { epoch_old, generation_new });
                fx.push(StoreEffect::EraseAccount { generation_new, erase_encrypted: matches!(cause, Cause::SessionReset | Cause::ServerEnded) });
            }
            StoreInput::ClearModels { generation_new, models, .. } => {
                self.erasing = Some(Erasing::Models { generation_new, models: models.clone() });
                fx.push(StoreEffect::EraseModels { generation_new, models });
            }
            StoreInput::Erased { result } => match (self.erasing.take(), result) {
                (None, _) => {}
                (Some(_), Err(error)) => fx.push(StoreEffect::ClearFailed(error)),
                (Some(Erasing::Account { epoch_old, generation_new }), Ok(())) => {
                    self.revision += 1;
                    self.generations.clear();
                    self.base_generation = generation_new;
                    fx.push(StoreEffect::Cleared { epoch_old });
                    fx.push(StoreEffect::BroadcastCommitted { revision: self.revision, models: None });
                }
                (Some(Erasing::Models { generation_new, models }), Ok(())) => {
                    self.revision += 1;
                    for model in &models {
                        self.generations.insert(model.clone(), generation_new);
                    }
                    fx.push(StoreEffect::ModelsCleared { generation_new });
                    fx.push(StoreEffect::BroadcastCommitted { revision: self.revision, models: Some(models.into_iter().collect()) });
                }
            },
            StoreInput::Published(published) => {
                self.epoch = published.epoch;
                self.saved_epoch = published.epoch;
                self.saved_generation = published.generation;
                fx.push(StoreEffect::SavePublished { epoch: published.epoch, generation: published.generation });
            }
            StoreInput::EpochEnded(EpochEnded { epoch, .. }) => self.epoch = epoch + 1,
            StoreInput::Close => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::CloseConnections);
            }
            StoreInput::Boot(_) | StoreInput::Idle => {}
        }
    }

    fn boot(&mut self, found: BootFound, fx: &mut Vec<StoreEffect>) {
        let open = |saved_epoch, saved_generation, saved_identity, changed_models| StoreEffect::OpenLifecycle(OpenMessage { saved_epoch, saved_generation, saved_identity, changed_models });
        match found {
            BootFound::Fresh => {
                fx.push(StoreEffect::Prepare { format_change: false });
                fx.push(open(0, 0, None, BTreeSet::new()));
            }
            BootFound::Same { saved_epoch, saved_generation, saved_identity } => {
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation;
                fx.push(open(saved_epoch, saved_generation, saved_identity, BTreeSet::new()));
            }
            BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, changed_models } => {
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation;
                fx.push(open(saved_epoch, saved_generation, saved_identity, changed_models));
            }
            BootFound::FormatChange { saved_epoch, saved_generation, saved_identity } => {
                self.revision += 1;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation + 1;
                self.base_generation = saved_generation + 1;
                self.generations.clear();
                fx.push(StoreEffect::Prepare { format_change: true });
                fx.push(open(saved_epoch, saved_generation + 1, saved_identity, BTreeSet::new()));
            }
            BootFound::SqliteError(error) => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::OpenFailed(error));
            }
        }
    }
}
