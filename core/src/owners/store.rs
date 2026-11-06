//! Owner `StoreWriter`: the only writer of SQLite. SQLite runs as the effects `Write` and `Erase`;
//! the result comes back as the input `Written` or `Erased` (R2), then `revision` grows and the author gets its answer.

use std::collections::{BTreeMap, BTreeSet};

use std::collections::VecDeque;

use super::contract::{BatchAuthor, BatchId, EpochEnded, Published, PublishedState, RejectReason};
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

/// Membership of a row in a list: `Join` puts it at `order_key`, `Leave` takes it out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectMember {
    pub model: String,
    pub list: String,
    pub row_id: String,
    pub order_key: String,
    pub leave: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectTombstone {
    pub model: String,
    pub row_id: String,
}

/// The parent row changes its `updated_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectTouch {
    pub model: String,
    pub row_id: String,
}

/// What an operation changes before the server answers; `RollBack` takes it back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntentEffects {
    pub rows: Vec<EffectRow>,
    pub members: Vec<EffectMember>,
    pub tombstones: Vec<EffectTombstone>,
    pub touches: Vec<EffectTouch>,
}

impl IntentEffects {
    pub fn is_empty(&self) -> bool { self.rows.is_empty() && self.members.is_empty() && self.tombstones.is_empty() && self.touches.is_empty() }
}

/// A row of the server answer or of a local write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowValue {
    pub row_id: String,
    /// The values as JSON text.
    pub values: String,
}

/// Why the batch writes the row of `sync_upload`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadKind {
    SyncUpload,
    ConfirmedParts,
    Completed,
    Failed,
    Retrying,
}

/// The columns of a row of `sync_upload`: the whole value of a transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRow {
    pub key: String,
    pub seq: u64,
    pub source: String,
    pub parts: u32,
    /// The group of the elements of a batch.
    pub batch: Option<u64>,
    /// The fields of an element of a batch in the list of the mutation of the batch (JSON).
    pub fields: Option<String>,
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
    pub on_failure: crate::schema::OnFailure,
    /// The request the steps of the file are built from.
    pub steps_path: String,
    pub steps_body: String,
    pub transfer_id: u64,
    pub cycle_id: u64,
    pub signed_id: Option<String>,
    pub aborted: bool,
    /// The operation the optimistic effects of the transfer belong to (`ReserveOp`).
    pub op_id: Option<u64>,
    /// The etag of each confirmed part, in the order of the parts (`sync_upload_part`).
    pub etags: Vec<Option<String>>,
}

/// One write of a batch, by value (R3). The writer executor turns it into SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// `encrypted_pending` = this row. `credential_digest` is the full digest of the credential, `response_key` seals the answer.
    Pending { pending: PendingCall, credential_digest: String, wire_id: String, response_key: String },
    /// Remove the row `encrypted_pending`.
    RemovePending,
    /// Remove the row `encrypted_session` (a sign-in that the server refused).
    EraseSession,
    /// `encrypted_answer`: the terminal outcome of the `Mutate` of `request_id`, written with `RemovePending` and kept until `RemoveAnswer`.
    Answer { request_id: u64, operation: Option<i64>, result: crate::transport::encrypted::state::StoredAnswer },
    RemoveAnswer { request_id: u64 },
    /// `encrypted_session`: `digest = None` keeps the stored digest, `sequence = None` keeps the stored sequence.
    Session { digest: Option<String>, sequence: Option<u64> },
    /// The intent of an operation with its optimistic `effects`.
    Intent { op_id: u64, model: String, row_id: String, action: String, payload: String, effects: IntentEffects },
    /// The durable attempt counter and the cycle of an operation: written before the `Mutate` of that attempt.
    Attempt { op_id: u64, attempts: u32, cycle_id: u64 },
    /// A `create` with a temporary id landed: the row `temp` becomes `real` with its references and membership.
    Swap { model: String, temp: String, real: String },
    /// The rows, membership, tombstones and touches of a call that has no operation (a local model, an action without a mutation): final at once.
    Planned { effects: IntentEffects },
    /// A file for a field of a row: the bytes go to the directory of the files or to the table, the row gets the descriptor.
    AttachFile { model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String },
    DetachFile { model: String, row_id: String, field: String },
    /// Rows of a `memory` model, written to the TEMP tables.
    EphemeralRows { model: String, rows: Vec<RowValue> },
    /// A local field of a row that is not in the base yet: it sits on the row when the row lands.
    LocalPending { model: String, rows: Vec<RowValue> },
    /// Take back the `effects` of the operation.
    RollBack { op_id: u64 },
    /// The operation leaves the queue (its answer landed or it ended).
    RemoveIntent { op_id: u64 },
    /// The answer of an action landed: the stored windows of these (model, relation) pairs are stale, whoever reads them.
    StaleFamilies { families: Vec<(String, String)> },
    /// Set or clear the error shown on the row of the operation (`on_failure: keep`).
    MarkError { op_id: u64, error: Option<String> },
    /// Rows of the server answer of `model`.
    Rows { model: String, rows: Vec<RowValue> },
    /// Rows of a local model (`set_local`, `set_ephemeral`, `attach`, `detach`).
    LocalRows { model: String, rows: Vec<RowValue> },
    RemoveRows { model: String, ids: Vec<String> },
    Upload { row: UploadRow, kind: UploadKind },
    RemoveUpload { key: String },
    /// A multipart upload of a removed transfer: `abort` goes to the server until the server answers.
    UploadOrphan { upload_id: String, key: String },
    RemoveOrphan { upload_id: String },
    /// How the elements of a batch go out together: `joint` is the text `Joint::to_text` gives.
    UploadBatch { batch_id: u64, joint: String },
    /// Integer fields of a row set to numbers (the position and the size of an element of a batch): `writes` are paths with their values.
    Numbers { model: String, row_id: String, writes: Vec<(String, i64)> },
    /// The server blob of a file with this content: a later upload of the same bytes skips the steps while the blob lives.
    BlobCachePut { content_sha256: String, blob_id: String, created_at: u64 },
    BlobCacheRemove { content_sha256: String },
    /// A page of a window: the rows of the answer, the window record (`page`, `cursor`) and whether it replaces the rows of the window.
    Window { key: String, model: String, page: u32, replace_all: bool, cursor: Option<String>, roots: Vec<RowValue>, related: Vec<(String, RowValue)>, total: Option<i64>, base_revision: i64 },
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
    Same { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, revision: u64 },
    /// The fingerprint of a server model changed: the cache is erased in the `Boot` transaction and the generation grows.
    ChangedModels { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, revision: u64 },
    FormatChange { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, revision: u64 },
    /// The fingerprint of a `storage: local` model changed, the server models match: only that model is rebuilt.
    LocalModelChanged { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, revision: u64 },
    SqliteError(String),
    /// `saved_epoch` or `saved_generation` is not a u64.
    CorruptManifest(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenMessage {
    pub saved_epoch: Epoch,
    pub saved_generation: u64,
    pub saved_identity: Option<Identity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreReply {
    Committed { revision: u64 },
    Rejected(RejectReason),
}

/// What a finished transaction of SQLite reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Done {
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreEffect {
    /// Create the tables (boot of a new database) or rebuild the cache tables (format change).
    Prepare { format_change: bool },
    OpenLifecycle(OpenMessage),
    OpenFailed(String),
    OpenCorrupt(String),
    /// One transaction: `rows`, `intents`, `files`.
    Write { batch: StoreBatch },
    /// One transaction: erase the account (`erase_encrypted` for `SessionReset` and `ServerEnded`).
    EraseAccount { io_id: u64, generation_new: u64, erase_encrypted: bool },
    EraseModels { io_id: u64, generation_new: u64, models: BTreeSet<String> },
    /// The secret of `CredentialStore` after the commit of SQLite.
    ClearSecret { io_id: u64 },
    /// One transaction: `saved_epoch`, `saved_generation`, `saved_identity`.
    SavePublished { io_id: u64, publication_id: u64, published: Published },
    Reply { author: BatchAuthor, batch: BatchId, epoch: Epoch, reply: StoreReply },
    Cleared { epoch_old: Epoch, erase_id: u64 },
    ModelsCleared { generation_new: u64, erase_id: u64 },
    ClearFailed { erase_id: u64, error: String },
    Saved { publication_id: u64 },
    SaveFailed { publication_id: u64, error: String },
    /// Broadcast `Committed{revision, tables, models}`; `None` = all tables and models.
    BroadcastCommitted { revision: u64, models: Option<Vec<String>> },
    /// The snapshot of the intents for Outbox, Transfer and Session.
    ReadIntents { epoch: Epoch },
    CloseReads,
    CloseConnections,
    StoreClosed,
}

#[derive(Clone, Debug)]
pub enum StoreInput {
    Boot(BootFound),
    Commit(StoreBatch),
    Written { author: BatchAuthor, batch: BatchId, result: Result<Done, String> },
    ClearAccount { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: u64 },
    ClearModels { epoch: Epoch, generation_new: u64, models: BTreeSet<String>, erase_id: u64 },
    Erased { io_id: u64, result: Result<Done, String> },
    Published { publication_id: u64, published: Published },
    EpochEnded(EpochEnded),
    ReadIntents { epoch: Epoch },
    Close,
    ReadsClosed { ids: Vec<u64> },
    ReadFinished { read_id: u64 },
    Idle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMode {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IoKind {
    Account { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: u64 },
    Models { generation_new: u64, models: BTreeSet<String>, erase_id: u64 },
    Secret { epoch_old: Epoch, erase_id: u64, revision: u64 },
    Save { publication_id: u64, published: Published },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Io {
    id: u64,
    kind: IoKind,
    started: bool,
}

pub struct StoreState {
    mode: StoreMode,
    closing: bool,
    revision: u64,
    epoch: Epoch,
    saved_epoch: Epoch,
    saved_generation: u64,
    generations: BTreeMap<String, u64>,
    /// `model_generation[*]` of the models without an entry.
    base_generation: u64,
    writing: Option<StoreBatch>,
    io: VecDeque<Io>,
    next_io: u64,
    deferred: VecDeque<StoreInput>,
    reads: BTreeSet<u64>,
    reads_closed: bool,
}

impl StoreState {
    pub fn new() -> Self {
        Self {
            mode: StoreMode::Open,
            closing: false,
            revision: 0,
            epoch: 0,
            saved_epoch: 0,
            saved_generation: 0,
            generations: BTreeMap::new(),
            base_generation: 0,
            writing: None,
            io: VecDeque::new(),
            next_io: 1,
            deferred: VecDeque::new(),
            reads: BTreeSet::new(),
            reads_closed: false,
        }
    }

    pub fn mode(&self) -> StoreMode { self.mode }

    pub fn revision(&self) -> u64 { self.revision }

    pub fn epoch(&self) -> Epoch { self.epoch }

    pub fn closing(&self) -> bool { self.closing }

    pub fn generation(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    fn max_generation(&self) -> u64 { self.generations.values().copied().max().unwrap_or(0).max(self.base_generation) }

    fn busy(&self) -> bool { self.writing.is_some() || !self.io.is_empty() }

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
    /// One row of the table per match arm, in table order.
    fn run(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if let StoreInput::Boot(found) = input {
            self.boot(found, fx);
            return;
        }
        if self.mode == StoreMode::Closed {
            match input {
                StoreInput::Commit(batch) => fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Closed) }),
                StoreInput::ClearAccount { erase_id, .. } | StoreInput::ClearModels { erase_id, .. } => fx.push(StoreEffect::ClearFailed { erase_id, error: "closed".into() }),
                _ => {}
            }
            return;
        }
        let ends_a_step = matches!(input, StoreInput::Written { .. } | StoreInput::Erased { .. } | StoreInput::ReadsClosed { .. } | StoreInput::ReadFinished { .. });
        self.apply(input, fx);
        if ends_a_step {
            self.check_close(fx);
        }
    }

    /// Starts the head of `io` when it waits and nothing is being written.
    fn start_head(&mut self, fx: &mut Vec<StoreEffect>) {
        if self.writing.is_some() {
            return;
        }
        let Some(head) = self.io.front_mut() else { return };
        if head.started {
            return;
        }
        head.started = true;
        if let IoKind::Save { publication_id, published } = &head.kind {
            fx.push(StoreEffect::SavePublished { io_id: head.id, publication_id: *publication_id, published: published.clone() });
        }
    }

    /// Applies the inputs that waited, in order, while the Store is free.
    fn drain(&mut self, fx: &mut Vec<StoreEffect>) {
        self.start_head(fx);
        while !self.busy() {
            let Some(input) = self.deferred.pop_front() else { break };
            self.apply(input, fx);
        }
    }

    fn check_close(&mut self, fx: &mut Vec<StoreEffect>) {
        if self.closing && self.reads_closed && self.writing.is_none() && self.io.is_empty() && self.reads.is_empty() {
            self.mode = StoreMode::Closed;
            fx.push(StoreEffect::CloseConnections);
            fx.push(StoreEffect::StoreClosed);
        }
    }

    fn wipe_account(&mut self, generation_new: u64) {
        self.generations.clear();
        self.base_generation = generation_new;
    }

    fn apply(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if self.closing {
            match &input {
                StoreInput::Commit(batch) => {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Closed) });
                    return;
                }
                StoreInput::ClearAccount { erase_id, .. } | StoreInput::ClearModels { erase_id, .. } => {
                    fx.push(StoreEffect::ClearFailed { erase_id: *erase_id, error: "closed".into() });
                    return;
                }
                StoreInput::Published { publication_id, .. } => {
                    fx.push(StoreEffect::SaveFailed { publication_id: *publication_id, error: "closed".into() });
                    return;
                }
                StoreInput::ReadIntents { .. } => return,
                _ => {}
            }
        }
        match input {
            StoreInput::Commit(batch) => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::Commit(batch));
                } else if batch.epoch != self.epoch {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::NoSession) });
                } else if batch.rows.iter().any(|(model, _)| !batch.basis.contains_key(model)) {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Invalid) });
                } else if !self.stale_models(&batch).is_empty() {
                    let models = self.stale_models(&batch);
                    let generation = models.iter().map(|model| self.generation(model)).max().unwrap_or(self.base_generation);
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::StaleGeneration { models, generation }) });
                } else {
                    self.writing = Some(batch.clone());
                    fx.push(StoreEffect::Write { batch });
                }
            }
            StoreInput::Written { author, batch, result } => {
                if !self.writing.as_ref().is_some_and(|writing| writing.author == author && writing.id == batch) {
                    return;
                }
                let Some(written) = self.writing.take() else { return };
                match result {
                    Err(error) => fx.push(StoreEffect::Reply { author, batch, epoch: written.epoch, reply: StoreReply::Rejected(RejectReason::Storage(error)) }),
                    Ok(Done { revision }) => {
                        self.revision = revision;
                        let models: Vec<String> = written.rows.iter().map(|(model, _)| model.clone()).collect::<BTreeSet<_>>().into_iter().collect();
                        fx.push(StoreEffect::Reply { author, batch, epoch: written.epoch, reply: StoreReply::Committed { revision } });
                        fx.push(StoreEffect::BroadcastCommitted { revision, models: Some(models) });
                    }
                }
                self.drain(fx);
            }
            StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id });
                } else if generation_new <= self.max_generation() {
                    fx.push(StoreEffect::ClearFailed { erase_id, error: "stale generation".into() });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Account { epoch_old, cause, generation_new, erase_id }, started: true });
                    fx.push(StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted: matches!(cause, Cause::SessionReset | Cause::ServerEnded) });
                }
            }
            StoreInput::ClearModels { epoch, generation_new, models, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::ClearModels { epoch, generation_new, models, erase_id });
                } else if models.iter().any(|model| generation_new <= self.generation(model)) {
                    fx.push(StoreEffect::ClearFailed { erase_id, error: "stale generation".into() });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Models { generation_new, models: models.clone(), erase_id }, started: true });
                    fx.push(StoreEffect::EraseModels { io_id, generation_new, models });
                }
            }
            StoreInput::Erased { io_id, result } => {
                if self.io.front().map(|head| head.id) != Some(io_id) {
                    return;
                }
                let Some(head) = self.io.pop_front() else { return };
                match (head.kind, result) {
                    (IoKind::Account { erase_id, .. } | IoKind::Models { erase_id, .. }, Err(error)) => fx.push(StoreEffect::ClearFailed { erase_id, error }),
                    (IoKind::Save { publication_id, .. }, Err(error)) => fx.push(StoreEffect::SaveFailed { publication_id, error }),
                    (IoKind::Account { epoch_old, cause, generation_new, erase_id }, Ok(Done { revision })) => {
                        self.wipe_account(generation_new);
                        if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                            self.io.push_front(Io { id: head.id, kind: IoKind::Secret { epoch_old, erase_id, revision }, started: true });
                            fx.push(StoreEffect::ClearSecret { io_id: head.id });
                            return;
                        }
                        self.revision = revision;
                        fx.push(StoreEffect::Cleared { epoch_old, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, models: None });
                    }
                    (IoKind::Secret { epoch_old, erase_id, revision }, Ok(_) | Err(_)) => {
                        self.revision = revision;
                        fx.push(StoreEffect::Cleared { epoch_old, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, models: None });
                    }
                    (IoKind::Models { generation_new, models, erase_id }, Ok(Done { revision })) => {
                        for model in &models {
                            self.generations.insert(model.clone(), generation_new);
                        }
                        self.revision = revision;
                        fx.push(StoreEffect::ModelsCleared { generation_new, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, models: Some(models.into_iter().collect()) });
                    }
                    (IoKind::Save { publication_id, published }, Ok(_)) => {
                        if matches!(published.state, PublishedState::NoSession | PublishedState::Session | PublishedState::Resetting) {
                            self.epoch = self.epoch.max(published.epoch);
                        }
                        self.saved_epoch = published.epoch;
                        self.saved_generation = published.generation;
                        fx.push(StoreEffect::Saved { publication_id });
                    }
                }
                self.drain(fx);
            }
            StoreInput::Published { publication_id, published } => {
                let id = self.next_io;
                self.next_io += 1;
                self.io.push_back(Io { id, kind: IoKind::Save { publication_id, published }, started: false });
                self.start_head(fx);
            }
            StoreInput::EpochEnded(EpochEnded { epoch, cause }) => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::EpochEnded(EpochEnded { epoch, cause }));
                } else if epoch + 1 > self.epoch {
                    self.epoch = epoch + 1;
                }
            }
            StoreInput::ReadIntents { epoch } => fx.push(StoreEffect::ReadIntents { epoch }),
            StoreInput::Close => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::Close);
                } else if !self.closing {
                    self.closing = true;
                    self.reads_closed = false;
                    fx.push(StoreEffect::CloseReads);
                }
            }
            StoreInput::ReadsClosed { ids } => {
                self.reads = ids.into_iter().collect();
                self.reads_closed = true;
            }
            StoreInput::ReadFinished { read_id } => {
                self.reads.remove(&read_id);
            }
            StoreInput::Boot(_) | StoreInput::Idle => {}
        }
    }

    fn boot(&mut self, found: BootFound, fx: &mut Vec<StoreEffect>) {
        let open = |saved_epoch, saved_generation, saved_identity| StoreEffect::OpenLifecycle(OpenMessage { saved_epoch, saved_generation, saved_identity });
        match found {
            BootFound::Fresh => {
                fx.push(StoreEffect::Prepare { format_change: false });
                fx.push(open(0, 0, None));
            }
            BootFound::Same { saved_epoch, saved_generation, saved_identity, revision } | BootFound::LocalModelChanged { saved_epoch, saved_generation, saved_identity, revision } => {
                if matches!(found, BootFound::LocalModelChanged { .. }) {
                    fx.push(StoreEffect::Prepare { format_change: false });
                }
                self.revision = revision;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation;
                self.base_generation = saved_generation;
                self.generations.clear();
                fx.push(open(saved_epoch, saved_generation, saved_identity));
            }
            BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, revision } | BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, revision } => {
                self.revision = revision + 1;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation + 1;
                self.base_generation = saved_generation + 1;
                self.generations.clear();
                fx.push(StoreEffect::Prepare { format_change: true });
                fx.push(open(saved_epoch, saved_generation + 1, saved_identity));
            }
            BootFound::SqliteError(error) => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::OpenFailed(error));
            }
            BootFound::CorruptManifest(error) => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::OpenCorrupt(error));
            }
        }
    }
}
