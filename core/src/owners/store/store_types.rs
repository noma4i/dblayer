use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeOwner {
    Operation,
    Transfer,
}

impl OutcomeOwner {
    pub(crate) fn column(self) -> &'static str {
        match self {
            Self::Operation => "operation",
            Self::Transfer => "transfer",
        }
    }
}

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

/// What an operation changes before the server answers; `RollBack` takes it back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntentEffects {
    pub rows: Vec<EffectRow>,
    pub members: Vec<EffectMember>,
    pub tombstones: Vec<EffectTombstone>,
    /// The `(model, row_id)` the receipt of the call shows: the Store reads it in the transaction of the intent and replies with it.
    pub shown: Option<(String, String)>,
}

impl IntentEffects {
    pub fn is_empty(&self) -> bool { self.rows.is_empty() && self.members.is_empty() && self.tombstones.is_empty() }
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
    AwaitingSource,
    SyncUpload,
    ConfirmedParts,
    Completed,
    Failed,
    Retrying,
}

impl UploadKind {
    /// The value that the status columns of this write hold.
    pub fn shown(self) -> crate::owners::write::transfer::Shown {
        use crate::owners::write::transfer::Shown;
        match self {
            Self::AwaitingSource => Shown::WaitingSource,
            Self::SyncUpload | Self::Retrying => Shown::Queued { progress: 0 },
            Self::ConfirmedParts => Shown::Uploading { progress: 0 },
            Self::Completed => Shown::Uploading { progress: 100 },
            Self::Failed => Shown::Failed { error: String::new() },
        }
    }
}

/// The columns of a row of `sync_upload`: the whole value of a transfer.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadRow {
    pub key: String,
    pub seq: u64,
    pub source: Option<String>,
    pub parts: u32,
    /// The group of the elements of a batch.
    pub batch: Option<u64>,
    /// The fields of an element of a batch in the list of the mutation of the batch (JSON).
    pub fields: Option<String>,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    /// The share of the file shown last, in hundredths.
    pub progress: u32,
    /// The size of the copy that weighs the element in its batch (D-415).
    pub weight: Option<u64>,
    pub weighed_prepare: bool,
    /// The share of the batch the row shows, in hundredths.
    pub batch_progress: u32,
    pub attempts: u32,
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
    pub steps_path: Option<String>,
    pub steps_body: Option<String>,
    pub transfer_id: u64,
    pub cycle_id: u64,
    pub signed_id: Option<String>,
    pub aborted: bool,
    /// The new copy is committed and the rename of the temporary file is not.
    pub staged: bool,
    /// `Clock.now()` of the move to `Failed`: the expiry counts from it.
    pub failed_at: Option<u64>,
    /// `Clock.now()` of the answer of the creation of the blob: the term of its cache entry counts from it.
    pub blob_created_at: Option<u64>,
    /// The final mutation waits for `Commit`: the file goes up to the last part and no further.
    pub held: bool,
    /// The operation the optimistic effects of the transfer belong to (`ReserveOp`).
    pub op_id: Option<u64>,
    /// The etag of each confirmed part, in the order of the parts (`sync_upload_part`).
    pub etags: Vec<Option<String>>,
    /// The settings of the prepare step: written with the row (`sync_upload_prepare`).
    pub prepare: Option<Vec<u8>>,
    /// The largest file that may go to the network (`sync_upload.max_bytes`).
    pub max_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntentOrigin {
    Api,
    Transfer { transfer_id: u64 },
    TransferGroup { batch_id: u64, transfer_ids: Vec<u64> },
}

/// One write of a batch, by value (R3). The writer executor turns it into SQL.
#[derive(Clone, Debug, PartialEq)]
pub enum StoreOp {
    /// `encrypted_pending` = this row. `credential_digest` is the full digest of the credential, `response_key` seals the answer.
    Pending { pending: PendingCall, credential_digest: String, wire_id: String, response_key: String },
    /// Remove the row `encrypted_pending`.
    RemovePending,
    ResumeAnswer { answer: crate::owners::session::SessionAnswer },
    RemoveResumeAnswer,
    /// Remove the row `encrypted_session` (a sign-in that the server refused).
    EraseSession,
    JournalSessionEnd,
    /// `encrypted_answer`: the terminal outcome of the `Mutate` of `request_id`, written with `RemovePending` and kept until `RemoveAnswer`.
    Answer { request_id: u64, operation: Option<i64>, result: crate::store::session_state::StoredAnswer },
    RemoveAnswer { request_id: u64 },
    ReleasePendingOwner { request_id: u64 },
    /// The element of `restore_failed` that an operation ended with `Failed{RestoreFailed}` leaves with its discard.
    DiscardRestoreFailed { table: String, key: String },
    /// `encrypted_session`: `digest = None` keeps the stored digest, `sequence = None` keeps the stored sequence.
    Session { digest: Option<String>, sequence: Option<u64> },
    /// The intent of an operation with its optimistic `effects`.
    /// `created_at` = the time the owner that writes the intent gives the operation (P-OUT-59): the Store keeps it and reads no clock for it.
    Intent { op_id: u64, model: String, row_id: String, action: String, payload: String, effects: IntentEffects, origin: IntentOrigin, attempts: u32, cycle_id: u64, created_at: u64 },
    IntentOrigin { op_id: u64, origin: IntentOrigin },
    Effects { op_id: u64, model: String, row_id: String, action: String, payload: String, effects: IntentEffects, created_at: u64 },
    /// The terminal outcome of a receipt: the owner's terminal batch writes it, `model` names the table of the `Confirmed` row.
    Outcome { owner: OutcomeOwner, id: u64, cycle_id: u64, model: String, outcome: crate::owners::write::outbox::OutboxOutcome },
    RetireTransferOutcome { transfer_id: u64, cycle_id: u64 },
    /// The durable attempt counter and the cycle of an operation: written before the `Mutate` of that attempt.
    Attempt { op_id: u64, attempts: u32, cycle_id: u64, cause: Option<crate::value::sync::FailureKind> },
    /// The cause of the last failed attempt of the operation row.
    Cause { op_id: u64, cause: Option<crate::value::sync::FailureKind> },
    /// An operation that left the network returns to the queue: its status is `pending`.
    Requeue { op_id: u64 },
    /// A `create` with a temporary id landed: the row `temp` becomes `real` with its references and membership.
    Swap { model: String, temp: String, real: String },
    /// The row the batch answers with: the Store reads it in the transaction of the batch and replies with it.
    Show { model: String, row_id: String },
    /// The rows, membership and tombstones of a call that has no operation (a local model, an action without a mutation): final at once.
    Planned { effects: IntentEffects },
    /// A file for a field of a row: the bytes go to the directory of the files or to the table, the row gets the descriptor.
    AttachFile { model: String, row_id: String, field: String, bytes: Vec<u8>, name: String, mime: String },
    /// A file the library already copied for a field of a row: the descriptor and the placement come from the copy.
    AttachCopied { model: String, row_id: String, field: String, copy: crate::store::files::CopiedFile, name: String, mime: String },
    DetachFile { model: String, row_id: String, field: String },
    /// Rows of a `memory` model, written to the TEMP tables.
    EphemeralRows { model: String, rows: Vec<RowValue> },
    /// Local fields joined to the stored row in the transaction that holds them: `values` is a JSON object of the local fields only.
    LocalFields { model: String, row_id: String, values: String },
    /// A local field of a row that is not in the base yet: it sits on the row when the row lands.
    LocalPending { model: String, rows: Vec<RowValue> },
    /// Take back the `effects` of the operation.
    RollBack { op_id: u64 },
    /// The operation leaves the queue (its answer landed or it ended).
    RemoveIntent { op_id: u64 },
    /// The answer of an action landed: the stored windows of these (model, relation) pairs are stale, whoever reads them.
    StaleFamilies { families: Vec<(String, String)> },
    /// The windows of `models` and the windows `keys` owe a reread: `stale_since` is set in the transaction of the batch.
    /// The windows of `models` except the families `kept` (model, relation), and the windows `keys`, owe a reread: `stale_since` is set in the transaction of the batch.
    MarkStale { models: Vec<String>, keys: Vec<(String, String)>, kept: Vec<(String, String)> },
    OweReread { model: String, id: String, relation: String },
    /// Set or clear the error shown on the row of the operation (`on_failure: keep`).
    MarkError { op_id: u64, error: Option<String> },
    /// Rows of the server answer of `model`.
    Rows { model: String, rows: Vec<RowValue> },
    /// Rows of a local model (`set_local`, `set_ephemeral`, `attach`, `detach`).
    LocalRows { model: String, rows: Vec<RowValue> },
    RemoveRows { model: String, ids: Vec<String> },
    RemoveServerRows { model: String, ids: Vec<String> },
    ExpireRow { model: String, row_id: String, write_revision: u64 },
    /// The field of an ephemeral row goes back to its default when the overlay still holds the value written at `position`.
    ResetEphemeral { model: String, row_id: String, field: String, position: u64 },
    Upload { row: UploadRow, kind: UploadKind },
    RemoveUpload { key: String },
    /// Remove the prepare settings of the transfer.
    RemovePrepare { key: String },
    RemoveUploadByTransferId { transfer_id: u64 },
    UploadRoundError { key: String, error: Option<String> },
    /// The shown value of a transfer: `status`, `stage` and `wait` of its row in `sync_upload`.
    UploadShown { key: String, shown: crate::owners::write::transfer::Shown, batch: u32, sent: u32, total: u32, attempts: u32, error: Option<String>, virtual_transfer: Option<(u64, Option<u64>)> },
    /// The swap of the copy of a transfer is done: `staged` of its row is cleared.
    UploadStaged { key: String },
    /// A multipart upload of a removed transfer: `abort` goes to the server until the server answers.
    UploadOrphan { upload_id: String, key: String },
    RemoveOrphan { upload_id: String },
    /// How the elements of a batch go out together: `joint` is the text `Joint::to_text` gives.
    UploadBatch { batch_id: u64, op_id: u64, joint: String },
    /// Integer fields of a row set to numbers (the position and the size of an element of a batch): `writes` are paths with their values.
    Numbers { model: String, row_id: String, writes: Vec<(String, i64)> },
    /// The server blob of a file with this content: a later upload of the same bytes skips the steps while the blob lives.
    BlobCachePut { content_sha256: String, blob_id: String, created_at: u64 },
    BlobCacheRemove { content_sha256: String },
    /// A page of a window: the rows of the answer, the window record (`page`, `cursor`) and whether it replaces the rows of the window.
    /// `source` = `NodeReread` when the window owner marked the load for a reread by event (P-DATA-100).
    Window { landed_at: i64, key: String, model: String, page: u32, replace_all: bool, cursor: Option<String>, roots: Vec<RowValue>, related: Vec<(String, RowValue)>, total: Option<i64>, base_revision: i64, source: crate::schema::RowSource },
    /// The ids a node read answered with no row (`null`, `NOT_FOUND`): the stored rows of them are deleted like a server delete, unless written after `base_revision`.
    WindowAbsent { model: String, ids: Vec<String>, base_revision: i64 },
    /// The marker of a media window: it replaces the stored one and moves `landed_at` of the window, the rows stay.
    WindowMarker { landed_at: Option<i64>, key: String, model: String, marker: crate::store::WindowMarker },
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoreBatch {
    pub author: BatchAuthor,
    pub id: BatchId,
    pub stamp: crate::runtime::EpochStamp,
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
    Same { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, revision: u64 },
    /// The fingerprint of a server model changed: the cache is erased in the `Boot` transaction and the generation grows.
    ChangedModels { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, revision: u64 },
    FormatChange { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, revision: u64 },
    /// The fingerprint of a `storage: local` model changed, the server models match: only that model is rebuilt.
    LocalModelChanged { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, revision: u64 },
    SqliteError(String),
    /// `saved_epoch` or `saved_generation` is not a u64.
    CorruptManifest(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenMessage {
    pub saved_epoch: Epoch,
    pub saved_generation: u64,
    pub saved_identity: Option<Identity>,
    pub saved_unowned_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreReply {
    Committed { revision: u64, stale_parents: Vec<String>, rows: crate::owners::contract::ChangedRows, shown: Vec<Option<crate::store::Row>>, files_removed: usize, refused: Vec<usize> },
    Rejected(RejectReason),
}

/// What a finished transaction of SQLite reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Done {
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryExpiry {
    pub model: String,
    pub row_id: String,
    pub written_at: u64,
    pub expires_after: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EphemeralReset {
    pub model: String,
    pub row_id: String,
    pub field: String,
    pub position: u64,
    pub deadline: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteDone {
    pub revision: u64,
    pub observed: Vec<String>,
    pub expiring: Vec<MemoryExpiry>,
    pub removed_memory: Vec<(String, String)>,
    pub stale_parents: Vec<String>,
    pub removed_operations: Vec<u64>,
    pub removed_rows: Vec<(String, String)>,
    pub ended_temps: Vec<(String, String)>,
    pub swaps: Vec<(String, String, String)>,
    pub resets: Vec<EphemeralReset>,
    pub notice: Option<crate::owners::contract::CommittedBroadcast>,
    pub shown: Vec<Option<crate::store::Row>>,
    pub files_removed: usize,
    /// The events of a Feed batch refused alone (P-DATA-99), by their place in the batch.
    pub refused: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoreEffect {
    /// Create the tables (boot of a new database) or rebuild the cache tables (format change).
    Prepare { format_change: bool },
    OpenLifecycle(OpenMessage),
    OpenFailed(String),
    OpenCorrupt(String),
    /// One transaction: `rows`, `intents`, `files`.
    Write { batch: StoreBatch, min_pin_revision: Option<u64> },
    Pinned { epoch: Epoch, request_id: u64, base_revision: u64 },
    ReadReady { epoch: Epoch, read_id: u64 },
    /// One transaction: erase the account (`erase_encrypted` for `SessionReset` and `ServerEnded`).
    EraseAccount { io_id: u64, generation_new: u64, erase_encrypted: bool },
    /// The secret of `CredentialStore` after the commit of SQLite.
    ClearSecret { io_id: u64 },
    /// One transaction: `saved_epoch`, `saved_generation`, `saved_identity`.
    SavePublished { io_id: u64, publication_id: u64, published: Published },
    Reply { author: BatchAuthor, batch: BatchId, stamp: crate::runtime::EpochStamp, reply: StoreReply },
    Cleared { epoch_old: Epoch, erase_id: u64 },
    ClearFailed { erase_id: u64, error: String },
    Saved { publication_id: u64 },
    SaveFailed { publication_id: u64, error: String },
    Observed { epoch: Epoch, batch: BatchId, payloads: Vec<String> },
    ArmExpiry { epoch: Epoch, model: String, row_id: String, revision: u64, deadline: u64 },
    DisarmExpiry { epoch: Epoch, model: String, row_id: String },
    /// The server removed rows: their operations left the queue in the batch.
    ArmReset { epoch: Epoch, reset: EphemeralReset },
    OperationsGone { epoch: Epoch, ops: Vec<u64> },
    RowsRemoved { epoch: Epoch, rows: Vec<(String, String)>, revision: u64 },
    AliasFacts { epoch: Epoch, swaps: Vec<(String, String, String)>, ended: Vec<(String, String)> },
    ReadIntents { epoch: Epoch, direct: bool },
    CloseReads,
    CloseConnections,
    StoreClosed,
}

#[derive(Clone, Debug)]
pub enum StoreInput {
    Boot(BootFound),
    Commit(StoreBatch),
    Pin { epoch: Epoch, request_id: u64 },
    Unpin { request_id: u64 },
    /// The saved window read of `read_id` starts once every batch queued before this input is committed.
    ReadAfter { epoch: Epoch, read_id: u64 },
    Written { author: BatchAuthor, batch: BatchId, result: Result<WriteDone, RejectReason> },
    ClearAccount { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: u64 },
    Erased { io_id: u64, result: Result<Done, String> },
    Published { publication_id: u64, published: Published },
    EpochEnded(EpochEnded),
    ReadIntents { epoch: Epoch },
    Close,
    ReadsClosed { ids: Vec<u64> },
    ReadFinished { read_id: u64 },
    Idle,
}
