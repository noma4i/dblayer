use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use uuid::Uuid;

use crate::store::files::FileRecord;
use crate::identity::{Generation, ModelKey, RowId, RowRef, ListKey};
use crate::owners::write::operation_status::OperationStatus;
use crate::value::sync::OperationFailure;
use crate::value::duplicates;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestroyReason {
    Server,
    Local,
    Expired,
    Replaced { by: RowId },
}

impl DestroyReason {
    pub fn writes_tombstone(&self) -> bool {
        matches!(self, DestroyReason::Server | DestroyReason::Local)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntityOp {
    Upsert { model: ModelKey, id: RowId, row: Map<String, Value> },
    Destroy { model: ModelKey, id: RowId, reason: DestroyReason },
    Touch { model: ModelKey, id: RowId },
}

impl EntityOp {
    pub fn row_ref(&self) -> RowRef {
        match self {
            EntityOp::Upsert { model, id, .. } | EntityOp::Destroy { model, id, .. } | EntityOp::Touch { model, id } => {
                RowRef::new(model.clone(), id.clone())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileOp {
    Put(FileRecord),
    Remove { model: ModelKey, row_id: RowId, field: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScopeRef {
    pub model: ModelKey,
    pub key: ListKey,
}

impl ScopeRef {
    pub fn new(model: ModelKey, key: ListKey) -> Self {
        Self { model, key }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeOp {
    Append { scope: ScopeRef, row: RowId, order_key: String },
    Move { scope: ScopeRef, row: RowId, order_key: String },
    Detach { scope: ScopeRef, row: RowId },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EdgeValue {
    Whole(i64),
    Fraction(f64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgeWrite {
    pub scope: ScopeRef,
    pub row: RowId,
    pub values: Vec<(String, EdgeValue)>,
}

impl ScopeOp {
    pub fn scope(&self) -> &ScopeRef {
        match self {
            ScopeOp::Append { scope, .. }
            | ScopeOp::Move { scope, .. }
            | ScopeOp::Detach { scope, .. } => scope,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationRecord {
    pub seq: Option<i64>,
    pub action_key: String,
    pub status: OperationStatus,
    pub row_ids: Vec<RowId>,
    pub temp_id: Option<RowId>,
    pub input: Value,
    pub base_revision: Option<i64>,
    pub created_at: i64,
    pub dedupe_key: Option<String>,
    pub rollback: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationTransition {
    Begin(Box<OperationRecord>),
    Close { seq: i64, status: OperationStatus },
    Fail { seq: i64, status: OperationStatus, failure: OperationFailure },
    Retry { seq: i64 },
    Attempt { seq: i64, attempts: i64, cycle_id: i64, cause: Option<crate::value::sync::FailureKind> },
    Sending { seq: i64 },
    Requeue { seq: i64 },
    Remove { seq: i64, expected: Option<OperationStatus> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowMarker {
    pub sequence: i64,
    pub values: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSave {
    pub marker: Option<WindowMarker>,
    pub stale_since: Option<i64>,
    pub key: String,
    pub scope: ScopeRef,
    pub cursor: Option<String>,
    pub has_next: bool,
    pub total: Option<i64>,
    pub pages: i64,
    pub anchor: Option<String>,
    pub issued: i64,
    pub applied: i64,
    pub cleared_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowDebt {
    pub model: String,
    pub relation: String,
    pub since: i64,
}

/// The one point of order between a change of the network and the commit of a start. A change of the network, the account or the connection happens while it holds this line, and a start checks its admission and commits while it holds it.
#[derive(Clone, Default)]
pub struct NetworkLine {
    line: std::sync::Arc<std::sync::Mutex<()>>,
    #[cfg(test)]
    pub(crate) waiting: std::sync::Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>,
}

impl NetworkLine {
    pub(crate) fn enter(&self) -> std::sync::MutexGuard<'_, ()> {
        #[cfg(test)]
        {
            *self.waiting.0.lock().unwrap_or_else(|error| error.into_inner()) += 1;
            self.waiting.1.notify_all();
        }
        crate::lock::acquire(&self.line)
    }
}

#[derive(Clone)]
pub struct NetworkAdmission {
    expected: (i64, i64, bool),
    current: std::sync::Arc<dyn Fn() -> (i64, i64, bool) + Send + Sync>,
    line: NetworkLine,
}

impl NetworkAdmission {
    pub fn new(expected: (i64, i64, bool), current: std::sync::Arc<dyn Fn() -> (i64, i64, bool) + Send + Sync>, line: NetworkLine) -> Self { Self { expected, current, line } }

    pub fn holds(&self) -> bool { (self.current)() == self.expected }

    pub(crate) fn enter(&self) -> std::sync::MutexGuard<'_, ()> { self.line.enter() }
}

impl std::fmt::Debug for NetworkAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "NetworkAdmission({:?})", self.expected) }
}

impl PartialEq for NetworkAdmission {
    fn eq(&self, other: &Self) -> bool { self.expected == other.expected && std::sync::Arc::ptr_eq(&self.current, &other.current) }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchPlan {
    pub tx_id: Uuid,
    pub entity_ops: Vec<EntityOp>,
    pub scope_ops: Vec<ScopeOp>,
    pub edge_writes: Vec<EdgeWrite>,
    pub file_ops: Vec<FileOp>,
    pub operation_transitions: Vec<OperationTransition>,
    pub window: Option<WindowSave>,
    pub window_debts: Vec<WindowDebt>,
    pub snapshot: Option<RowRef>,
    pub item_snapshots: Vec<(String, RowRef)>,
    pub generation: Generation,
    pub source: crate::schema::RowSource,
    pub pending_locals: Vec<PendingLocal>,
    pub network: Option<NetworkAdmission>,
    /// The temporary row each real id replaced in this batch: the prior values of the confirmed child (P-DATA-57).
    pub replaced: BTreeMap<RowRef, Map<String, Value>>,
}

/// A local value for a row that is not stored yet. It waits in `local_pending` and joins the row when it arrives.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingLocal {
    pub model: String,
    pub row_id: String,
    pub path: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlanFault {
    #[error("envelope generation is stale")]
    StaleGeneration,
    #[error("the network moved between the plan and the commit")]
    NetworkMoved,
    #[error("envelope holds several entity ops for one row")]
    ConflictingRowOps,
    #[error("envelope holds several scope ops for one member")]
    DuplicateScopeOps,
    #[error("upsert row id differs from its payload id")]
    RowIdMismatch,
}

impl BatchPlan {
    pub fn new(generation: Generation) -> Self {
        Self {
            tx_id: Uuid::new_v4(),
            entity_ops: Vec::new(),
            scope_ops: Vec::new(),
            edge_writes: Vec::new(),
            file_ops: Vec::new(),
            operation_transitions: Vec::new(),
            window: None,
            window_debts: Vec::new(),
            snapshot: None,
            item_snapshots: Vec::new(),
            generation,
            source: crate::schema::RowSource::Page,
            pending_locals: Vec::new(),
            network: None,
            replaced: BTreeMap::new(),
        }
    }

    pub fn from_source(generation: Generation, source: crate::schema::RowSource) -> Self { Self { source, ..Self::new(generation) } }

    pub fn is_empty(&self) -> bool {
        self.entity_ops.is_empty() && self.scope_ops.is_empty() && self.edge_writes.is_empty() && self.file_ops.is_empty() && self.operation_transitions.is_empty() && self.window.is_none() && self.window_debts.is_empty() && self.pending_locals.is_empty()
    }

    /// P-DATA-98: the one check of the final plan, after the follow operations and the reduced repeats.
    pub fn validate(&self, current: Generation) -> Result<(), PlanFault> {
        if self.generation != current {
            return Err(PlanFault::StaleGeneration);
        }
        let upsert_mismatch = self.entity_ops.iter().any(|operation| match operation {
            EntityOp::Upsert { id, row, .. } => row.get("id").and_then(Value::as_str) != Some(id.as_str()),
            _ => false,
        });
        if upsert_mismatch {
            return Err(PlanFault::RowIdMismatch);
        }
        let row_keys: Vec<(ModelKey, RowId)> = self.touched_row_keys();
        if !duplicates(&row_keys).is_empty() {
            return Err(PlanFault::ConflictingRowOps);
        }
        let member_keys: Vec<(ScopeRef, RowId)> = self
            .scope_ops
            .iter()
            .map(|operation| match operation {
                ScopeOp::Append { scope, row, .. } | ScopeOp::Move { scope, row, .. } | ScopeOp::Detach { scope, row } => {
                    (scope.clone(), row.clone())
                }
            })
            .collect();
        if !duplicates(&member_keys).is_empty() {
            return Err(PlanFault::DuplicateScopeOps);
        }
        Ok(())
    }

    /// P-DATA-98: the repeats of a row fold before the check: the last write of a row stays, a touch gives way to a write or a removal of its row.
    /// A write and a removal of one row stay both, the check refuses them.
    pub(crate) fn reduce_repeats(&mut self) {
        let mut kept: Vec<EntityOp> = Vec::with_capacity(self.entity_ops.len());
        let mut at: std::collections::HashMap<RowRef, usize> = std::collections::HashMap::new();
        for operation in std::mem::take(&mut self.entity_ops) {
            let row = operation.row_ref();
            let Some(&index) = at.get(&row) else {
                at.insert(row, kept.len());
                kept.push(operation);
                continue;
            };
            match (&kept[index], &operation) {
                (EntityOp::Upsert { .. }, EntityOp::Upsert { .. }) | (EntityOp::Destroy { .. }, EntityOp::Destroy { .. }) | (EntityOp::Touch { .. }, EntityOp::Upsert { .. } | EntityOp::Destroy { .. }) => kept[index] = operation,
                (EntityOp::Upsert { .. } | EntityOp::Destroy { .. } | EntityOp::Touch { .. }, EntityOp::Touch { .. }) => {}
                _ => kept.push(operation),
            }
        }
        self.entity_ops = kept;
        let mut scope_ops: Vec<ScopeOp> = Vec::with_capacity(self.scope_ops.len());
        for operation in std::mem::take(&mut self.scope_ops) {
            if !scope_ops.contains(&operation) { scope_ops.push(operation); }
        }
        self.scope_ops = scope_ops;
    }

    fn touched_row_keys(&self) -> Vec<(ModelKey, RowId)> {
        self.entity_ops
            .iter()
            .map(|operation| {
                let row = operation.row_ref();
                (row.model, row.id)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowChangeKind {
    Inserted,
    Updated,
    Touched,
    Destroyed,
    Synced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    pub row: RowRef,
    pub kind: RowChangeKind,
    pub fields: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitResult {
    pub tx_id: Uuid,
    pub revision: Option<i64>,
    pub rows: Vec<RowChange>,
    pub renewed: Vec<RowRef>,
    pub scopes: BTreeSet<ScopeRef>,
    pub members: BTreeSet<RowRef>,
    pub operations: BTreeSet<i64>,
    /// A transfer row of `sync_upload` changed: the queue view shows it.
    pub uploads_changed: bool,
    pub(crate) queue_operations: BTreeMap<i64, Option<crate::owners::write::operation_status::Operation>>,
    pub replaced: Vec<(RowRef, RowId)>,
    /// The rows a server delete (event or NOT_FOUND answer) removed, cascade children included.
    pub server_removed: Vec<RowRef>,
    pub unknown_operation_acks: u32,
    pub(crate) snapshot: Option<crate::store::Row>,
    pub(crate) item_snapshots: Vec<(String, Option<crate::store::Row>)>,
    /// The version of the overlay of ephemeral values the commit published at its revision.
    pub position: u64,
    pub pointer_rows_read: usize,
}

impl CommitResult {
    pub fn noop(tx_id: Uuid) -> Self {
        Self {
            tx_id,
            revision: None,
            rows: Vec::new(),
            renewed: Vec::new(),
            scopes: BTreeSet::new(),
            members: BTreeSet::new(),
            operations: BTreeSet::new(),
            uploads_changed: false,
            queue_operations: BTreeMap::new(),
            replaced: Vec::new(),
            server_removed: Vec::new(),
            unknown_operation_acks: 0,
            snapshot: None,
            item_snapshots: Vec::new(),
            position: 0,
            pointer_rows_read: 0,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.revision.is_none()
    }
}
