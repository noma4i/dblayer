use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::store::Record;
use crate::store::operation::enum_text;
use crate::schema::{FieldKind, FieldSpec, ModelSpec, TYPENAME};
use crate::store::files::FileDescriptor;
use crate::value::ScalarKind;
use crate::value::sync::{OperationFailure, SyncInfo, SyncState};
use crate::owners::write::upload::{UploadInfo, UploadStatus};

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum Cell {
    Null,
    Bool { value: bool },
    Int { value: i64 },
    Real { value: f64 },
    Text { value: String },
    Seq { items: Vec<Cell> },
    Keyed { entries: Vec<CellEntry> },
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct CellEntry {
    pub key: String,
    pub value: Cell,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireFile {
    pub id: String,
    pub size: u64,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub sha256: Option<String>,
}

impl From<FileDescriptor> for WireFile {
    fn from(file: FileDescriptor) -> Self {
        Self { id: file.id, size: file.size, name: file.name, mime: file.mime, sha256: file.sha256 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireSyncState {
    Synced,
    Pending,
    Sending,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireUploadStatus {
    Waiting,
    Preparing,
    Queued,
    Uploading,
    Sending,
    Done,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireFailure {
    pub kind: String,
    pub session_code: Option<crate::transport::SessionCode>,
    /// Set for `request_too_large`: the size of the sealed request and the limit, in bytes.
    pub request_size: Option<u64>,
    pub request_limit: Option<u64>,
    pub message: String,
    pub extensions: Option<crate::transport::GraphqlExtensions>,
    /// Set for `response_too_large`: the limit of the answer in bytes.
    #[uniffi(default = None)]
    pub response_limit: Option<u64>,
    /// Set for `prepare_failed`: `failed` or `device_cannot_decode`.
    #[uniffi(default = None)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireUpload {
    pub status: WireUploadStatus,
    pub stage: String,
    pub sent_bytes: u64,
    pub total_bytes: u64,
    /// The share of the file 0 to 1 in steps of 0.01 that the status shows: it never goes down.
    pub progress: f64,
    /// The share of the batch of the row 0 to 1 in steps of 0.01: the same as `progress` for a row outside a batch, it never goes down.
    pub batch_progress: f64,
    pub wait_reason: Option<String>,
    pub attempts: u32,
    pub error: Option<WireFailure>,
    /// The file URL of the copy of the library while the transfer holds it.
    pub local_url: Option<String>,
}

/// The progress is a finite share of two counters, so equality is total.
impl Eq for WireUpload {}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadEntry {
    pub action: String,
    pub upload: WireUpload,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireSync {
    pub state: WireSyncState,
    pub actions: Vec<String>,
    pub dirty_fields: Vec<String>,
    pub error: Option<WireFailure>,
    pub attempts: u32,
    pub since: i64,
}

impl Default for WireSync {
    fn default() -> Self { Self { state: WireSyncState::Synced, actions: Vec::new(), dirty_fields: Vec::new(), error: None, attempts: 0, since: 0 } }
}

/// The sync state of a row the server has not touched.
#[uniffi::export]
pub fn default_sync() -> WireSync { WireSync::default() }

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireRow {
    pub id: String,
    pub revision: i64,
    pub estimated_bytes: u64,
    pub layout: String,
    pub cells: Vec<Cell>,
    pub sync: WireSync,
    pub uploads: Vec<WireUploadEntry>,
}

pub fn wire_fields(spec: &ModelSpec) -> Vec<&FieldSpec> {
    spec.fields.iter().chain(spec.ephemeral.iter().map(|field| &field.field)).filter(|field| field.name != "id").collect()
}

pub fn wire_layout(spec: &ModelSpec) -> String {
    let mut text = String::new();
    for field in wire_fields(spec) {
        describe(field, &mut text);
        text.push(';');
    }
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn member_key(field: &FieldSpec) -> &str {
    field.from.as_deref().unwrap_or(&field.name)
}

fn describe(field: &FieldSpec, out: &mut String) {
    out.push_str(&field.name);
    out.push(':');
    match (&field.kind, field.app_type.is_some()) {
        (FieldKind::Scalar { kind }, _) => out.push_str(&format!("s{kind:?}")),
        (FieldKind::Enum { .. }, _) => out.push('e'),
        (FieldKind::Raw, _) => out.push_str(if field.parse_json { "rj" } else { "r" }),
        (FieldKind::File, _) => out.push('f'),
        (FieldKind::List { element }, _) => out.push_str(&format!("l{element:?}")),
        (FieldKind::Object, true) => members_text(field, out, "o"),
        (FieldKind::Array, true) if field.fields.is_empty() => out.push_str(&format!("a^{}", field.app_type.as_deref().unwrap_or_default())),
        (FieldKind::Array, true) => members_text(field, out, "a"),
        (FieldKind::Union | FieldKind::UnionArray, _) => {
            out.push_str(if field.kind == FieldKind::Union { "u{" } else { "ua{" });
            if !field.shared.is_empty() {
                members_text_of(&field.shared, out, "s");
            }
            for variant in &field.fields {
                out.push_str(variant.app_type.as_deref().unwrap_or_default());
                out.push('=');
                describe(variant, out);
                out.push(',');
            }
            out.push('}');
        }
        (FieldKind::Object, false) => out.push('o'),
        (FieldKind::Array, false) => out.push('a'),
    }
}

fn members_text(field: &FieldSpec, out: &mut String, tag: &str) {
    members_text_of(&field.fields, out, tag);
}

fn members_text_of(members: &[FieldSpec], out: &mut String, tag: &str) {
    out.push_str(tag);
    out.push('{');
    for member in members {
        out.push_str(member_key(member));
        out.push('=');
        describe(member, out);
        out.push(',');
    }
    out.push('}');
}

type Types<'a> = Vec<(&'a str, &'a [FieldSpec])>;

fn scalar_cell(value: &Value) -> Cell {
    match value {
        Value::Bool(flag) => Cell::Bool { value: *flag },
        Value::Number(number) => number.as_i64().map_or_else(|| Cell::Real { value: number.as_f64().unwrap_or(0.0) }, |value| Cell::Int { value }),
        Value::String(text) => Cell::Text { value: text.clone() },
        _ => Cell::Null,
    }
}

fn typed_scalar(kind: ScalarKind, value: &Value) -> Cell {
    match (kind, value) {
        (ScalarKind::Id, Value::Number(number)) => number.as_i64().map_or_else(|| scalar_cell(value), |id| Cell::Text { value: id.to_string() }),
        (ScalarKind::Num, Value::Number(number)) => Cell::Real { value: number.as_f64().unwrap_or(0.0) },
        (ScalarKind::Int, Value::Number(number)) => number.as_i64().or_else(|| number.as_f64().filter(|real| real.fract() == 0.0 && real.abs() < 9.0e15).map(|real| real as i64)).map_or_else(|| scalar_cell(value), |value| Cell::Int { value }),
        _ => scalar_cell(value),
    }
}

fn file_cell(value: &Value) -> Cell {
    FileDescriptor::from_value(value).map_or(Cell::Null, |file| {
        let text = |value: Option<String>| value.map_or(Cell::Null, |value| Cell::Text { value });
        Cell::Seq { items: vec![Cell::Text { value: file.id }, Cell::Int { value: file.size as i64 }, text(file.name), text(file.mime), text(file.sha256)] }
    })
}

pub(crate) fn free_cell(value: &Value) -> Cell {
    match value {
        Value::Array(items) => Cell::Seq { items: items.iter().map(free_cell).collect() },
        Value::Object(fields) => Cell::Keyed { entries: fields.iter().map(|(key, value)| CellEntry { key: key.clone(), value: free_cell(value) }).collect() },
        other => scalar_cell(other),
    }
}

impl Cell {
    /// The JSON value of a cell. A real that is not finite is `null`.
    #[cfg(test)]
    pub(crate) fn json(&self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Bool { value } => Value::Bool(*value),
            Cell::Int { value } => Value::from(*value),
            Cell::Real { value } => serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number),
            Cell::Text { value } => Value::String(value.clone()),
            Cell::Seq { items } => Value::Array(items.iter().map(Cell::json).collect()),
            Cell::Keyed { entries } => Value::Object(entries.iter().map(|entry| (entry.key.clone(), entry.value.json())).collect()),
        }
    }
}

fn member_cells<'a>(members: &'a [FieldSpec], object: &Map<String, Value>, types: &mut Types<'a>) -> Vec<Cell> {
    members.iter().map(|member| cell(member, object.get(member_key(member)), types)).collect()
}

fn union_cell<'a>(field: &'a FieldSpec, value: &Value, types: &mut Types<'a>) -> Cell {
    let Some(typename) = value.get(TYPENAME).and_then(Value::as_str) else { return Cell::Null };
    let members = field.fields.iter().find(|variant| variant.app_type.as_deref() == Some(typename)).zip(value.as_object());
    let body = members.map_or(Cell::Null, |(variant, object)| Cell::Seq { items: member_cells(&field.shared, object, types).into_iter().chain(member_cells(&variant.fields, object, types)).collect() });
    Cell::Seq { items: vec![Cell::Text { value: typename.to_string() }, body] }
}

fn cell<'a>(field: &'a FieldSpec, value: Option<&Value>, types: &mut Types<'a>) -> Cell {
    let Some(value) = value.filter(|value| field.nullable || !value.is_null()).or(field.default.as_ref()).filter(|value| !value.is_null()) else { return Cell::Null };
    match (&field.kind, field.app_type.as_deref()) {
        (FieldKind::Scalar { kind }, _) => typed_scalar(*kind, value),
        (FieldKind::Enum { .. }, _) => scalar_cell(value),
        (FieldKind::File, _) => file_cell(value),
        (FieldKind::List { element }, _) => value.as_array().map_or(Cell::Null, |items| Cell::Seq { items: items.iter().map(|item| typed_scalar(*element, item)).collect() }),
        (FieldKind::Object, Some(_)) => value.as_object().map_or(Cell::Null, |object| Cell::Seq { items: member_cells(&field.fields, object, types) }),
        (FieldKind::Union, _) => union_cell(field, value, types),
        (FieldKind::UnionArray, _) => value.as_array().map_or(Cell::Null, |items| Cell::Seq { items: items.iter().filter(|item| item.get(TYPENAME).is_some_and(Value::is_string)).map(|item| union_cell(field, item, types)).collect() }),
        (FieldKind::Array, Some(name)) => {
            let Some(items) = value.as_array() else { return Cell::Null };
            let own: &'a [FieldSpec] = if field.fields.is_empty() {
                types.iter().rev().find(|(declared, _)| *declared == name).map_or(&[][..], |(_, members)| *members)
            } else {
                &field.fields
            };
            types.push((name, own));
            let elements = items.iter().filter_map(Value::as_object).map(|object| Cell::Seq { items: member_cells(own, object, types) }).collect();
            types.pop();
            Cell::Seq { items: elements }
        }
        _ => free_cell(value),
    }
}

pub(super) fn failure(failure: &OperationFailure) -> WireFailure {
    let session_code = match failure.kind { crate::value::sync::FailureKind::Session(code) => Some(code), _ => None };
    let (request_size, request_limit) = match failure.kind { crate::value::sync::FailureKind::RequestTooLarge { size, limit } => (Some(size), Some(limit)), _ => (None, None) };
    let response_limit = match failure.kind { crate::value::sync::FailureKind::ResponseTooLarge { limit } => Some(limit), _ => None };
    let reason = match failure.kind { crate::value::sync::FailureKind::PrepareFailed { reason } => Some(reason.name().to_string()), _ => None };
    let kind = if reason.is_some() { "prepare_failed".to_string() } else if request_size.is_some() { "request_too_large".to_string() } else if response_limit.is_some() { "response_too_large".to_string() } else { enum_text(&failure.kind).unwrap_or_default() };
    WireFailure { kind, session_code, request_size, request_limit, message: failure.message.clone(), extensions: failure.extensions.clone(), response_limit, reason }
}

pub(crate) fn upload(info: &UploadInfo) -> WireUpload {
    WireUpload {
        status: match info.status {
            UploadStatus::Waiting => WireUploadStatus::Waiting,
            UploadStatus::Preparing => WireUploadStatus::Preparing,
            UploadStatus::Queued => WireUploadStatus::Queued,
            UploadStatus::Uploading => WireUploadStatus::Uploading,
            UploadStatus::Sending => WireUploadStatus::Sending,
            UploadStatus::Done => WireUploadStatus::Done,
            UploadStatus::Failed => WireUploadStatus::Failed,
        },
        stage: enum_text(&info.stage).unwrap_or_default(),
        sent_bytes: info.sent_bytes,
        total_bytes: info.total_bytes,
        progress: match info.status { UploadStatus::Waiting => 0.0, _ => f64::from(info.share) / 100.0 },
        batch_progress: match (info.batch_share, info.status) { (Some(share), _) => f64::from(share) / 100.0, (None, UploadStatus::Waiting) => 0.0, (None, _) => f64::from(info.share) / 100.0 },
        wait_reason: info.wait_reason.and_then(|reason| enum_text(&reason)),
        attempts: info.attempts,
        error: info.error.as_ref().map(failure),
        local_url: info.local_url.clone(),
    }
}

fn sync(info: &SyncInfo) -> WireSync {
    WireSync {
        state: match info.state {
            SyncState::Synced => WireSyncState::Synced,
            SyncState::Pending => WireSyncState::Pending,
            SyncState::Sending => WireSyncState::Sending,
            SyncState::Failed => WireSyncState::Failed,
        },
        actions: info.actions.clone(),
        dirty_fields: info.dirty_fields.clone(),
        error: info.error.as_ref().map(failure),
        attempts: info.attempts,
        since: info.since,
    }
}

pub struct WireModel {
    layout: String,
    fields: Vec<FieldSpec>,
}

impl WireModel {
    pub fn of(spec: &ModelSpec) -> Self {
        Self { layout: wire_layout(spec), fields: wire_fields(spec).into_iter().cloned().collect() }
    }

    pub fn row(&self, record: &Record) -> WireRow {
        let mut types = Types::new();
        WireRow {
            id: record.id.to_string(),
            revision: record.revision,
            estimated_bytes: crate::store::row_bytes("", record) as u64,
            layout: self.layout.clone(),
            cells: self.fields.iter().map(|field| cell(field, record.data.get(&field.name), &mut types)).collect(),
            sync: sync(&record.sync),
            uploads: record.sync.uploads.iter().map(|(action, info)| WireUploadEntry { action: action.clone(), upload: upload(info) }).collect(),
        }
    }
}

#[cfg(test)]
mod tests;


