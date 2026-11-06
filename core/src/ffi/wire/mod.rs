use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::db::Record;
use crate::operation::enum_text;
use crate::schema::{FieldKind, FieldSpec, ModelSpec, TYPENAME};
use crate::syncstate::{OperationFailure, SyncInfo, SyncState};
use crate::upload::{UploadInfo, UploadStatus};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireSyncState {
    Synced,
    Pending,
    Sending,
    Failed,
    DeliveryUnknown,
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
    DeliveryUnknown,
    Cancelled,
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
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUpload {
    pub status: WireUploadStatus,
    pub stage: String,
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub wait_reason: Option<String>,
    pub attempts: u32,
    pub error: Option<WireFailure>,
    /// The file URL of the copy of the library while the transfer holds it.
    pub local_url: Option<String>,
    /// The server works on the file after the transfer: the progress it reports.
    pub processing: Option<WireProcessing>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireProcessing {
    pub progress: Option<f64>,
}

/// The progress is a finite number read from JSON, so equality is total.
impl Eq for WireProcessing {}

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

fn free_cell(value: &Value) -> Cell {
    match value {
        Value::Array(items) => Cell::Seq { items: items.iter().map(free_cell).collect() },
        Value::Object(fields) => Cell::Keyed { entries: fields.iter().map(|(key, value)| CellEntry { key: key.clone(), value: free_cell(value) }).collect() },
        other => scalar_cell(other),
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
        (FieldKind::Scalar { .. } | FieldKind::Enum { .. }, _) => scalar_cell(value),
        (FieldKind::List { .. }, _) => value.as_array().map_or(Cell::Null, |items| Cell::Seq { items: items.iter().map(scalar_cell).collect() }),
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
    let session_code = match failure.kind { crate::syncstate::FailureKind::Session(code) => Some(code), _ => None };
    let (request_size, request_limit) = match failure.kind { crate::syncstate::FailureKind::RequestTooLarge { size, limit } => (Some(size), Some(limit)), _ => (None, None) };
    let kind = if request_size.is_some() { "request_too_large".to_string() } else { enum_text(&failure.kind).unwrap_or_default() };
    WireFailure { kind, session_code, request_size, request_limit, message: failure.message.clone(), extensions: failure.extensions.clone() }
}

fn upload(info: &UploadInfo) -> WireUpload {
    WireUpload {
        status: match info.status {
            UploadStatus::Waiting => WireUploadStatus::Waiting,
            UploadStatus::Preparing => WireUploadStatus::Preparing,
            UploadStatus::Queued => WireUploadStatus::Queued,
            UploadStatus::Uploading => WireUploadStatus::Uploading,
            UploadStatus::Sending => WireUploadStatus::Sending,
            UploadStatus::Done => WireUploadStatus::Done,
            UploadStatus::Failed => WireUploadStatus::Failed,
            UploadStatus::DeliveryUnknown => WireUploadStatus::DeliveryUnknown,
            UploadStatus::Cancelled => WireUploadStatus::Cancelled,
        },
        stage: enum_text(&info.stage).unwrap_or_default(),
        sent_bytes: info.sent_bytes,
        total_bytes: info.total_bytes,
        wait_reason: info.wait_reason.and_then(|reason| enum_text(&reason)),
        attempts: info.attempts,
        error: info.error.as_ref().map(failure),
        local_url: info.local_url.clone(),
        processing: info.processing.as_ref().map(|processing| WireProcessing { progress: processing.progress }),
    }
}

fn sync(info: &SyncInfo) -> WireSync {
    WireSync {
        state: match info.state {
            SyncState::Synced => WireSyncState::Synced,
            SyncState::Pending => WireSyncState::Pending,
            SyncState::Sending => WireSyncState::Sending,
            SyncState::Failed => WireSyncState::Failed,
            SyncState::DeliveryUnknown => WireSyncState::DeliveryUnknown,
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
            estimated_bytes: crate::db::row_bytes("", record) as u64,
            layout: self.layout.clone(),
            cells: self.fields.iter().map(|field| cell(field, record.data.get(&field.name), &mut types)).collect(),
            sync: sync(&record.sync),
            uploads: record.sync.uploads.iter().map(|(action, info)| WireUploadEntry { action: action.clone(), upload: upload(info) }).collect(),
        }
    }
}

#[cfg(test)]
mod wire_tests;

#[cfg(test)]
mod union_tests;

#[cfg(test)]
mod interface_tests;
