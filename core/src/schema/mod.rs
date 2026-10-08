use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ddl::{column_name, column_type, SERVICE_COLUMNS};
use crate::identity::{ModelKey, ScopeKey};
use crate::value::{
    duplicates, read_array, read_enum, read_raw, read_scalar, relation_scope_key, ScalarKind, ScopeKeyError,
    SortDirection,
};

mod ephemeral;

pub use ephemeral::{EphemeralSpec, Storage};

const FORBIDDEN_ACTION_MEMBERS: [&str; 6] = ["after", "invalidate", "onCommit", "extract", "complete", "resume"];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("missing field {0}")]
    MissingField(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclarationError {
    #[error("model key is empty")]
    EmptyModelKey,
    #[error("model key {0} is declared twice")]
    DuplicateModelKey(String),
    #[error("model has no fields")]
    NoFields,
    #[error("field name is empty")]
    EmptyFieldName,
    #[error("field name {0} is reserved")]
    ReservedFieldName(String),
    #[error("field {0} is declared twice")]
    DuplicateField(String),
    #[error("indexed field {0} is not a scalar or enum")]
    IndexedFieldNotScalar(String),
    #[error("column of field {0} collides with another column")]
    ColumnCollision(String),
    #[error("composite index needs at least 2 fields")]
    CompositeIndexTooShort,
    #[error("composite index field {0} is not an indexed field")]
    CompositeIndexFieldNotIndexed(String),
    #[error("association {0} has no foreign key")]
    EmptyForeignKey(String),
    #[error("foreign key of association {0} is not indexed")]
    ForeignKeyNotIndexed(String),
    #[error("association {0} declares dependent without owning children")]
    DependentOnNonOwner(String),
    #[error("association {0} targets an unknown model")]
    UnknownTarget(String),
    #[error("action {0} destroys a record and cannot keep it on failure")]
    KeepOnDestroy(String),
    #[error("relation name is empty")]
    EmptyRelationName,
    #[error("relation name {0} is reserved")]
    ReservedRelationName(String),
    #[error("relation {0} groups by no columns")]
    EmptyByColumns(String),
    #[error("relation {relation} groups by unknown column {column}")]
    UnknownByColumn { relation: String, column: String },
    #[error("relation {relation} groups by unindexed column {column}")]
    IndexMissing { relation: String, column: String },
    #[error("relation {relation} sorts by unknown column {column}")]
    UnknownSortColumn { relation: String, column: String },
    #[error("relation {relation} remote is invalid: {fault:?}")]
    Remote { relation: String, fault: RemoteFault },
    #[error("relation {0} is declared twice")]
    DuplicateRelation(String),
    #[error("relation {0} collides with an association name")]
    AssociationNameCollision(String),
    #[error("name {0} collides with a model member")]
    NameCollision(String),
    #[error("action name {0} collides with another name")]
    ActionNameCollision(String),
    #[error("optimistic upsert {0} needs temp row retention")]
    MissingTempRowRetention(String),
    #[error("optimistic destroy {0} cannot cascade")]
    OptimisticDestroyWithCascade(String),
    #[error("action declares forbidden member {0}")]
    ForbiddenMember(String),
    #[error("once action {0} needs a dedupe key")]
    OnceKeyRequired(String),
    #[error("action {action} refetches {relation}, which is not a remote relation")]
    UnknownRefetch { action: String, relation: String },
    #[error("action {action} patches unknown field {field}")]
    UnknownOptimisticField { action: String, field: String },
    #[error("ephemeral field {0} cannot be a file")]
    EphemeralFile(String),
    #[error("ephemeral field {0} needs a default or null: true")]
    EphemeralNeedsDefault(String),
    #[error("ephemeral field {0} sets reset_after without a default")]
    ResetWithoutDefault(String),
    #[error("ephemeral field {0} cannot be indexed")]
    EphemeralIndexed(String),
    #[error("relation {relation} uses ephemeral field {field}")]
    EphemeralInRelation { relation: String, field: String },
    #[error("duration {0} is not a positive count with a unit")]
    InvalidDuration(String),
    #[error("memory model has file field {0}")]
    MemoryHasFile(String),
    #[error("memory model declares action {0}")]
    MemoryHasActions(String),
    #[error("expire_after needs storage: :memory")]
    ExpireNeedsMemory,
    #[error("relation {relation} sets {field}, which is not an ephemeral field")]
    UnknownSetField { relation: String, field: String },
    #[error("relation {relation} sets {field}, which has no reset_after")]
    SetWithoutReset { relation: String, field: String },
}

pub type DroppedRow = (usize, SchemaError);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelFault {
    pub model: ModelKey,
    pub error: DeclarationError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldKind {
    Scalar { kind: ScalarKind },
    Enum { values: Vec<String> },
    Raw,
    Object,
    Array,
    List { element: ScalarKind },
    File,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub kind: FieldKind,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub id_from_parent: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub app_type: Option<String>,
}

impl FieldSpec {
    fn with_kind(name: &str, kind: FieldKind) -> Self {
        Self {
            name: name.to_string(),
            kind,
            nullable: false,
            optional: false,
            default: None,
            indexed: false,
            id_from_parent: None,
            from: None,
            app_type: None,
        }
    }

    pub fn scalar(name: &str, kind: ScalarKind) -> Self {
        Self::with_kind(name, FieldKind::Scalar { kind })
    }

    pub fn raw(name: &str) -> Self {
        Self::with_kind(name, FieldKind::Raw)
    }

    pub fn object(name: &str) -> Self {
        Self::with_kind(name, FieldKind::Object)
    }

    pub fn file(name: &str) -> Self {
        Self::with_kind(name, FieldKind::File)
    }

    pub fn array(name: &str) -> Self {
        Self::with_kind(name, FieldKind::Array)
    }

    pub fn list(name: &str, element: ScalarKind) -> Self {
        Self::with_kind(name, FieldKind::List { element })
    }

    pub fn enumeration(name: &str, values: &[&str]) -> Self {
        Self::with_kind(name, FieldKind::Enum { values: values.iter().map(|value| value.to_string()).collect() })
    }

    pub fn parent_id(name: &str, parent: &str) -> Self {
        Self { id_from_parent: Some(parent.to_string()), ..Self::scalar(name, ScalarKind::Id) }
    }

    pub fn read_from(self, path: &str) -> Self {
        Self { from: Some(path.to_string()), ..self }
    }

    pub fn nullable(self) -> Self {
        Self { nullable: true, ..self }
    }

    pub fn optional(self) -> Self {
        Self { optional: true, ..self }
    }

    pub fn indexed(self) -> Self {
        Self { indexed: true, ..self }
    }

    pub fn with_default(self, value: Value) -> Self {
        Self { default: Some(value), ..self }
    }

    pub fn read(&self, input: &Map<String, Value>) -> Option<Value> {
        if let Some(value) = input.get(&self.name) {
            return Some(value.clone());
        }
        if let Some(path) = &self.from {
            return read_path(input, path);
        }
        let parent = self.id_from_parent.as_ref()?;
        input.get(parent)?.as_object()?.get("id").cloned()
    }

    pub fn decode(&self, value: &Value) -> Option<Value> {
        match &self.kind {
            FieldKind::Scalar { kind } => read_scalar(value, *kind),
            FieldKind::Enum { values } => read_enum(value, values).map(Value::String),
            FieldKind::Raw => read_raw(value),
            FieldKind::Object => value.is_object().then(|| value.clone()),
            FieldKind::Array => value.is_array().then(|| value.clone()),
            FieldKind::File => is_file_descriptor(value).then(|| value.clone()),
            FieldKind::List { element } => {
                read_array(value, |item| read_scalar(item, *element)).map(Value::Array)
            }
        }
    }
}

fn read_path(input: &Map<String, Value>, path: &str) -> Option<Value> {
    let mut parts = path.split('.');
    let mut current = input.get(parts.next()?)?;
    for part in parts {
        current = current.as_object()?.get(part)?;
    }
    (!current.is_null()).then(|| current.clone())
}

fn is_file_descriptor(value: &Value) -> bool {
    value.as_object().is_some_and(|object| object.get("id").is_some_and(Value::is_string) && object.get("size").is_some_and(Value::is_u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssociationKind {
    BelongsTo,
    HasMany,
    HasOne,
    References,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dependent {
    Destroy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPlan {
    Find,
    Where,
    ByIds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssociationSpec {
    pub name: String,
    pub kind: AssociationKind,
    pub target: ModelKey,
    pub foreign_key: String,
    #[serde(default)]
    pub dependent: Option<Dependent>,
    #[serde(default)]
    pub touches_parent: bool,
    #[serde(default)]
    pub from: Option<String>,
}

impl AssociationSpec {
    pub fn new(name: &str, kind: AssociationKind, target: &str, foreign_key: &str) -> Self {
        Self {
            name: name.to_string(),
            kind,
            target: ModelKey::new(target),
            foreign_key: foreign_key.to_string(),
            dependent: None,
            touches_parent: false,
            from: None,
        }
    }

    pub fn owns_children(&self) -> bool {
        matches!(self.kind, AssociationKind::HasMany | AssociationKind::HasOne)
    }

    pub fn requires_foreign_key(&self) -> bool {
        self.kind != AssociationKind::References
    }

    pub fn read_plan(&self) -> ReadPlan {
        match self.kind {
            AssociationKind::BelongsTo | AssociationKind::HasOne => ReadPlan::Find,
            AssociationKind::HasMany => ReadPlan::Where,
            AssociationKind::References => ReadPlan::ByIds,
        }
    }

    fn check_local(&self, indexed: &HashSet<&str>) -> Result<(), DeclarationError> {
        let name = || self.name.clone();
        if self.requires_foreign_key() {
            if self.foreign_key.is_empty() {
                return Err(DeclarationError::EmptyForeignKey(name()));
            }
            if !self.owns_children() && !indexed.contains(self.foreign_key.as_str()) {
                return Err(DeclarationError::ForeignKeyNotIndexed(name()));
            }
        }
        if self.dependent.is_some() && !self.owns_children() {
            return Err(DeclarationError::DependentOnNonOwner(name()));
        }
        Ok(())
    }

    fn check_target(&self, target: Option<&ModelSpec>) -> Result<(), DeclarationError> {
        let target = target.ok_or_else(|| DeclarationError::UnknownTarget(self.name.clone()))?;
        if self.owns_children() && !target.is_indexed(&self.foreign_key) {
            return Err(DeclarationError::ForeignKeyNotIndexed(self.name.clone()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coverage {
    Page,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageDirection {
    Forward,
    Backward,
}

/// Where the value of a GraphQL variable comes from when a relation loads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum VariableSource {
    Param { name: String },
    Constant { value: Value },
}

/// A server that pages by its own variable instead of Relay `first`/`after`: the next page is
/// asked for with `variable`, whose value is read from `from` inside the connection object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorSpec {
    pub variable: String,
    pub from: String,
    #[serde(default)]
    pub numeric: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShape {
    Single,
    List { coverage: Coverage },
    Connection {
        direction: PageDirection,
        coverage: Coverage,
        max_pages: Option<u32>,
        #[serde(default)]
        cursor: Option<CursorSpec>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteFault {
    SingleHasPaging,
    ConnectionWithoutPageInfo,
    MaxPagesNotPositive,
    MissingOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdatesSpec {
    pub subscription: String,
    #[serde(default)]
    pub deleted_by: Option<String>,
    #[serde(default)]
    pub set: Map<String, Value>,
    #[serde(default)]
    pub removes: bool,
    #[serde(default)]
    pub debounce: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSpec {
    pub operation: String,
    pub shape: RemoteShape,
    pub stale_time: String,
    #[serde(default)]
    pub empty_stale_time: Option<String>,
    #[serde(default)]
    pub selects_page_info: bool,
    #[serde(default)]
    pub updates: Vec<UpdatesSpec>,
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default)]
    pub variables: BTreeMap<String, VariableSource>,
    #[serde(default)]
    pub root: Option<String>,
}

impl RemoteSpec {
    pub fn supports_paging(&self) -> bool {
        matches!(self.shape, RemoteShape::Connection { .. })
    }

    pub fn coverage(&self) -> Coverage {
        match &self.shape {
            RemoteShape::Single => Coverage::Complete,
            RemoteShape::List { coverage } | RemoteShape::Connection { coverage, .. } => *coverage,
        }
    }

    pub fn fault(&self) -> Option<RemoteFault> {
        if self.operation.is_empty() {
            return Some(RemoteFault::MissingOperation);
        }
        match &self.shape {
            RemoteShape::Single if self.selects_page_info => Some(RemoteFault::SingleHasPaging),
            RemoteShape::Connection { .. } if !self.selects_page_info => Some(RemoteFault::ConnectionWithoutPageInfo),
            RemoteShape::Connection { max_pages: Some(0), .. } => Some(RemoteFault::MaxPagesNotPositive),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortField {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sort {
    #[default]
    ServerOrder,
    Fields(Vec<SortField>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationSpec {
    pub name: String,
    pub by_columns: Vec<String>,
    #[serde(default)]
    pub remote: Option<RemoteSpec>,
    #[serde(default)]
    pub is_global: bool,
    #[serde(default)]
    pub sort: Sort,
}

impl RelationSpec {
    pub fn new(name: &str, by_columns: &[&str]) -> Self {
        Self {
            name: name.to_string(),
            by_columns: by_columns.iter().map(|column| column.to_string()).collect(),
            remote: None,
            is_global: false,
            sort: Sort::ServerOrder,
        }
    }

    pub fn global(name: &str) -> Self {
        Self { is_global: true, ..Self::new(name, &[]) }
    }

    pub fn is_local(&self) -> bool {
        self.remote.is_none()
    }

    pub fn has_more(&self) -> bool {
        self.remote.as_ref().is_some_and(RemoteSpec::supports_paging)
    }

    pub fn scope_key(&self, model: &str, params: &BTreeMap<String, String>) -> Result<ScopeKey, ScopeKeyError> {
        relation_scope_key(model, &self.name, params)
    }

    fn check(&self, model: &ModelSpec) -> Result<(), DeclarationError> {
        if self.name.is_empty() {
            return Err(DeclarationError::EmptyRelationName);
        }
        if is_reserved_name(&self.name) {
            return Err(DeclarationError::ReservedRelationName(self.name.clone()));
        }
        if self.by_columns.is_empty() && !self.is_global {
            return Err(DeclarationError::EmptyByColumns(self.name.clone()));
        }
        if let Some(fault) = self.remote.as_ref().and_then(RemoteSpec::fault) {
            return Err(DeclarationError::Remote { relation: self.name.clone(), fault });
        }
        let relation = || self.name.clone();
        for column in &self.by_columns {
            if model.ephemeral_field(column).is_some() {
                return Err(DeclarationError::EphemeralInRelation { relation: relation(), field: column.clone() });
            }
            if model.field(column).is_none() {
                return Err(DeclarationError::UnknownByColumn { relation: relation(), column: column.clone() });
            }
            if !model.is_indexed(column) {
                return Err(DeclarationError::IndexMissing { relation: relation(), column: column.clone() });
            }
        }
        if let Sort::Fields(fields) = &self.sort {
            if let Some(field) = fields.iter().find(|field| model.ephemeral_field(&field.column).is_some()) {
                return Err(DeclarationError::EphemeralInRelation { relation: relation(), field: field.column.clone() });
            }
            if let Some(field) = fields.iter().find(|field| model.field(&field.column).is_none()) {
                return Err(DeclarationError::UnknownSortColumn { relation: relation(), column: field.column.clone() });
            }
        }
        let set = self.remote.iter().flat_map(|remote| remote.updates.iter()).flat_map(|updates| updates.set.keys());
        for field in set {
            let Some(spec) = model.ephemeral_field(field) else {
                return Err(DeclarationError::UnknownSetField { relation: relation(), field: field.clone() });
            };
            if spec.reset_after.is_none() {
                return Err(DeclarationError::SetWithoutReset { relation: relation(), field: field.clone() });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    #[default]
    Rollback,
    Keep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootShape {
    Upsert,
    Destroy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionDeclaration {
    pub name: String,
    pub root_shape: RootShape,
    #[serde(default)]
    pub on_failure: OnFailure,
    #[serde(default)]
    pub has_optimistic: bool,
    #[serde(default)]
    pub has_temp_row_retention: bool,
    #[serde(default)]
    pub has_cascade_dependents: bool,
    #[serde(default)]
    pub has_once: bool,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub mutation: Option<String>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default)]
    pub optimistic: Map<String, Value>,
    #[serde(default)]
    pub refetch: Vec<String>,
}

impl ActionDeclaration {
    pub fn new(name: &str, root_shape: RootShape) -> Self {
        Self {
            name: name.to_string(),
            root_shape,
            on_failure: OnFailure::Rollback,
            has_optimistic: false,
            has_temp_row_retention: false,
            has_cascade_dependents: false,
            has_once: false,
            dedupe_key: None,
            members: Vec::new(),
            mutation: None,
            variables: BTreeMap::new(),
            optimistic: Map::new(),
            refetch: Vec::new(),
        }
    }

    fn check(&self, model: &ModelSpec, taken_names: &HashSet<String>) -> Result<(), DeclarationError> {
        let name = || self.name.clone();
        if taken_names.contains(&self.name) {
            return Err(DeclarationError::ActionNameCollision(name()));
        }
        if self.has_optimistic && self.root_shape == RootShape::Upsert && !self.has_temp_row_retention {
            return Err(DeclarationError::MissingTempRowRetention(name()));
        }
        if self.has_optimistic && self.root_shape == RootShape::Destroy && self.has_cascade_dependents {
            return Err(DeclarationError::OptimisticDestroyWithCascade(name()));
        }
        if let Some(member) = self.members.iter().find(|member| FORBIDDEN_ACTION_MEMBERS.contains(&member.as_str())) {
            return Err(DeclarationError::ForbiddenMember(member.clone()));
        }
        if self.root_shape == RootShape::Destroy && self.on_failure == OnFailure::Keep {
            return Err(DeclarationError::KeepOnDestroy(name()));
        }
        if self.has_once && self.dedupe_key.as_deref().is_none_or(str::is_empty) {
            return Err(DeclarationError::OnceKeyRequired(name()));
        }
        if let Some(relation) = self.refetch.iter().find(|relation| !model.has_remote(relation)) {
            return Err(DeclarationError::UnknownRefetch { action: name(), relation: relation.clone() });
        }
        match self.optimistic.keys().find(|field| model.field(field).is_none()) {
            Some(field) => Err(DeclarationError::UnknownOptimisticField { action: name(), field: field.clone() }),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelSpec {
    pub key: ModelKey,
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub associations: Vec<AssociationSpec>,
    #[serde(default)]
    pub relations: Vec<RelationSpec>,
    #[serde(default)]
    pub composite_indexes: Vec<Vec<String>>,
    #[serde(default)]
    pub actions: Vec<ActionDeclaration>,
    #[serde(default)]
    pub ephemeral: Vec<EphemeralSpec>,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub expire_after: Option<String>,
}

fn is_reserved_name(name: &str) -> bool {
    name == "orderKey" || name == "sync" || name.starts_with('$')
}

impl ModelSpec {
    pub fn new(key: &str, fields: Vec<FieldSpec>) -> Self {
        Self {
            key: ModelKey::new(key),
            fields,
            associations: Vec::new(),
            relations: Vec::new(),
            composite_indexes: Vec::new(),
            actions: Vec::new(),
            ephemeral: Vec::new(),
            storage: Storage::Disk,
            expire_after: None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn has_remote(&self, relation: &str) -> bool {
        self.relations.iter().any(|candidate| candidate.name == relation && candidate.remote.is_some())
    }

    pub fn is_indexed(&self, name: &str) -> bool {
        name == "id" || self.field(name).is_some_and(|field| field.indexed)
    }

    pub fn indexed_names(&self) -> Vec<String> {
        self.fields.iter().filter(|field| field.indexed).map(|field| field.name.clone()).collect()
    }

    pub fn normalize(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, SchemaError> {
        let mut normalized = Map::new();
        for field in &self.fields {
            let value = field.read(input);
            if let Some(decoded) = value.as_ref().filter(|value| !value.is_null()).and_then(|value| field.decode(value)) {
                normalized.insert(field.name.clone(), decoded);
            } else if field.optional && value.is_none() {
                continue;
            } else if field.nullable {
                normalized.insert(field.name.clone(), Value::Null);
            } else if let Some(default) = &field.default {
                normalized.insert(field.name.clone(), default.clone());
            } else if !field.optional {
                return Err(SchemaError::MissingField(field.name.clone()));
            }
        }
        Ok(normalized)
    }

    pub fn normalize_all(&self, inputs: &[Map<String, Value>]) -> (Vec<Map<String, Value>>, Vec<DroppedRow>) {
        let mut rows = Vec::with_capacity(inputs.len());
        let mut dropped = Vec::new();
        for (index, input) in inputs.iter().enumerate() {
            match self.normalize(input) {
                Ok(row) => rows.push(row),
                Err(error) => dropped.push((index, error)),
            }
        }
        (rows, dropped)
    }

    pub fn patch(&self, input: &Map<String, Value>) -> Map<String, Value> {
        let mut patched = Map::new();
        for field in &self.fields {
            let Some(value) = field.read(input) else { continue };
            if value.is_null() {
                patched.insert(field.name.clone(), Value::Null);
            } else if let Some(decoded) = field.decode(&value) {
                patched.insert(field.name.clone(), decoded);
            }
        }
        patched
    }

    pub fn validate(&self, reserved_members: &[&str]) -> Result<(), DeclarationError> {
        if self.key.as_str().is_empty() {
            return Err(DeclarationError::EmptyModelKey);
        }
        if self.fields.is_empty() {
            return Err(DeclarationError::NoFields);
        }
        self.check_fields()?;
        self.check_ephemeral()?;
        self.check_storage()?;
        self.check_indexes()?;
        let indexed: HashSet<&str> = self.fields.iter().filter(|field| field.indexed).map(|field| field.name.as_str()).collect();
        for association in &self.associations {
            association.check_local(&indexed)?;
        }
        for relation in &self.relations {
            relation.check(self)?;
        }
        self.check_names(reserved_members)?;
        self.check_actions(reserved_members)
    }

    fn check_fields(&self) -> Result<(), DeclarationError> {
        for field in &self.fields {
            if field.name.is_empty() {
                return Err(DeclarationError::EmptyFieldName);
            }
            if is_reserved_name(&field.name) {
                return Err(DeclarationError::ReservedFieldName(field.name.clone()));
            }
        }
        let names: Vec<&str> = self.fields.iter().map(|field| field.name.as_str()).collect();
        match duplicates(&names).first() {
            Some(name) => Err(DeclarationError::DuplicateField(name.to_string())),
            None => Ok(()),
        }
    }

    fn check_indexes(&self) -> Result<(), DeclarationError> {
        let mut columns: HashSet<String> = SERVICE_COLUMNS.iter().map(|column| column.to_string()).collect();
        for field in self.fields.iter().filter(|field| field.indexed) {
            if column_type(&field.kind).is_none() {
                return Err(DeclarationError::IndexedFieldNotScalar(field.name.clone()));
            }
            if !columns.insert(column_name(&field.name)) {
                return Err(DeclarationError::ColumnCollision(field.name.clone()));
            }
        }
        for composite in &self.composite_indexes {
            if composite.len() < 2 {
                return Err(DeclarationError::CompositeIndexTooShort);
            }
            if let Some(name) = composite.iter().find(|name| !self.is_indexed(name)) {
                return Err(DeclarationError::CompositeIndexFieldNotIndexed(name.clone()));
            }
        }
        Ok(())
    }

    fn check_names(&self, reserved_members: &[&str]) -> Result<(), DeclarationError> {
        let relation_names: Vec<&str> = self.relations.iter().map(|relation| relation.name.as_str()).collect();
        if let Some(name) = duplicates(&relation_names).first() {
            return Err(DeclarationError::DuplicateRelation(name.to_string()));
        }
        let association_names: Vec<&str> = self.associations.iter().map(|association| association.name.as_str()).collect();
        if let Some(name) = relation_names.iter().find(|name| association_names.contains(*name)) {
            return Err(DeclarationError::AssociationNameCollision(name.to_string()));
        }
        match relation_names.iter().chain(&association_names).find(|name| reserved_members.contains(*name)) {
            Some(name) => Err(DeclarationError::NameCollision(name.to_string())),
            None => Ok(()),
        }
    }

    fn check_actions(&self, reserved_members: &[&str]) -> Result<(), DeclarationError> {
        let mut taken: HashSet<String> = reserved_members.iter().map(|name| name.to_string()).collect();
        taken.extend(self.relations.iter().map(|relation| relation.name.clone()));
        taken.extend(self.associations.iter().map(|association| association.name.clone()));
        for action in &self.actions {
            action.check(self, &taken)?;
            taken.insert(action.name.clone());
        }
        Ok(())
    }
}

pub fn validate_models(models: &[ModelSpec], reserved_members: &[&str]) -> Vec<ModelFault> {
    let mut faults = Vec::new();
    let keys: Vec<&str> = models.iter().map(|model| model.key.as_str()).collect();
    for key in duplicates(&keys) {
        faults.push(ModelFault { model: ModelKey::new(key), error: DeclarationError::DuplicateModelKey(key.to_string()) });
    }
    let by_key: BTreeMap<&str, &ModelSpec> = models.iter().map(|model| (model.key.as_str(), model)).collect();
    for model in models {
        let result = model.validate(reserved_members).and_then(|()| {
            model
                .associations
                .iter()
                .try_for_each(|association| association.check_target(by_key.get(association.target.as_str()).copied()))
        });
        if let Err(error) = result {
            faults.push(ModelFault { model: model.key.clone(), error });
        }
    }
    faults
}

#[cfg(test)]
mod ephemeral_tests;
#[cfg(test)]
mod schema_tests;
