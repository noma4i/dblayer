use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ddl::{column_name, column_type, SERVICE_COLUMNS};
use crate::identity::{ModelKey, ListKey};
use crate::value::{
    duplicates, read_array, read_enum, read_raw, read_scalar, relation_scope_key, ScalarKind, ListKeyError,
    SortDirection,
};

mod effect;
mod ephemeral;
pub(crate) mod keyed;
mod local;
mod merge;
mod newer;
mod search;
mod types;

pub use effect::{ActionEffect, EffectFault, EffectSource, Selection, SelectionKeyError};
pub use ephemeral::{EphemeralSpec, Storage};
pub use merge::MergeRule;
pub use search::{search_text, SearchPath};
pub use types::{resolve_types, ValueFault};

pub const TYPENAME: &str = "__typename";
pub const COLUMN_LIMIT: usize = 2000;

const FORBIDDEN_ACTION_MEMBERS: [&str; 6] = ["after", "invalidate", "onCommit", "extract", "complete", "resume"];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("missing field {0}")]
    MissingField(String),
    #[error("value of field {0} does not fit its type")]
    InvalidValue(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclarationError {
    #[error("invalid upload batch declaration: {0}")]
    InvalidUploadBatch(String),
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
    #[error("searchable field {0} is not a string or a list of strings")]
    SearchableNotText(String),
    #[error("field {0} is declared twice")]
    DuplicateField(String),
    #[error("field {0} parses JSON only as raw with an object or null default")]
    InvalidJsonParse(String),
    #[error("edge field {0} cannot parse JSON")]
    JsonParseEdge(String),
    #[error("indexed field {0} is not a scalar or enum")]
    IndexedFieldNotScalar(String),
    #[error("table {table} would have {count} columns, above the SQLite limit of {COLUMN_LIMIT}")]
    TooManyColumns { table: String, count: usize },
    #[error("union field {0} declares a variant twice")]
    DuplicateVariant(String),
    #[error("column of field {0} collides with another column")]
    ColumnCollision(String),
    #[error("index needs at least 1 field")]
    EmptyIndex,
    #[error("index column {0} must name a scalar or enum field")]
    InvalidIndexColumn(String),
    #[error("association {0} has no foreign key")]
    EmptyForeignKey(String),
    #[error("foreign key of association {0} is not indexed")]
    ForeignKeyNotIndexed(String),
    #[error("references key of association {0} is not a declared field")]
    ReferencesKeyNotField(String),
    #[error("association {0} declares dependent without owning children")]
    DependentOnNonOwner(String),
    #[error("association {0} targets an unknown model")]
    UnknownTarget(String),
    #[error("field {0} links to an unknown model")]
    UnknownFieldLink(String),
    #[error("field {0} links by an id attribute that is not declared beside it")]
    FieldLinkWithoutId(String),
    #[error("action {0} correlates on a field that is not declared")]
    CorrelateUnknownField(String),
    #[error("field {0} has a merge rule that does not fit its type")]
    MergeRuleMismatch(String),
    #[error("local field {0} needs optional: true or null: true and cannot be indexed")]
    LocalNotOptional(String),
    #[error("association {0} sets or compares a field that is not declared")]
    SetsUnknownField(String),
    #[error("action {action} lands the unknown model {model}")]
    UnknownLanding { action: String, model: String },
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
    #[error("relation {relation} lists {param} as a text param but the model has a field with that name")]
    TextParamIsField { relation: String, param: String },
    #[error("relation {relation} types {param} as int or bool but it is not a typed param of that kind")]
    ScalarParamKind { relation: String, param: String },
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
    #[error("action {action} refetches {relation}, which is not a remote relation")]
    UnknownRefetch { action: String, relation: String },
    #[error("action {action} patches unknown field {field}")]
    UnknownOptimisticField { action: String, field: String },
    #[error("action {action} declares an invalid effect: {fault:?}")]
    Effect { action: String, fault: EffectFault },
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
    #[error("local model has no server: {0}")]
    LocalHasServer(String),
    #[error("expire_after needs storage: :memory")]
    ExpireNeedsMemory,
    #[error("relation {relation} sets {field}, which is not an ephemeral field")]
    UnknownSetField { relation: String, field: String },
    #[error("relation {relation} sets {field}, which has no reset_after")]
    SetWithoutReset { relation: String, field: String },
    #[error("relation {relation} keeps a stable order and needs {needs}")]
    StableOrderNeeds { relation: String, needs: &'static str },
}

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
    Union,
    UnionArray,
    File,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldLink {
    pub model: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub parse_json: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<FieldLink>,
    #[serde(default)]
    pub app_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    #[serde(default)]
    pub merge: Option<MergeRule>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub searchable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shared: Vec<FieldSpec>,
}

impl FieldSpec {
    fn with_kind(name: &str, kind: FieldKind) -> Self {
        Self {
            name: name.to_string(),
            kind,
            parse_json: false,
            nullable: false,
            optional: false,
            default: None,
            indexed: false,
            id_from_parent: None,
            from: None,
            link: None,
            app_type: None,
            values: Vec::new(),
            merge: None,
            searchable: false,
            fields: Vec::new(),
            shared: Vec::new(),
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

    pub fn union(name: &str, type_name: &str, variants: Vec<FieldSpec>) -> Self {
        Self { app_type: Some(type_name.to_string()), fields: variants, ..Self::with_kind(name, FieldKind::Union) }
    }

    pub fn union_array(name: &str, type_name: &str, variants: Vec<FieldSpec>) -> Self {
        Self { app_type: Some(type_name.to_string()), fields: variants, ..Self::with_kind(name, FieldKind::UnionArray) }
    }

    pub fn with_shared(self, shared: Vec<FieldSpec>) -> Self {
        Self { shared, ..self }
    }

    pub fn variant(case: &str, type_name: &str, fields: Vec<FieldSpec>) -> Self {
        Self { app_type: Some(type_name.to_string()), fields, ..Self::with_kind(case, FieldKind::Object) }
    }

    pub fn enumeration(name: &str, values: &[&str]) -> Self {
        Self::with_kind(name, FieldKind::Enum { values: values.iter().map(|value| value.to_string()).collect() })
    }

    #[cfg(test)]
    pub fn parent_id(name: &str, parent: &str) -> Self {
        Self { id_from_parent: Some(parent.to_string()), ..Self::scalar(name, ScalarKind::Id) }
    }

    #[cfg(test)]
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
            return read_path(input, path).cloned();
        }
        let parent = self.id_from_parent.as_ref()?;
        input.get(parent)?.as_object()?.get("id").cloned()
    }

    fn zero(&self, now_ms: i64) -> Option<Value> {
        match &self.kind {
            FieldKind::Scalar { kind } => match kind {
                ScalarKind::Str => Some(Value::String(String::new())),
                ScalarKind::Int | ScalarKind::Num => Some(Value::from(0)),
                ScalarKind::Bool => Some(Value::Bool(false)),
                ScalarKind::Date => Some(Value::from(now_ms)),
                ScalarKind::Id => None,
            },
            FieldKind::Enum { values } => values.first().cloned().map(Value::String),
            FieldKind::Object => Some(Value::Object(Map::new())),
            FieldKind::Array | FieldKind::UnionArray | FieldKind::List { .. } => Some(Value::Array(Vec::new())),
            FieldKind::Raw | FieldKind::File | FieldKind::Union => None,
        }
    }

    pub fn decode(&self, value: &Value) -> Option<Value> {
        match &self.kind {
            FieldKind::Scalar { kind } => read_scalar(value, *kind),
            FieldKind::Enum { .. } => read_enum(value).map(Value::String),
            FieldKind::Raw if self.parse_json => crate::value::read_json_text(value),
            FieldKind::Raw => read_raw(value),
            FieldKind::Object => value.is_object().then(|| value.clone()),
            FieldKind::Array | FieldKind::UnionArray => value.is_array().then(|| value.clone()),
            FieldKind::Union => value.get(TYPENAME).is_some_and(Value::is_string).then(|| value.clone()),
            FieldKind::File => is_file_descriptor(value).then(|| value.clone()),
            FieldKind::List { element } => {
                read_array(value, |item| read_scalar(item, *element)).map(Value::Array)
            }
        }
    }

    fn decode_stored(&self, value: &Value) -> Option<Value> {
        if self.parse_json { read_raw(value) } else { self.decode(value) }
    }

    fn check_json_parse(&self) -> Result<(), DeclarationError> {
        if self.parse_json && (self.kind != FieldKind::Raw || self.default.as_ref().is_some_and(|value| !value.is_object() && !value.is_null())) {
            return Err(DeclarationError::InvalidJsonParse(self.name.clone()));
        }
        for child in self.fields.iter().chain(&self.shared) { child.check_json_parse()?; }
        Ok(())
    }
}

pub(crate) fn read_path<'a>(input: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut current = input.get(parts.next()?)?;
    for part in parts {
        if current.is_null() {
            return Some(current);
        }
        current = current.as_object()?.get(part)?;
    }
    Some(current)
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
    pub sets: BTreeMap<String, String>,
    #[serde(default)]
    pub newer: Vec<Newer>,
    #[serde(default)]
    pub from: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Newer {
    pub child: String,
    pub parent: String,
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
            sets: BTreeMap::new(),
            newer: Vec::new(),
            from: None,
        }
    }

    pub fn owns_children(&self) -> bool {
        matches!(self.kind, AssociationKind::HasMany | AssociationKind::HasOne)
    }

    pub fn requires_foreign_key(&self) -> bool {
        self.kind != AssociationKind::References
    }

    fn check_local(&self, indexed: &HashSet<&str>, fields: &HashSet<&str>) -> Result<(), DeclarationError> {
        let name = || self.name.clone();
        if self.kind == AssociationKind::References && !self.foreign_key.is_empty() && !fields.contains(self.foreign_key.as_str()) {
            return Err(DeclarationError::ReferencesKeyNotField(name()));
        }
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
        let compared = self.newer.iter().map(|newer| &newer.child);
        if self.sets.values().chain(compared).any(|field| !fields.contains(field.as_str())) {
            return Err(DeclarationError::SetsUnknownField(name()));
        }
        Ok(())
    }

    fn check_target(&self, target: Option<&ModelSpec>) -> Result<(), DeclarationError> {
        let target = target.ok_or_else(|| DeclarationError::UnknownTarget(self.name.clone()))?;
        if self.owns_children() && !target.is_indexed(&self.foreign_key) {
            return Err(DeclarationError::ForeignKeyNotIndexed(self.name.clone()));
        }
        let compared = self.newer.iter().map(|newer| &newer.parent);
        if self.sets.keys().chain(compared).any(|field| !target.fields.iter().any(|declared| declared.name == *field)) {
            return Err(DeclarationError::SetsUnknownField(self.name.clone()));
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum VariableSource {
    Param { name: String },
    Constant { value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorSpec {
    pub variable: String,
    pub from: String,
    #[serde(default)]
    pub numeric: bool,
}

pub const NODE_PARAM: &str = "nodeId";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_unless: Option<String>,
    #[serde(default)]
    pub set: Map<String, Value>,
    #[serde(default)]
    pub removes: bool,
    #[serde(default)]
    pub debounce: Option<String>,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub id: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply: Option<SubscriptionApply>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionApply {
    #[serde(flatten)]
    pub action: SubscriptionAction,
    #[serde(default)]
    pub matches: BTreeMap<String, MatchValues>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValues {
    OneOf(BTreeSet<String>),
    Except { not: BTreeSet<String> },
}

impl MatchValues {
    pub fn accepts(&self, value: &Value) -> bool {
        let Some(value) = value.as_str() else { return false };
        match self {
            Self::OneOf(values) => values.contains(value),
            Self::Except { not } => !not.contains(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum SubscriptionAction {
    Upsert,
    Delete,
    Ephemeral { fields: BTreeMap<String, String> },
    Reread { via: String },
    Ignore,
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
    pub page_size: Option<u32>,
    #[serde(default)]
    pub variables: BTreeMap<String, VariableSource>,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_from: Option<String>,
}

impl RemoteSpec {
    #[cfg(test)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRead {
    One,
    Many,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Membership {
    Contains(String),
    Equals(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "test", rename_all = "snake_case")]
pub enum RowTest {
    In { values: Vec<Value> },
    Equals { value: Value },
    GreaterThan { parameter: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowRule {
    pub field: String,
    #[serde(flatten)]
    pub test: RowTest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_rules: Vec<RowRule>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub membership: BTreeMap<String, Membership>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub joins_node: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updates: Vec<UpdatesSpec>,
    pub by_columns: Vec<String>,
    #[serde(default)]
    pub text_params: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub text_types: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scalar_types: BTreeMap<String, ScalarKind>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub text_defaults: BTreeMap<String, String>,
    #[serde(default)]
    pub remote: Option<RemoteSpec>,
    #[serde(default)]
    pub is_global: bool,
    #[serde(default)]
    pub sort: Sort,
    #[serde(default)]
    pub joins_events: bool,
    #[serde(default)]
    pub joins_created: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub order_stable: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub refresh_on_foreground: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeRead>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edge_fields: Vec<FieldSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload: Option<BTreeMap<String, PreloadChoice>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preload_order: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub members_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaSpec {
    pub marker: String,
    pub from: String,
    pub fields: Vec<String>,
    pub sequence: String,
    pub tail: String,
    pub after: String,
    pub marker_variables: BTreeMap<String, VariableSource>,
    pub tail_variables: BTreeMap<String, VariableSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PreloadChoice {
    Listed(Vec<Value>),
    RowsOf { from: PreloadSource },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreloadSource {
    pub model: String,
    pub relation: String,
}

impl RelationSpec {
    pub fn new(name: &str, by_columns: &[&str]) -> Self {
        Self {
            name: name.to_string(),
            row_rules: Vec::new(),
            membership: BTreeMap::new(),
            joins_node: false,
            updates: Vec::new(),
            by_columns: by_columns.iter().map(|column| column.to_string()).collect(),
            text_params: Vec::new(),
            text_types: BTreeMap::new(),
            scalar_types: BTreeMap::new(),
            text_defaults: BTreeMap::new(),
            remote: None,
            is_global: false,
            sort: Sort::ServerOrder,
            joins_events: false,
            joins_created: false,
            order_stable: false,
            refresh_on_foreground: false,
            node: None,
            edge_fields: Vec::new(),
            preload: None,
            preload_limit: None,
            preload_order: Vec::new(),
            members_complete: false,
            media: None,
        }
    }

    pub fn global(name: &str) -> Self {
        Self { is_global: true, ..Self::new(name, &[]) }
    }

    #[cfg(test)]
    pub fn is_local(&self) -> bool {
        self.remote.is_none()
    }

    pub fn is_singleton(&self) -> bool {
        self.is_global && self.remote.as_ref().is_some_and(|remote| remote.shape == RemoteShape::Single)
    }

    pub fn takes_created_rows(&self) -> bool {
        self.joins_created && self.remote.is_some() && self.by_columns.is_empty() && self.text_params.is_empty()
    }

    pub fn lists_every_row_of_its_params(&self) -> bool {
        let Some(_) = &self.remote else { return false };
        (!self.by_columns.is_empty() || !self.membership.is_empty())
            && self.text_params.iter().all(|parameter| self.membership.contains_key(parameter))
            && self.constants_described()
    }

    pub fn constants_described(&self) -> bool {
        !self.row_rules.is_empty() || !self.remote.as_ref().is_some_and(|remote| remote.variables.values().any(|source| matches!(source, VariableSource::Constant { .. })))
    }

    pub fn has_threshold(&self) -> bool {
        self.row_rules.iter().any(|rule| matches!(rule.test, RowTest::GreaterThan { .. }))
    }

    pub fn row_passes_literals(&self, row: &Map<String, Value>) -> bool {
        self.row_rules.iter().all(|rule| {
            let value = row.get(&rule.field).filter(|value| !value.is_null());
            match &rule.test {
                RowTest::In { values } => value.is_some_and(|value| values.contains(value)),
                RowTest::Equals { value: literal } => value == Some(literal),
                RowTest::GreaterThan { .. } => true,
            }
        })
    }

    pub fn row_passes_thresholds(&self, row: &Map<String, Value>, window: &BTreeMap<String, String>) -> bool {
        self.row_rules.iter().all(|rule| {
            let RowTest::GreaterThan { parameter } = &rule.test else { return true };
            let bound = window.get(parameter).and_then(|text| text.strip_prefix("d:")).and_then(|text| text.parse::<i64>().ok());
            matches!((bound, row.get(&rule.field).and_then(Value::as_i64)), (Some(bound), Some(value)) if value > bound)
        })
    }

    pub fn row_params(&self, row: &Map<String, Value>) -> Vec<Map<String, Value>> {
        if !self.row_passes_literals(row) { return Vec::new(); }
        let mut params = vec![Map::new()];
        for column in &self.by_columns {
            let Some(value) = row.get(column) else { return Vec::new() };
            for values in &mut params { values.insert(column.clone(), value.clone()); }
        }
        for (parameter, rule) in &self.membership {
            let values = match rule {
                Membership::Contains(field) => row.get(field).and_then(Value::as_array).cloned().unwrap_or_default(),
                Membership::Equals(field) => row.get(field).filter(|value| !value.is_null()).cloned().into_iter().collect(),
            };
            params = params.into_iter().flat_map(|params| values.iter().map(move |value| {
                let mut params = params.clone();
                params.insert(parameter.clone(), value.clone());
                params
            })).collect();
        }
        params
    }

    #[cfg(test)]
    pub fn has_more(&self) -> bool {
        self.remote.as_ref().is_some_and(RemoteSpec::supports_paging)
    }

    pub fn scope_key(&self, model: &str, params: &BTreeMap<String, String>) -> Result<ListKey, ListKeyError> {
        relation_scope_key(model, &self.name, params)
    }

    fn check(&self, model: &ModelSpec) -> Result<(), DeclarationError> {
        if let Some(field) = self.edge_fields.iter().find(|field| field.parse_json) {
            return Err(DeclarationError::JsonParseEdge(field.name.clone()));
        }
        if self.name.is_empty() {
            return Err(DeclarationError::EmptyRelationName);
        }
        if is_reserved_name(&self.name) {
            return Err(DeclarationError::ReservedRelationName(self.name.clone()));
        }
        if self.by_columns.is_empty() && self.text_params.is_empty() && !self.is_global {
            return Err(DeclarationError::EmptyByColumns(self.name.clone()));
        }
        if let Some(fault) = self.remote.as_ref().and_then(RemoteSpec::fault) {
            return Err(DeclarationError::Remote { relation: self.name.clone(), fault });
        }
        let relation = || self.name.clone();
        if self.order_stable {
            let needs = match (&self.remote, &self.sort) {
                (None, _) => Some("a fetch"),
                (_, Sort::ServerOrder) => Some("an order"),
                _ if self.by_columns.is_empty() || !self.text_params.is_empty() => Some("where columns and no text params"),
                _ => None,
            };
            if let Some(needs) = needs {
                return Err(DeclarationError::StableOrderNeeds { relation: relation(), needs });
            }
        }
        if let Some(param) = self.text_params.iter().find(|param| model.field(param).is_some()) {
            return Err(DeclarationError::TextParamIsField { relation: relation(), param: param.clone() });
        }
        if let Some(param) = self.scalar_types.iter().find(|(param, kind)| !self.text_params.contains(param) || !matches!(kind, ScalarKind::Int | ScalarKind::Bool)).map(|(param, _)| param) {
            return Err(DeclarationError::ScalarParamKind { relation: relation(), param: param.clone() });
        }
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
        let set = self.updates.iter().flat_map(|updates| updates.set.keys());
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<UploadBatchDeclaration>,
    #[serde(default)]
    pub client_mutation_id: bool,
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
    pub members: Vec<String>,
    #[serde(default)]
    pub mutation: Option<String>,
    #[serde(default)]
    pub upload: bool,
    /// The command survives a missing network and a restart of the app: sending a message.
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default)]
    pub optimistic: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effects: Vec<ActionEffect>,
    #[serde(default)]
    pub refetch: Vec<String>,
    #[serde(default)]
    pub args: BTreeMap<String, String>,
    #[serde(default)]
    pub local: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub local_from: BTreeMap<String, EffectSource>,
    #[serde(default)]
    pub correlate: Vec<String>,
    #[serde(default)]
    pub ignore_response: bool,
    #[serde(default)]
    pub wrap: Option<String>,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub lands: Vec<ActionLanding>,
    #[serde(default)]
    pub result: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionLanding {
    pub model: String,
    pub root: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadBatchDeclaration {
    pub min: usize,
    pub max: usize,
    pub items: String,
    pub single: String,
    pub input: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renumber: Option<UploadBatchRenumber>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadBatchRenumber {
    pub position: String,
    pub size: String,
    pub position_from: usize,
}

impl ActionDeclaration {
    pub fn new(name: &str, root_shape: RootShape) -> Self {
        Self {
            batch: None,
            client_mutation_id: false,
            name: name.to_string(),
            root_shape,
            on_failure: OnFailure::Rollback,
            has_optimistic: false,
            has_temp_row_retention: false,
            has_cascade_dependents: false,
            members: Vec::new(),
            mutation: None,
            upload: false,
            durable: false,
            variables: BTreeMap::new(),
            optimistic: Map::new(),
            effects: Vec::new(),
            refetch: Vec::new(),
            args: BTreeMap::new(),
            local: Vec::new(),
            local_from: BTreeMap::new(),
            correlate: Vec::new(),
            ignore_response: false,
            wrap: None,
            root: None,
            lands: Vec::new(),
            result: BTreeMap::new(),
        }
    }

    fn check(&self, model: &ModelSpec, taken_names: &HashSet<String>) -> Result<(), DeclarationError> {
        if let Some(batch) = &self.batch {
            if let Some(fields) = &batch.renumber {
                for path in [&fields.position, &fields.size] {
                    if model.integer_path(path).is_none() {
                        return Err(DeclarationError::InvalidUploadBatch(format!("{} renumber field `{path}` must name an integer field", self.name)));
                    }
                }
            }
            let single = model.actions.iter().find(|action| action.name == batch.single);
            if !self.upload || batch.min < 2 || batch.max > 10 || batch.min > batch.max
                || batch.items.is_empty() || self.correlate.len() != 1
                || !batch.input.values().any(|field| self.correlate.contains(field))
                || batch.input.values().any(|field| model.field(field).is_none())
                || batch.renumber.as_ref().is_some_and(|fields| fields.position_from > 1 || fields.position == fields.size)
                || single.is_none_or(|action| !action.upload || action.batch.is_some() || action.correlate != self.correlate)
            {
                return Err(DeclarationError::InvalidUploadBatch(self.name.clone()));
            }
        }
        let name = || self.name.clone();
        let unknown = self.correlate.iter().any(|path| !model.fields.iter().any(|field| field.name == path.split('.').next().unwrap_or_default()));
        if unknown {
            return Err(DeclarationError::CorrelateUnknownField(name()));
        }
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
        if let Some(relation) = self.refetch.iter().find(|relation| !relation.contains('.') && !model.has_remote(relation)) {
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
    pub indexes: Vec<Vec<String>>,
    #[serde(default)]
    pub actions: Vec<ActionDeclaration>,
    #[serde(default)]
    pub ephemeral: Vec<EphemeralSpec>,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub expire_after: Option<String>,
    #[serde(default)]
    pub listens: Vec<UpdatesSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ChangeRead>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_feed: Option<ChangeFeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeFeed {
    pub subscription: String,
    pub root: String,
    pub area: String,
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRead {
    pub area: String,
    pub relation: String,
    pub parameter: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removes_on: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub observed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_of: Option<ChangeIdOf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeIdOf {
    pub model: String,
    pub field: String,
}

fn is_reserved_name(name: &str) -> bool {
    name == "orderKey" || name == "sync" || name.starts_with('$')
}

impl ModelSpec {
    pub fn upload_action(&self, name: &str) -> Option<&ActionDeclaration> {
        self.actions.iter().find(|action| action.upload && action.name == name)
    }

    pub fn upload_family(&self, action: &str) -> Vec<String> {
        let mut family = vec![action.to_string()];
        for declared in self.actions.iter().filter(|declared| declared.upload) {
            let Some(batch) = &declared.batch else { continue };
            if declared.name == action { family.push(batch.single.clone()); }
            if batch.single == action { family.push(declared.name.clone()); }
        }
        family
    }

    pub fn is_batch_upload(&self, action_key: &str) -> bool {
        crate::operation::Operation::upload_action(action_key).and_then(|(_, name)| self.upload_action(&name)).is_some_and(|action| action.batch.is_some())
    }

    pub fn new(key: &str, fields: Vec<FieldSpec>) -> Self {
        Self {
            key: ModelKey::new(key),
            fields,
            associations: Vec::new(),
            relations: Vec::new(),
            indexes: Vec::new(),
            actions: Vec::new(),
            ephemeral: Vec::new(),
            storage: Storage::Disk,
            expire_after: None,
            listens: Vec::new(),
            changes: Vec::new(),
            change_feed: None,
            server_revision: None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub(crate) fn integer_path<'a>(&'a self, path: &str) -> Option<Vec<&'a str>> {
        fn resolve<'a>(fields: &'a [FieldSpec], path: &str) -> Option<Vec<&'a str>> {
            if let Some(field) = fields.iter().find(|field| field.name == path) {
                return matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Int }).then(|| vec![field.name.as_str()]);
            }
            fields.iter().filter(|field| field.kind == FieldKind::Object).find_map(|field| {
                let tail = path.strip_prefix(&field.name)?.strip_prefix('.')?;
                let mut keys = resolve(&field.fields, tail)?;
                keys.insert(0, &field.name);
                Some(keys)
            })
        }
        resolve(&self.fields, path)
    }

    pub fn column_scalar_kind(&self, column: &str) -> Option<ScalarKind> {
        match self.field(column).map(|field| &field.kind) {
            Some(FieldKind::Scalar { kind }) => Some(*kind),
            Some(FieldKind::Enum { .. }) => Some(ScalarKind::Str),
            _ => None,
        }
    }

    pub fn list_scopes(&self, row: &Map<String, Value>) -> Vec<ListKey> {
        self.relations
            .iter()
            .filter(|relation| relation.lists_every_row_of_its_params() || relation.takes_created_rows())
            .flat_map(|relation| relation.row_params(row).into_iter().filter_map(|params| self.selection_key(&relation.name, &params).ok()))
            .collect()
    }

    pub fn stable_order(&self, scope: &ListKey) -> Option<&[SortField]> {
        let relation = self.relations.iter().filter(|relation| relation.order_stable).find(|relation| {
            scope.as_str().starts_with(ListKey::compose(&[self.key.as_str(), relation.name.as_str()]).as_str())
        })?;
        match &relation.sort {
            Sort::Fields(fields) => Some(fields),
            Sort::ServerOrder => None,
        }
    }

    pub fn has_remote(&self, relation: &str) -> bool {
        self.relations.iter().any(|candidate| candidate.name == relation && candidate.remote.is_some())
    }

    pub fn is_indexed(&self, name: &str) -> bool {
        name == "id" || self.field(name).is_some_and(|field| field.indexed) || self.indexes.iter().any(|fields| fields.first().is_some_and(|first| first == name))
    }

    #[cfg(test)]
    pub fn indexed_names(&self) -> Vec<String> {
        self.fields.iter().filter(|field| field.indexed).map(|field| field.name.clone()).collect()
    }

    pub fn normalize(&self, input: &Map<String, Value>) -> Result<Map<String, Value>, SchemaError> {
        self.normalize_values(input, FieldSpec::decode)
    }

    fn normalize_values(&self, input: &Map<String, Value>, decode: fn(&FieldSpec, &Value) -> Option<Value>) -> Result<Map<String, Value>, SchemaError> {
        let mut normalized = Map::new();
        for field in self.fields.iter().filter(|field| !field.is_local()) {
            let value = field.read(input);
            if let Some(decoded) = value.as_ref().filter(|value| !value.is_null()).and_then(|value| decode(field, value)) {
                normalized.insert(field.name.clone(), decoded);
            } else if let Some(default) = field.default.as_ref().filter(|_| value.is_none() && (field.parse_json || !field.optional)) {
                normalized.insert(field.name.clone(), default.clone());
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

    /// Normalizes a locally created row. Server-owned fields the caller did not give take a zero value of their type, dates take `now_ms`.
    pub fn normalize_local(&self, input: &Map<String, Value>, now_ms: i64) -> Result<Map<String, Value>, SchemaError> {
        let mut completed = input.clone();
        for field in self.fields.iter().filter(|field| !field.is_local()) {
            if field.nullable || field.optional || field.default.is_some() || field.read(input).is_some() {
                continue;
            }
            if let Some(zero) = field.zero(now_ms) {
                completed.insert(field.name.clone(), zero);
            }
        }
        self.normalize(&completed)
    }

    pub fn patch(&self, input: &Map<String, Value>) -> Map<String, Value> {
        let mut patched = Map::new();
        for field in self.fields.iter().filter(|field| !field.is_local()) {
            let Some(value) = field.read(input) else { continue };
            if value.is_null() {
                let emptied = if field.nullable { None } else { field.default.clone() };
                patched.insert(field.name.clone(), emptied.unwrap_or(Value::Null));
            } else if let Some(decoded) = field.decode(&value) {
                patched.insert(field.name.clone(), decoded);
            }
        }
        patched
    }

    pub(crate) fn merged(&self, stored: Option<&Map<String, Value>>, patch: &Map<String, Value>) -> Option<Map<String, Value>> {
        let Some(stored) = stored else { return self.normalize_values(patch, FieldSpec::decode_stored).ok().filter(|row| row.get("id").is_some()) };
        let mut row = stored.clone();
        for (name, value) in patch {
            let rule = self.field(name).and_then(|field| field.merge.as_ref());
            let value = rule.map_or_else(|| value.clone(), |rule| rule.apply(stored.get(name), value.clone()));
            row.insert(name.clone(), value);
        }
        Some(row)
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
        let declared: HashSet<&str> = self.fields.iter().map(|field| field.name.as_str()).collect();
        for association in &self.associations {
            association.check_local(&indexed, &declared)?;
        }
        for relation in &self.relations {
            relation.check(self)?;
        }
        self.check_names(reserved_members)?;
        self.check_unions()?;
        self.check_actions(reserved_members)
    }

    fn check_unions(&self) -> Result<(), DeclarationError> {
        for field in self.fields.iter().filter(|field| matches!(field.kind, FieldKind::Union | FieldKind::UnionArray)) {
            let cases: Vec<&str> = field.fields.iter().map(|variant| variant.name.as_str()).collect();
            let typenames: Vec<&str> = field.fields.iter().filter_map(|variant| variant.app_type.as_deref()).collect();
            if !duplicates(&cases).is_empty() || !duplicates(&typenames).is_empty() {
                return Err(DeclarationError::DuplicateVariant(field.name.clone()));
            }
            for variant in &field.fields {
                let members: Vec<&str> = field.shared.iter().chain(variant.fields.iter()).map(|member| member.name.as_str()).collect();
                if let Some(name) = duplicates(&members).first() {
                    return Err(DeclarationError::DuplicateField(name.to_string()));
                }
            }
        }
        match crate::ddl::table_widths(self).into_iter().find(|(_, count)| *count > COLUMN_LIMIT) {
            Some((table, count)) => Err(DeclarationError::TooManyColumns { table, count }),
            None => Ok(()),
        }
    }

    fn check_fields(&self) -> Result<(), DeclarationError> {
        for field in &self.fields {
            field.check_json_parse()?;
            if field.name.is_empty() {
                return Err(DeclarationError::EmptyFieldName);
            }
            if is_reserved_name(&field.name) {
                return Err(DeclarationError::ReservedFieldName(field.name.clone()));
            }
            if field.merge.as_ref().is_some_and(|rule| !rule.fits(&field.kind)) {
                return Err(DeclarationError::MergeRuleMismatch(field.name.clone()));
            }
            if field.is_local() && (field.indexed || !(field.nullable || field.optional)) {
                return Err(DeclarationError::LocalNotOptional(field.name.clone()));
            }
            field.check_searchable()?;
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
        }
        for field in self.fields.iter().filter(|field| field.name != "id" || field.indexed) {
            if !columns.insert(column_name(&field.name)) {
                return Err(DeclarationError::ColumnCollision(field.name.clone()));
            }
        }
        for columns in &self.indexes {
            if columns.is_empty() {
                return Err(DeclarationError::EmptyIndex);
            }
            if let Some(name) = columns.iter().find(|name| self.field(name).is_none_or(|field| column_type(&field.kind).is_none())) {
                return Err(DeclarationError::InvalidIndexColumn(name.clone()));
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

fn check_links(field: &FieldSpec, siblings: &[&FieldSpec], by_key: &BTreeMap<&str, &ModelSpec>) -> Result<(), DeclarationError> {
    if let Some(link) = &field.link {
        if !by_key.contains_key(link.model.as_str()) {
            return Err(DeclarationError::UnknownFieldLink(field.name.clone()));
        }
        if !siblings.iter().any(|sibling| sibling.name == link.id && matches!(sibling.kind, FieldKind::Scalar { .. })) {
            return Err(DeclarationError::FieldLinkWithoutId(field.name.clone()));
        }
    }
    let inner: Vec<&FieldSpec> = field.shared.iter().chain(&field.fields).collect();
    inner.iter().try_for_each(|child| check_links(child, &inner, by_key))
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
                .and_then(|()| model.fields.iter().try_for_each(|field| check_links(field, &[], &by_key)))
                .and_then(|()| {
                    let missing = model.actions.iter().flat_map(|action| action.refetch.iter().filter_map(|name| name.split_once('.')).map(move |(target, relation)| (action, target, relation)));
                    missing
                        .into_iter()
                        .find(|(_, target, relation)| !by_key.get(target).is_some_and(|target| target.has_remote(relation)))
                        .map_or(Ok(()), |(action, target, relation)| Err(DeclarationError::UnknownRefetch { action: action.name.clone(), relation: format!("{target}.{relation}") }))
                })
                .and_then(|()| effect::check_effects(model, &by_key))
                .and_then(|()| {
                    let unknown = model.actions.iter().flat_map(|action| action.lands.iter().map(move |landing| (action, landing))).find(|(_, landing)| !by_key.contains_key(landing.model.as_str()));
                    unknown.map_or(Ok(()), |(action, landing)| Err(DeclarationError::UnknownLanding { action: action.name.clone(), model: landing.model.clone() }))
                })
        });
        if let Err(error) = result {
            faults.push(ModelFault { model: model.key.clone(), error });
        }
    }
    faults
}

#[cfg(test)]
mod effect_tests;
#[cfg(test)]
mod ephemeral_tests;
#[cfg(test)]
mod merge_tests;
#[cfg(test)]
mod path_tests;
#[cfg(test)]
mod newer_tests;
#[cfg(test)]
mod schema_tests;
#[cfg(test)]
mod stable_order_tests;
#[cfg(test)]
mod union_tests;

#[cfg(test)]
pub(crate) mod screens_fixture;

#[cfg(test)]
mod interface_tests;
