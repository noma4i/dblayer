use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ddl::{column_name, column_type, SERVICE_COLUMNS};
use crate::identity::{ModelKey, ScopeKey};
use crate::value::{
    duplicates, read_array, read_enum, read_raw, read_scalar, relation_scope_key, ScalarKind, ScopeKeyError,
    SortDirection,
};

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
    #[error("association {0} declares a counter cache without being belongs_to")]
    CounterCacheWithoutOwner(String),
    #[error("association {0} targets an unknown model")]
    UnknownTarget(String),
    #[error("counter field of association {0} is not numeric")]
    CounterFieldNotNumeric(String),
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
    #[error("durable action {0} must not destroy the root")]
    DurableRootNotInsert(String),
    #[error("action declares forbidden member {0}")]
    ForbiddenMember(String),
    #[error("once action {0} needs a dedupe key")]
    OnceKeyRequired(String),
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
            FieldKind::List { element } => {
                read_array(value, |item| read_scalar(item, *element)).map(Value::Array)
            }
        }
    }

    fn is_numeric(&self) -> bool {
        matches!(self.kind, FieldKind::Scalar { kind: ScalarKind::Int | ScalarKind::Num })
    }
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
    pub counter_cache_field: Option<String>,
    #[serde(default)]
    pub touches_parent: bool,
}

impl AssociationSpec {
    pub fn new(name: &str, kind: AssociationKind, target: &str, foreign_key: &str) -> Self {
        Self {
            name: name.to_string(),
            kind,
            target: ModelKey::new(target),
            foreign_key: foreign_key.to_string(),
            dependent: None,
            counter_cache_field: None,
            touches_parent: false,
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
        if self.counter_cache_field.is_some() && self.kind != AssociationKind::BelongsTo {
            return Err(DeclarationError::CounterCacheWithoutOwner(name()));
        }
        Ok(())
    }

    fn check_target(&self, target: Option<&ModelSpec>) -> Result<(), DeclarationError> {
        let target = target.ok_or_else(|| DeclarationError::UnknownTarget(self.name.clone()))?;
        if self.owns_children() && !target.is_indexed(&self.foreign_key) {
            return Err(DeclarationError::ForeignKeyNotIndexed(self.name.clone()));
        }
        match &self.counter_cache_field {
            Some(field) if !field.is_empty() && !target.field(field).is_some_and(FieldSpec::is_numeric) => {
                Err(DeclarationError::CounterFieldNotNumeric(self.name.clone()))
            }
            _ => Ok(()),
        }
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShape {
    Single,
    List { coverage: Coverage },
    Connection { direction: PageDirection, coverage: Coverage, max_pages: Option<u32> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteFault {
    SingleHasPaging,
    ConnectionWithoutPageInfo,
    MaxPagesNotPositive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSpec {
    pub shape: RemoteShape,
    pub stale_time: String,
    #[serde(default)]
    pub empty_stale_time: Option<String>,
    #[serde(default)]
    pub selects_page_info: bool,
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
            if model.field(column).is_none() {
                return Err(DeclarationError::UnknownByColumn { relation: relation(), column: column.clone() });
            }
            if !model.is_indexed(column) {
                return Err(DeclarationError::IndexMissing { relation: relation(), column: column.clone() });
            }
        }
        if let Sort::Fields(fields) = &self.sort {
            if let Some(field) = fields.iter().find(|field| model.field(&field.column).is_none()) {
                return Err(DeclarationError::UnknownSortColumn { relation: relation(), column: field.column.clone() });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionMode {
    Request,
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootShape {
    Upsert,
    Destroy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionDeclaration {
    pub name: String,
    pub mode: ActionMode,
    pub root_shape: RootShape,
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
}

impl ActionDeclaration {
    pub fn new(name: &str, mode: ActionMode, root_shape: RootShape) -> Self {
        Self {
            name: name.to_string(),
            mode,
            root_shape,
            has_optimistic: false,
            has_temp_row_retention: false,
            has_cascade_dependents: false,
            has_once: false,
            dedupe_key: None,
            members: Vec::new(),
        }
    }

    fn check(&self, taken_names: &HashSet<String>) -> Result<(), DeclarationError> {
        let name = || self.name.clone();
        if taken_names.contains(&self.name) {
            return Err(DeclarationError::ActionNameCollision(name()));
        }
        if self.mode == ActionMode::Durable && self.root_shape == RootShape::Destroy {
            return Err(DeclarationError::DurableRootNotInsert(name()));
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
        if self.has_once && self.dedupe_key.as_deref().is_none_or(str::is_empty) {
            return Err(DeclarationError::OnceKeyRequired(name()));
        }
        Ok(())
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
}

fn is_reserved_name(name: &str) -> bool {
    name == "orderKey" || name.starts_with('$')
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
        }
    }

    pub fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn is_indexed(&self, name: &str) -> bool {
        self.field(name).is_some_and(|field| field.indexed)
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
            action.check(&taken)?;
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
mod tests {
    use super::*;
    use serde_json::json;

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    fn id_field() -> FieldSpec {
        FieldSpec::raw("id")
    }

    fn model(fields: Vec<FieldSpec>) -> ModelSpec {
        ModelSpec::new("messages", fields)
    }

    fn paged_remote(selects_page_info: bool) -> RemoteSpec {
        RemoteSpec {
            shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None },
            stale_time: "paged_session".into(),
            empty_stale_time: None,
            selects_page_info,
        }
    }

    fn complete_remote() -> RemoteSpec {
        RemoteSpec {
            shape: RemoteShape::List { coverage: Coverage::Complete },
            stale_time: "catalog".into(),
            empty_stale_time: Some("empty_retry".into()),
            selects_page_info: false,
        }
    }

    fn association(name: &str, kind: AssociationKind, target: &str, foreign_key: &str) -> AssociationSpec {
        AssociationSpec::new(name, kind, target, foreign_key)
    }

    #[test]
    fn normalize_reads_rejects_defaults_and_nulls() {
        let plain = model(vec![id_field()]);
        let defaulted = model(vec![id_field().with_default(json!("x"))]);
        let nullable = model(vec![id_field().nullable()]);
        let undecodable = model(vec![FieldSpec::object("id").nullable()]);
        let optional = model(vec![id_field().optional()]);
        let optional_nullable = model(vec![id_field().optional().nullable()]);

        assert_eq!(plain.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": "7"})));
        assert_eq!(plain.normalize(&Map::new()), Err(SchemaError::MissingField("id".into())));
        assert_eq!(defaulted.normalize(&Map::new()).unwrap(), object(json!({"id": "x"})));
        assert_eq!(nullable.normalize(&Map::new()).unwrap(), object(json!({"id": null})));
        assert_eq!(undecodable.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": null})));
        assert_eq!(optional.normalize(&Map::new()).unwrap(), Map::new());
        assert_eq!(optional.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": "7"})));
        assert_eq!(optional_nullable.normalize(&Map::new()).unwrap(), Map::new());
        assert_eq!(optional_nullable.normalize(&object(json!({"id": null}))).unwrap(), object(json!({"id": null})));
    }

    #[test]
    fn patch_keeps_present_values_and_indexed_names() {
        let kind = FieldSpec::raw("kind").optional().indexed();
        let spec = model(vec![id_field(), kind]);

        let value_patch = spec.patch(&object(json!({"id": "7"})));
        let null_patch = spec.patch(&object(json!({"id": null})));

        assert_eq!(value_patch.len(), 1);
        assert_eq!(value_patch["id"], json!("7"));
        assert_eq!(null_patch, object(json!({"id": null})));
        assert_eq!(spec.indexed_names(), vec!["kind".to_string()]);
    }

    #[test]
    fn scalar_fields_decode_or_reject_input() {
        let count = FieldSpec::scalar("count", ScalarKind::Int);
        let created_at = FieldSpec::scalar("createdAt", ScalarKind::Date);
        let chat_id = FieldSpec::scalar("chatId", ScalarKind::Id);

        assert_eq!(count.decode(&json!("42")), Some(json!(42)));
        assert_eq!(count.decode(&json!("abc")), None);
        assert_eq!(chat_id.decode(&json!("")), None);
        let date = created_at.decode(&json!(1_700_000_000_000_i64)).unwrap();
        assert!(date.as_str().unwrap().starts_with("2023-"));
    }

    #[test]
    fn list_object_array_and_enum_fields_keep_only_valid_shapes() {
        let list = FieldSpec::list("counts", ScalarKind::Int);
        let record = FieldSpec::object("metadata");
        let array = FieldSpec::array("values");
        let media = FieldSpec::enumeration("media", &["image", "video"]);
        let object_value = json!({"enabled": true});
        let array_value = json!(["first", 2]);

        assert_eq!(list.decode(&json!([1, "x", "3"])), Some(json!([1, 3])));
        assert_eq!(record.decode(&object_value), Some(object_value.clone()));
        assert_eq!(record.decode(&json!("metadata")), None);
        assert_eq!(array.decode(&array_value), Some(array_value.clone()));
        assert_eq!(array.decode(&json!(2)), None);
        assert_eq!(media.decode(&json!("image")), Some(json!("image")));
        assert_eq!(media.decode(&json!("audio")), None);
        assert_eq!(FieldSpec::raw("any").decode(&Value::Null), None);
    }

    #[test]
    fn parent_id_field_reads_direct_value_then_parent_id() {
        let field = FieldSpec::parent_id("authorId", "author");
        let parent = json!({"id": "parent-id"});

        assert_eq!(field.read(&object(json!({"authorId": "direct-id", "author": parent.clone()}))), Some(json!("direct-id")));
        assert_eq!(field.read(&object(json!({"author": parent}))), Some(json!("parent-id")));
        assert_eq!(field.read(&object(json!({"other": "value"}))), None);
        assert_eq!(field.decode(&json!("7")), Some(json!("7")));
    }

    #[test]
    fn normalize_all_keeps_valid_rows_and_reports_dropped() {
        let spec = model(vec![FieldSpec::scalar("id", ScalarKind::Str)]);
        let inputs = vec![object(json!({"id": "1"})), Map::new(), object(json!({"id": "3"}))];

        let (rows, dropped) = spec.normalize_all(&inputs);
        let (none, none_dropped) = spec.normalize_all(&[]);

        assert_eq!(rows, vec![object(json!({"id": "1"})), object(json!({"id": "3"}))]);
        assert_eq!(dropped, vec![(1, SchemaError::MissingField("id".into()))]);
        assert!(none.is_empty());
        assert!(none_dropped.is_empty());
    }

    #[test]
    fn valid_model_passes() {
        assert_eq!(model(vec![id_field()]).validate(&[]), Ok(()));
    }

    #[test]
    fn model_reports_invalid_key_fields_and_names() {
        let empty_key = ModelSpec::new("", vec![id_field()]);
        let no_fields = model(vec![]);
        let duplicate = model(vec![id_field(), id_field()]);
        let reserved = model(vec![FieldSpec::scalar("orderKey", ScalarKind::Str)]);
        let dollar = model(vec![FieldSpec::scalar("$meta", ScalarKind::Str)]);
        let empty_name = model(vec![FieldSpec::scalar("", ScalarKind::Str)]);

        assert_eq!(empty_key.validate(&[]), Err(DeclarationError::EmptyModelKey));
        assert_eq!(no_fields.validate(&[]), Err(DeclarationError::NoFields));
        assert_eq!(duplicate.validate(&[]), Err(DeclarationError::DuplicateField("id".into())));
        assert_eq!(reserved.validate(&[]), Err(DeclarationError::ReservedFieldName("orderKey".into())));
        assert_eq!(dollar.validate(&[]), Err(DeclarationError::ReservedFieldName("$meta".into())));
        assert_eq!(empty_name.validate(&[]), Err(DeclarationError::EmptyFieldName));
    }

    #[test]
    fn indexed_fields_must_be_scalar_and_map_to_distinct_columns() {
        let object_index = model(vec![FieldSpec::object("media").indexed()]);
        let service_column = model(vec![FieldSpec::scalar("payload", ScalarKind::Str).indexed()]);
        let id_column = model(vec![FieldSpec::scalar("id", ScalarKind::Id).indexed()]);
        let same_column = model(vec![
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("chat_id", ScalarKind::Id).indexed(),
        ]);
        let fine = model(vec![FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::enumeration("kind", &["a"]).indexed()]);

        assert_eq!(object_index.validate(&[]), Err(DeclarationError::IndexedFieldNotScalar("media".into())));
        assert_eq!(service_column.validate(&[]), Err(DeclarationError::ColumnCollision("payload".into())));
        assert_eq!(id_column.validate(&[]), Err(DeclarationError::ColumnCollision("id".into())));
        assert_eq!(same_column.validate(&[]), Err(DeclarationError::ColumnCollision("chat_id".into())));
        assert_eq!(fine.validate(&[]), Ok(()));
    }

    #[test]
    fn composite_indexes_need_two_indexed_fields() {
        let fields = vec![
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str),
        ];
        let with = |composite: Vec<&str>| ModelSpec {
            composite_indexes: vec![composite.into_iter().map(String::from).collect()],
            ..model(fields.clone())
        };

        assert_eq!(with(vec!["chatId", "sequenceNumber"]).validate(&[]), Ok(()));
        assert_eq!(with(vec!["chatId"]).validate(&[]), Err(DeclarationError::CompositeIndexTooShort));
        assert_eq!(with(vec!["chatId", "ghost"]).validate(&[]), Err(DeclarationError::CompositeIndexFieldNotIndexed("ghost".into())));
        assert_eq!(with(vec!["chatId", "body"]).validate(&[]), Err(DeclarationError::CompositeIndexFieldNotIndexed("body".into())));
    }

    #[test]
    fn model_accepts_indexed_association_and_reports_missing_index() {
        let fields = vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()];
        let accepted = ModelSpec {
            associations: vec![association("chat", AssociationKind::BelongsTo, "chats", "chatId")],
            ..model(fields.clone())
        };
        let rejected = ModelSpec {
            associations: vec![association("author", AssociationKind::BelongsTo, "chats", "authorId")],
            ..model(fields)
        };

        assert_eq!(accepted.indexed_names(), vec!["chatId".to_string()]);
        assert_eq!(accepted.validate(&[]), Ok(()));
        assert_eq!(rejected.validate(&[]), Err(DeclarationError::ForeignKeyNotIndexed("author".into())));
    }

    #[test]
    fn association_kinds_choose_ownership_and_read_plan() {
        let belongs_to = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
        let has_many = association("messages", AssociationKind::HasMany, "chats", "chatId");
        let has_one = association("last", AssociationKind::HasOne, "chats", "chatId");
        let references = association("chats", AssociationKind::References, "chats", "");

        assert!(!belongs_to.owns_children());
        assert!(belongs_to.requires_foreign_key());
        assert_eq!(belongs_to.read_plan(), ReadPlan::Find);
        assert!(has_many.owns_children());
        assert_eq!(has_many.read_plan(), ReadPlan::Where);
        assert_eq!(has_one.read_plan(), ReadPlan::Find);
        assert!(!references.requires_foreign_key());
        assert_eq!(references.read_plan(), ReadPlan::ByIds);

        let host = ModelSpec { associations: vec![references], ..model(vec![id_field()]) };
        assert_eq!(host.validate(&[]), Ok(()));
    }

    #[test]
    fn association_faults_for_foreign_key_dependent_and_counter_cache() {
        let host = |association: AssociationSpec| ModelSpec {
            associations: vec![association],
            ..model(vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()])
        };
        let with_dependent = |kind| AssociationSpec { dependent: Some(Dependent::Destroy), ..association("a", kind, "chats", "chatId") };
        let with_counter = |kind| AssociationSpec { counter_cache_field: Some("count".into()), ..association("a", kind, "chats", "chatId") };

        assert_eq!(
            host(association("a", AssociationKind::HasMany, "chats", "")).validate(&[]),
            Err(DeclarationError::EmptyForeignKey("a".into()))
        );
        assert_eq!(
            host(with_dependent(AssociationKind::BelongsTo)).validate(&[]),
            Err(DeclarationError::DependentOnNonOwner("a".into()))
        );
        assert_eq!(host(with_dependent(AssociationKind::HasMany)).validate(&[]), Ok(()));
        assert_eq!(
            host(with_counter(AssociationKind::HasOne)).validate(&[]),
            Err(DeclarationError::CounterCacheWithoutOwner("a".into()))
        );
        assert_eq!(host(with_counter(AssociationKind::BelongsTo)).validate(&[]), Ok(()));
    }

    #[test]
    fn cross_model_checks_report_unknown_target_foreign_key_and_counter_field() {
        let chats = ModelSpec::new("chats", vec![id_field(), FieldSpec::scalar("unreadCount", ScalarKind::Int)]);
        let messages = |association: AssociationSpec| ModelSpec {
            associations: vec![association],
            ..ModelSpec::new("messages", vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()])
        };
        let plain = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
        let unknown = association("chat", AssociationKind::BelongsTo, "ghosts", "chatId");
        let counter = AssociationSpec { counter_cache_field: Some("unreadCount".into()), ..plain.clone() };
        let bad_counter = AssociationSpec { counter_cache_field: Some("title".into()), ..plain.clone() };
        let owner = association("messages", AssociationKind::HasMany, "messages", "chatId");
        let bad_owner = association("messages", AssociationKind::HasMany, "messages", "authorId");
        let owner_host = |association: AssociationSpec| ModelSpec { associations: vec![association], ..chats.clone() };

        assert!(validate_models(&[chats.clone(), messages(plain)], &[]).is_empty());
        assert_eq!(
            validate_models(&[chats.clone(), messages(unknown)], &[]),
            vec![ModelFault { model: ModelKey::new("messages"), error: DeclarationError::UnknownTarget("chat".into()) }]
        );
        assert!(validate_models(&[chats.clone(), messages(counter)], &[]).is_empty());
        assert_eq!(
            validate_models(&[chats.clone(), messages(bad_counter)], &[])[0].error,
            DeclarationError::CounterFieldNotNumeric("chat".into())
        );
        let messages_plain = messages(association("chat", AssociationKind::BelongsTo, "chats", "chatId"));
        assert!(validate_models(&[owner_host(owner), messages_plain.clone()], &[]).is_empty());
        assert_eq!(
            validate_models(&[owner_host(bad_owner), messages_plain], &[])[0].error,
            DeclarationError::ForeignKeyNotIndexed("messages".into())
        );
    }

    #[test]
    fn local_and_global_relations_are_local_without_remote() {
        let local = RelationSpec::new("thread", &["chatId"]);
        let global = RelationSpec::global("catalog");
        let non_global_empty = RelationSpec::new("catalog", &[]);

        assert!(local.is_local());
        assert!(!local.has_more());
        assert!(global.by_columns.is_empty());
        assert!(global.is_global);
        assert!(global.is_local());
        assert!(!non_global_empty.is_global);
        let key = local.scope_key("messages", &[("chatId".to_string(), "7".to_string())].into_iter().collect()).unwrap();
        assert!(key.as_str().contains("messages"));
        assert!(key.as_str().contains("thread"));
    }

    #[test]
    fn remote_relation_follows_paging() {
        let paged = RelationSpec { remote: Some(paged_remote(true)), ..RelationSpec::new("thread", &["chatId"]) };
        let global = RelationSpec { remote: Some(complete_remote()), ..RelationSpec::global("catalog") };

        assert!(!paged.is_local());
        assert!(paged.has_more());
        assert!(!global.is_local());
        assert!(!global.has_more());
        assert_eq!(paged.remote.as_ref().unwrap().coverage(), Coverage::Page);
        assert_eq!(global.remote.as_ref().unwrap().coverage(), Coverage::Complete);
        assert_eq!(RemoteSpec { shape: RemoteShape::Single, ..complete_remote() }.coverage(), Coverage::Complete);
    }

    fn relation_host(relation: RelationSpec) -> ModelSpec {
        ModelSpec {
            relations: vec![relation],
            ..ModelSpec::new(
                "messages",
                vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::raw("body")],
            )
        }
    }

    #[test]
    fn relation_name_by_columns_and_remote_faults_match_spec() {
        assert_eq!(relation_host(RelationSpec::new("thread", &["chatId"])).validate(&[]), Ok(()));
        assert_eq!(relation_host(RelationSpec::global("catalog")).validate(&[]), Ok(()));
        assert_eq!(
            relation_host(RelationSpec::new("", &["chatId"])).validate(&[]),
            Err(DeclarationError::EmptyRelationName)
        );
        assert_eq!(
            relation_host(RelationSpec::new("$id", &["chatId"])).validate(&[]),
            Err(DeclarationError::ReservedRelationName("$id".into()))
        );
        assert_eq!(
            relation_host(RelationSpec::new("orderKey", &["chatId"])).validate(&[]),
            Err(DeclarationError::ReservedRelationName("orderKey".into()))
        );
        assert_eq!(
            relation_host(RelationSpec::new("thread", &[])).validate(&[]),
            Err(DeclarationError::EmptyByColumns("thread".into()))
        );
        let broken = RelationSpec { remote: Some(paged_remote(false)), ..RelationSpec::new("thread", &["chatId"]) };
        assert_eq!(
            relation_host(broken).validate(&[]),
            Err(DeclarationError::Remote { relation: "thread".into(), fault: RemoteFault::ConnectionWithoutPageInfo })
        );
        let single = RelationSpec {
            remote: Some(RemoteSpec { shape: RemoteShape::Single, selects_page_info: true, ..complete_remote() }),
            ..RelationSpec::global("one")
        };
        assert_eq!(
            relation_host(single).validate(&[]),
            Err(DeclarationError::Remote { relation: "one".into(), fault: RemoteFault::SingleHasPaging })
        );
        let zero_pages = RelationSpec {
            remote: Some(RemoteSpec {
                shape: RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: Some(0) },
                ..paged_remote(true)
            }),
            ..RelationSpec::new("thread", &["chatId"])
        };
        assert_eq!(
            relation_host(zero_pages).validate(&[]),
            Err(DeclarationError::Remote { relation: "thread".into(), fault: RemoteFault::MaxPagesNotPositive })
        );
    }

    #[test]
    fn relation_columns_must_exist_and_be_indexed() {
        assert_eq!(relation_host(RelationSpec::new("thread", &["chatId"])).validate(&[]), Ok(()));
        assert_eq!(
            relation_host(RelationSpec::new("thread", &["body"])).validate(&[]),
            Err(DeclarationError::IndexMissing { relation: "thread".into(), column: "body".into() })
        );
        assert_eq!(
            relation_host(RelationSpec::new("thread", &["ghost"])).validate(&[]),
            Err(DeclarationError::UnknownByColumn { relation: "thread".into(), column: "ghost".into() })
        );
        let sorted = RelationSpec {
            sort: Sort::Fields(vec![SortField { column: "ghost".into(), direction: SortDirection::Desc }]),
            ..RelationSpec::new("thread", &["chatId"])
        };
        assert_eq!(
            relation_host(sorted).validate(&[]),
            Err(DeclarationError::UnknownSortColumn { relation: "thread".into(), column: "ghost".into() })
        );
    }

    #[test]
    fn relation_names_must_be_unique_and_not_collide() {
        let two = |first: &str, second: &str| ModelSpec {
            relations: vec![RelationSpec::new(first, &["chatId"]), RelationSpec::new(second, &["chatId"])],
            ..relation_host(RelationSpec::new("x", &["chatId"]))
        };
        assert_eq!(two("thread", "thread").validate(&[]), Err(DeclarationError::DuplicateRelation("thread".into())));
        assert_eq!(two("thread", "other").validate(&[]), Ok(()));

        let colliding = ModelSpec {
            associations: vec![association("chat", AssociationKind::References, "chats", "")],
            ..relation_host(RelationSpec::new("chat", &["chatId"]))
        };
        assert_eq!(colliding.validate(&[]), Err(DeclarationError::AssociationNameCollision("chat".into())));

        assert_eq!(
            relation_host(RelationSpec::new("find", &["chatId"])).validate(&["find"]),
            Err(DeclarationError::NameCollision("find".into()))
        );
        let member_clash = ModelSpec {
            associations: vec![association("find", AssociationKind::References, "chats", "")],
            ..relation_host(RelationSpec::new("thread", &["chatId"]))
        };
        assert_eq!(member_clash.validate(&["find"]), Err(DeclarationError::NameCollision("find".into())));
        let clean = ModelSpec {
            associations: vec![association("author", AssociationKind::References, "chats", "")],
            ..relation_host(RelationSpec::new("thread", &["chatId"]))
        };
        assert_eq!(clean.validate(&["find"]), Ok(()));
    }

    fn action(name: &str, mode: ActionMode, root_shape: RootShape) -> ActionDeclaration {
        ActionDeclaration::new(name, mode, root_shape)
    }

    fn with_actions(actions: Vec<ActionDeclaration>) -> ModelSpec {
        ModelSpec { actions, ..model(vec![id_field()]) }
    }

    #[test]
    fn action_rules_reject_invalid_combinations() {
        let collision = action("send", ActionMode::Durable, RootShape::Upsert);
        let durable_destroy = action("remove", ActionMode::Durable, RootShape::Destroy);
        let durable_upsert = action("save", ActionMode::Durable, RootShape::Upsert);
        let optimistic = ActionDeclaration { has_optimistic: true, ..action("create", ActionMode::Request, RootShape::Upsert) };
        let retained = ActionDeclaration { has_temp_row_retention: true, ..optimistic.clone() };
        let cascade = ActionDeclaration {
            has_optimistic: true,
            has_cascade_dependents: true,
            ..action("remove", ActionMode::Request, RootShape::Destroy)
        };
        let no_cascade = ActionDeclaration { has_cascade_dependents: false, ..cascade.clone() };
        let valid = action("save", ActionMode::Request, RootShape::Upsert);

        assert_eq!(with_actions(vec![collision]).validate(&["send"]), Err(DeclarationError::ActionNameCollision("send".into())));
        assert_eq!(with_actions(vec![durable_destroy]).validate(&[]), Err(DeclarationError::DurableRootNotInsert("remove".into())));
        assert_eq!(with_actions(vec![durable_upsert]).validate(&[]), Ok(()));
        assert_eq!(with_actions(vec![optimistic]).validate(&[]), Err(DeclarationError::MissingTempRowRetention("create".into())));
        assert_eq!(with_actions(vec![retained]).validate(&[]), Ok(()));
        assert_eq!(with_actions(vec![cascade]).validate(&[]), Err(DeclarationError::OptimisticDestroyWithCascade("remove".into())));
        assert_eq!(with_actions(vec![no_cascade]).validate(&[]), Ok(()));
        assert_eq!(with_actions(vec![valid.clone()]).validate(&[]), Ok(()));
        assert_eq!(
            with_actions(vec![valid.clone(), valid]).validate(&[]),
            Err(DeclarationError::ActionNameCollision("save".into()))
        );
    }

    #[test]
    fn forbidden_members_and_once_key_are_checked() {
        let with_member = |member: &str| ActionDeclaration {
            members: vec![member.to_string()],
            ..action("save", ActionMode::Request, RootShape::Upsert)
        };
        let once = |dedupe_key: Option<&str>| ActionDeclaration {
            has_once: true,
            dedupe_key: dedupe_key.map(String::from),
            ..action("save", ActionMode::Request, RootShape::Upsert)
        };

        assert_eq!(with_actions(vec![with_member("after")]).validate(&[]), Err(DeclarationError::ForbiddenMember("after".into())));
        assert_eq!(with_actions(vec![with_member("resume")]).validate(&[]), Err(DeclarationError::ForbiddenMember("resume".into())));
        assert_eq!(with_actions(vec![with_member("commit")]).validate(&[]), Ok(()));
        assert_eq!(with_actions(vec![once(None)]).validate(&[]), Err(DeclarationError::OnceKeyRequired("save".into())));
        assert_eq!(with_actions(vec![once(Some(""))]).validate(&[]), Err(DeclarationError::OnceKeyRequired("save".into())));
        assert_eq!(with_actions(vec![once(Some("key"))]).validate(&[]), Ok(()));
    }

    #[test]
    fn model_spec_round_trips_through_json() {
        let spec = ModelSpec {
            associations: vec![association("chat", AssociationKind::BelongsTo, "chats", "chatId")],
            relations: vec![RelationSpec { remote: Some(paged_remote(true)), ..RelationSpec::new("thread", &["chatId"]) }],
            composite_indexes: vec![vec!["chatId".into(), "sequenceNumber".into()]],
            actions: vec![action("send", ActionMode::Durable, RootShape::Upsert)],
            ..model(vec![
                FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
                FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().indexed(),
                FieldSpec::list("tags", ScalarKind::Str).with_default(json!([])),
            ])
        };

        let text = serde_json::to_string(&spec).unwrap();
        let decoded: ModelSpec = serde_json::from_str(&text).unwrap();

        assert_eq!(decoded, spec);
        let minimal: ModelSpec = serde_json::from_value(json!({
            "key": "m",
            "fields": [{"name": "id", "kind": {"type": "raw"}}]
        }))
        .unwrap();
        assert_eq!(minimal, ModelSpec::new("m", vec![FieldSpec::raw("id")]));
    }
}
