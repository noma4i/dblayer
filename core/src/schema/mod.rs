use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::store::ddl::{column_name, column_type, SERVICE_COLUMNS};
use crate::identity::{ModelKey, ListKey};
use crate::value::{
    duplicates, read_array, read_enum, read_raw, read_scalar, relation_scope_key, ScalarKind, ListKeyError,
    SortDirection,
};

mod action;
mod effect;
mod error;
mod field;
mod relation;
mod ephemeral;
pub(crate) mod keyed;
mod local;
mod merge;
mod nested_decode;
mod newer;
mod reserved;
mod search;
mod specs;
mod types;

pub use action::*;
pub use error::*;
pub use field::*;
pub use relation::*;
pub use effect::{EffectSource, SelectionKeyError};
pub use ephemeral::{EphemeralSpec, Storage};
pub use merge::MergeRule;
pub use reserved::{generated_member_names, reserved_member_names, ReservedMember, LAYOUT, MODEL_NAME, RESERVED_MEMBERS, SINGLETON, SYNC, UPLOADS, WIRE_LAYOUT};
pub use search::{search_text, SearchPath};
pub use specs::{OperationSpec, Specs};
pub use types::{resolve_types, ValueFault};

pub const TYPENAME: &str = "__typename";
pub const COLUMN_LIMIT: usize = 2000;

const FORBIDDEN_ACTION_MEMBERS: [&str; 6] = ["after", "invalidate", "onCommit", "extract", "complete", "resume"];

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
    /// The steps of file uploads declared once per project (`uploads do`): the generator copies them to every model with an upload action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_steps: Option<crate::owners::write::upload::UploadOperations>,
    /// A row of the named model that lands removes the rows of this model whose fields equal its own (M40).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_by: Vec<RemovedBy>,
}

/// `removed_by Model, match: { field: :model_field }`: `matches` maps a field of this model to a field of `model`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovedBy {
    pub model: String,
    pub matches: BTreeMap<String, String>,
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
            upload_steps: None,
            removed_by: Vec::new(),
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
            .filter(|relation| relation.lists_every_row_of_its_params())
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
        match crate::store::ddl::table_widths(self).into_iter().find(|(_, count)| *count > COLUMN_LIMIT) {
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
                .and_then(|()| {
                    let unknown = model.removed_by.iter().flat_map(|removal| removal.matches.iter().map(move |(own, theirs)| (removal, own, theirs))).find(|(removal, own, theirs)| {
                        !model.fields.iter().any(|field| &field.name == *own) || !by_key.get(removal.model.as_str()).is_some_and(|source| source.fields.iter().any(|field| &field.name == *theirs))
                    });
                    unknown.map_or(Ok(()), |(removal, own, theirs)| Err(DeclarationError::UnknownRemovedBy { model: removal.model.clone(), field: if model.fields.iter().any(|field| &field.name == own) { theirs.clone() } else { own.clone() } }))
                })
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
mod tests;

#[cfg(test)]
pub(crate) mod screens_fixture;

