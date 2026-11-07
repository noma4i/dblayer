use super::*;

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

    #[cfg(test)]
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

    pub(super) fn zero(&self, now_ms: i64) -> Option<Value> {
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

    pub(super) fn decode_stored(&self, value: &Value) -> Option<Value> {
        if self.parse_json { read_raw(value) } else { self.decode(value) }
    }

    pub(super) fn check_json_parse(&self) -> Result<(), DeclarationError> {
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

    pub(super) fn check_local(&self, indexed: &HashSet<&str>, fields: &HashSet<&str>) -> Result<(), DeclarationError> {
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

    pub(super) fn check_target(&self, target: Option<&ModelSpec>) -> Result<(), DeclarationError> {
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

