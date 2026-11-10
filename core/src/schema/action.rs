use super::*;

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
    /// Seconds a queued operation may wait before it is rolled back with `Rejected{Expired}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<u64>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default)]
    pub optimistic: Map<String, Value>,
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
    /// The `returns` block: an object of nullable fields read from the answer by the normalizer of standalone operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<FieldSpec>,
    /// The server works on the file after `Confirmed`: the field, its terminal values and the field of the progress (M37).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing: Option<ProcessingDeclaration>,
    /// Closed effect: the receiver row leaves every list before the answer and returns when the server refuses.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub removes_receiver: bool,
    /// Closed effect: a copy of the receiver row appears with `field` set to the argument `arg` before the answer, and the answer row replaces it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copies_receiver_to: Option<ReceiverCopy>,
    /// Closed effect: rows join or leave the member lists of a scope before the answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_effects: Vec<ScopeEffect>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReceiverCopy {
    pub arg: String,
    pub field: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub set: BTreeMap<String, CopyValue>,
}

/// The value a copy of the receiver gets in a field: a literal, or a field of the receiver or of a singleton row at the time of the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyValue {
    Literal(Value),
    Source(EffectSource),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeEffectKind {
    Join,
    Leave,
}

/// `rows` names an argument that holds the ids. `with` binds each parameter of the scope to `id` (the receiver) or to an argument. The parameter `order_param` takes each value of `orders`, so one effect changes every sort order of the list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeEffect {
    pub kind: ScopeEffectKind,
    pub model: String,
    pub scope: String,
    pub rows: String,
    pub with: BTreeMap<String, String>,
    pub order_param: String,
    pub orders: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingDeclaration {
    pub field: String,
    pub done: Vec<String>,
    #[serde(default)]
    pub progress: Option<String>,
}

impl ProcessingDeclaration {
    /// `Some(progress)` while the value of `field` in `data` is not terminal; `None` when it is terminal or absent.
    pub fn progress_in(&self, data: &Map<String, Value>) -> Option<Option<f64>> {
        let value = read_path(data, &self.field).and_then(Value::as_str)?;
        (!self.done.iter().any(|done| done == value)).then(|| self.progress.as_deref().and_then(|path| read_path(data, path)).and_then(Value::as_f64))
    }
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
            expires_after: None,
            variables: BTreeMap::new(),
            optimistic: Map::new(),
            refetch: Vec::new(),
            args: BTreeMap::new(),
            local: Vec::new(),
            local_from: BTreeMap::new(),
            correlate: Vec::new(),
            ignore_response: false,
            wrap: None,
            root: None,
            lands: Vec::new(),
            returns: None,
            processing: None,
            removes_receiver: false,
            copies_receiver_to: None,
            scope_effects: Vec::new(),
        }
    }

    pub(super) fn check(&self, model: &ModelSpec, taken_names: &HashSet<String>) -> Result<(), DeclarationError> {
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

