use super::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("missing field {0}")]
    MissingField(String),
    #[error("value of field {0} does not fit its type")]
    InvalidValue(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclarationError {
    #[error("invalid related row removal: {0}")]
    InvalidRelatedRowRemoval(String),
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
    #[error("association {0} sets a field of its parent that is not declared derived: true")]
    SetsTargetNotDerived(String),
    #[error("action {action} lands the unknown model {model}")]
    UnknownLanding { action: String, model: String },
    #[error("removed_by {model} matches the field {field} that is not declared")]
    UnknownRemovedBy { model: String, field: String },
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

