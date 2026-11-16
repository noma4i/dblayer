
use dblayer::schema::{FieldSpec, MergeRule, NodeRead, OnFailure, Storage};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::Value as Json;

use crate::parser::Call;

mod lower;
mod params;

pub(crate) use lower::{lower_calls, lower_models, lower_uploads};
pub(crate) use params::{ParamBase, ParamType};
#[cfg(test)]
pub(crate) use params::PARAM_TYPES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pos {
    pub line: usize,
    pub column: usize,
}

impl Pos {
    pub fn of(call: &Call) -> Self {
        Self { line: call.line, column: call.column }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Spanned<T> {
    pub item: T,
    pub pos: Pos,
}

impl<T> Spanned<T> {
    pub fn new(item: T, pos: Pos) -> Self {
        Self { item, pos }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListElement {
    Scalar(ScalarKind),
    Enum,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeRef {
    Scalar(ScalarKind),
    Enum,
    Json,
    File,
    Named(String),
    List(ListElement),
    NamedList(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttributeDecl {
    pub pos: Pos,
    pub name: String,
    pub ty: TypeRef,
    pub null: bool,
    pub optional: bool,
    pub default: Option<Spanned<Json>>,
    pub id_from: Option<String>,
    pub link: Option<(String, String)>,
    pub from: Option<Spanned<String>>,
    pub merge: Option<Spanned<MergeRule>>,
    pub local: bool,
    pub derived: bool,
    pub values: Option<Vec<String>>,
    pub as_name: Option<String>,
    pub parse_json: bool,
    pub reset_after: Option<String>,
    pub block: AttributeBlock,
}

impl AttributeDecl {
    pub fn is_union(&self) -> bool {
        matches!(self.block, AttributeBlock::Union { .. })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AttributeBlock {
    None,
    Fields(Vec<AttributeDecl>),
    Union { shared: Option<Spanned<Vec<AttributeDecl>>>, variants: Vec<VariantDecl> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariantDecl {
    pub pos: Pos,
    pub type_name: String,
    pub case: Option<String>,
    pub fields: Option<Vec<AttributeDecl>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssociationKind {
    BelongsTo,
    HasMany,
    HasOne,
    References,
}

impl AssociationKind {
    pub fn word(self) -> &'static str {
        match self {
            Self::BelongsTo => "belongs_to",
            Self::HasMany => "has_many",
            Self::HasOne => "has_one",
            Self::References => "references",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssociationDecl {
    pub pos: Pos,
    pub kind: AssociationKind,
    pub name: String,
    pub class_name: Option<String>,
    pub foreign_key: Option<String>,
    pub touch: bool,
    pub sets: Vec<(String, String)>,
    pub newer: Vec<String>,
    pub newer_pos: Option<Pos>,
    pub from: Option<String>,
    pub dependent: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexDecl {
    pub pos: Pos,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParameterDecl {
    pub pos: Pos,
    pub name: String,
    pub ty: ParamType,
    pub null: bool,
    pub default: Option<Spanned<Json>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariableValue {
    Parameter(String),
    Constant(Json),
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariableBinding {
    pub pos: Pos,
    pub name: String,
    pub value: VariableValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOn {
    Server,
    Create,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseShape {
    One,
    List,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseRead {
    pub pos: Pos,
    pub from: Option<Spanned<String>>,
    pub shape: Option<ResponseShape>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageDirectionDecl {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorType {
    String,
    Int,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CursorDecl {
    pub parameter: String,
    pub from: Spanned<String>,
    pub ty: CursorType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PaginateDecl {
    pub pos: Pos,
    pub size: u32,
    pub direction: PageDirectionDecl,
    pub max_pages: Option<Spanned<u32>>,
    pub cursor: Option<CursorDecl>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FreshnessDecl {
    pub pos: Pos,
    pub expires_in: Spanned<String>,
    pub empty_in: Option<Spanned<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreloadDecl {
    pub pos: Pos,
    pub limit: Option<usize>,
    pub values: Vec<(Spanned<String>, PreloadChoice)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PreloadChoice {
    Listed(Vec<Spanned<Json>>),
    RowsOf(Spanned<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereDecl {
    pub pos: Pos,
    pub pairs: Vec<(Spanned<String>, Spanned<String>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderDecl {
    pub pos: Pos,
    pub fields: Vec<(String, SortDirection, Pos)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryDecl {
    pub pos: Pos,
    pub operation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Upsert,
    Delete,
    Ephemeral,
    Ignore,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplyDecl {
    pub pos: Pos,
    pub effect: Effect,
    pub fields: Vec<(String, String, Pos)>,
    pub values: Vec<(String, Json, Pos)>,
    pub matches: Vec<(String, dblayer::schema::MatchValues, Pos)>,
    pub delete_if: Option<(Spanned<String>, bool)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeDecl {
    pub pos: Pos,
    pub name: String,
    pub from: Option<Spanned<String>>,
    pub id: Vec<Spanned<String>>,
    pub apply: ApplyDecl,
    pub debounce: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarkerDecl {
    pub pos: Pos,
    pub operation: String,
    pub from: Spanned<String>,
    pub fields: Vec<Spanned<String>>,
    pub sequence: Spanned<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailDecl {
    pub pos: Pos,
    pub operation: String,
    pub after: Spanned<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeDecl {
    pub marker: Option<MarkerDecl>,
    pub tail: Option<TailDecl>,
    pub pos: Pos,
    pub name: String,
    pub node: Option<NodeRead>,
    pub insert_on: Option<Spanned<InsertOn>>,
    pub stable_order: Option<Pos>,
    pub refresh_on_foreground: Option<Pos>,
    pub keep_on_gap: Option<Pos>,
    pub members_complete: bool,
    pub parameters: Vec<ParameterDecl>,
    pub where_: Option<WhereDecl>,
    pub membership: Vec<(String, dblayer::schema::Membership, Pos)>,
    pub row_rules: Vec<(dblayer::schema::RowRule, Pos)>,
    pub order: Option<OrderDecl>,
    pub query: Option<QueryDecl>,
    pub variables: Option<Vec<VariableBinding>>,
    pub response: Option<ResponseRead>,
    pub paginate: Option<PaginateDecl>,
    pub freshness: Option<FreshnessDecl>,
    pub preload: Option<PreloadDecl>,
    pub total: Option<Spanned<String>>,
    pub edges: Vec<AttributeDecl>,
    pub subscriptions: Vec<SubscribeDecl>,
    pub has_block: bool,
}

impl ScopeDecl {
    pub fn is_remote(&self) -> bool {
        self.query.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangesDecl {
    pub pos: Pos,
    pub subscription: String,
    pub from: Spanned<String>,
    pub area: Spanned<String>,
    pub kind: Spanned<String>,
    pub id: Spanned<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemovedByDecl {
    pub pos: Pos,
    pub model: String,
    pub matches: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangedByDecl {
    pub pos: Pos,
    pub area: String,
    pub via: String,
    pub parameter: Option<String>,
    pub removes_on: Option<Vec<String>>,
    pub id_of: Option<(String, String)>,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Create,
    Update,
    Destroy,
    Custom,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimisticValue {
    Literal(Json),
    Parameter(String),
    Keyed { by: String, element: Vec<(String, OptimisticElement)> },
    Mapped { from: String, map: Vec<(String, Json)> },
    Include { source: SourceDecl, when: String },
    AtLeast { min: i64, when: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimisticElement {
    Literal(Json),
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimisticDecl {
    Off,
    Fields(Vec<(String, OptimisticValue, Pos)>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceDecl {
    Owner(String),
    Row { model: String, row: Option<String>, field: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseDecl {
    Ignore,
    Read(ResponseRead),
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpsertDecl {
    pub pos: Pos,
    pub model: String,
    pub from: Spanned<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefetchDecl {
    pub pos: Pos,
    pub scope: String,
    pub model: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenumberDecl {
    pub position: Spanned<String>,
    pub size: Spanned<String>,
    pub start: usize,
    pub start_pos: Pos,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchDecl {
    pub pos: Pos,
    pub items: String,
    pub single: String,
    pub input: Vec<(String, String)>,
    pub renumber: Option<RenumberDecl>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LengthDecl {
    pub pos: Pos,
    pub attribute: String,
    pub minimum: usize,
    pub maximum: usize,
}

/// `dimensions width: :width, height: :height`: the attributes that hold the size of the uploaded file as a user sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct DimensionsDecl {
    pub pos: Pos,
    pub width: String,
    pub height: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationDecl {
    pub pos: Pos,
    pub name: String,
    pub kind: OperationKind,
    pub upload: Option<Pos>,
    pub expires_after: Option<u64>,
    pub dimensions: Option<DimensionsDecl>,
    pub mutation: Option<Spanned<String>>,
    pub parameters: Vec<ParameterDecl>,
    pub variables: Option<Vec<VariableBinding>>,
    pub local: Option<Vec<String>>,
    pub local_from: Vec<(String, SourceDecl, Pos)>,
    pub local_sites: Vec<(String, Pos)>,
    pub wrap: Option<String>,
    pub response: Option<ResponseDecl>,
    pub match_by: Option<Vec<Spanned<String>>>,
    pub on_failure: Option<Spanned<OnFailure>>,
    pub optimistic: Option<(OptimisticDecl, Pos)>,
    pub upserts: Vec<UpsertDecl>,
    pub refetch: Vec<RefetchDecl>,
    pub returns: Option<Pos>,
    pub answer: Option<FieldSpec>,
    pub batch: Option<BatchDecl>,
    pub length: Option<LengthDecl>,
    pub removes_receiver: Option<Pos>,
    pub copies_receiver_to: Option<ReceiverCopyDecl>,
    pub scope_effects: Vec<ScopeEffectDecl>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiverCopyDecl {
    pub pos: Pos,
    pub arg: String,
    pub field: String,
    pub set: Vec<(String, CopySetDecl, Pos)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySetDecl {
    Literal(Json),
    Source(SourceDecl),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeEffectDecl {
    pub pos: Pos,
    pub joins: bool,
    pub scope: String,
    pub model: String,
    pub rows: String,
    pub with: Vec<(String, String)>,
    pub order: String,
    pub orders: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EphemeralDecl {
    pub pos: Pos,
    pub attributes: Vec<AttributeDecl>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelDecl {
    pub name: String,
    pub pos: Pos,
    pub file: usize,
    pub storage: Storage,
    pub expires_in: Option<Spanned<String>>,
    pub attributes: Vec<AttributeDecl>,
    pub ephemeral: Vec<EphemeralDecl>,
    pub indexes: Vec<IndexDecl>,
    pub search: Vec<Spanned<String>>,
    pub associations: Vec<AssociationDecl>,
    pub scopes: Vec<ScopeDecl>,
    pub subscriptions: Vec<SubscribeDecl>,
    pub changes: Option<ChangesDecl>,
    pub changed_by: Vec<ChangedByDecl>,
    pub removed_by: Vec<RemovedByDecl>,
    pub server_revision: Option<String>,
    pub cable_when: Option<(String, Pos)>,
    pub operations: Vec<OperationDecl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Query,
    Mutation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallDecl {
    pub name: String,
    pub pos: Pos,
    pub file: usize,
    pub kind: Option<(CallKind, String, Pos)>,
    pub wrap: Option<String>,
    pub parameters: Vec<ParameterDecl>,
    pub answer: Option<FieldSpec>,
    pub shape: Option<(ResponseShape, Pos)>,
    pub upserts: Vec<UpsertDecl>,
    pub refetch: Vec<RefetchDecl>,
}

