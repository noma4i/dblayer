use super::*;

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
    pub debounce: Option<String>,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub id: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply: Option<SubscriptionApply>,
}

impl UpdatesSpec {
    pub fn takes_variables(&self, relation: &RelationSpec) -> bool {
        relation.remote.as_ref().is_some_and(|remote| self.id.iter().any(|name| remote.variables.contains_key(name)))
    }
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

/// The source of the rows of one write batch (P-DATA-100).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowSource {
    /// The answer of a page of a window, of `nodes` or of a `node` read by the caller.
    #[default]
    Page,
    /// The answer of a `node` read that a `changed_by` event started or entered.
    NodeReread,
    Event,
    Response,
    Create,
    Local,
    /// The pass that writes the derived fields of the parents a batch touched (P-DATA-56).
    Derived,
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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub keep_on_gap: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeRead>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edge_fields: Vec<FieldSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload: Option<BTreeMap<String, PreloadChoice>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload_when: Option<PreloadWhen>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preload_order: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub members_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_freshness: Option<SequenceFreshnessSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceFreshnessSpec {
    pub association: String,
    pub parent_field: String,
    pub member_field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreloadWhen {
    pub of: String,
    pub field: String,
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
    /// The declared sort of a paginated relation whose members persist that order: the read follows the membership keys.
    pub(crate) fn ranked_order(&self) -> Option<&[SortField]> {
        match (&self.sort, self.remote.as_ref().map(|remote| &remote.shape)) {
            (Sort::Fields(fields), Some(RemoteShape::Connection { .. })) if !self.order_stable && !self.members_complete => Some(fields),
            _ => None,
        }
    }

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
            keep_on_gap: false,
            node: None,
            edge_fields: Vec::new(),
            preload: None,
            preload_when: None,
            preload_limit: None,
            preload_order: Vec::new(),
            members_complete: false,
            media: None,
            sequence_freshness: None,
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

    /// P-DATA-100: whether a row written by a batch of `source` may enter a loaded window of this selection.
    pub fn admits(&self, source: RowSource) -> bool {
        match source {
            RowSource::Page | RowSource::Local | RowSource::Derived => false,
            RowSource::NodeReread => self.joins_node,
            RowSource::Event | RowSource::Response => self.joins_events,
            RowSource::Create => (self.joins_created && self.by_columns.is_empty() && self.text_params.is_empty()) || self.lists_every_row_of_its_params(),
        }
    }

    /// P-DATA-100: whether the row enters a loaded window: a `Create` batch only adds a row the store did not hold.
    pub fn joins(&self, source: RowSource, row_is_new: bool) -> bool {
        self.remote.is_some() && self.admits(source) && (source != RowSource::Create || row_is_new)
    }

    /// P-DATA-100: a row of an event or a node reread is whole, so a membership field it lacks is an error.
    pub fn requires_membership_fields(source: RowSource) -> bool {
        matches!(source, RowSource::Event | RowSource::NodeReread)
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

    pub(super) fn check(&self, model: &ModelSpec) -> Result<(), DeclarationError> {
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

