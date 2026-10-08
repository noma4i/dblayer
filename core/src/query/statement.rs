use rusqlite::types::Value as SqlValue;
use serde::{Deserialize, Serialize};

use crate::ddl::table_name;
use crate::identity::{ModelKey, ScopeKey};
use crate::schema::ModelSpec;
use crate::value::SortDirection;

use super::filter::{column_sql, Where};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    #[error("unknown field {0}")]
    UnknownField(String),
    #[error("value of field {0} is not a scalar")]
    UnsupportedValue(String),
    #[error("query targets model {query} but the spec is for {model}")]
    ModelMismatch { query: String, model: String },
    #[error("sqlite: {0}")]
    Sqlite(String),
}

impl From<rusqlite::Error> for QueryError {
    fn from(error: rusqlite::Error) -> Self {
        QueryError::Sqlite(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledStatement {
    pub sql: String,
    pub bindings: Vec<SqlValue>,
}

impl CompiledStatement {
    pub fn placeholder_count(&self) -> usize {
        self.sql.matches('?').count()
    }

    pub fn is_balanced(&self) -> bool {
        self.placeholder_count() == self.bindings.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub field: String,
    pub direction: SortDirection,
}

impl Order {
    pub fn new(field: &str, direction: SortDirection) -> Self {
        Self { field: field.to_string(), direction }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub model: ModelKey,
    #[serde(default)]
    pub r#where: Option<Where>,
    #[serde(default)]
    pub order: Vec<Order>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u32>,
}

impl Query {
    pub fn new(model: &str) -> Self {
        Self { model: ModelKey::new(model), r#where: None, order: Vec::new(), limit: None, offset: None }
    }

    pub fn filtered(self, clause: Where) -> Self {
        Self { r#where: Some(clause), ..self }
    }

    pub fn ordered(self, order: Vec<Order>) -> Self {
        Self { order, ..self }
    }

    pub fn limited(self, limit: u32) -> Self {
        Self { limit: Some(limit), ..self }
    }

    pub fn offset_by(self, offset: u32) -> Self {
        Self { offset: Some(offset), ..self }
    }

    pub fn unindexed_fields(&self, model: &ModelSpec) -> Vec<String> {
        let filtered = self.r#where.iter().flat_map(Where::fields);
        let ordered = self.order.iter().map(|order| order.field.clone());
        let mut found: Vec<String> = Vec::new();
        for field in filtered.chain(ordered) {
            let promoted = field == "id" || model.is_indexed(&field);
            if !promoted && !found.contains(&field) {
                found.push(field);
            }
        }
        found
    }

    pub fn is_index_only(&self, model: &ModelSpec) -> bool {
        self.unindexed_fields(model).is_empty()
    }
}

const ROW_COLUMNS: &str = "r.id, r.record_version, r.payload";

fn check_model(query: &Query, model: &ModelSpec) -> Result<(), QueryError> {
    if query.model == model.key {
        return Ok(());
    }
    Err(QueryError::ModelMismatch { query: query.model.as_str().to_string(), model: model.key.as_str().to_string() })
}

fn filter_clause(query: &Query, model: &ModelSpec, bindings: &mut Vec<SqlValue>) -> Result<String, QueryError> {
    match &query.r#where {
        Some(clause) => Ok(format!(" WHERE {}", clause.compile(model, bindings)?)),
        None => Ok(String::new()),
    }
}

fn order_clause(query: &Query, model: &ModelSpec) -> Result<String, QueryError> {
    if query.order.is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for order in &query.order {
        let direction = match order.direction {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };
        parts.push(format!("{} {direction}", column_sql(model, &order.field)?));
    }
    if !query.order.iter().any(|order| order.field == "id") {
        parts.push("r.id".to_string());
    }
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

fn window_clause(query: &Query, bindings: &mut Vec<SqlValue>) -> String {
    let mut sql = String::new();
    match (query.limit, query.offset) {
        (None, None) => {}
        (Some(limit), _) => {
            sql.push_str(" LIMIT ?");
            bindings.push(SqlValue::Integer(i64::from(limit)));
        }
        (None, Some(_)) => sql.push_str(" LIMIT -1"),
    }
    if let Some(offset) = query.offset {
        sql.push_str(" OFFSET ?");
        bindings.push(SqlValue::Integer(i64::from(offset)));
    }
    sql
}

pub fn compile(query: &Query, model: &ModelSpec) -> Result<CompiledStatement, QueryError> {
    check_model(query, model)?;
    let mut bindings = Vec::new();
    let filter = filter_clause(query, model, &mut bindings)?;
    let order = order_clause(query, model)?;
    let window = window_clause(query, &mut bindings);
    let sql = format!("SELECT {ROW_COLUMNS} FROM {} AS r{filter}{order}{window}", table_name(query.model.as_str()));
    Ok(CompiledStatement { sql, bindings })
}

pub fn compile_count(query: &Query, model: &ModelSpec) -> Result<CompiledStatement, QueryError> {
    check_model(query, model)?;
    let mut bindings = Vec::new();
    let filter = filter_clause(query, model, &mut bindings)?;
    let sql = format!("SELECT COUNT(*) FROM {} AS r{filter}", table_name(query.model.as_str()));
    Ok(CompiledStatement { sql, bindings })
}

fn scope_join(model: &ModelKey) -> String {
    format!(
        "FROM scope_member AS m JOIN {} AS r ON r.id = m.row_id WHERE m.model = ? AND m.scope_key = ?",
        table_name(model.as_str())
    )
}

fn scope_bindings(model: &ModelKey, scope: &ScopeKey) -> Vec<SqlValue> {
    vec![SqlValue::Text(model.as_str().to_string()), SqlValue::Text(scope.as_str().to_string())]
}

pub fn scope_statement(model: &ModelKey, scope: &ScopeKey, limit: Option<usize>) -> CompiledStatement {
    let mut bindings = scope_bindings(model, scope);
    let mut sql = format!("SELECT {ROW_COLUMNS} {} ORDER BY m.order_key, m.row_id", scope_join(model));
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        bindings.push(SqlValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)));
    }
    CompiledStatement { sql, bindings }
}

pub fn scope_count_statement(model: &ModelKey, scope: &ScopeKey) -> CompiledStatement {
    CompiledStatement { sql: format!("SELECT COUNT(*) {}", scope_join(model)), bindings: scope_bindings(model, scope) }
}
