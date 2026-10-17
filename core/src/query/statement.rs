use rusqlite::types::Value as SqlValue;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::Value;

use crate::ddl::{fts_name, table_name};
use crate::identity::{ModelKey, ScopeKey};
use crate::schema::ModelSpec;
use crate::value::SortDirection;

use super::filter::{column_sql, fts_expression, Where, SYNC_FIELD};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    #[error("unknown field {0}")]
    UnknownField(String),
    #[error("field {0} is ephemeral and cannot be filtered or ordered")]
    EphemeralField(String),
    #[error("value of field {0} is not a scalar")]
    UnsupportedValue(String),
    #[error("query targets model {query} but the spec is for {model}")]
    ModelMismatch { query: String, model: String },
    #[error("model {0} has no searchable field")]
    NoSearchableField(String),
    #[error("a query holds at most one search")]
    SeveralSearches,
    #[error("a window needs a query with an order and without limit, offset, include or search; a scope needs a local sort by fields")]
    NotWindowable,
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

#[cfg(test)]
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
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub scope: Option<ScopeKey>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stable: bool,
}

impl Query {
    pub fn new(model: &str) -> Self {
        Self { model: ModelKey::new(model), r#where: None, order: Vec::new(), limit: None, offset: None, include: Vec::new(), scope: None, stable: false }
    }

    #[cfg(test)]
    pub fn unsynced(model: &str) -> Self {
        Self::new(model).filtered(Where::not_equal(SYNC_FIELD, Value::from("synced")))
    }

    #[cfg(test)]
    pub fn failed(model: &str) -> Self {
        Self::new(model).filtered(Where::equal(SYNC_FIELD, Value::from("failed")))
    }

    #[cfg(test)]
    pub fn pending(model: &str) -> Self {
        Self::new(model).filtered(Where::equal(SYNC_FIELD, Value::from("pending")))
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

    #[cfg(test)]
    pub fn offset_by(self, offset: u32) -> Self {
        Self { offset: Some(offset), ..self }
    }

    pub fn including(self, names: &[&str]) -> Self {
        Self { include: names.iter().map(|name| name.to_string()).collect(), ..self }
    }

    pub fn in_scope(self, scope: ScopeKey) -> Self {
        Self { scope: Some(scope), ..self }
    }
}

const KEY_COLUMNS: &str = "r.id, r.revision";

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

fn join_clause(query: &Query, bindings: &mut Vec<SqlValue>, from_rows: bool) -> String {
    let Some(scope) = &query.scope else { return String::new() };
    bindings.extend(scope_bindings(&query.model, scope));
    let join = if from_rows { "CROSS JOIN" } else { "JOIN" };
    format!(" {join} scope_member AS m ON m.row_id = r.id AND m.model = ? AND m.scope_key = ?")
}

fn sync_join(query: &Query, bindings: &mut Vec<SqlValue>) -> String {
    let filtered = query.r#where.iter().flat_map(Where::fields);
    let ordered = query.order.iter().map(|order| order.field.clone());
    if !filtered.chain(ordered).any(|field| field == SYNC_FIELD) {
        return String::new();
    }
    bindings.push(SqlValue::Text(query.model.as_str().to_string()));
    " LEFT JOIN row_sync AS s ON s.row_id = r.id AND s.model = ?".to_string()
}

fn search_join(query: &Query, model: &ModelSpec, bindings: &mut Vec<SqlValue>) -> Result<String, QueryError> {
    let searches = query.r#where.iter().flat_map(Where::searches).collect::<Vec<_>>();
    let Some(&(text, whole_words)) = searches.first() else { return Ok(String::new()) };
    if searches.len() > 1 {
        return Err(QueryError::SeveralSearches);
    }
    if model.search_paths().is_empty() {
        return Err(QueryError::NoSearchableField(model.key.as_str().to_string()));
    }
    let Some(expression) = fts_expression(text, whole_words) else { return Ok(String::new()) };
    bindings.push(SqlValue::Text(expression));
    let table = fts_name(model.key.as_str());
    Ok(format!(" LEFT JOIN (SELECT rowid AS match_id, rank AS match_rank FROM {table} WHERE {table} MATCH ?) AS q ON q.match_id = r.rowid"))
}

fn searching(query: &Query) -> bool {
    query.r#where.iter().any(|clause| clause.searches().iter().any(|(text, whole_words)| fts_expression(text, *whole_words).is_some()))
}

struct SortTerm {
    clause: String,
    expression: String,
    descending: bool,
}

fn keeps_member_order(query: &Query) -> bool {
    query.stable && query.scope.is_some()
}

fn sort_terms(query: &Query, model: &ModelSpec) -> Result<Vec<SortTerm>, QueryError> {
    if keeps_member_order(query) {
        return Ok(vec![SortTerm { clause: "m.order_key".to_string(), expression: "m.order_key".to_string(), descending: false }]);
    }
    let mut terms = Vec::new();
    for order in &query.order {
        let column = column_sql(model, &order.field)?;
        let flag = |expression: String| SortTerm { clause: expression.clone(), expression, descending: false };
        let value = |suffix: &str, descending: bool| SortTerm { clause: format!("{column} {suffix}"), expression: column.clone(), descending };
        match order.direction {
            SortDirection::Asc => terms.push(value("ASC", false)),
            SortDirection::Desc => terms.push(value("DESC", true)),
            SortDirection::AscNullsLast => terms.extend([flag(format!("{column} IS NULL")), value("ASC", false)]),
            SortDirection::DescNullsFirst => terms.extend([flag(format!("{column} IS NOT NULL")), value("DESC", true)]),
        }
    }
    Ok(terms)
}

fn order_clause(query: &Query, model: &ModelSpec) -> Result<String, QueryError> {
    if query.order.is_empty() {
        return Ok(match (query.scope.is_some(), searching(query)) {
            (true, _) => " ORDER BY m.order_key, m.row_id".to_string(),
            (false, true) => " ORDER BY q.match_rank, r.id".to_string(),
            (false, false) => String::new(),
        });
    }
    let mut parts: Vec<String> = sort_terms(query, model)?.into_iter().map(|term| term.clause).collect();
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

fn compile_columns(query: &Query, model: &ModelSpec, columns: &str) -> Result<CompiledStatement, QueryError> {
    check_model(query, model)?;
    let mut bindings = Vec::new();
    let join = join_clause(query, &mut bindings, false) + &sync_join(query, &mut bindings) + &search_join(query, model, &mut bindings)?;
    let filter = filter_clause(query, model, &mut bindings)?;
    let order = order_clause(query, model)?;
    let window = window_clause(query, &mut bindings);
    let sql = format!("SELECT {columns} FROM {} AS r{join}{filter}{order}{window}", table_name(query.model.as_str()));
    Ok(CompiledStatement { sql, bindings })
}

pub fn compile_keys(query: &Query, model: &ModelSpec) -> Result<CompiledStatement, QueryError> {
    compile_columns(query, model, KEY_COLUMNS)
}

pub fn ranking(query: &Query) -> bool {
    query.scope.is_none() && query.limit.is_none() && query.offset.is_none() && query.include.is_empty() && !query.order.is_empty() && query.r#where.iter().all(|clause| clause.searches().is_empty())
}

pub fn windowable(query: &Query) -> bool {
    query.limit.is_none() && query.offset.is_none() && query.include.is_empty() && !query.order.is_empty() && query.r#where.iter().all(|clause| clause.searches().is_empty())
}

pub struct RankedStatement {
    pub statement: CompiledStatement,
    pub descending: Vec<bool>,
}

pub fn compile_ranked(query: &Query, model: &ModelSpec, only: Option<&[String]>) -> Result<RankedStatement, QueryError> {
    check_model(query, model)?;
    let terms = sort_terms(query, model)?;
    let columns: Vec<&str> = std::iter::once(KEY_COLUMNS).chain(terms.iter().map(|term| term.expression.as_str())).collect();
    let mut bindings = Vec::new();
    let join = join_clause(query, &mut bindings, only.is_some()) + &sync_join(query, &mut bindings) + &search_join(query, model, &mut bindings)?;
    let (filter, order) = match only {
        None => (filter_clause(query, model, &mut bindings)?, order_clause(query, model)?),
        Some(ids) => {
            let mut parts = Vec::new();
            if let Some(clause) = &query.r#where {
                parts.push(format!("({})", clause.compile(model, &mut bindings)?));
            }
            bindings.extend(ids.iter().map(|id| SqlValue::Text(id.clone())));
            parts.push(format!("r.id IN ({})", vec!["?"; ids.len()].join(", ")));
            (format!(" WHERE {}", parts.join(" AND ")), String::new())
        }
    };
    let sql = format!("SELECT {} FROM {} AS r{join}{filter}{order}", columns.join(", "), table_name(query.model.as_str()));
    Ok(RankedStatement { statement: CompiledStatement { sql, bindings }, descending: terms.iter().map(|term| term.descending).collect() })
}

pub fn compile_count(query: &Query, model: &ModelSpec) -> Result<CompiledStatement, QueryError> {
    check_model(query, model)?;
    let mut bindings = Vec::new();
    let join = join_clause(query, &mut bindings, false) + &sync_join(query, &mut bindings) + &search_join(query, model, &mut bindings)?;
    let filter = filter_clause(query, model, &mut bindings)?;
    let sql = format!("SELECT COUNT(*) FROM {} AS r{join}{filter}", table_name(query.model.as_str()));
    Ok(CompiledStatement { sql, bindings })
}

#[cfg(test)]
fn scope_join(model: &ModelKey) -> String {
    format!(
        "FROM scope_member AS m JOIN {} AS r ON r.id = m.row_id WHERE m.model = ? AND m.scope_key = ?",
        table_name(model.as_str())
    )
}

fn scope_bindings(model: &ModelKey, scope: &ScopeKey) -> Vec<SqlValue> {
    vec![SqlValue::Text(model.as_str().to_string()), SqlValue::Text(scope.as_str().to_string())]
}

#[cfg(test)]
pub fn scope_statement(model: &ModelKey, scope: &ScopeKey, limit: Option<usize>) -> CompiledStatement {
    let mut bindings = scope_bindings(model, scope);
    let mut sql = format!("SELECT {KEY_COLUMNS} {} ORDER BY m.order_key, m.row_id", scope_join(model));
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        bindings.push(SqlValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)));
    }
    CompiledStatement { sql, bindings }
}

#[cfg(test)]
pub fn scope_count_statement(model: &ModelKey, scope: &ScopeKey) -> CompiledStatement {
    CompiledStatement { sql: format!("SELECT COUNT(*) {}", scope_join(model)), bindings: scope_bindings(model, scope) }
}
