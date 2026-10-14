use rusqlite::types::Value as SqlValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ddl::{column_name, column_type, layout, member_present, FieldPlan};
use crate::schema::ModelSpec;

use super::statement::QueryError;

pub(super) const SYNC_FIELD: &str = "sync";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl CompareOp {
    fn as_sql(self) -> &'static str {
        match self {
            CompareOp::Less => "<",
            CompareOp::LessOrEqual => "<=",
            CompareOp::Greater => ">",
            CompareOp::GreaterOrEqual => ">=",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Where {
    Equal { field: String, #[serde(default)] value: Value },
    NotEqual { field: String, #[serde(default)] value: Value },
    Compare { field: String, op: CompareOp, value: Value },
    InList { field: String, values: Vec<Value> },
    NotInList { field: String, values: Vec<Value> },
    IsNull { field: String },
    AllOf { clauses: Vec<Where> },
    AnyOf { clauses: Vec<Where> },
    Not { clause: Box<Where> },
    Search { text: String, #[serde(default)] whole_words: bool },
}

impl Where {
    pub fn equal(field: &str, value: Value) -> Self {
        Where::Equal { field: field.to_string(), value }
    }

    #[cfg(test)]
    pub fn not_equal(field: &str, value: Value) -> Self {
        Where::NotEqual { field: field.to_string(), value }
    }

    #[cfg(test)]
    pub fn compare(field: &str, op: CompareOp, value: Value) -> Self {
        Where::Compare { field: field.to_string(), op, value }
    }

    pub fn in_list(field: &str, values: Vec<Value>) -> Self {
        Where::InList { field: field.to_string(), values }
    }

    #[cfg(test)]
    pub fn not_in_list(field: &str, values: Vec<Value>) -> Self {
        Where::NotInList { field: field.to_string(), values }
    }

    #[cfg(test)]
    pub fn is_null(field: &str) -> Self {
        Where::IsNull { field: field.to_string() }
    }

    #[cfg(test)]
    pub fn all_of(clauses: Vec<Where>) -> Self {
        Where::AllOf { clauses }
    }

    #[cfg(test)]
    pub fn any_of(clauses: Vec<Where>) -> Self {
        Where::AnyOf { clauses }
    }

    #[cfg(test)]
    pub fn search(text: &str, whole_words: bool) -> Self {
        Where::Search { text: text.to_string(), whole_words }
    }

    pub(super) fn searches(&self) -> Vec<(&str, bool)> {
        match self {
            Where::Search { text, whole_words } => vec![(text, *whole_words)],
            Where::AllOf { clauses } | Where::AnyOf { clauses } => clauses.iter().flat_map(Where::searches).collect(),
            Where::Not { clause } => clause.searches(),
            _ => Vec::new(),
        }
    }

    pub fn negated(clause: Where) -> Self {
        Where::Not { clause: Box::new(clause) }
    }

    pub fn fields(&self) -> Vec<String> {
        match self {
            Where::Equal { field, .. }
            | Where::NotEqual { field, .. }
            | Where::Compare { field, .. }
            | Where::InList { field, .. }
            | Where::NotInList { field, .. }
            | Where::IsNull { field } => vec![field.clone()],
            Where::AllOf { clauses } | Where::AnyOf { clauses } => clauses.iter().flat_map(Where::fields).collect(),
            Where::Not { clause } => clause.fields(),
            Where::Search { .. } => Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn is_null_safe(&self) -> bool {
        match self {
            Where::InList { values, .. } => !values.iter().any(Value::is_null),
            Where::AllOf { clauses } | Where::AnyOf { clauses } => clauses.iter().all(Where::is_null_safe),
            Where::Not { clause } => clause.is_null_safe(),
            _ => true,
        }
    }

    pub(super) fn compile(&self, model: &ModelSpec, bindings: &mut Vec<SqlValue>) -> Result<String, QueryError> {
        match self {
            Where::Equal { field, value } if value.is_null() => Ok(format!("{} IS NULL", column_sql(model, field)?)),
            Where::Equal { field, value } => {
                let column = column_sql(model, field)?;
                bindings.push(binding(field, value)?);
                Ok(format!("{column} = ?"))
            }
            Where::NotEqual { field, value } if value.is_null() => {
                Ok(format!("{} IS NOT NULL", column_sql(model, field)?))
            }
            Where::NotEqual { field, value } => {
                let column = column_sql(model, field)?;
                bindings.push(binding(field, value)?);
                Ok(format!("{column} IS NOT ?"))
            }
            Where::Compare { field, op, value } => {
                let column = column_sql(model, field)?;
                bindings.push(binding(field, value)?);
                Ok(format!("{column} {} ?", op.as_sql()))
            }
            Where::InList { field, values } => compile_list(model, field, values, bindings, false),
            Where::NotInList { field, values } => compile_list(model, field, values, bindings, true),
            Where::IsNull { field } => Ok(format!("{} IS NULL", column_sql(model, field)?)),
            Where::AllOf { clauses } => compile_group(model, clauses, bindings, " AND ", "1"),
            Where::AnyOf { clauses } => compile_group(model, clauses, bindings, " OR ", "0"),
            Where::Not { clause } => Ok(format!("NOT COALESCE({}, 0)", clause.compile(model, bindings)?)),
            Where::Search { text, whole_words } => Ok(if fts_expression(text, *whole_words).is_some() { "q.match_id IS NOT NULL".to_string() } else { "0".to_string() }),
        }
    }
}

fn compile_list(
    model: &ModelSpec,
    field: &str,
    values: &[Value],
    bindings: &mut Vec<SqlValue>,
    negated: bool,
) -> Result<String, QueryError> {
    let column = column_sql(model, field)?;
    let members: Vec<&Value> = values.iter().filter(|value| !value.is_null()).collect();
    if members.is_empty() {
        return Ok(if negated { "1" } else { "0" }.to_string());
    }
    for value in &members {
        bindings.push(binding(field, value)?);
    }
    let placeholders = vec!["?"; members.len()].join(", ");
    Ok(if negated {
        format!("({column} IS NULL OR {column} NOT IN ({placeholders}))")
    } else {
        format!("{column} IN ({placeholders})")
    })
}

fn compile_group(
    model: &ModelSpec,
    clauses: &[Where],
    bindings: &mut Vec<SqlValue>,
    separator: &str,
    empty: &str,
) -> Result<String, QueryError> {
    if clauses.is_empty() {
        return Ok(empty.to_string());
    }
    let parts = clauses.iter().map(|clause| clause.compile(model, bindings)).collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", parts.join(separator)))
}

pub(super) fn column_sql(model: &ModelSpec, field: &str) -> Result<String, QueryError> {
    if field == "id" {
        return Ok("r.id".to_string());
    }
    if field == SYNC_FIELD {
        return Ok("COALESCE(s.state, 'synced')".to_string());
    }
    if model.ephemeral_field(field).is_some() {
        return Err(QueryError::EphemeralField(field.to_string()));
    }
    let spec = model.field(field).ok_or_else(|| QueryError::UnknownField(field.to_string()))?;
    if column_type(&spec.kind).is_some() {
        return Ok(format!("r.{}", column_name(field)));
    }
    match layout(model).plan(field) {
        Some(FieldPlan::Typed { member, .. }) => Ok(format!("r.{}", member_present(member))),
        // TEMPORARY: fields without a declared shape are read from the JSON bytes of `rest`.
        _ => Ok(format!("json_extract(r.rest, '$.{field}')")),
    }
}

pub(super) fn binding(field: &str, value: &Value) -> Result<SqlValue, QueryError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(flag) => Ok(SqlValue::Integer(i64::from(*flag))),
        Value::String(text) => Ok(SqlValue::Text(text.clone())),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                return Ok(SqlValue::Integer(integer));
            }
            let float = number.as_f64().unwrap_or(f64::NAN);
            if float.fract() == 0.0 && float.abs() < 9.0e18 {
                return Ok(SqlValue::Integer(float as i64));
            }
            Ok(SqlValue::Real(float))
        }
        Value::Array(_) | Value::Object(_) => Err(QueryError::UnsupportedValue(field.to_string())),
    }
}

/// The words of a search text as FTS5 terms: every word quoted, the last one a prefix unless whole words are asked.
pub(super) fn fts_expression(text: &str, whole_words: bool) -> Option<String> {
    let words: Vec<String> = text.split_whitespace().map(|word| format!("\"{}\"", word.replace('"', "\"\""))).collect();
    let last = words.len().checked_sub(1)?;
    Some(words.into_iter().enumerate().map(|(index, word)| if index == last && !whole_words { word + "*" } else { word }).collect::<Vec<_>>().join(" "))
}
