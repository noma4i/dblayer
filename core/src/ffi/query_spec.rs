//! A query as the platform declares it: values, no text. The core compiles the record into the one `Query` it runs.

use serde_json::Value;

use super::wire::Cell;
use super::DbError;
use crate::store::query::{CompareOp, Order, Query, Where};
use crate::value::SortDirection;

/// One test on the rows. A scalar value is a `Cell` of the kinds `Null`, `Bool`, `Int`, `Real` and `Text`.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum QueryCondition {
    Equal { field: String, value: Cell },
    NotEqual { field: String, value: Cell },
    Compare { field: String, op: CompareOp, value: Cell },
    InList { field: String, values: Vec<Cell> },
    NotInList { field: String, values: Vec<Cell> },
    IsNull { field: String },
    AllOf { clauses: Vec<QueryCondition> },
    AnyOf { clauses: Vec<QueryCondition> },
    Not { clause: Box<QueryCondition> },
    Search { text: String, whole_words: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct QueryOrder {
    pub field: String,
    pub direction: SortDirection,
}

/// What a read of rows asks: the model, the conditions that all hold, the order, the window of rows, and for a server relation its name and parameters.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct QuerySpec {
    pub model: String,
    pub relation: Option<String>,
    /// The parameters of the relation as a JSON object. Read only with a relation.
    pub params_json: String,
    pub conditions: Vec<QueryCondition>,
    pub order: Vec<QueryOrder>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// The stream loads its window again when it opens, also when the window is fresh.
    pub refresh_on_connect: bool,
}

fn scalar(cell: &Cell) -> Result<Value, DbError> {
    Ok(match cell {
        Cell::Null => Value::Null,
        Cell::Bool { value } => Value::Bool(*value),
        Cell::Int { value } => Value::from(*value),
        Cell::Real { value } => serde_json::Number::from_f64(*value).map(Value::Number).ok_or_else(|| DbError::Invalid { detail: "a query value is not a finite number".into() })?,
        Cell::Text { value } => Value::String(value.clone()),
        Cell::Seq { .. } | Cell::Keyed { .. } => return Err(DbError::Invalid { detail: "a query value must be a scalar".into() }),
    })
}

fn scalars(cells: &[Cell]) -> Result<Vec<Value>, DbError> {
    cells.iter().map(scalar).collect()
}

impl QueryCondition {
    fn filter(&self) -> Result<Where, DbError> {
        Ok(match self {
            QueryCondition::Equal { field, value } => Where::Equal { field: field.clone(), value: scalar(value)? },
            QueryCondition::NotEqual { field, value } => Where::NotEqual { field: field.clone(), value: scalar(value)? },
            QueryCondition::Compare { field, op, value } => Where::Compare { field: field.clone(), op: *op, value: scalar(value)? },
            QueryCondition::InList { field, values } => Where::InList { field: field.clone(), values: scalars(values)? },
            QueryCondition::NotInList { field, values } => Where::NotInList { field: field.clone(), values: scalars(values)? },
            QueryCondition::IsNull { field } => Where::IsNull { field: field.clone() },
            QueryCondition::AllOf { clauses } => Where::AllOf { clauses: clauses.iter().map(Self::filter).collect::<Result<_, _>>()? },
            QueryCondition::AnyOf { clauses } => Where::AnyOf { clauses: clauses.iter().map(Self::filter).collect::<Result<_, _>>()? },
            QueryCondition::Not { clause } => Where::negated(clause.filter()?),
            QueryCondition::Search { text, whole_words } => Where::Search { text: text.clone(), whole_words: *whole_words },
        })
    }
}

impl QuerySpec {
    pub(super) fn query(&self) -> Result<Query, DbError> {
        if self.limit == Some(0) { return Err(DbError::Invalid { detail: "a query limit must be positive".into() }); }
        let mut clauses = self.conditions.iter().map(QueryCondition::filter).collect::<Result<Vec<_>, _>>()?;
        let mut query = Query::new(&self.model).ordered(self.order.iter().map(|term| Order::new(&term.field, term.direction)).collect());
        query.r#where = match clauses.len() {
            0 => None,
            1 => clauses.pop(),
            _ => Some(Where::AllOf { clauses }),
        };
        query.limit = self.limit;
        query.offset = self.offset;
        Ok(query)
    }
}
