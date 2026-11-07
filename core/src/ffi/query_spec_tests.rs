use serde::Deserialize;
use serde_json::{Map, Value};

use super::*;
use crate::ffi::wire::free_cell;
use crate::store::query::{CompareOp, Order, Query, Where};
use crate::value::SortDirection;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Shorthand {
    model: String,
    relation: Option<String>,
    #[serde(default)]
    params: Map<String, Value>,
    #[serde(default)]
    refresh_on_connect: bool,
    r#where: Option<Where>,
    #[serde(default)]
    order: Vec<Order>,
    limit: Option<u32>,
    offset: Option<u32>,
}

fn condition(clause: &Where) -> QueryCondition {
    let cell = free_cell;
    match clause {
        Where::Equal { field, value } => QueryCondition::Equal { field: field.clone(), value: cell(value) },
        Where::NotEqual { field, value } => QueryCondition::NotEqual { field: field.clone(), value: cell(value) },
        Where::Compare { field, op, value } => QueryCondition::Compare { field: field.clone(), op: *op, value: cell(value) },
        Where::InList { field, values } => QueryCondition::InList { field: field.clone(), values: values.iter().map(cell).collect() },
        Where::NotInList { field, values } => QueryCondition::NotInList { field: field.clone(), values: values.iter().map(cell).collect() },
        Where::IsNull { field } => QueryCondition::IsNull { field: field.clone() },
        Where::AllOf { clauses } => QueryCondition::AllOf { clauses: clauses.iter().map(condition).collect() },
        Where::AnyOf { clauses } => QueryCondition::AnyOf { clauses: clauses.iter().map(condition).collect() },
        Where::Not { clause } => QueryCondition::Not { clause: Box::new(condition(clause)) },
        Where::Search { text, whole_words } => QueryCondition::Search { text: text.clone(), whole_words: *whole_words },
    }
}

impl QuerySpec {
    /// A test shorthand: the record of a query written as the JSON of its fields.
    pub(crate) fn json(text: impl AsRef<str>) -> QuerySpec {
        let shorthand: Shorthand = serde_json::from_str(text.as_ref()).expect("the shorthand of a query");
        QuerySpec {
            model: shorthand.model,
            relation: shorthand.relation,
            params_json: Value::Object(shorthand.params).to_string(),
            conditions: shorthand.r#where.iter().map(condition).collect(),
            order: shorthand.order.into_iter().map(|term| QueryOrder { field: term.field, direction: term.direction }).collect(),
            limit: shorthand.limit,
            offset: shorthand.offset,
            refresh_on_connect: shorthand.refresh_on_connect,
        }
    }
}

fn spec(conditions: Vec<QueryCondition>) -> QuerySpec {
    QuerySpec { model: "Note".into(), relation: None, params_json: "{}".into(), conditions, order: Vec::new(), limit: None, offset: None, refresh_on_connect: false }
}

fn text(field: &str, value: &str) -> QueryCondition {
    QueryCondition::Equal { field: field.into(), value: Cell::Text { value: value.into() } }
}

fn expected(json: &str) -> Query {
    serde_json::from_str(json).unwrap()
}

/// P-DATA-83, P-DATA-88, P-DATA-91, P-DATA-39: a spec compiles to the query the JSON text of the same fields gave, for every operator, direction, window and condition tree.
#[test]
fn a_spec_compiles_to_the_query_of_its_fields() {
    let mut cases: Vec<(QuerySpec, Query)> = Vec::new();
    let operators = [(CompareOp::Less, "less"), (CompareOp::LessOrEqual, "less_or_equal"), (CompareOp::Greater, "greater"), (CompareOp::GreaterOrEqual, "greater_or_equal")];
    for (op, name) in operators {
        let json = format!(r#"{{"model":"Note","where":{{"type":"compare","field":"n","op":"{name}","value":3}}}}"#);
        cases.push((spec(vec![QueryCondition::Compare { field: "n".into(), op, value: Cell::Int { value: 3 } }]), expected(&json)));
    }
    let directions = [(SortDirection::Asc, "asc"), (SortDirection::Desc, "desc"), (SortDirection::AscNullsLast, "asc_nulls_last"), (SortDirection::DescNullsFirst, "desc_nulls_first")];
    for (direction, name) in directions {
        let mut ordered = spec(Vec::new());
        ordered.order = vec![QueryOrder { field: "text".into(), direction }];
        cases.push((ordered, expected(&format!(r#"{{"model":"Note","order":[{{"field":"text","direction":"{name}"}}]}}"#))));
    }
    let mut window = spec(Vec::new());
    window.limit = Some(5);
    window.offset = Some(2);
    cases.push((window, expected(r#"{"model":"Note","limit":5,"offset":2}"#)));
    cases.push((spec(vec![QueryCondition::NotEqual { field: "a".into(), value: Cell::Null }]), expected(r#"{"model":"Note","where":{"type":"not_equal","field":"a","value":null}}"#)));
    cases.push((spec(vec![QueryCondition::InList { field: "id".into(), values: vec![Cell::Text { value: "1".into() }, Cell::Real { value: 2.5 }] }]), expected(r#"{"model":"Note","where":{"type":"in_list","field":"id","values":["1",2.5]}}"#)));
    cases.push((spec(vec![QueryCondition::NotInList { field: "id".into(), values: vec![Cell::Bool { value: true }] }]), expected(r#"{"model":"Note","where":{"type":"not_in_list","field":"id","values":[true]}}"#)));
    cases.push((spec(vec![QueryCondition::IsNull { field: "a".into() }]), expected(r#"{"model":"Note","where":{"type":"is_null","field":"a"}}"#)));
    cases.push((spec(vec![QueryCondition::Search { text: "hi".into(), whole_words: true }]), expected(r#"{"model":"Note","where":{"type":"search","text":"hi","whole_words":true}}"#)));
    cases.push((
        spec(vec![QueryCondition::Not { clause: Box::new(QueryCondition::AnyOf { clauses: vec![text("a", "x"), text("b", "y")] }) }, text("c", "z")]),
        expected(r#"{"model":"Note","where":{"type":"all_of","clauses":[{"type":"not","clause":{"type":"any_of","clauses":[{"type":"equal","field":"a","value":"x"},{"type":"equal","field":"b","value":"y"}]}},{"type":"equal","field":"c","value":"z"}]}}"#),
    ));
    for (record, query) in cases {
        assert_eq!(record.query().unwrap(), query);
    }
}

#[test]
fn a_spec_refuses_a_zero_limit_and_a_value_that_is_not_a_scalar() {
    let mut zero = spec(Vec::new());
    zero.limit = Some(0);
    assert!(matches!(zero.query(), Err(DbError::Invalid { .. })));
    let list = spec(vec![QueryCondition::Equal { field: "a".into(), value: Cell::Seq { items: Vec::new() } }]);
    assert!(matches!(list.query(), Err(DbError::Invalid { .. })));
    let infinite = spec(vec![QueryCondition::Equal { field: "a".into(), value: Cell::Real { value: f64::INFINITY } }]);
    assert!(matches!(infinite.query(), Err(DbError::Invalid { .. })));
}
