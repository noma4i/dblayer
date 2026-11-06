use std::collections::BTreeMap;

use dblayer::store::ddl;
use dblayer::store::query::{compile_keys, Order, Query, Where};
use dblayer::schema::{ModelSpec, Sort};
use rusqlite::Connection;
use serde_json::{json, Value};

pub(super) fn report(models: &[ModelSpec]) -> Result<String, Box<dyn std::error::Error>> {
    let connection = Connection::open_in_memory()?;
    ddl::migrate_core(&connection)?;
    ddl::create_model_tables(&connection, models)?;
    let mut report = Vec::new();
    for model in models {
        let mut scopes = Vec::new();
        for relation in &model.relations {
            let mut query = Query::new(model.key.as_str());
            if let Sort::Fields(fields) = &relation.sort {
                query = query.ordered(fields.iter().map(|field| Order::new(&field.column, field.direction)).collect());
            }
            if relation.remote.is_some() {
                let params: BTreeMap<_, _> = relation.by_columns.iter().chain(&relation.text_params).map(|name| (name.clone(), "sample".into())).collect();
                query = query.in_scope(relation.scope_key(model.key.as_str(), &params)?);
            } else if !relation.by_columns.is_empty() {
                query = query.filtered(Where::AllOf { clauses: relation.by_columns.iter().map(|name| Where::equal(name, Value::String("sample".into()))).collect() });
            }
            let compiled = compile_keys(&query, model)?;
            let mut statement = connection.prepare(&format!("EXPLAIN QUERY PLAN {}", compiled.sql))?;
            let plan = statement.query_map(rusqlite::params_from_iter(&compiled.bindings), |row| row.get::<_, String>(3))?.collect::<Result<Vec<_>, _>>()?;
            scopes.push(json!({ "name": relation.name, "sql": compiled.sql, "plan": plan }));
        }
        report.push(json!({ "model": model.key.as_str(), "ddl": ddl::model_statements(model), "scopes": scopes }));
    }
    Ok(serde_json::to_string_pretty(&report)?)
}
