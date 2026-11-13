use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::store::ddl::{sql_ident, FieldPlan, Member};
use crate::identity::RowId;
use crate::store::query::{compile_keys, fetch_keys, Query, Where};
use crate::schema::AssociationKind;

use super::{ModelInfo, Reader, StoreError};
use super::row_write::RowWrite;

pub(super) struct ReferencingRow {
    pub model: String,
    pub field: String,
    pub row: Map<String, Value>,
}

pub(super) fn references_to(models: &BTreeMap<String, ModelInfo>, model: &str) -> Vec<(String, String)> {
    let mut found = BTreeSet::new();
    for info in models.values() {
        let spec = &info.spec;
        for association in &spec.associations {
            if association.kind == AssociationKind::BelongsTo && association.target.as_str() == model {
                found.insert((spec.key.to_string(), association.foreign_key.clone()));
            }
            if association.kind == AssociationKind::References && association.target.as_str() == model {
                let field = if association.foreign_key.is_empty() { &association.name } else { &association.foreign_key };
                found.insert((spec.key.to_string(), field.clone()));
            }
            if association.owns_children() && spec.key.as_str() == model {
                found.insert((association.target.to_string(), association.foreign_key.clone()));
            }
        }
    }
    found.into_iter().collect()
}

pub(super) fn rows_referencing(writer: &mut RowWrite<'_>, references: &[(String, String)], id: &RowId) -> Result<Vec<ReferencingRow>, StoreError> {
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let mut matches = Vec::new();
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for (model, field) in references {
        let info = models.get(model).ok_or_else(|| StoreError::UnknownModel(model.clone()))?;
        let layout = &info.layout;
        let array = match layout.plan(field) {
            Some(FieldPlan::Typed { member: Member::List { table, .. }, .. }) => Some(format!(include_str!("sql/references/rows_referencing_select.sql"), sql_ident(&layout.tables[*table].name))),
            Some(FieldPlan::Typed { member: Member::Raw { table, .. }, .. }) => Some(format!(include_str!("sql/references/rows_referencing_select_2.sql"), sql_ident(&layout.tables[*table].name))),
            Some(FieldPlan::Rest { .. }) => layout.value_table.as_ref().map(|table| format!(include_str!("sql/references/rows_referencing_select_3.sql"), sql_ident(table))),
            _ => None,
        };
        let ids = if let Some(sql) = array {
            let mut statement = reader.connection().prepare_cached(&sql)?;
            let values = if statement.parameter_count() == 2 { vec![id.as_str(), field.as_str()] } else { vec![id.as_str()] };
            let matches = statement.query_map(rusqlite::params_from_iter(values), |row| row.get::<_, String>(0))?;
            matches.collect::<Result<Vec<_>, _>>()?
        } else {
            let query = Query::new(model).filtered(Where::equal(field, json!(id.as_str())));
            let statement = compile_keys(&query, &info.spec).map_err(|error| StoreError::Constraint(error.to_string()))?;
            fetch_keys(reader.connection(), &statement).map_err(|error| StoreError::Constraint(error.to_string()))?.into_iter().map(|(id, _)| id).collect()
        };
        for child_id in ids {
            wanted.entry(model.clone()).or_default().push(child_id.clone());
            matches.push((model.clone(), field.clone(), child_id));
        }
    }
    for (model, ids) in wanted { writer.prefetch(&model, &ids, &writer.closing.clone())?; }
    let mut rows = Vec::new();
    for (model, field, id) in matches {
        if let Some(row) = writer.stored(&model, &id)? { rows.push(ReferencingRow { model, field, row: row.row }); }
    }
    Ok(rows)
}
