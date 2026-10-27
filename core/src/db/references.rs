use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use crate::ddl::{layout, sql_ident, FieldPlan, Member};
use crate::identity::RowId;
use crate::query::{Query, Where};
use crate::schema::AssociationKind;
use crate::store::Reader;

use super::error::DbError;
use super::open::Db;

pub(crate) struct ReferencingRow {
    pub model: String,
    pub field: String,
    pub row: Map<String, Value>,
}

impl Db {
    pub(crate) fn references_to(&self, model: &str) -> Vec<(String, String)> {
        let mut found = BTreeSet::new();
        for spec in self.shared.specs.values() {
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

    pub(crate) fn rows_referencing(&self, reader: &Reader<'_>, references: &[(String, String)], id: &RowId) -> Result<Vec<ReferencingRow>, DbError> {
        let mut rows = Vec::new();
        for (model, field) in references {
            let layout = layout(self.shared.spec(model)?);
            let array = match layout.plan(field) {
                Some(FieldPlan::Typed { member: Member::List { table, .. }, .. }) => Some(format!("SELECT DISTINCT row_id FROM {} WHERE value = ?1", sql_ident(&layout.tables[*table].name))),
                Some(FieldPlan::Typed { member: Member::Raw { table, .. }, .. }) => Some(format!("SELECT DISTINCT row_id FROM {} WHERE parent = 0 AND text = ?1", sql_ident(&layout.tables[*table].name))),
                Some(FieldPlan::Rest { .. }) => layout.value_table.as_ref().map(|table| format!("SELECT DISTINCT row_id FROM {} WHERE field = ?2 AND parent = 0 AND text = ?1", sql_ident(table))),
                _ => None,
            };
            let ids = if let Some(sql) = array {
                let mut statement = reader.connection().prepare_cached(&sql)?;
                let values = if statement.parameter_count() == 2 { vec![id.as_str(), field.as_str()] } else { vec![id.as_str()] };
                let matches = statement.query_map(rusqlite::params_from_iter(values), |row| row.get::<_, String>(0))?;
                matches.collect::<Result<Vec<_>, _>>()?
            } else {
                let query = Query::new(model).filtered(Where::equal(field, json!(id.as_str())));
                self.shared.query_keys(reader, &query)?.into_iter().map(|(id, _)| id).collect()
            };
            for child_id in ids {
                if let Some(row) = reader.point(model, &child_id)? {
                    rows.push(ReferencingRow { model: model.clone(), field: field.clone(), row });
                }
            }
        }
        Ok(rows)
    }
}
