use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

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
            let query = Query::new(model).filtered(Where::equal(field, json!(id.as_str())));
            for (child_id, _) in self.shared.query_keys(reader, &query)? {
                if let Some(row) = reader.point(model, &child_id)? {
                    rows.push(ReferencingRow { model: model.clone(), field: field.clone(), row });
                }
            }
        }
        Ok(rows)
    }
}
