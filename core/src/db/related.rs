use serde_json::{json, Value};

use crate::identity::RowId;
use crate::query::{Query, Where};
use crate::schema::{AssociationKind, AssociationSpec};

use super::error::DbError;
use super::open::Db;
use super::row::Row;

impl Db {
    fn association(&self, model: &str, name: &str, kind: AssociationKind) -> Result<&AssociationSpec, DbError> {
        let association = self
            .shared
            .spec(model)?
            .associations
            .iter()
            .find(|association| association.name == name)
            .ok_or_else(|| DbError::UnknownAssociation { model: model.to_string(), name: name.to_string() })?;
        if association.kind != kind {
            return Err(DbError::WrongAssociation { model: model.to_string(), name: name.to_string() });
        }
        Ok(association)
    }

    pub fn belongs_to(&self, model: &str, row: &Row, name: &str) -> Result<Option<Row>, DbError> {
        let association = self.association(model, name, AssociationKind::BelongsTo)?;
        let Some(parent) = row.get(&association.foreign_key).and_then(crate::value::read_id) else {
            return Ok(None);
        };
        self.find(association.target.as_str(), &parent)
    }

    fn children_query(association: &AssociationSpec, id: &RowId) -> Query {
        Query::new(association.target.as_str()).filtered(Where::equal(&association.foreign_key, json!(id.as_str())))
    }

    pub fn has_many_query(&self, model: &str, id: &RowId, name: &str) -> Result<Query, DbError> {
        let association = self.association(model, name, AssociationKind::HasMany)?;
        Ok(Self::children_query(association, id))
    }

    pub fn has_one(&self, model: &str, row: &Row, name: &str) -> Result<Option<Row>, DbError> {
        let association = self.association(model, name, AssociationKind::HasOne)?;
        self.first(&Self::children_query(association, &row.id))
    }

    pub fn references(&self, model: &str, row: &Row, name: &str) -> Result<Vec<Row>, DbError> {
        let association = self.association(model, name, AssociationKind::References)?;
        let field = if association.foreign_key.is_empty() { &association.name } else { &association.foreign_key };
        let ids: Vec<RowId> = row.get(field).and_then(Value::as_array).into_iter().flatten().filter_map(crate::value::read_id).collect();
        self.find_many(association.target.as_str(), &ids)
    }

    pub fn has_many(&self, model: &str, row: &Row, name: &str) -> Result<Vec<Row>, DbError> {
        self.query(&self.has_many_query(model, &row.id, name)?)
    }

    pub fn has_many_count(&self, model: &str, row: &Row, name: &str) -> Result<i64, DbError> {
        self.count(&self.has_many_query(model, &row.id, name)?)
    }
}
