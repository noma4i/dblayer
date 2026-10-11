use serde_json::{Map, Value};

use super::error::DbError;
use super::open::Db;

impl Db {
    pub(crate) fn merged(&self, model: &str, stored: Option<&Map<String, Value>>, patch: &Map<String, Value>) -> Result<Option<Map<String, Value>>, DbError> {
        let Some(mut row) = self.shared.spec(model)?.merged(stored, patch) else { return Ok(None) };
        let Some(stored) = stored else { return Ok(Some(row)) };
        for association in self.shared.pointer_guards.get(model).into_iter().flatten() {
            let moves = association.sets.keys().any(|field| row.get(field) != stored.get(field));
            if !moves || association.parent_is_newer(stored, &row) {
                continue;
            }
            for field in association.sets.keys() {
                match stored.get(field) {
                    Some(value) => row.insert(field.clone(), value.clone()),
                    None => row.remove(field),
                };
            }
        }
        Ok(Some(row))
    }
}
