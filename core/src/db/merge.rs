use serde_json::{Map, Value};

use crate::commit::WriteOrigin;
use crate::store::{admit_upsert, Reader, UpsertAdmission};
use crate::value::read_id;

use super::error::DbError;
use super::open::Db;

impl Db {
    pub(crate) fn admitted(&self, reader: &Reader<'_>, origin: WriteOrigin, base: Option<i64>, closing: Option<i64>, model: &str, patch: &Map<String, Value>, drops: &mut usize) -> Result<Option<Map<String, Value>>, DbError> {
        let Some(id) = patch.get("id").and_then(read_id) else { return Ok(None) };
        if origin == WriteOrigin::Optimistic {
            return Ok(Some(patch.clone()));
        }
        let stored = reader.stored_row(model, id.as_str())?;
        let tombstone = reader.tombstone_seq(model, id.as_str())?;
        let (mut row, mut dropped) = match admit_upsert(base, stored.as_ref(), tombstone, patch.clone()) {
            UpsertAdmission::Row { row, dropped_fields } => (row, !dropped_fields.is_empty()),
            UpsertAdmission::DropExistence => {
                *drops += 1;
                return Ok(stored.map(|stored| stored.row));
            }
        };
        for field in super::prior::Prior::unconfirmed_fields(reader, closing, model, id.as_str())? {
            dropped |= row.contains_key(&field);
            match stored.as_ref().and_then(|stored| stored.row.get(&field)) {
                Some(value) => row.insert(field.clone(), value.clone()),
                None => row.remove(&field),
            };
        }
        *drops += usize::from(dropped);
        self.merged(model, stored.as_ref().map(|stored| &stored.row), &row)
    }

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
