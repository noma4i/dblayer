use std::collections::HashMap;
use std::sync::Arc;

use crate::identity::RowId;
use crate::query::{compile_count, compile_keys, fetch_count, fetch_keys, Query};
use crate::store::Reader;

use super::error::DbError;
use super::open::{locked, Db, Shared};
use super::row::{Record, Row};

pub(crate) type RowKey = (String, i64);

impl Shared {
    pub(crate) fn load(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<HashMap<String, Row>, DbError> {
        let stored = reader.stored_rows(model, ids)?;
        let mut cache = locked(&self.cache);
        let mut loaded = HashMap::with_capacity(stored.len());
        for (id, record) in stored {
            let Some(data) = record.row else { continue };
            let row: Row = Arc::new(Record { id: record.id, revision: record.revision, data });
            cache.put(model, row.clone());
            loaded.insert(id, row);
        }
        Ok(loaded)
    }

    pub(crate) fn rows_for_keys(&self, reader: &Reader<'_>, model: &str, keys: &[RowKey]) -> Result<Vec<Row>, DbError> {
        let mut slots: Vec<Option<Row>> = Vec::with_capacity(keys.len());
        let mut missing: Vec<String> = Vec::new();
        {
            let mut cache = locked(&self.cache);
            for (id, revision) in keys {
                let hit = cache.fresh(model, id, *revision);
                if hit.is_none() {
                    missing.push(id.clone());
                }
                slots.push(hit);
            }
        }
        if !missing.is_empty() {
            let loaded = self.load(reader, model, &missing)?;
            for (slot, (id, _)) in slots.iter_mut().zip(keys) {
                if slot.is_none() {
                    *slot = loaded.get(id).cloned();
                }
            }
        }
        Ok(slots.into_iter().flatten().collect())
    }

    pub(crate) fn query_keys(&self, reader: &Reader<'_>, query: &Query) -> Result<Vec<RowKey>, DbError> {
        let statement = compile_keys(query, self.spec(query.model.as_str())?)?;
        Ok(fetch_keys(reader.connection(), &statement)?)
    }
}

impl Db {
    pub fn find(&self, model: &str, id: &RowId) -> Result<Option<Row>, DbError> {
        self.shared.spec(model)?;
        if let Some(row) = locked(&self.shared.cache).get(model, id.as_str()) {
            return Ok(Some(row));
        }
        let ids = [id.to_string()];
        let mut loaded = self.shared.read(|reader| self.shared.load(reader, model, &ids))?;
        Ok(loaded.remove(id.as_str()))
    }

    pub fn find_many(&self, model: &str, ids: &[RowId]) -> Result<Vec<Row>, DbError> {
        self.shared.spec(model)?;
        let mut slots: Vec<Option<Row>> = Vec::with_capacity(ids.len());
        let mut missing: Vec<String> = Vec::new();
        {
            let mut cache = locked(&self.shared.cache);
            for id in ids {
                let hit = cache.get(model, id.as_str());
                if hit.is_none() {
                    missing.push(id.to_string());
                }
                slots.push(hit);
            }
        }
        if !missing.is_empty() {
            let loaded = self.shared.read(|reader| self.shared.load(reader, model, &missing))?;
            for (slot, id) in slots.iter_mut().zip(ids) {
                if slot.is_none() {
                    *slot = loaded.get(id.as_str()).cloned();
                }
            }
        }
        Ok(slots.into_iter().flatten().collect())
    }

    pub fn query(&self, query: &Query) -> Result<Vec<Row>, DbError> {
        let model = query.model.as_str();
        self.shared.read(|reader| {
            let keys = self.shared.query_keys(reader, query)?;
            self.shared.rows_for_keys(reader, model, &keys)
        })
    }

    pub fn count(&self, query: &Query) -> Result<i64, DbError> {
        let statement = compile_count(query, self.shared.spec(query.model.as_str())?)?;
        self.shared.read(|reader| Ok(fetch_count(reader.connection(), &statement)?))
    }

    pub fn exists(&self, query: &Query) -> Result<bool, DbError> {
        let probe = Query { limit: Some(1), ..query.clone() };
        self.shared.read(|reader| Ok(!self.shared.query_keys(reader, &probe)?.is_empty()))
    }

    pub fn first(&self, query: &Query) -> Result<Option<Row>, DbError> {
        let probe = Query { limit: Some(1), ..query.clone() };
        Ok(self.query(&probe)?.into_iter().next())
    }
}
