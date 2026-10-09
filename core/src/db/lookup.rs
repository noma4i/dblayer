use std::collections::HashMap;

use serde_json::{Map, Value};
use std::sync::Arc;

use crate::identity::RowId;
use crate::query::{compile_count, compile_keys, fetch_count, fetch_keys, Order, Query};
use crate::schema::Sort;
use crate::value::relation_param_text;
use crate::store::{Reader, StoreError};
use crate::syncstate::read_sync;

use super::error::DbError;
use super::open::{locked, Db, Shared};
use super::row::{Record, Row};

pub(crate) type RowKey = (String, i64);

impl Shared {
    pub(crate) fn load(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<HashMap<String, Row>, DbError> {
        let stored = reader.stored_rows(model, ids)?;
        let mut syncs = read_sync(reader.connection(), model, ids).map_err(StoreError::from)?;
        let spec = self.spec(model)?;
        let overlay = (!spec.ephemeral.is_empty()).then(|| self.ephemeral.lock());
        let mut cache = locked(&self.cache);
        let mut loaded = HashMap::with_capacity(stored.len());
        for (id, record) in stored {
            let Some(mut data) = record.row else { continue };
            let ephemeral = overlay.as_ref().map_or(0, |state| state.overlay(spec, &id, &mut data));
            let row: Row = Arc::new(Record { id: record.id, revision: record.revision, data, sync: syncs.remove(&id).unwrap_or_default(), ephemeral });
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

impl Db {
    pub fn remote_relation_query(&self, model: &str, name: &str, params: &Map<String, Value>) -> Result<Option<Query>, DbError> {
        let spec = self.spec(model)?;
        let relation = spec.relations.iter().find(|relation| relation.name == name).ok_or_else(|| DbError::UnknownAssociation { model: model.to_string(), name: name.to_string() })?;
        if relation.remote.is_none() {
            return Ok(None);
        }
        let strings = params.iter().map(|(key, value)| (key.clone(), relation_param_text(value))).collect();
        let scope = relation.scope_key(model, &strings).map_err(|_| DbError::UnknownAssociation { model: model.to_string(), name: name.to_string() })?;
        let order = match &relation.sort {
            Sort::ServerOrder => Vec::new(),
            Sort::Fields(fields) => fields.iter().map(|field| Order::new(&field.column, field.direction)).collect(),
        };
        Ok(Some(Query::new(model).in_scope(scope).ordered(order)))
    }
}
