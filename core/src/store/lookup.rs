//! The rows of the base as the platform and the owners read them: by id, by query and as a view at one revision.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::identity::touches::touch;
use crate::identity::RowId;
use crate::query::{compile_count, compile_keys, fetch_count, fetch_keys, Order, Query};
use crate::schema::Sort;
use crate::syncstate::read_sync;

use super::db_error::DbError;
use super::record::{Record, Row};
use super::{Reader, Store, StoreError};

pub(crate) type RowKey = (String, i64);

/// A query with the relation of its window: `Some((name, params))` when the relation is remote and watched.
pub(crate) type ParsedQuery = (Query, Option<(String, Map<String, Value>)>);

pub(crate) enum ViewRead {
    /// The rows and the total the server gave for their window.
    Rows(Vec<(String, i64, Row)>, Option<u64>),
    Count(i64),
    Changes { keys: Vec<(String, i64)>, rows: Vec<(String, i64, Row)>, total: Option<u64> },
    Overflow,
}

#[derive(Clone, Copy)]
pub(crate) struct HeldVersion {
    pub from: (i64, u64),
    pub limit: usize,
}

fn fault(error: serde_json::Error) -> DbError { DbError::Internal(error.to_string()) }

impl Store {
    pub(crate) fn load(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<HashMap<String, Row>, DbError> {
        let stored = reader.stored_rows(model, ids)?;
        let mut syncs = read_sync(reader.connection(), model, ids).map_err(StoreError::from)?;
        let spec = self.spec(model)?;
        let linked = !super::plan::nested_ref::link_targets(spec).is_empty();
        let mut built = HashMap::with_capacity(stored.len());
        for (id, record) in stored {
            let mut data = record.row;
            let mut stamp = record.revision;
            let mut sync = syncs.remove(&id).unwrap_or_default();
            self.finish_uploads(spec, &data, &mut sync);
            if linked {
                stamp = stamp.max(self.resolve_nested_refs(reader, spec, &mut data)?);
            }
            built.insert(id, Arc::new(Record { id: record.id, revision: stamp, data, sync }));
        }
        Ok(built)
    }

    /// `uploads[action]` of a row: the file URL of the copy while the transfer holds one, and the processing the server reports in the row after the transfer.
    fn finish_uploads(&self, spec: &crate::schema::ModelSpec, data: &Map<String, Value>, sync: &mut crate::syncstate::SyncInfo) {
        let files = self.files();
        for info in sync.uploads.values_mut() {
            if let Some(id) = info.copy_file.take() {
                info.local_url = files.path(&id).filter(|path| path.exists()).or_else(|| Some(files.cache_target(&id, "source")).filter(|path| path.exists())).map(|path| format!("file://{}", path.display()));
            }
        }
        for action in spec.actions.iter().filter(|action| action.upload) {
            let Some(progress) = action.processing.as_ref().and_then(|processing| processing.progress_in(data)) else { continue };
            let processing = Some(crate::upload::UploadProcessing { progress });
            match sync.uploads.get_mut(&action.name) {
                Some(info) => info.processing = processing,
                None => {
                    sync.uploads.insert(action.name.clone(), crate::upload::UploadInfo { status: crate::upload::UploadStatus::Done, stage: crate::upload::UploadStage::Mutation, sent_bytes: 0, total_bytes: 0, wait_reason: None, attempts: 0, error: None, local_url: None, processing, copy_file: None });
                }
            }
        }
    }

    fn rows_for_keys(&self, reader: &Reader<'_>, model: &str, keys: &[RowKey]) -> Result<Vec<Row>, DbError> {
        let ids: Vec<String> = keys.iter().map(|(id, _)| id.clone()).collect();
        let loaded = self.load(reader, model, &ids)?;
        Ok(keys.iter().filter_map(|(id, _)| loaded.get(id).cloned()).collect())
    }

    pub(crate) fn query_keys(&self, reader: &Reader<'_>, query: &Query) -> Result<Vec<RowKey>, DbError> {
        let statement = compile_keys(query, self.spec(query.model.as_str())?)?;
        let keys = fetch_keys(reader.connection(), &statement)?;
        touch(keys.len());
        Ok(keys)
    }

    pub fn find(&self, model: &str, id: &RowId) -> Result<Option<Row>, DbError> {
        self.spec(model)?;
        let ids = [id.to_string()];
        let mut loaded = self.read_models(&[model], |reader| self.load(reader, model, &ids))?;
        Ok(loaded.remove(id.as_str()))
    }

    pub fn query(&self, query: &Query) -> Result<Vec<Row>, DbError> {
        let model = query.model.as_str();
        self.read_models(&[model], |reader| {
            let keys = self.query_keys(reader, query)?;
            self.rows_for_keys(reader, model, &keys)
        })
    }

    pub fn count(&self, query: &Query) -> Result<i64, DbError> {
        let statement = compile_count(query, self.spec(query.model.as_str())?)?;
        self.read_models(&[query.model.as_str()], |reader| Ok(fetch_count(reader.connection(), &statement)?))
    }

    pub fn remote_relation_query(&self, model: &str, name: &str, params: &Map<String, Value>) -> Result<Option<Query>, DbError> {
        let spec = self.spec(model)?;
        let relation = spec.relations.iter().find(|relation| relation.name == name).ok_or_else(|| DbError::UnknownAssociation { model: model.to_string(), name: name.to_string() })?;
        if relation.remote.is_none() {
            return Ok(None);
        }
        let scope = spec.selection_key(name, params)?;
        let order = match &relation.sort {
            Sort::ServerOrder if relation.members_complete => vec![Order::new("id", crate::value::SortDirection::Asc)],
            Sort::ServerOrder => Vec::new(),
            Sort::Fields(fields) => fields.iter().map(|field| Order::new(&field.column, field.direction)).collect(),
        };
        Ok(Some(Query { stable: relation.order_stable || relation.members_complete, ..Query::new(model).in_scope(scope).ordered(order) }))
    }

    pub(crate) fn parse_query(&self, json: &str) -> Result<ParsedQuery, DbError> {
        let description: Value = serde_json::from_str(json).map_err(fault)?;
        let mut query: Query = serde_json::from_value(description.clone()).map_err(fault)?;
        let remote = description["relation"].as_str().map(|name| -> Result<_, DbError> {
            let params: Map<String, Value> = description.get("params").map(|value| serde_json::from_value(value.clone())).transpose().map_err(fault)?.unwrap_or_default();
            let params = self.spec(query.model.as_str())?.selection_values(name, &params)?;
            let relation_query = self.remote_relation_query(query.model.as_str(), name, &params)?;
            let watch = relation_query.is_some() || self.spec(query.model.as_str())?.relations.iter().any(|relation| relation.name == name && !relation.updates.is_empty());
            if let Some(relation_query) = relation_query {
                query.scope = relation_query.scope;
                query.stable = relation_query.stable;
                if query.order.is_empty() { query.order = relation_query.order; }
            }
            Ok(watch.then_some((name.to_string(), params)))
        }).transpose()?.flatten();
        Ok((query, remote))
    }

    /// Reads `query` in one snapshot that is not older than `floor`; gives the revision of the snapshot.
    pub(crate) fn read_view(&self, query: &Query, count: bool, floor: i64, window: Option<&crate::owners::read::windows::WindowKey>, held: Option<HeldVersion>) -> Result<(i64, ViewRead), DbError> {
        let model = query.model.as_str();
        self.read_models(&[model], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(DbError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            if count {
                let statement = compile_count(query, self.spec(model)?)?;
                return Ok((seen, ViewRead::Count(fetch_count(reader.connection(), &statement)?)));
            }
            let named = match held {
                Some(held) if super::plan::nested_ref::link_targets(self.spec(model)?).is_empty() => self.context.changed.changed_between(held.from, seen, held.limit),
                _ => None,
            };
            if held.is_some() && named.is_none() {
                return Ok((seen, ViewRead::Overflow));
            }
            let keys = self.query_keys(reader, query)?;
            let wanted: Vec<RowKey> = match &named {
                Some(named) => keys.iter().filter(|(id, _)| named.contains(&(model.to_string(), id.clone()))).cloned().collect(),
                None => keys.clone(),
            };
            let rows = self.rows_for_keys(reader, model, &wanted)?;
            if rows.len() != wanted.len() {
                return Err(DbError::Internal(format!("the snapshot at revision {seen} lost {} of {} rows of {model}", wanted.len() - rows.len(), wanted.len())));
            }
            self.rows_read.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
            let total = match window.map(crate::owners::read::windows::WindowKey::kind) {
                Some(crate::owners::read::windows::WindowKind::Relation { relation, params }) => {
                    let spec = self.spec(model)?;
                    let remote = spec.relations.iter().find(|declared| declared.name == relation).and_then(|declared| declared.remote.as_ref());
                    match remote {
                        Some(remote) => {
                            let (_, key) = super::window_identity(spec, model, &relation, remote, &params)?;
                            rusqlite::OptionalExtension::optional(reader.connection().query_row("SELECT total FROM query_window WHERE key = ?1", [key], |row| row.get::<_, Option<i64>>(0)))?.flatten().and_then(|total| u64::try_from(total).ok())
                        }
                        None => None,
                    }
                }
                _ => None,
            };
            let stamped = wanted.into_iter().zip(rows).map(|((id, stamp), row)| (id, stamp.max(row.revision), row)).collect();
            Ok((seen, if named.is_some() { ViewRead::Changes { keys, rows: stamped, total } } else { ViewRead::Rows(stamped, total) }))
        })
    }
}
