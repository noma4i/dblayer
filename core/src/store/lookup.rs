//! The rows of the base as the platform and the owners read them: by id, by query and as a view at one revision.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::identity::touches::touch;
use crate::identity::RowId;
use crate::store::query::{compile_count, compile_keys, fetch_count, fetch_keys, Order, Query};
use crate::schema::Sort;
use crate::store::sync_state::read_sync;

use super::db_error::CoreError;
use super::record::{Record, Row};
use super::{Reader, Store, StoreError};

pub(crate) type RowKey = (String, i64);

/// A transfer that has not ended, with the value `uploads[action]` shows.
pub(crate) struct ActiveTransfer {
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub transfer_id: i64,
    pub info: crate::owners::write::upload::UploadInfo,
}

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


/// The revision of a snapshot, what the query read in it and the temporary ids swapped for server ids up to it.
pub(crate) type ViewSnapshot = (i64, ViewRead, Vec<(RowId, RowId)>);

impl Store {
    pub(crate) fn load(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<HashMap<String, Row>, CoreError> {
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
    fn finish_uploads(&self, spec: &crate::schema::ModelSpec, data: &Map<String, Value>, sync: &mut crate::value::sync::SyncInfo) {
        let files = self.files();
        for info in sync.uploads.values_mut() {
            if let Some(id) = info.copy_file.take() {
                info.local_url = files.path(&id).filter(|path| path.exists()).or_else(|| Some(files.cache_target(&id, "source")).filter(|path| path.exists())).map(|path| format!("file://{}", path.display()));
            }
        }
        for action in spec.actions.iter().filter(|action| action.upload) {
            let Some(progress) = action.processing.as_ref().and_then(|processing| processing.progress_in(data)) else { continue };
            let processing = Some(crate::owners::write::upload::UploadProcessing { progress });
            match sync.uploads.get_mut(&action.name) {
                Some(info) => info.processing = processing,
                None => {
                    sync.uploads.insert(action.name.clone(), crate::owners::write::upload::UploadInfo { status: crate::owners::write::upload::UploadStatus::Done, stage: crate::owners::write::upload::UploadStage::Mutation, sent_bytes: 0, total_bytes: 0, wait_reason: None, attempts: 0, error: None, local_url: None, processing, copy_file: None });
                }
            }
        }
    }

    fn rows_for_keys(&self, reader: &Reader<'_>, model: &str, keys: &[RowKey]) -> Result<Vec<Row>, CoreError> {
        let ids: Vec<String> = keys.iter().map(|(id, _)| id.clone()).collect();
        let loaded = self.load(reader, model, &ids)?;
        Ok(keys.iter().filter_map(|(id, _)| loaded.get(id).cloned()).collect())
    }

    pub(crate) fn query_keys(&self, reader: &Reader<'_>, query: &Query) -> Result<Vec<RowKey>, CoreError> {
        let statement = compile_keys(query, self.spec(query.model.as_str())?)?;
        let keys = fetch_keys(reader.connection(), &statement)?;
        touch(keys.len());
        Ok(keys)
    }

    pub fn find(&self, model: &str, id: &RowId) -> Result<Option<Row>, CoreError> {
        self.spec(model)?;
        let ids = [id.to_string()];
        let mut loaded = self.read_models(&[model], |reader| self.load(reader, model, &ids))?;
        Ok(loaded.remove(id.as_str()))
    }

    pub fn query(&self, query: &Query) -> Result<Vec<Row>, CoreError> {
        let model = query.model.as_str();
        self.read_models(&[model], |reader| {
            let keys = self.query_keys(reader, query)?;
            self.rows_for_keys(reader, model, &keys)
        })
    }

    pub fn count(&self, query: &Query) -> Result<i64, CoreError> {
        let statement = compile_count(query, self.spec(query.model.as_str())?)?;
        self.read_models(&[query.model.as_str()], |reader| Ok(fetch_count(reader.connection(), &statement)?))
    }

    pub fn remote_relation_query(&self, model: &str, name: &str, params: &Map<String, Value>) -> Result<Option<Query>, CoreError> {
        let spec = self.spec(model)?;
        let relation = spec.relations.iter().find(|relation| relation.name == name).ok_or_else(|| CoreError::UnknownAssociation { model: model.to_string(), name: name.to_string() })?;
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

    /// The query of a request with the scope, order and stability of its relation, and the relation with its resolved params when the window is watched.
    pub(crate) fn select_query(&self, mut query: Query, relation: Option<(&str, Map<String, Value>)>) -> Result<ParsedQuery, CoreError> {
        let remote = relation.map(|(name, params)| -> Result<_, CoreError> {
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

    /// The open operations of the queue in one snapshot that is not older than `floor`; gives the revision of the snapshot.
    pub(crate) fn read_operations(&self, floor: i64) -> Result<(i64, Vec<crate::owners::write::operation_status::Operation>), CoreError> {
        self.read_models(&[], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            Ok((seen, crate::store::operation::open_operations(reader.connection())?))
        })
    }

    /// The temporary ids of the rows of the read that were swapped after `after`: the pairs of the log for the model and the models its rows link to; when the log dropped the range, the pairs of `row_alias` for the rows of the result.
    fn replaced_of(&self, reader: &Reader<'_>, model: &str, keys: &[RowKey], after: i64, seen: i64) -> Result<Vec<(RowId, RowId)>, CoreError> {
        let models: Vec<String> = std::iter::once(model.to_string()).chain(super::plan::nested_ref::link_targets(self.spec(model)?)).collect();
        if let Some(pairs) = self.replaced().replaced_between(&models, after, seen) { return Ok(pairs); }
        let mut statement = reader.connection().prepare_cached(include_str!("sql/lookup/read_aliases_of.sql"))?;
        let mut pairs = Vec::new();
        for (id, _) in keys {
            let real = RowId::new(id).map_err(|error| CoreError::Internal(error.to_string()))?;
            for temp in statement.query_map([model, id.as_str()], |row| row.get::<_, String>(0))? {
                pairs.push((RowId::new(&temp?).map_err(|error| CoreError::Internal(error.to_string()))?, real.clone()));
            }
        }
        Ok(pairs)
    }

    /// The open operations and the active transfers in one snapshot not older than `floor`. A transfer is addressed by its key: its row may not exist.
    pub(crate) fn read_queue(&self, floor: i64) -> Result<(i64, Vec<crate::owners::write::operation_status::Operation>, Vec<ActiveTransfer>), CoreError> {
        self.read_models(&[], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            let connection = reader.connection();
            let mut statement = connection.prepare_cached("SELECT model, row_id, action, transfer_id FROM sync_upload WHERE transfer_id IS NOT NULL ORDER BY seq")?;
            let keys = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, i64>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut transfers = Vec::new();
            for (model, row_id, action, transfer_id) in keys {
                let info = read_sync(connection, &model, std::slice::from_ref(&row_id))?.remove(&row_id).and_then(|mut sync| sync.uploads.remove(&action));
                if let Some(info) = info { transfers.push(ActiveTransfer { model, row_id, action, transfer_id, info }); }
            }
            Ok((seen, crate::store::operation::open_operations(connection)?, transfers))
        })
    }

    /// The value of `uploads[action]` of a transfer by its key, whether or not the row exists.
    pub(crate) fn read_transfer(&self, model: &str, id: &RowId, action: &str, floor: i64) -> Result<(i64, Option<crate::owners::write::upload::UploadInfo>), CoreError> {
        self.spec(model)?;
        let ids = [id.to_string()];
        self.read_models(&[model], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            Ok((seen, read_sync(reader.connection(), model, &ids)?.remove(id.as_str()).and_then(|mut sync| sync.uploads.remove(action))))
        })
    }

    /// Reads `query` in one snapshot that is not older than `floor`; gives the revision of the snapshot and the temporary ids swapped for server ids after `after` up to it.
    pub(crate) fn read_view(&self, query: &Query, count: bool, floor: i64, after: i64, window: Option<&crate::owners::read::windows::WindowKey>, held: Option<HeldVersion>) -> Result<ViewSnapshot, CoreError> {
        #[cfg(test)]
        assert!(self.panic_reads.load(std::sync::atomic::Ordering::SeqCst) & super::PANIC_VIEW_READ == 0, "the snapshot read failed");
        let model = query.model.as_str();
        self.read_models(&[model], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            if count {
                let statement = compile_count(query, self.spec(model)?)?;
                return Ok((seen, ViewRead::Count(fetch_count(reader.connection(), &statement)?), Vec::new()));
            }
            let named = match held {
                Some(held) if super::plan::nested_ref::link_targets(self.spec(model)?).is_empty() => self.context.changed.changed_between(held.from, seen, held.limit),
                _ => None,
            };
            if held.is_some() && named.is_none() {
                return Ok((seen, ViewRead::Overflow, Vec::new()));
            }
            let keys = self.query_keys(reader, query)?;
            let wanted: Vec<RowKey> = match &named {
                Some(named) => keys.iter().filter(|(id, _)| named.contains(&(model.to_string(), id.clone()))).cloned().collect(),
                None => keys.clone(),
            };
            let rows = self.rows_for_keys(reader, model, &wanted)?;
            if rows.len() != wanted.len() {
                return Err(CoreError::Internal(format!("the snapshot at revision {seen} lost {} of {} rows of {model}", wanted.len() - rows.len(), wanted.len())));
            }
            self.rows_read.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
            let total = match window.map(crate::owners::read::windows::WindowKey::kind) {
                Some(crate::owners::read::windows::WindowKind::Relation { relation, params }) => {
                    let spec = self.spec(model)?;
                    let remote = spec.relations.iter().find(|declared| declared.name == relation).and_then(|declared| declared.remote.as_ref());
                    match remote {
                        Some(remote) => {
                            let (_, key) = super::window_identity(spec, model, &relation, remote, &params)?;
                            rusqlite::OptionalExtension::optional(reader.connection().query_row(include_str!("sql/lookup/read_view.sql"), [key], |row| row.get::<_, Option<i64>>(0)))?.flatten().and_then(|total| u64::try_from(total).ok())
                        }
                        None => None,
                    }
                }
                _ => None,
            };
            let stamped = wanted.into_iter().zip(rows).map(|((id, stamp), row)| (id, stamp.max(row.revision), row)).collect();
            let replaced = self.replaced_of(reader, model, &keys, after, seen)?;
            Ok((seen, if named.is_some() { ViewRead::Changes { keys, rows: stamped, total } } else { ViewRead::Rows(stamped, total) }, replaced))
        })
    }
}
