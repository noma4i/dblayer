//! The rows of the base as the platform and the owners read them: by id, by query and as a view at one revision.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::identity::touches::touch;
use crate::identity::RowId;
use crate::store::query::{compile_count, compile_keys, fetch_count, fetch_keys, Order, Query};
use crate::schema::Sort;
use crate::owners::write::upload::TransferFile;
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
    /// The operation of the transfer in the queue: the reserved one, or the one of its group.
    pub op_id: Option<i64>,
    pub info: Option<crate::owners::write::upload::UploadInfo>,
    pub restore_error: Option<String>,
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
pub(crate) type ViewSnapshot = (i64, u64, Option<(i64, u64)>, ViewRead, Vec<(RowId, RowId)>);

impl Store {
    fn view_version(&self, reader: &Reader<'_>, view: &crate::owners::read::deliveries::View, from: (i64, u64)) -> Result<crate::owners::read::deliveries::ReadVersion, CoreError> {
        let revision = reader.revision_seq()?;
        let position = reader.position();
        let relevant = self.context.changed.relevant(view, from, (revision, position));
        Ok(crate::owners::read::deliveries::ReadVersion { revision, position, relevant, touching: Vec::new() })
    }

    pub(crate) fn touching(&self, view: &crate::owners::read::deliveries::View, from: (i64, u64), upto: (i64, u64)) -> crate::owners::read::deliveries::Touching { self.context.changed.touching(view, from, upto) }
    pub(crate) fn load(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<HashMap<String, Row>, CoreError> {
        self.load_with_sync(reader, model, ids).map(|(rows, _)| rows)
    }

    fn load_with_sync(&self, reader: &Reader<'_>, model: &str, ids: &[String]) -> Result<(HashMap<String, Row>, HashMap<String, crate::value::sync::SyncInfo>), CoreError> {
        let stored = reader.stored_rows(model, ids)?;
        let mut syncs = read_sync(reader.connection(), model, ids).map_err(StoreError::from)?;
        let spec = self.spec(model)?;
        let linked = !super::plan::nested_ref::link_targets(spec).is_empty();
        let mut built = HashMap::with_capacity(stored.len());
        for (id, record) in stored {
            let data = record.row;
            let mut sync = syncs.remove(&id).unwrap_or_default();
            self.finish_uploads(spec, &data, &mut sync);
            built.insert(id, Arc::new(Record { id: record.id, revision: record.revision, data, sync }));
        }
        if linked { self.resolve_nested_refs(reader, spec, &mut built)?; }
        Ok((built, syncs))
    }

    /// `uploads[action]` of a row: the file URL of the copy while the transfer holds one, and the processing the server reports in the row after the transfer.
    fn finish_uploads(&self, spec: &crate::schema::ModelSpec, data: &Map<String, Value>, sync: &mut crate::value::sync::SyncInfo) {
        let files = self.files();
        for info in sync.uploads.values_mut() {
            if let Some(file) = info.copy_file.take() {
                let path = match file {
                    TransferFile::Copy(id) => files.path(&id).filter(|path| path.exists()).or_else(|| Some(files.cache_target(&id, "source")).filter(|path| path.exists())),
                    TransferFile::Caller(path) => Some(std::path::PathBuf::from(path)).filter(|path| path.exists()),
                };
                info.local_url = path.map(|path| format!("file://{}", path.display()));
            }
        }
        for action in spec.actions.iter().filter(|action| action.upload) {
            let Some(progress) = action.processing.as_ref().and_then(|processing| processing.progress_in(data)) else { continue };
            let processing = Some(crate::owners::write::upload::UploadProcessing { progress });
            match sync.uploads.get_mut(&action.name) {
                Some(info) => info.processing = processing,
                None => {
                    sync.uploads.insert(action.name.clone(), crate::owners::write::upload::UploadInfo { status: crate::owners::write::upload::UploadStatus::Done, stage: crate::owners::write::upload::UploadStage::Mutation, sent_bytes: 0, total_bytes: 0, wait_reason: None, attempts: 0, error: None, local_url: None, processing, copy_file: None, share: 100, batch_share: None });
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
        self.read_models(&[model], |reader| {
            let id = super::readers::resolve_in(reader, model, id)?;
            Ok(self.load(reader, model, &[id.to_string()])?.remove(id.as_str()))
        })
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

    /// The temporary ids of the rows of the read that were swapped after `after`: the pairs of the log for the model and the models its rows link to; when the log dropped the range, the pairs of `row_alias` for the rows of the result.
    fn replaced_of(&self, reader: &Reader<'_>, model: &str, keys: &[RowKey], after: i64, seen: i64) -> Result<Vec<(RowId, RowId)>, CoreError> {
        let models: Vec<String> = std::iter::once(model.to_string()).chain(super::plan::nested_ref::link_targets(self.spec(model)?)).collect();
        if let Some(pairs) = self.replaced().replaced_between(&models, after, seen) { return Ok(pairs); }
        let mut statement = reader.connection().prepare_cached(include_str!("sql/lookup/read_aliases_of.sql"))?;
        let ids: Vec<&str> = keys.iter().map(|(id, _)| id.as_str()).collect();
        let ids = serde_json::to_string(&ids).map_err(|error| CoreError::Internal(error.to_string()))?;
        let mut pairs = Vec::new();
        for pair in statement.query_map(rusqlite::params![model, ids], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))? {
            let (temp, real) = pair?;
            pairs.push((RowId::new(&temp).map_err(|error| CoreError::Internal(error.to_string()))?, RowId::new(&real).map_err(|error| CoreError::Internal(error.to_string()))?));
        }
        Ok(pairs)
    }

    /// The open operations and the active transfers in one snapshot not older than `floor`. A transfer is addressed by its key and linked to its operation by identity: its row may not exist.
    pub(crate) fn read_queue(&self, floor: i64, from: (i64, u64), view: &crate::owners::read::deliveries::View) -> Result<(crate::owners::read::deliveries::ReadVersion, Vec<crate::owners::write::operation_status::Operation>, Vec<ActiveTransfer>), CoreError> {
        let models: Vec<String> = self.read_models(&[], |reader| {
            let mut statement = reader.connection().prepare_cached("SELECT DISTINCT model FROM sync_upload WHERE transfer_id IS NOT NULL")?;
            let found = statement.query_map([], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(found)
        })?;
        let pinned: Vec<&str> = models.iter().map(String::as_str).collect();
        self.read_models(&pinned, |reader| {
            let version = self.view_version(reader, view, from)?;
            let seen = version.revision;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            let connection = reader.connection();
            let mut links = connection.prepare_cached("SELECT transfer_id, op_id FROM op_transfer")?;
            let linked: HashMap<i64, i64> = links.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?.collect::<rusqlite::Result<_>>()?;
            let mut statement = connection.prepare_cached("SELECT model, row_id, action, transfer_id, op_id FROM sync_upload WHERE transfer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM outcome WHERE outcome.owner = 'transfer' AND outcome.id = sync_upload.transfer_id AND outcome.cycle_id = sync_upload.cycle_id AND outcome.kind IN ('confirmed', 'discarded')) ORDER BY seq")?;
            let keys = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, i64>(3)?, row.get::<_, Option<i64>>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
            for (model, row_id, _, _, _) in &keys { grouped.entry(model.clone()).or_default().push(row_id.clone()); }
            let mut shown = HashMap::new();
            for (model, ids) in grouped {
                let (rows, mut syncs) = self.load_with_sync(reader, &model, &ids)?;
                for (id, row) in rows { shown.insert((model.clone(), id), row.sync.uploads.clone()); }
                for (id, mut sync) in syncs.drain() {
                    self.finish_uploads(self.spec(&model)?, &Map::new(), &mut sync);
                    shown.insert((model.clone(), id), sync.uploads);
                }
            }
            let mut transfers = Vec::new();
            for (model, row_id, action, transfer_id, reserved) in keys {
                if let Some(info) = shown.get(&(model.clone(), row_id.clone())).and_then(|uploads| uploads.get(&action)).cloned() {
                    transfers.push(ActiveTransfer { op_id: linked.get(&transfer_id).copied().or(reserved), model, row_id, action, transfer_id, info: Some(info), restore_error: None });
                }
            }
            if let Some(virtual_transfers) = reader.virtual_transfers() {
                for (key, transfer) in virtual_transfers {
                    let Some((model, row_id, action)) = super::upload_rows::split_transfer_key(key) else { continue };
                    let transfer_id = i64::try_from(transfer.transfer_id).map_err(|_| CoreError::Internal("transfer id overflow".into()))?;
                    let op_id = transfer.op_id.and_then(|id| i64::try_from(id).ok());
                    transfers.push(ActiveTransfer { model: model.into(), row_id: row_id.into(), action: action.into(), transfer_id, op_id, info: None, restore_error: Some(transfer.error.clone()) });
                }
            }
            Ok((version, crate::store::operation::open_operations(connection)?, transfers))
        })
    }

    /// The value of `uploads[action]` of a transfer: the row shows it when it exists, the key alone gives it when the row does not.
    fn transfer_info(&self, reader: &Reader<'_>, (model, row_id): (&str, &str), action: &str) -> Result<Option<crate::owners::write::upload::UploadInfo>, CoreError> {
        let ids = [row_id.to_string()];
        let (mut rows, mut syncs) = self.load_with_sync(reader, model, &ids)?;
        if let Some(row) = rows.remove(row_id) { return Ok(row.sync.uploads.get(action).cloned()); }
        let Some(mut sync) = syncs.remove(row_id) else { return Ok(None) };
        self.finish_uploads(self.spec(model)?, &Map::new(), &mut sync);
        Ok(sync.uploads.remove(action))
    }

    /// The value of `uploads[action]` of a transfer by its key, whether or not the row exists.
    pub(crate) fn read_transfer(&self, model: &str, id: &RowId, action: &str, floor: i64, from: (i64, u64), view: &crate::owners::read::deliveries::View) -> Result<(crate::owners::read::deliveries::ReadVersion, Option<crate::owners::write::upload::UploadInfo>), CoreError> {
        self.spec(model)?;
        self.read_models(&[model], |reader| {
            let version = self.view_version(reader, view, from)?;
            let seen = version.revision;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            Ok((version, self.transfer_info(reader, (model, id.as_str()), action)?))
        })
    }

    /// Reads `query` in one snapshot that is not older than `floor`; gives the revision of the snapshot and the temporary ids swapped for server ids after `after` up to it.
    pub(crate) fn read_view(&self, query: &Query, count: bool, floor: i64, after: i64, window: Option<&crate::owners::read::windows::WindowKey>, held: Option<HeldVersion>, view: &crate::owners::read::deliveries::View, from: (i64, u64)) -> Result<ViewSnapshot, CoreError> {
        #[cfg(test)]
        assert!(self.panic_reads.load(std::sync::atomic::Ordering::SeqCst) & super::PANIC_VIEW_READ == 0, "the snapshot read failed");
        #[cfg(test)]
        if let Some(gate) = self.view_gate.lock().unwrap_or_else(|error| error.into_inner()).clone() { gate.wait_blocking("the view read waits for its gate"); }
        let model = query.model.as_str();
        self.read_models(&[model], |reader| {
            let version = self.view_version(reader, view, from)?;
            let seen = version.revision;
            if seen < floor {
                return Err(CoreError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            let position = version.position;
            let relevant = version.relevant;
            if count {
                let statement = compile_count(query, self.spec(model)?)?;
                return Ok((seen, position, relevant, ViewRead::Count(fetch_count(reader.connection(), &statement)?), Vec::new()));
            }
            let named = match held {
                Some(held) if super::plan::nested_ref::link_targets(self.spec(model)?).is_empty() => self.context.changed.changed_between(held.from, (seen, reader.position()), held.limit),
                _ => None,
            };
            if held.is_some() && named.is_none() {
                return Ok((seen, position, relevant, ViewRead::Overflow, Vec::new()));
            }
            let keys = self.query_keys(reader, query)?;
            let table = crate::store::ddl::table_name(model);
            let wanted: Vec<RowKey> = match &named {
                Some(named) => keys.iter().filter(|(id, _)| named.contains(&(table.clone(), id.clone()))).cloned().collect(),
                None => keys.clone(),
            };
            let rows = self.rows_for_keys(reader, model, &wanted)?;
            if rows.len() != wanted.len() {
                return Err(CoreError::Internal(format!("the snapshot at revision {seen} lost {} of {} rows of {model}", wanted.len() - rows.len(), wanted.len())));
            }
            self.rows_read.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
            let total = match window.map(crate::owners::read::windows::WindowKey::kind).transpose().map_err(crate::store::StoreError::Constraint)? {
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
            Ok((seen, position, relevant, if named.is_some() { ViewRead::Changes { keys, rows: stamped, total } } else { ViewRead::Rows(stamped, total) }, replaced))
        })
    }
}
