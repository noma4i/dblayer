use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::commit::WindowMarker;
use crate::schema::{Coverage, MediaSpec, RemoteShape, VariableSource};
use crate::store::PageInfo;
use crate::transport::GraphQLRequest;

use super::extract::{extract, root_value};
use super::land::{Landed, Landing};
use super::load::{stamp_scope_values, Executed, LoadOutcome, LoadStatus, Resolved};
use super::normalize::normalize_nodes;
use super::registry::OperationDef;
use super::variables::load_variables;
use super::{locked, Inner, SyncError};

fn bound(params: &Map<String, Value>, sources: &BTreeMap<String, VariableSource>) -> Map<String, Value> {
    sources.iter().filter_map(|(variable, source)| match source {
        VariableSource::Param { name } => params.get(name).map(|value| (variable.clone(), value.clone())),
        VariableSource::Constant { value } => Some((variable.clone(), value.clone())),
    }).collect()
}

fn integer(value: &Value, field: &str) -> Result<i64, SyncError> {
    value.as_i64().or_else(|| value.as_str().and_then(|text| text.parse().ok())).ok_or_else(|| SyncError::Malformed(format!("marker {field} must be an integer")))
}

impl Inner {
    fn media_operation(&self, name: &str) -> Result<Arc<OperationDef>, SyncError> {
        locked(&self.operations).get(name).cloned().ok_or_else(|| SyncError::UnknownOperation(name.to_string()))
    }

    async fn media_marker(&self, resolved: &Resolved<'_>, media: &MediaSpec, prefix: i64) -> Result<WindowMarker, SyncError> {
        let operation = self.media_operation(&media.marker)?;
        let mut variables = bound(&resolved.params, &media.marker_variables);
        variables.insert("upToSequence".into(), json!(prefix));
        let (response, _) = self.query_with_retry(GraphQLRequest::new(&operation.document, &operation.name, Value::Object(variables))).await?;
        let data = response.data.ok_or_else(|| SyncError::Malformed("marker response has no data".into()))?;
        let row = root_value(&data, &media.from).and_then(Value::as_object).ok_or_else(|| SyncError::Malformed(format!("marker {} is not an object", media.from)))?;
        let get = |name: &str| integer(row.get(name).ok_or_else(|| SyncError::Malformed(format!("marker has no {name}")))?, name);
        let sequence = get(&media.sequence)?;
        if sequence < 0 { return Err(SyncError::Malformed("marker sequence must not be negative".into())); }
        let values = media.fields.iter().map(|field| Ok((field.clone(), get(field)?))).collect::<Result<_, SyncError>>()?;
        Ok(WindowMarker { sequence, values })
    }

    pub(super) async fn media_load(&self, resolved: &Resolved<'_>, media: &MediaSpec) -> Result<Executed, SyncError> {
        let attempt = self.begin_attempt(&resolved.window_key)?;
        let stored = self.db.shared.store.marker(&resolved.window_key, attempt.generation.value() as i64)?;
        let prefix = stored.as_ref().map_or(0, |marker| marker.sequence);
        let before = self.media_marker(resolved, media, prefix).await?;
        let tail = stored.as_ref().is_some_and(|stored| stored.values == before.values);
        let window = self.db.shared.store.window(&resolved.window_key)?;
        let mut nodes = Vec::new();
        let mut page_info = PageInfo { has_next_page: false, end_cursor: window.as_ref().and_then(|window| window.cursor.clone()), total: window.as_ref().and_then(|window| window.total) };
        let mut attempts = 0;
        let mut confirmed = before.clone();
        let mut stable = true;
        if !tail || before.sequence > prefix {
            let operation = if tail { self.media_operation(&media.tail)? } else { resolved.operation.clone() };
            let mut params = if tail { bound(&resolved.params, &media.tail_variables) } else { resolved.params.clone() };
            if tail { params.insert(media.after.clone(), json!(prefix)); }
            let mapping = if tail { BTreeMap::new() } else { resolved.remote.variables.clone() };
            let mut cursor = None;
            let mut cursors = HashSet::new();
            let mut pages = 0;
            loop {
                let variables = load_variables(&params, &resolved.remote.shape, resolved.page_size(), cursor.as_deref(), &mapping);
                let (response, used) = self.query_with_retry(GraphQLRequest::new(&operation.document, &operation.name, variables)).await?;
                attempts += used;
                let data = response.data.ok_or_else(|| SyncError::Malformed("media response has no data".into()))?;
                let extracted = extract(&data, resolved.remote.root.as_deref().unwrap_or(&operation.root_path), &resolved.remote.shape, resolved.remote.total_from.as_deref())?;
                nodes.extend(extracted.nodes);
                page_info = extracted.page_info.ok_or_else(|| SyncError::Malformed("media response must be a connection".into()))?;
                pages += 1;
                if !page_info.has_next_page { break; }
                if let RemoteShape::Connection { max_pages: Some(max), .. } = resolved.remote.shape {
                    if pages >= max { return Err(SyncError::Incomplete { model: resolved.model.clone(), relation: resolved.relation.clone(), reason: "max_pages stopped media coverage".into() }); }
                }
                let next = page_info.end_cursor.clone().ok_or_else(|| SyncError::Malformed("media next page has no cursor".into()))?;
                if !cursors.insert(next.clone()) { return Err(SyncError::Incomplete { model: resolved.model.clone(), relation: resolved.relation.clone(), reason: "media cursor did not advance".into() }); }
                cursor = Some(next);
            }
            let checked = self.media_marker(resolved, media, prefix).await?;
            stable = checked == before;
            if stable && before.sequence != prefix {
                confirmed = self.media_marker(resolved, media, before.sequence).await?;
                stable = confirmed.sequence == before.sequence && confirmed.values.get("revision") == before.values.get("revision");
            }
        } else {
            confirmed.sequence = prefix;
        }
        let normalized = normalize_nodes(&self.db, &resolved.model, &stamp_scope_values(nodes, &resolved.scope_values))?;
        let dropped = normalized.dropped;
        if dropped != 0 { return Err(SyncError::Malformed("media coverage contains invalid rows".into())); }
        let result = self.land(Landing {
            marker: stable.then_some(confirmed), leave_stale: !stable,
            node_reread: false, model: resolved.model.clone(), scope: resolved.scope.clone(), window_key: resolved.window_key.clone(), normalized,
            is_first_page: !tail, replace_window: !tail, plans_as_first: !tail, coverage: Coverage::Complete,
            edge_fields: resolved.edge_fields.to_vec(), requested_cursor: if tail { window.and_then(|window| window.cursor) } else { None },
            page_info: Some(page_info), keeps_total: true, attempt,
        })?;
        Ok(match result {
            Landed::Applied { rows, invalid, revision } => {
                self.preload_on_source(&resolved.model, &resolved.relation);
                Executed::Landed(LoadOutcome { status: LoadStatus::Loaded, rows, dropped: dropped + invalid, has_next: false, attempts, kept_error: None }, revision)
            }
            Landed::Discarded(reason) => Executed::Landed(LoadOutcome::of(LoadStatus::Discarded(reason)), 0),
        })
    }
}
