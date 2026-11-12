use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;
use serde_json::{Map, Value};

use crate::store::EntityOp;
use crate::identity::{RowId, RowRef};
use crate::schema::{AssociationKind, AssociationSpec, ModelSpec};
use crate::value::{read_id, same_fields};

use super::{ModelInfo, Reader, StoreError};

type Row = Map<String, Value>;

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

fn setting(spec: &ModelSpec) -> impl Iterator<Item = &AssociationSpec> {
    spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && !association.sets.is_empty())
}

pub(super) fn affected_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, operations: &[EntityOp], rows: &super::apply::Prefetched, parents: &mut BTreeSet<RowRef>) -> Result<(), StoreError> {
    for operation in operations {
        let row = operation.row_ref();
        let spec = &models.get(row.model.as_str()).ok_or_else(|| StoreError::UnknownModel(row.model.to_string()))?.spec;
        if matches!(operation, EntityOp::Upsert { .. }) && spec.server_values().next().is_some() { parents.insert(row.clone()); }
        let stored = match rows.get(&(row.model.to_string(), row.id.to_string())) { Some(stored) => stored.as_ref().map(|stored| stored.row.clone()), None => reader.point(row.model.as_str(), row.id.as_str())? };
        if let Some(stored) = stored { parents_of(spec, &stored, parents)?; }
    }
    Ok(())
}

pub(crate) fn parents_of(spec: &ModelSpec, row: &Row, parents: &mut BTreeSet<RowRef>) -> Result<(), StoreError> {
    for association in setting(spec) {
        let Some(parent) = row.get(&association.foreign_key).and_then(read_id) else { continue };
        parents.insert(RowRef::new(association.target.clone(), RowId::new(parent.as_str()).map_err(fault)?));
    }
    Ok(())
}

/// The one entry of the derived parent fields for the parents a write touched, the derived rows go through `apply` in the same transaction.
pub(super) fn settle_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, results: &[super::CommitResult], apply: impl FnOnce(super::BatchPlan) -> Result<super::CommitResult, StoreError>, generation: crate::identity::Generation) -> Result<(Option<super::CommitResult>, usize, BTreeSet<String>), StoreError> {
    let parents: BTreeSet<RowRef> = results.iter().flat_map(|result| result.parents.iter().cloned()).collect();
    let (derived, read, stale) = derived_parents(reader, models, &parents)?;
    let written = if derived.is_empty() { None } else { Some(apply(super::BatchPlan { entity_ops: derived, ..super::BatchPlan::new(generation) })?) };
    Ok((written, read, stale))
}

fn derived_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, parents: &BTreeSet<RowRef>) -> Result<(Vec<EntityOp>, usize, BTreeSet<String>), StoreError> {
    let mut derived = Vec::new();
    let mut read = 0;
    let mut stale = BTreeSet::new();
    let mut hidden: Option<super::apply::HiddenRows> = None;
    for parent in parents {
        let Some(stored) = reader.stored_row(parent.model.as_str(), parent.id.as_str())?.map(|stored| stored.row) else { continue };
        let info = models.get(parent.model.as_str()).ok_or_else(|| StoreError::UnknownModel(parent.model.to_string()))?;
        let server: Row = info.spec.server_values().filter_map(|(declared, hidden)| stored.get(hidden).map(|value| (declared.to_string(), value.clone()))).collect();
        let mut next = stored.clone();
        for child in models.values() {
            for association in setting(&child.spec).filter(|association| association.target == parent.model) {
                let named = association.pointer_field().and_then(|field| server.get(field)).and_then(read_id);
                let removed = match named {
                    Some(id) if reader.tombstone_seq(child.spec.key.as_str(), id.as_str())?.is_some() && reader.stored_row(child.spec.key.as_str(), id.as_str())?.is_none() => {
                        let open = match &hidden { Some(open) => open, None => hidden.insert(super::apply::hidden_rows(reader.connection(), &[])?) };
                        Some(!open.contains(&(child.spec.key.to_string(), id.to_string())))
                    }
                    _ => None,
                };
                let mut server = server.clone();
                if let Some(closed) = removed {
                    if closed { stale.insert(parent.model.to_string()); }
                    for (declared, hidden) in info.spec.server_values().filter(|(declared, _)| association.sets.contains_key(*declared)) {
                        server.remove(declared);
                        if closed { next.remove(hidden); }
                    }
                }
                let mut newest = |permanent: bool| -> Result<Option<Row>, StoreError> {
                    let Some(id) = first_ranked(reader, &child.table, association, &parent.id, permanent)? else { return Ok(None) };
                    read += 1;
                    reader.point(child.spec.key.as_str(), id.as_str())
                };
                let first = newest(false)?;
                let predicted = first.as_ref().and_then(|row| row.get("id")).and_then(Value::as_str).is_some_and(crate::identity::is_temp_id);
                let confirmed = if predicted { newest(true)? } else { None };
                let values = association.derived(&server, first.as_ref(), confirmed.as_ref());
                for field in association.sets.keys() {
                    match values.get(field) {
                        Some(value) => next.insert(field.clone(), value.clone()),
                        None => next.remove(field),
                    };
                }
            }
        }
        if !same_fields(&next, &stored) { derived.push(EntityOp::Upsert { model: parent.model.clone(), id: parent.id.clone(), row: next }); }
    }
    Ok((derived, read, stale))
}

fn first_ranked(reader: &Reader<'_>, table: &str, association: &AssociationSpec, parent_id: &RowId, permanent: bool) -> Result<Option<RowId>, StoreError> {
    let order = crate::store::ddl::newer_order(association).into_iter().map(|(term, descending)| if descending { format!("{term} DESC") } else { term }).collect::<Vec<_>>().join(", ");
    let foreign_key = crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&association.foreign_key));
    let only = if permanent { format!(" AND {}", crate::identity::permanent_row_flag(&crate::store::ddl::sql_ident("id"))) } else { String::new() };
    let mut statement = reader.connection().prepare_cached(&format!("SELECT id FROM {table} WHERE {foreign_key} = ?1{only} ORDER BY {order} LIMIT 1"))?;
    let id: Option<String> = statement.query_row([parent_id.as_str()], |row| row.get(0)).optional()?;
    id.map(|id| RowId::new(&id).map_err(fault)).transpose()
}
