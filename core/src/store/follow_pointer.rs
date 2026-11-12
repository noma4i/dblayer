use std::collections::{BTreeMap, BTreeSet};

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
        let Some(stored) = stored else { continue };
        for association in setting(spec) {
            let Some(parent) = stored.get(&association.foreign_key).and_then(read_id) else { continue };
            parents.insert(RowRef::new(association.target.clone(), RowId::new(parent.as_str()).map_err(fault)?));
        }
    }
    Ok(())
}

pub(super) fn derived_parents(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, parents: &BTreeSet<RowRef>) -> Result<(Vec<EntityOp>, usize), StoreError> {
    let mut derived = Vec::new();
    let mut read = 0;
    for parent in parents {
        let Some(stored) = reader.point(parent.model.as_str(), parent.id.as_str())? else { continue };
        let info = models.get(parent.model.as_str()).ok_or_else(|| StoreError::UnknownModel(parent.model.to_string()))?;
        let server: Row = info.spec.server_values().filter_map(|(declared, hidden)| stored.get(hidden).map(|value| (declared.to_string(), value.clone()))).collect();
        let mut next = stored.clone();
        for child in models.values() {
            for association in setting(&child.spec).filter(|association| association.target == parent.model) {
                let mut newest = |permanent: bool| -> Result<Option<Row>, StoreError> {
                    let mut rows = Vec::new();
                    for id in first_ranked(reader, &child.table, association, &parent.id, permanent)? {
                        read += 1;
                        rows.extend(reader.point(child.spec.key.as_str(), id.as_str())?);
                    }
                    Ok(association.newest(&rows).cloned())
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
    Ok((derived, read))
}

fn first_ranked(reader: &Reader<'_>, table: &str, association: &AssociationSpec, parent_id: &RowId, permanent: bool) -> Result<Vec<RowId>, StoreError> {
    let order = crate::store::ddl::newer_order(association).into_iter().map(|(term, descending)| if descending { format!("{term} DESC") } else { term }).collect::<Vec<_>>().join(", ");
    let fields: String = association.newer.iter().map(|newer| format!(", {}", crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&newer.child)))).collect();
    let foreign_key = crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&association.foreign_key));
    let only = if permanent { format!(" AND {}", crate::identity::permanent_row_flag(&crate::store::ddl::sql_ident("id"))) } else { String::new() };
    let mut statement = reader.connection().prepare_cached(&format!("SELECT id{fields} FROM {table} WHERE {foreign_key} = ?1{only} ORDER BY {order}"))?;
    let mut rows = statement.query([parent_id.as_str()])?;
    let mut first: Option<(bool, Vec<rusqlite::types::Value>)> = None;
    let mut found = Vec::new();
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let rank = (crate::identity::is_temp_id(&id), (1..=association.newer.len()).map(|column| row.get(column)).collect::<Result<Vec<_>, _>>()?);
        if first.as_ref().is_some_and(|first| *first != rank) {
            break;
        }
        first.get_or_insert(rank);
        found.push(RowId::new(&id).map_err(fault)?);
    }
    Ok(found)
}
