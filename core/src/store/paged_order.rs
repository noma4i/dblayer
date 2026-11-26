use std::collections::BTreeMap;

use crate::identity::{ListKey, ModelKey};
use crate::store::ddl::{manifest_read, manifest_write};
use crate::store::rows::Reader;
use crate::store::{ModelInfo, ScopeRef, StoreError};

const PAGED_SCOPE_ORDER_KEY: &str = "paged_scope_order";

pub(super) fn rank_cached_scopes(connection: &rusqlite::Connection, models: &BTreeMap<String, ModelInfo>) -> Result<(), StoreError> {
    if manifest_read(connection, PAGED_SCOPE_ORDER_KEY)?.is_some() {
        return Ok(());
    }
    let transaction = rusqlite::Transaction::new_unchecked(connection, rusqlite::TransactionBehavior::Immediate)?;
    if manifest_read(&transaction, PAGED_SCOPE_ORDER_KEY)?.is_some() {
        return Ok(());
    }
    for (model, info) in models {
        let ranked: Vec<_> = info.spec.relations.iter().filter(|relation| relation.ranked_order().is_some()).collect();
        if ranked.is_empty() {
            continue;
        }
        let keys: Vec<String> = transaction.prepare(include_str!("sql/boot/paged_scope_keys.sql"))?.query_map([model], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        for key in keys {
            let key = ListKey::from_raw(&key);
            let Some(segments) = key.decompose() else { continue };
            if segments.first().map(String::as_str) != Some(model.as_str()) { continue; }
            let Some(relation) = ranked.iter().find(|relation| segments.get(1).is_some_and(|name| *name == relation.name)) else { continue };
            let Some(fields) = relation.ranked_order() else { continue };
            let scope = ScopeRef::new(ModelKey::new(model), key);
            let moves = super::replace::rank_ops(&Reader::new(&transaction, models), &scope, fields).map_err(|error| StoreError::Constraint(error.to_string()))?;
            super::apply::apply_scope_ops(&transaction, models, &moves, None, &mut std::collections::BTreeSet::new(), &mut std::collections::BTreeSet::new())?;
        }
    }
    manifest_write(&transaction, PAGED_SCOPE_ORDER_KEY, "1")?;
    transaction.commit()?;
    Ok(())
}
