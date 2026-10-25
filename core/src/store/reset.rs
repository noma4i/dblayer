use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::ddl::{fingerprint, fts_tables, manifest_read, manifest_stamp, manifest_write, FieldPlan, REVISION_SEQ_KEY};
use crate::files::{clear_all, clear_model};
use crate::identity::ScopeKey;
use crate::nodes::read_trees;
use crate::operation::{delete_operation, quarantine_add, read_value, Operation, QuarantineReason};
use crate::schema::MergeRule;
use crate::syncstate::clear_sync;

use super::rows::fetch_stored;
use super::shape::leaf_value;
use super::{save_pending, Inner, ModelInfo, Store, StoreError, StoreEvent};

fn like_pattern(prefix: &str) -> String {
    let mut pattern = String::new();
    for character in prefix.chars() {
        if matches!(character, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('%');
    pattern
}

pub(crate) fn write_intent(connection: &Connection, mode: &str, models: &[String], now: i64) -> Result<(), StoreError> {
    let transaction = connection.unchecked_transaction()?;
    clear_intent(&transaction)?;
    transaction.execute("INSERT INTO reset_intent (id, mode, started_at) VALUES (1, ?1, ?2)", params![mode, now])?;
    for (pos, model) in models.iter().enumerate() {
        transaction.execute("INSERT INTO reset_intent_model (pos, model) VALUES (?1, ?2)", params![pos as i64, model])?;
    }
    transaction.commit()?;
    Ok(())
}

fn bump_revision(connection: &Connection) -> Result<i64, StoreError> {
    let current = manifest_read(connection, REVISION_SEQ_KEY)?.and_then(|text| text.parse::<i64>().ok()).unwrap_or(0);
    manifest_write(connection, REVISION_SEQ_KEY, &(current + 1).to_string())?;
    Ok(current + 1)
}

pub(crate) fn pending_intent(connection: &Connection) -> Result<Option<String>, StoreError> {
    Ok(connection.query_row("SELECT mode FROM reset_intent", [], |row| row.get(0)).optional()?)
}

pub(crate) fn clear_intent(connection: &Connection) -> Result<(), StoreError> {
    connection.execute("DELETE FROM reset_intent", [])?;
    connection.execute("DELETE FROM reset_intent_model", [])?;
    Ok(())
}

pub(crate) fn row_tables(connection: &Connection) -> Result<Vec<String>, StoreError> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'row\\_%' ESCAPE '\\'
         UNION ALL SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name LIKE 'row\\_%' ESCAPE '\\'",
    )?;
    let names = statement.query_map([], |row| row.get(0))?;
    Ok(names.collect::<Result<_, _>>()?)
}

pub(crate) fn wipe_model_cache(connection: &Connection, model: &str, table: &str, drop_table: bool) -> Result<(), StoreError> {
    let children: Vec<String> = row_tables(connection)?.into_iter().filter(|name| name.starts_with(&format!("{table}__"))).collect();
    for name in fts_tables(connection)?.iter().filter(|name| **name == format!("fts_{table}")) {
        connection.execute_batch(&format!("{} {name}", if drop_table { "DROP TABLE IF EXISTS" } else { "DELETE FROM" }))?;
    }
    for name in children.iter().map(String::as_str).chain([table]) {
        if drop_table {
            connection.execute_batch(&format!("DROP TABLE IF EXISTS {name}"))?;
        } else {
            connection.execute_batch(&format!("DELETE FROM {name}"))?;
        }
    }
    connection.execute("DELETE FROM scope_member WHERE model = ?1", params![model])?;
    clear_model(connection, model)?;
    connection.execute("DELETE FROM tombstone WHERE model = ?1", params![model])?;
    connection.execute("DELETE FROM local_pending WHERE model = ?1", params![model])?;
    connection.execute("DELETE FROM query_window WHERE key LIKE ?1 ESCAPE '\\'", params![like_pattern(ScopeKey::compose(&[model]).as_str())])?;
    Ok(())
}

pub(crate) fn retire_operations(connection: &Connection, models: Option<&[String]>, quarantined: Option<QuarantineReason>) -> Result<(), StoreError> {
    let open: Vec<(i64, String)> = connection.prepare("SELECT seq, action_key FROM op")?.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    for (seq, action_key) in open {
        let owner = Operation::split_action_key(&action_key).map(|(owner, _)| owner).unwrap_or_default();
        if models.is_some_and(|models| !models.contains(&owner)) {
            continue;
        }
        if let Some(reason) = quarantined {
            let input = read_value(connection, seq, "input")?.unwrap_or(serde_json::Value::Null);
            quarantine_add(connection, Some(&owner), &input, reason)?;
        }
        delete_operation(connection, seq)?;
    }
    match models {
        Some(models) => models.iter().try_for_each(|model| clear_sync(connection, Some(model)))?,
        None => clear_sync(connection, None)?,
    }
    Ok(())
}

pub(crate) fn wipe_all_cache(connection: &Connection, drop_tables: bool) -> Result<(), StoreError> {
    for name in fts_tables(connection)? {
        connection.execute_batch(&format!("{} {name}", if drop_tables { "DROP TABLE IF EXISTS" } else { "DELETE FROM" }))?;
    }
    for table in row_tables(connection)?.into_iter().filter(|table| table != "row_sync") {
        if drop_tables {
            connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
        } else {
            connection.execute_batch(&format!("DELETE FROM {table}"))?;
        }
    }
    connection.execute_batch("DELETE FROM scope_member; DELETE FROM tombstone; DELETE FROM query_window; DELETE FROM local_pending;")?;
    connection.execute_batch(
        "DELETE FROM cloud_field_node; DELETE FROM cloud_field; DELETE FROM cloud_row; \
         DELETE FROM cloud_log_node; DELETE FROM cloud_log; DELETE FROM cloud_cursor; DELETE FROM cloud_replica;",
    )?;
    clear_sync(connection, None)?;
    clear_all(connection)?;
    Ok(())
}

pub(crate) fn models_to_reset(models: &BTreeMap<String, ModelInfo>, changed: &[String]) -> Vec<String> {
    let mut reset: BTreeSet<String> = changed.iter().cloned().collect();
    if changed.iter().any(|model| models.get(model).is_none_or(|info| !info.spec.is_local())) {
        reset.extend(models.iter().filter(|(_, info)| !info.spec.is_memory() && !info.spec.is_local()).map(|(key, _)| key.clone()));
    }
    reset.into_iter().collect()
}

pub(crate) struct PendingValue {
    model: String,
    row_id: String,
    path: String,
    value: Value,
}

fn local_paths(info: &ModelInfo) -> Vec<String> {
    let mut paths = Vec::new();
    for field in &info.spec.fields {
        match &field.merge {
            Some(MergeRule::Local) => paths.push(field.name.clone()),
            Some(rule) => paths.extend(rule.local_keys().map(|key| format!("{}.{key}", field.name))),
            None => {}
        }
    }
    paths
}

pub(crate) fn stash_local_values(connection: &Connection, models: &BTreeMap<String, ModelInfo>, reset: &[String], changed: &[String]) -> Result<Vec<PendingValue>, StoreError> {
    let tables = row_tables(connection)?;
    let mut stash = Vec::new();
    for model in reset {
        let Some(info) = models.get(model).filter(|info| !info.spec.is_local()) else { continue };
        for (owner, value) in read_trees(connection, "local_pending", &["model", "row_id", "path"], "model = ?1", &[rusqlite::types::Value::Text(model.clone())])? {
            if let [_, rusqlite::types::Value::Text(row_id), rusqlite::types::Value::Text(path)] = owner.as_slice() {
                stash.push(PendingValue { model: model.clone(), row_id: row_id.clone(), path: path.clone(), value });
            }
        }
        let paths = local_paths(info);
        if paths.is_empty() || !tables.contains(&info.table) {
            continue;
        }
        if changed.contains(model) {
            stash_surviving_columns(connection, info, model, &paths, &mut stash)?;
            continue;
        }
        let ids: Vec<String> = connection.prepare(&format!("SELECT id FROM {}", info.table))?.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
        for (id, stored) in fetch_stored(connection, info, &ids, false)? {
            for path in &paths {
                let found = match path.split_once('.') {
                    None => stored.row.get(path),
                    Some((name, key)) => stored.row.get(name).and_then(|object| object.get(key)),
                };
                if let Some(value) = found.filter(|value| !value.is_null()) {
                    stash.push(PendingValue { model: model.clone(), row_id: id.clone(), path: path.clone(), value: value.clone() });
                }
            }
        }
    }
    Ok(stash)
}

fn stash_surviving_columns(connection: &Connection, info: &ModelInfo, model: &str, paths: &[String], stash: &mut Vec<PendingValue>) -> Result<(), StoreError> {
    let columns: BTreeMap<String, String> = connection
        .prepare(&format!("SELECT name, type FROM pragma_table_info('{}')", info.table))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    for plan in &info.layout.fields {
        let FieldPlan::Leaf { field, leaf } = plan else { continue };
        if !paths.contains(field) || columns.get(&leaf.column).is_none_or(|declared| !declared.eq_ignore_ascii_case(leaf.ty.as_sql())) {
            continue;
        }
        let mut statement = connection.prepare(&format!("SELECT id, {column} FROM {table} WHERE {column} IS NOT NULL", column = crate::ddl::sql_ident(&leaf.column), table = info.table))?;
        let cells = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, rusqlite::types::Value>(1)?)))?;
        for cell in cells {
            let (row_id, cell) = cell?;
            if let Some(value) = leaf_value(leaf, Some(&cell)) {
                stash.push(PendingValue { model: model.to_string(), row_id, path: field.clone(), value });
            }
        }
    }
    Ok(())
}

pub(crate) fn restore_local_values(connection: &Connection, stash: &[PendingValue]) -> Result<(), StoreError> {
    stash.iter().try_for_each(|item| save_pending(connection, &item.model, &item.row_id, &item.path, &item.value))
}

pub(crate) fn current_fingerprints(inner: &Inner) -> BTreeMap<String, String> {
    inner.models.iter().filter(|(_, info)| !info.spec.is_memory()).map(|(key, info)| (key.clone(), fingerprint(&info.spec))).collect()
}

pub(crate) fn finish_interrupted_reset(inner: &mut Inner) -> Result<bool, StoreError> {
    let Some(mode) = pending_intent(&inner.connection)? else { return Ok(false) };
    let models: Vec<String> = inner.connection.prepare("SELECT model FROM reset_intent_model ORDER BY pos")?.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
    if !matches!(mode.as_str(), "cache_only" | "incompatible") {
        let transaction = inner.connection.unchecked_transaction()?;
        quarantine_add(&transaction, None, &serde_json::json!({ "mode": mode }), QuarantineReason::CorruptResetIntent)?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        return Ok(false);
    }
    let transaction = inner.connection.unchecked_transaction()?;
    match mode.as_str() {
        "cache_only" => {
            for model in &models {
                let table = crate::ddl::table_name(model);
                wipe_model_cache(&transaction, model, &table, true)?;
            }
            retire_operations(&transaction, Some(&models), None)?;
        }
        _ => {
            wipe_all_cache(&transaction, true)?;
            retire_operations(&transaction, None, None)?;
            let data_version = inner.data_version.clone();
            manifest_stamp(&transaction, data_version.as_deref(), &current_fingerprints(inner))?;
        }
    }
    clear_intent(&transaction)?;
    transaction.commit()?;
    Ok(true)
}

impl Store {
    pub fn reset_cache_only(&self, models: &[&str]) -> Result<(), StoreError> {
        let _readers = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        let tables: Vec<(String, String)> = models
            .iter()
            .map(|model| inner.model(model).map(|info| (model.to_string(), info.table.clone())))
            .collect::<Result<_, _>>()?;
        inner.generation = inner.generation.next();
        let now = inner.now_ms();
        let names: Vec<String> = models.iter().map(|model| model.to_string()).collect();
        write_intent(&inner.connection, "cache_only", &names, now)?;
        let transaction = inner.connection.unchecked_transaction()?;
        for (model, table) in &tables {
            wipe_model_cache(&transaction, model, table, false)?;
        }
        retire_operations(&transaction, Some(&names), None)?;
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = None;
        self.announce(StoreEvent::Reset { revision, models: Some(names) });
        Ok(())
    }

    pub fn reset_incompatible(&self) -> Result<(), StoreError> {
        let _readers = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        inner.generation = inner.generation.next();
        let now = inner.now_ms();
        write_intent(&inner.connection, "incompatible", &[], now)?;
        let transaction = inner.connection.unchecked_transaction()?;
        wipe_all_cache(&transaction, false)?;
        retire_operations(&transaction, None, None)?;
        manifest_stamp(&transaction, inner.data_version.as_deref(), &current_fingerprints(&inner))?;
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = None;
        self.announce(StoreEvent::Reset { revision, models: None });
        Ok(())
    }

    #[cfg(test)]
    pub fn reset_intent(&self) -> Result<Option<String>, StoreError> {
        let inner = self.lock();
        pending_intent(&inner.connection)
    }
}
