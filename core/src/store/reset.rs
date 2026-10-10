use std::collections::BTreeMap;
use std::fs;

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::ddl::{fingerprint, manifest_read, manifest_stamp, manifest_write, RESET_INTENT_KEY, REVISION_SEQ_KEY};
use crate::files::{clear_all, clear_model};
use crate::identity::ScopeKey;
use crate::operation::{quarantine_add, Operation, QuarantineReason};

use super::location::{reset_marker, remove_database_files};
use super::{Inner, Store, StoreError, StoreEvent};

#[derive(Debug, Serialize, Deserialize)]
struct ResetIntent {
    mode: String,
    models: Vec<String>,
    started_at: i64,
}

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
    let intent = ResetIntent { mode: mode.to_string(), models: models.to_vec(), started_at: now };
    manifest_write(connection, RESET_INTENT_KEY, &serde_json::to_string(&intent)?)?;
    Ok(())
}

fn bump_revision(connection: &Connection) -> Result<i64, StoreError> {
    let current = manifest_read(connection, REVISION_SEQ_KEY)?.and_then(|text| text.parse::<i64>().ok()).unwrap_or(0);
    manifest_write(connection, REVISION_SEQ_KEY, &(current + 1).to_string())?;
    Ok(current + 1)
}

pub(crate) fn clear_intent(connection: &Connection) -> Result<(), StoreError> {
    connection.execute("DELETE FROM manifest WHERE key = ?1", params![RESET_INTENT_KEY])?;
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
    if drop_table {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
    } else {
        connection.execute_batch(&format!("DELETE FROM {table}"))?;
    }
    connection.execute("DELETE FROM scope_member WHERE model = ?1", params![model])?;
    clear_model(connection, model)?;
    connection.execute("DELETE FROM tombstone WHERE model = ?1", params![model])?;
    connection.execute(
        "DELETE FROM query_window WHERE key LIKE ?1 ESCAPE '\\'",
        params![like_pattern(ScopeKey::compose(&[model]).as_str())],
    )?;
    Ok(())
}

pub(crate) fn retire_operations(connection: &Connection, models: Option<&[String]>, quarantined: Option<QuarantineReason>) -> Result<(), StoreError> {
    let open: Vec<(i64, String, Vec<u8>)> = connection
        .prepare("SELECT seq, action_key, input FROM op")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?;
    for (seq, action_key, input) in open {
        let owner = Operation::split_action_key(&action_key).map(|(owner, _)| owner).unwrap_or_default();
        if models.is_some_and(|models| !models.contains(&owner)) {
            continue;
        }
        if let Some(reason) = quarantined {
            quarantine_add(connection, &owner, &input, reason)?;
        }
        connection.execute("DELETE FROM op WHERE seq = ?1", params![seq])?;
    }
    let has_row_sync: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'row_sync')", [], |row| row.get(0))?;
    if has_row_sync {
        match models {
            Some(models) => {
                for model in models {
                    connection.execute("DELETE FROM row_sync WHERE model = ?1", params![model])?;
                }
            }
            None => {
                connection.execute("DELETE FROM row_sync", [])?;
            }
        }
    }
    Ok(())
}

pub(crate) fn wipe_all_cache(connection: &Connection, drop_tables: bool) -> Result<(), StoreError> {
    for table in row_tables(connection)?.into_iter().filter(|table| table != "row_sync") {
        if drop_tables {
            connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
        } else {
            connection.execute_batch(&format!("DELETE FROM {table}"))?;
        }
    }
    connection.execute_batch("DELETE FROM scope_member; DELETE FROM tombstone; DELETE FROM query_window; DELETE FROM row_sync;")?;
    clear_all(connection)?;
    Ok(())
}

pub(crate) fn current_fingerprints(inner: &Inner) -> BTreeMap<String, String> {
    inner.models.iter().filter(|(_, info)| !info.spec.is_memory()).map(|(key, info)| (key.clone(), fingerprint(&info.spec))).collect()
}

pub(crate) fn finish_interrupted_reset(inner: &mut Inner) -> Result<bool, StoreError> {
    let Some(raw) = manifest_read(&inner.connection, RESET_INTENT_KEY)? else { return Ok(false) };
    let Ok(intent) = serde_json::from_str::<ResetIntent>(&raw) else {
        let transaction = inner.connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO quarantine (model, payload, reason) VALUES (NULL, ?1, 'corrupt-reset-intent')",
            params![raw.as_bytes()],
        )?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        return Ok(false);
    };
    let transaction = inner.connection.unchecked_transaction()?;
    match intent.mode.as_str() {
        "cache_only" => {
            for model in &intent.models {
                let table = crate::ddl::table_name(model);
                wipe_model_cache(&transaction, model, &table, true)?;
            }
        }
        _ => {
            wipe_all_cache(&transaction, true)?;
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
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        inner.poison = None;
        self.announce(StoreEvent::Reset { revision, models: Some(names) });
        Ok(())
    }

    pub fn reset_incompatible(&self) -> Result<(), StoreError> {
        let mut inner = self.lock();
        inner.generation = inner.generation.next();
        let now = inner.now_ms();
        write_intent(&inner.connection, "incompatible", &[], now)?;
        let transaction = inner.connection.unchecked_transaction()?;
        wipe_all_cache(&transaction, false)?;
        manifest_stamp(&transaction, inner.data_version.as_deref(), &current_fingerprints(&inner))?;
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        transaction.commit()?;
        inner.poison = None;
        self.announce(StoreEvent::Reset { revision, models: None });
        Ok(())
    }

    pub fn reset_intent(&self) -> Result<Option<String>, StoreError> {
        let inner = self.lock();
        Ok(manifest_read(&inner.connection, RESET_INTENT_KEY)?)
    }

    pub fn logout(self) -> Result<(), StoreError> {
        let Some(path) = self.path.clone() else { return Ok(()) };
        let marker = reset_marker(&path);
        fs::write(&marker, b"logout")?;
        drop(self);
        remove_database_files(&path)?;
        fs::remove_file(&marker)?;
        Ok(())
    }
}
