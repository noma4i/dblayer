use std::collections::BTreeMap;
use std::fs;

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::ddl::{fingerprint, manifest_read, manifest_stamp, manifest_write, RESET_INTENT_KEY, REVISION_SEQ_KEY};
use crate::files::{clear_all, clear_model};
use crate::identity::ScopeKey;

use super::location::{reset_marker, remove_database_files};
use super::{Inner, Store, StoreError, StoreEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetMode {
    CacheOnly { models: Vec<String> },
    Incompatible,
    Logout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetStep {
    WriteIntent(&'static str),
    DeleteRows { model: String },
    DeleteScopeMembers { model: Option<String> },
    DeleteTombstones { model: Option<String> },
    DeleteQueryWindows { model: Option<String> },
    WriteManifest,
    DeleteDatabaseFile,
    ClearIntent,
}

pub struct ResetPlan;

impl ResetPlan {
    pub fn intent_value(mode: &ResetMode) -> &'static str {
        match mode {
            ResetMode::CacheOnly { .. } => "cache_only",
            ResetMode::Incompatible => "incompatible",
            ResetMode::Logout => "logout",
        }
    }

    pub fn keeps_user_tables(mode: &ResetMode) -> bool {
        !matches!(mode, ResetMode::Logout)
    }

    pub fn blast_radius(mode: &ResetMode) -> Vec<String> {
        match mode {
            ResetMode::CacheOnly { models } => {
                let mut sorted = models.clone();
                sorted.sort();
                sorted
            }
            _ => Vec::new(),
        }
    }

    pub fn steps(mode: &ResetMode) -> Vec<ResetStep> {
        let intent = ResetStep::WriteIntent(Self::intent_value(mode));
        match mode {
            ResetMode::CacheOnly { .. } => {
                let mut steps = vec![intent];
                for model in Self::blast_radius(mode) {
                    steps.push(ResetStep::DeleteRows { model: model.clone() });
                    steps.push(ResetStep::DeleteScopeMembers { model: Some(model.clone()) });
                    steps.push(ResetStep::DeleteTombstones { model: Some(model.clone()) });
                    steps.push(ResetStep::DeleteQueryWindows { model: Some(model) });
                }
                steps.push(ResetStep::ClearIntent);
                steps
            }
            ResetMode::Incompatible => vec![
                intent,
                ResetStep::DeleteScopeMembers { model: None },
                ResetStep::DeleteTombstones { model: None },
                ResetStep::DeleteQueryWindows { model: None },
                ResetStep::WriteManifest,
                ResetStep::ClearIntent,
            ],
            ResetMode::Logout => vec![intent, ResetStep::DeleteDatabaseFile, ResetStep::ClearIntent],
        }
    }

    pub fn resume_steps(mode: &ResetMode, completed: i64) -> Vec<ResetStep> {
        let steps = Self::steps(mode);
        let offset = usize::try_from(completed.max(0)).unwrap_or(0);
        steps.into_iter().skip(offset).collect()
    }
}

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
    let mut statement =
        connection.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'row\\_%' ESCAPE '\\'")?;
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

pub(crate) fn wipe_all_cache(connection: &Connection, drop_tables: bool) -> Result<(), StoreError> {
    for table in row_tables(connection)? {
        if drop_tables {
            connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))?;
        } else {
            connection.execute_batch(&format!("DELETE FROM {table}"))?;
        }
    }
    connection.execute_batch("DELETE FROM scope_member; DELETE FROM tombstone; DELETE FROM query_window;")?;
    clear_all(connection)?;
    Ok(())
}

pub(crate) fn current_fingerprints(inner: &Inner) -> BTreeMap<String, String> {
    inner.models.iter().map(|(key, info)| (key.clone(), fingerprint(&info.spec))).collect()
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
