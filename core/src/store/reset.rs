use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::ddl::{fingerprint, fts_tables, manifest_stamp, manifest_write, FieldPlan, REVISION_SEQ_KEY};
use crate::files::{clear_all, clear_model};
use crate::identity::ListKey;
use crate::nodes::read_trees;
use crate::operation::{delete_operation, quarantine_add, read_value, Operation, QuarantineReason};
use crate::schema::MergeRule;
use crate::syncstate::clear_sync;

use super::rows::fetch_stored;
use super::shape::leaf_value;
use super::{save_pending, Inner, ModelInfo, Store, StoreError, StoreEvent};
use crate::transport::encrypted::state::{self, Pending};

pub(crate) struct ResetCommitted<'a> {
    store: &'a Store,
    ticket: Option<u64>,
    revision: i64,
    models: Option<Vec<String>>,
    operations: Vec<crate::operation::QueueOperation>,
}

impl ResetCommitted<'_> {
    pub(crate) fn run(mut self) {
        self.announce();
    }

    fn announce(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            let event = StoreEvent::Reset { revision: self.revision, models: self.models.clone(), operations: std::mem::take(&mut self.operations) };
            self.store.line.run(ticket, || self.store.announce(event));
        }
    }
}

impl Drop for ResetCommitted<'_> {
    fn drop(&mut self) { self.announce(); }
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
    let current = super::rows::read_revision(connection)?;
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
    connection.execute("DELETE FROM scope_revision WHERE model = ?1", params![model])?;
    clear_model(connection, model)?;
    connection.execute("DELETE FROM tombstone WHERE model = ?1", params![model])?;
    connection.execute("DELETE FROM local_pending WHERE model = ?1", params![model])?;
    connection.execute("DELETE FROM window_marker WHERE window_key LIKE ?1 ESCAPE '\\'", params![like_pattern(ListKey::compose(&[model]).as_str())])?;
    connection.execute("DELETE FROM query_window WHERE key LIKE ?1 ESCAPE '\\'", params![like_pattern(ListKey::compose(&[model]).as_str())])?;
    Ok(())
}

pub(crate) fn retire_operations(connection: &Connection, models: Option<&[String]>, quarantined: Option<QuarantineReason>) -> Result<(), StoreError> {
    if models.is_none() { connection.execute_batch("DELETE FROM blob_use; DELETE FROM blob_upload;")?; }
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
        super::blob::release(connection, seq, false)?;
        delete_operation(connection, seq)?;
    }
    match models {
        Some(models) => models.iter().try_for_each(|model| clear_sync(connection, Some(model)))?,
        None => {
            connection.execute("DELETE FROM transfer_abort", [])?;
            clear_sync(connection, None)?;
        }
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
    connection.execute_batch("DELETE FROM scope_member; DELETE FROM scope_revision; DELETE FROM tombstone; DELETE FROM window_marker; DELETE FROM query_window; DELETE FROM local_pending;")?;
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
    if !matches!(mode.as_str(), "cache_only" | "incompatible" | "session_end") {
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
    /// True once after an open that finished an interrupted session end: the credential is still to clear.
    pub(crate) fn take_session_end_replay(&self) -> bool {
        self.session_end_replay.swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub fn reset_intent(&self) -> Result<Option<String>, StoreError> {
        let inner = self.lock();
        pending_intent(&inner.connection)
    }
}

#[cfg(test)]
impl Store {
    /// The intent that `EraseAccount` writes before it clears, left unfinished.
    pub(crate) fn mark_session_end_in_tests(&self) -> Result<(), StoreError> {
        let inner = self.lock();
        let now = inner.now_ms();
        write_intent(&inner.connection, "session_end", &[], now)
    }

    /// The clearing the executor does for `ClearModels`, for the tests that run without the owners: the cache of the models goes, the generation moves and the leases hear it.
    pub(crate) fn clear_models_in_tests(&self, models: &[&str]) -> Result<(), StoreError> {
        let generation = self.generation().next();
        let names: Vec<String> = models.iter().map(|model| model.to_string()).collect();
        self.erase_models(generation, &names)?;
        self.lifecycle.models_reset(generation.value(), &names);
        Ok(())
    }

    /// The clearing the executor does for `ClearAccount` without the credential tables, for the tests that run without the owners: the epoch ends and the next one starts without a session.
    pub(crate) fn clear_account_in_tests(&self) -> Result<(), StoreError> {
        let generation = self.generation().next();
        self.lifecycle.epoch_ended(crate::runtime::Cause::IdentityChanged);
        self.erase_account(generation, false)?;
        let epoch = self.lifecycle.snapshot().session_epoch + 1;
        self.lifecycle.published(&crate::owners::contract::Published { epoch, identity: None, generation: generation.value(), state: crate::owners::contract::PublishedState::NoSession, closed: false });
        Ok(())
    }
}

const SAVED_EPOCH_KEY: &str = "saved_epoch";
const SAVED_GENERATION_KEY: &str = "saved_generation";

/// The first 8 bytes of the stored credential digest: the identity the owners compare.
pub(crate) fn identity_of(digest: &str) -> Option<u64> {
    u64::from_str_radix(digest.get(..16)?, 16).ok()
}

impl Store {
    /// `saved_epoch`, `saved_generation` and the identity of the stored session, read at open.
    pub(crate) fn saved(&self) -> Result<(u64, u64, Option<u64>), StoreError> {
        let inner = self.lock();
        let read = |key: &str| -> Result<u64, StoreError> {
            match crate::ddl::manifest_read(&inner.connection, key)? {
                None => Ok(0),
                Some(value) => value.parse().map_err(|_| StoreError::CorruptManifest(format!("{key} = {value:?} is not a u64"))),
            }
        };
        let identity = crate::transport::encrypted::state::session(&inner.connection)?.filter(|session| session.sequence > 0).and_then(|session| identity_of(&session.credential_digest));
        Ok((read(SAVED_EPOCH_KEY)?, read(SAVED_GENERATION_KEY)?, identity))
    }

    /// The effect `SavePublished`.
    pub(crate) fn save_published(&self, epoch: u64, generation: u64) -> Result<(), StoreError> {
        let inner = self.lock();
        let transaction = inner.connection.unchecked_transaction()?;
        manifest_write(&transaction, SAVED_EPOCH_KEY, &epoch.to_string())?;
        manifest_write(&transaction, SAVED_GENERATION_KEY, &generation.to_string())?;
        transaction.commit()?;
        Ok(())
    }

    /// The effect `EraseAccount`: the data of the account goes in one transaction. Returns the new revision.
    pub(crate) fn erase_account(&self, generation: crate::identity::Generation, erase_encrypted: bool) -> Result<i64, StoreError> {
        let readers = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        inner.generation = generation;
        let now = inner.now_ms();
        write_intent(&inner.connection, if erase_encrypted { "session_end" } else { "incompatible" }, &[], now)?;
        let transaction = inner.connection.unchecked_transaction()?;
        wipe_all_cache(&transaction, false)?;
        retire_operations(&transaction, None, None)?;
        if erase_encrypted {
            transaction.execute_batch("DELETE FROM encrypted_pending; DELETE FROM encrypted_answer; DELETE FROM encrypted_session; DELETE FROM restore_failed WHERE source_table = 'encrypted_pending';")?;
        }
        manifest_stamp(&transaction, inner.data_version.as_deref(), &current_fingerprints(&inner))?;
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        let operations = crate::operation::read_queue(&transaction)?;
        transaction.commit()?;
        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = None;
        drop(readers);
        let event = StoreEvent::Reset { revision, models: None, operations: operations.clone() };
        if let Some(log) = self.commit_log.get() { log(&event); }
        let ticket = self.line.ticket();
        drop(inner);
        ResetCommitted { store: self, ticket: Some(ticket), revision, models: None, operations }.run();
        Ok(revision)
    }

    /// The effect `EraseModels`: the cache of the models goes, intents and `encrypted_*` stay. Returns the new revision.
    pub(crate) fn erase_models(&self, generation: crate::identity::Generation, models: &[String]) -> Result<i64, StoreError> {
        let readers = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        let tables: Vec<(String, String)> = models.iter().map(|model| inner.model(model).map(|info| (model.clone(), info.table.clone()))).collect::<Result<_, _>>()?;
        inner.generation = generation;
        let now = inner.now_ms();
        write_intent(&inner.connection, "cache_only", models, now)?;
        let transaction = inner.connection.unchecked_transaction()?;
        for (model, table) in &tables {
            wipe_model_cache(&transaction, model, table, false)?;
        }
        retire_operations(&transaction, Some(models), None)?;
        let revision = bump_revision(&transaction)?;
        clear_intent(&transaction)?;
        let operations = crate::operation::read_queue(&transaction)?;
        transaction.commit()?;
        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = None;
        drop(readers);
        let event = StoreEvent::Reset { revision, models: Some(models.to_vec()), operations: operations.clone() };
        if let Some(log) = self.commit_log.get() { log(&event); }
        let ticket = self.line.ticket();
        drop(inner);
        ResetCommitted { store: self, ticket: Some(ticket), revision, models: Some(models.to_vec()), operations }.run();
        Ok(revision)
    }
}

/// A write of the effect `Write` to `encrypted_*`, resolved by the executor: strings and numbers as the tables hold them.
pub(crate) enum RowOp {
    PutPending(Pending),
    RemovePending,
    RemoveSession,
    PutAnswer { request_id: u64, operation: Option<i64>, answer: state::StoredAnswer },
    RemoveAnswer { request_id: u64 },
    /// `digest = None` keeps the stored digest, `sequence = None` keeps the stored sequence.
    PutSession { digest: Option<String>, sequence: Option<i64> },
    DiscardRestoreFailed { table: String, key: String },
    /// The durable attempt counter of an operation of the queue.
    PutAttempts { op_id: i64, attempts: i64 },
}

impl Store {
    /// The effect `Write`: the writes of one batch in one transaction. Returns the new revision.
    pub(crate) fn write_ops(&self, ops: Vec<RowOp>) -> Result<i64, StoreError> {
        let inner = self.lock();
        let transaction = inner.connection.unchecked_transaction()?;
        for op in ops {
            match op {
                RowOp::PutPending(pending) => {
                    transaction.execute("DELETE FROM encrypted_pending", [])?;
                    state::store_pending(&transaction, &pending)?;
                }
                RowOp::RemovePending => {
                    transaction.execute("DELETE FROM encrypted_pending", [])?;
                }
                RowOp::RemoveSession => {
                    transaction.execute("DELETE FROM encrypted_session", [])?;
                }
                RowOp::PutAnswer { request_id, operation, answer } => state::store_answer(&transaction, request_id, operation, &answer)?,
                RowOp::RemoveAnswer { request_id } => state::remove_answer(&transaction, request_id)?,
                RowOp::PutAttempts { op_id, attempts } => {
                    transaction.execute("UPDATE op SET attempts = ?2 WHERE seq = ?1", params![op_id, attempts])?;
                }
                RowOp::DiscardRestoreFailed { table, key } => {
                    transaction.execute("DELETE FROM restore_failed WHERE source_table = ?1 AND key = ?2", params![table, key])?;
                }
                RowOp::PutSession { digest, sequence } => {
                    let stored = state::session(&transaction)?;
                    let digest = digest.or_else(|| stored.as_ref().map(|session| session.credential_digest.clone())).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                    let sequence = sequence.or_else(|| stored.as_ref().map(|session| session.sequence)).unwrap_or(0);
                    transaction.execute(
                        "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, ?1, ?2) ON CONFLICT(id) DO UPDATE SET credential_digest = excluded.credential_digest, sequence = excluded.sequence",
                        params![digest, sequence],
                    )?;
                }
            }
        }
        let revision = bump_revision(&transaction)?;
        transaction.commit()?;
        Ok(revision)
    }
}
