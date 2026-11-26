use std::collections::BTreeMap;

use rusqlite::params;

use crate::store::ddl::{create_model_tables, fingerprint_key, manifest_stamp, manifest_verify, models_statements, table_name, ManifestVerdict};
use crate::schema::ModelSpec;

use super::reset::{
    current_fingerprints, finish_interrupted_reset, models_to_reset, restore_local_values, stash_local_values, wipe_model_cache,
};
use super::tombstones::{prune_tombstones, TombstoneRetention};
use super::{ModelInfo, Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootReport {
    pub recreated: Option<crate::store::ddl::RecreateReason>,
    pub verdict: ManifestVerdict,
    pub finished_interrupted_reset: bool,
    pub tombstone_drops: usize,
    pub reset_models: Vec<String>,
    pub dropped: Vec<super::open::DroppedRow>,
}

impl Store {
    pub fn boot(&self, models: &[ModelSpec]) -> Result<BootReport, StoreError> {
        let mut reading_models = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        inner.ready = false;
        inner.models = std::sync::Arc::new(models.iter().map(|spec| (spec.key.as_str().to_string(), ModelInfo::new(spec))).collect());
        *reading_models = inner.models.clone();
        let mut migrated_reset = None;
        let mut recreated = None;
        let mut rebuilt = false;
        let mut dropped = Vec::new();

        verify_saved(&inner.connection)?;
        if let Some(reason) = crate::store::ddl::recreate_reason(&inner.connection)? {
            let fingerprints = current_fingerprints(&inner)?;
            let transaction = inner.connection.unchecked_transaction()?;
            let saved_prints = rusqlite::OptionalExtension::optional(transaction.query_row(include_str!("sql/open/table_exists.sql"), ["manifest"], |row| row.get::<_, i64>(0)))?.is_some();
            let mut changed = Vec::new();
            for (model, print) in &fingerprints {
                let saved = if saved_prints { crate::store::ddl::manifest_read(&transaction, &fingerprint_key(model))? } else { None };
                if saved.as_deref() != Some(print.as_str()) { changed.push(model.clone()); }
            }
            let tables = super::reset::row_tables(&transaction)?;
            let mut stash = Vec::new();
            for (model, info) in inner.models.iter().filter(|(_, info)| !info.spec.is_memory()) { super::reset::stash_row_locals(&transaction, (model, info), &tables, changed.contains(model), &mut stash)?; }
            let kept: Vec<String> = inner.models.iter().filter(|(model, info)| info.spec.is_local() && !changed.contains(model)).flat_map(|(_, info)| tables.iter().filter(|name| **name == info.table || name.starts_with(&format!("{}__", info.table))).cloned().collect::<Vec<_>>()).collect();
            let super::open::Recreated { dropped: carried, aside } = super::open::recreate(&transaction, &kept)?;
            dropped = carried;
            for statement in models_statements(models) { transaction.execute_batch(&statement)?; }
            dropped.extend(super::open::carry_back(&transaction, &aside)?);
            for (model, info) in inner.models.iter().filter(|(model, info)| info.spec.is_local() && !changed.contains(model) && !info.search.is_empty()) {
                let ids: Vec<String> = transaction.prepare(&format!(include_str!("sql/reset/stash_local_values.sql"), info.table))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
                for (id, stored) in super::rows::Reader::new(&transaction, &inner.models).stored_rows(model, &ids)? { super::search::reindex(&transaction, info, &id, &stored.row)?; }
            }
            restore_local_values(&transaction, &stash)?;
            crate::store::plan::op_effect::rebuild_open_effects(&transaction, &inner.models)?;
            manifest_stamp(&transaction, &fingerprints)?;
            transaction.commit()?;
            recreated = Some(reason);
            rebuilt = true;
            migrated_reset = Some(models.iter().filter(|spec| !spec.is_memory() && (!spec.is_local() || changed.iter().any(|model| model == spec.key.as_str()))).map(|spec| spec.key.as_str().to_string()).collect());
        } else {
            crate::store::ddl::migrate_core(&inner.connection)?;
        }

        let finished_interrupted_reset = finish_interrupted_reset(&mut inner)?;
        let fingerprints: BTreeMap<String, String> = current_fingerprints(&inner)?;
        let verdict = if rebuilt { ManifestVerdict::Fresh } else { manifest_verify(&inner.connection, &fingerprints)? };
        let now = inner.now_ms();

        let migrated = migrated_reset.is_some();
        let mut reset_models = migrated_reset.unwrap_or_default();
        if !migrated { match &verdict {
            ManifestVerdict::Fresh | ManifestVerdict::Match => {
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, &fingerprints)?;
                transaction.commit()?;
            }
            ManifestVerdict::CacheOnly { models: changed } => {
                let reset = models_to_reset(&inner.models, changed);
                let transaction = inner.connection.unchecked_transaction()?;
                let stash = stash_local_values(&transaction, &inner.models, &reset, changed)?;
                for model in &reset {
                    wipe_model_cache(&transaction, model, &table_name(model), true, true)?;
                    if !fingerprints.contains_key(model) {
                        transaction.execute(include_str!("sql/boot/boot_delete_manifest.sql"), params![fingerprint_key(model)])?;
                    }
                }
                restore_local_values(&transaction, &stash)?;
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, &fingerprints)?;
                transaction.commit()?;
                reset_models = reset;
            }
        } }

        super::paged_order::rank_cached_scopes(&inner.connection, &inner.models)?;

        let memory: Vec<(String, String)> =
            inner.models.iter().filter(|(_, info)| info.spec.is_memory()).map(|(key, info)| (key.clone(), info.table.clone())).collect();
        if !memory.is_empty() {
            let transaction = inner.connection.unchecked_transaction()?;
            for (model, table) in &memory {
                wipe_model_cache(&transaction, model, table, false, false)?;
            }
            transaction.commit()?;
        }

        let tombstone_drops = prune_tombstones(&inner.connection, now, &TombstoneRetention::DEFAULT, None)?;
        inner.connection.execute(include_str!("sql/boot/boot_delete_stale_outcome.sql"), [now.saturating_sub(7 * 86_400_000)])?;
        inner.connection.execute("DELETE FROM prepared_cache WHERE created_at <= ?1", [now.saturating_sub(crate::owners::write::transfer::EXPIRY_MS as i64)])?;
        let database: String = inner.connection.query_row(include_str!("sql/boot/boot_select_pragma_database_list.sql"), [], |row| row.get(0))?;
        let files: std::collections::HashSet<String> = inner.connection.prepare(include_str!("sql/boot/boot_select_file.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        let prepared_files: std::collections::HashSet<String> = inner.connection.prepare("SELECT file FROM prepared_cache")?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        let mut transfer_holders: std::collections::HashSet<String> = inner.connection.prepare(include_str!("sql/boot/boot_select_sync_upload.sql"))?.query_map([], |row| row.get::<_, i64>(0).map(|id| id.to_string()))?.collect::<rusqlite::Result<_>>()?;
        let failed = super::restore::read_failed(&inner.connection)?;
        transfer_holders.extend(failed.iter().filter_map(|row| row.transfer_id().map(|id| id.to_string())));
        self.overlays.reset(super::rows::read_revision(&inner.connection)?);
        drop(inner);
        drop(reading_models);
        if !database.is_empty() {
            let directories = crate::store::files::FileDirectories::beside(std::path::Path::new(&database));
            if files.is_empty() && transfer_holders.is_empty() && prepared_files.is_empty() {
                directories.remove_all()?;
            } else {
                directories.recover_partials(&files)?;
                remove_unheld_transfer_copies(&directories, &transfer_holders)?;
                remove_unheld_prepared(&directories, &prepared_files)?;
            }
        }
        Ok(BootReport { recreated, verdict, finished_interrupted_reset, tombstone_drops, reset_models, dropped })
    }
}

fn remove_unheld_prepared(directories: &crate::store::files::FileDirectories, files: &std::collections::HashSet<String>) -> std::io::Result<()> {
    let Some(prepared) = directories.path("prepared") else { return Ok(()) };
    let entries = match std::fs::read_dir(&prepared) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if entry.file_type()?.is_file() && !name.to_str().is_some_and(|name| files.contains(name)) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn remove_unheld_transfer_copies(directories: &crate::store::files::FileDirectories, holders: &std::collections::HashSet<String>) -> std::io::Result<()> {
    let Some(copies) = directories.path("transfers") else { return Ok(()) };
    let entries = match std::fs::read_dir(&copies) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let transfer_id = name.to_str().and_then(|name| name.split('.').next());
        if entry.file_type()?.is_file() && !transfer_id.is_some_and(|id| holders.contains(id)) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn verify_saved(connection: &rusqlite::Connection) -> Result<(), StoreError> {
    let has_manifest: bool = connection.query_row(include_str!("sql/boot/verify_saved.sql"), [], |row| row.get(0))?;
    if !has_manifest {
        return Ok(());
    }
    for key in ["saved_epoch", "saved_generation"] {
        if let Some(value) = crate::store::ddl::manifest_read(connection, key)? {
            value.parse::<u64>().map_err(|_| StoreError::CorruptManifest(format!("{key} = {value:?} is not a u64")))?;
        }
    }
    Ok(())
}
