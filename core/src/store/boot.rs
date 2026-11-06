use std::collections::BTreeMap;

use rusqlite::params;

use crate::store::ddl::{create_model_tables, fingerprint_key, manifest_stamp, manifest_verify, models_statements, rebuild_cache_tables, table_name, ManifestVerdict};
use crate::schema::ModelSpec;

use super::reset::{
    clear_intent, current_fingerprints, finish_interrupted_reset, models_to_reset, restore_local_values, stash_local_values, wipe_all_cache,
    wipe_model_cache, write_intent,
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
    pub fn boot(&self, models: &[ModelSpec], data_version: Option<&str>) -> Result<BootReport, StoreError> {
        let mut reading_models = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        inner.ready = false;
        inner.data_version = data_version.map(str::to_string);
        inner.models = std::sync::Arc::new(models.iter().map(|spec| (spec.key.as_str().to_string(), ModelInfo::new(spec))).collect());
        *reading_models = inner.models.clone();
        let mut removed_files: Option<(String, Vec<String>)> = None;
        let mut remove_all_files = None;
        let mut migrated_reset = None;
        let mut recreated = None;
        let mut rebuilt = false;
        let mut dropped = Vec::new();

        verify_saved(&inner.connection)?;
        if let Some(reason) = crate::store::ddl::recreate_reason(&inner.connection)? {
            let fingerprints = current_fingerprints(&inner);
            let transaction = inner.connection.unchecked_transaction()?;
            let database: String = transaction.query_row(include_str!("sql/boot/boot_select_pragma_database_list.sql"), [], |row| row.get(0))?;
            let has_files: bool = transaction.query_row(include_str!("sql/boot/boot_select_sqlite_master.sql"), [], |row| row.get(0))?;
            let before: Vec<String> = if has_files { transaction.prepare(include_str!("sql/boot/boot_select_file.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()? } else { Vec::new() };
            let recreated_layout = super::open::recreate(&transaction)?;
            let outbox = recreated_layout.files_kept;
            dropped = recreated_layout.dropped;
            for statement in models_statements(models) { transaction.execute_batch(&statement)?; }
            crate::store::plan::op_effect::rebuild_open_effects(&transaction, &inner.models)?;
            let after: std::collections::HashSet<String> = transaction.prepare(include_str!("sql/boot/boot_select_file.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
            manifest_stamp(&transaction, data_version, &fingerprints)?;
            transaction.commit()?;
            if !database.is_empty() {
                if outbox { removed_files = Some((database, before.into_iter().filter(|id| !after.contains(id)).collect())); } else { remove_all_files = Some(database); }
            }
            recreated = Some(reason);
            rebuilt = true;
            migrated_reset = Some(models.iter().filter(|spec| !spec.is_memory()).map(|spec| spec.key.as_str().to_string()).collect());
        } else {
            crate::store::ddl::migrate_core(&inner.connection)?;
        }

        self.session_end_replay.store(super::reset::pending_intent(&inner.connection)?.as_deref() == Some("session_end"), std::sync::atomic::Ordering::SeqCst);
        let finished_interrupted_reset = finish_interrupted_reset(&mut inner)?;
        let fingerprints: BTreeMap<String, String> = current_fingerprints(&inner);
        let verdict = if rebuilt { ManifestVerdict::Fresh } else { manifest_verify(&inner.connection, data_version, &fingerprints)? };
        let now = inner.now_ms();

        let migrated = migrated_reset.is_some();
        let mut reset_models = migrated_reset.unwrap_or_default();
        if !migrated { match &verdict {
            ManifestVerdict::Fresh | ManifestVerdict::Match => {
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                transaction.commit()?;
            }
            ManifestVerdict::CacheOnly { models: changed } => {
                let reset = models_to_reset(&inner.models, changed);
                let transaction = inner.connection.unchecked_transaction()?;
                let stash = stash_local_values(&transaction, &inner.models, &reset, changed)?;
                for model in &reset {
                    wipe_model_cache(&transaction, model, &table_name(model), true)?;
                    if !fingerprints.contains_key(model) {
                        transaction.execute(include_str!("sql/boot/boot_delete_manifest.sql"), params![fingerprint_key(model)])?;
                    }
                }
                restore_local_values(&transaction, &stash)?;
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                transaction.commit()?;
                reset_models = reset;
            }
            ManifestVerdict::Mismatch => {
                write_intent(&inner.connection, "mismatched", &[], now)?;
                let transaction = inner.connection.unchecked_transaction()?;
                wipe_all_cache(&transaction, true, true)?;
                reset_models = models.iter().filter(|spec| !spec.is_memory()).map(|spec| spec.key.as_str().to_string()).collect();
                rebuild_cache_tables(&transaction)?;
                transaction.execute(include_str!("sql/boot/boot_delete_manifest_2.sql"), [])?;
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                clear_intent(&transaction)?;
                transaction.commit()?;
            }
        } }

        let memory: Vec<(String, String)> =
            inner.models.iter().filter(|(_, info)| info.spec.is_memory()).map(|(key, info)| (key.clone(), info.table.clone())).collect();
        if !memory.is_empty() {
            let transaction = inner.connection.unchecked_transaction()?;
            for (model, table) in &memory {
                wipe_model_cache(&transaction, model, table, false)?;
            }
            transaction.commit()?;
        }

        let tombstone_drops = prune_tombstones(&inner.connection, now, &TombstoneRetention::DEFAULT, None)?;
        let database: String = inner.connection.query_row(include_str!("sql/boot/boot_select_pragma_database_list.sql"), [], |row| row.get(0))?;
        let files: std::collections::HashSet<String> = inner.connection.prepare(include_str!("sql/boot/boot_select_file.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        let transfer_holders: std::collections::HashSet<String> = inner.connection.prepare(include_str!("sql/boot/boot_select_sync_upload.sql"))?.query_map([], |row| row.get::<_, i64>(0).map(|id| id.to_string()))?.collect::<rusqlite::Result<_>>()?;
        self.overlays.reset(super::rows::read_revision(&inner.connection)?);
        drop(inner);
        drop(reading_models);
        if let Some((database, files)) = removed_files { crate::store::files::FileDirectories::beside(std::path::Path::new(&database)).discard(&files)?; }
        if let Some(database) = remove_all_files { crate::store::files::FileDirectories::beside(std::path::Path::new(&database)).remove_all()?; }
        if !database.is_empty() {
            let directories = crate::store::files::FileDirectories::beside(std::path::Path::new(&database));
            directories.recover_partials(&files)?;
            remove_unheld_transfer_copies(&directories, &transfer_holders)?;
        }
        Ok(BootReport { recreated, verdict, finished_interrupted_reset, tombstone_drops, reset_models, dropped })
    }
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
