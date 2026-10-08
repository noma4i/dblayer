use std::collections::BTreeMap;

use rusqlite::{params, Connection};

use crate::ddl::{
    create_model_tables, fingerprint_key, manifest_read, manifest_stamp, manifest_verify, stale_version_delete,
    table_name, ManifestVerdict, RECORD_VERSION, RECORD_VERSION_KEY,
};
use crate::schema::ModelSpec;

use super::reset::{
    clear_intent, current_fingerprints, finish_interrupted_reset, wipe_all_cache, wipe_model_cache, write_intent,
};
use super::tombstones::{prune_tombstones, TombstoneRetention};
use super::{ModelInfo, Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootReport {
    pub verdict: ManifestVerdict,
    pub finished_interrupted_reset: bool,
    pub stale_version_drops: usize,
    pub tombstone_drops: usize,
}

fn drop_stale_versions(connection: &Connection, model: &str) -> Result<usize, StoreError> {
    let table = table_name(model);
    connection.execute(
        &format!("DELETE FROM scope_member WHERE model = ?1 AND row_id IN (SELECT id FROM {table} WHERE record_version <> ?2)"),
        params![model, RECORD_VERSION],
    )?;
    Ok(connection.execute(&stale_version_delete(model), [])?)
}

impl Store {
    pub fn boot(&self, models: &[ModelSpec], data_version: Option<&str>) -> Result<BootReport, StoreError> {
        let mut inner = self.lock();
        inner.ready = false;
        inner.data_version = data_version.map(str::to_string);
        inner.models = models.iter().map(|spec| (spec.key.as_str().to_string(), ModelInfo::new(spec))).collect();

        let finished_interrupted_reset = finish_interrupted_reset(&mut inner)?;
        let fingerprints: BTreeMap<String, String> = current_fingerprints(&inner);
        let verdict = manifest_verify(&inner.connection, data_version, &fingerprints)?;
        let stored_record_version = manifest_read(&inner.connection, RECORD_VERSION_KEY)?;
        let now = inner.now_ms();

        match &verdict {
            ManifestVerdict::Fresh | ManifestVerdict::Compatible => {
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                transaction.commit()?;
            }
            ManifestVerdict::CacheOnly { models: mismatched } => {
                write_intent(&inner.connection, "cache_only", mismatched, now)?;
                let transaction = inner.connection.unchecked_transaction()?;
                for model in mismatched {
                    wipe_model_cache(&transaction, model, &table_name(model), true)?;
                    if !fingerprints.contains_key(model) {
                        transaction.execute("DELETE FROM manifest WHERE key = ?1", params![fingerprint_key(model)])?;
                    }
                }
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                clear_intent(&transaction)?;
                transaction.commit()?;
            }
            ManifestVerdict::Incompatible => {
                write_intent(&inner.connection, "incompatible", &[], now)?;
                let transaction = inner.connection.unchecked_transaction()?;
                wipe_all_cache(&transaction, true)?;
                transaction.execute("DELETE FROM manifest WHERE key LIKE 'fingerprint:%'", [])?;
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                clear_intent(&transaction)?;
                transaction.commit()?;
            }
        }

        let memory: Vec<(String, String)> =
            inner.models.iter().filter(|(_, info)| info.spec.is_memory()).map(|(key, info)| (key.clone(), info.table.clone())).collect();
        if !memory.is_empty() {
            let transaction = inner.connection.unchecked_transaction()?;
            for (model, table) in &memory {
                wipe_model_cache(&transaction, model, table, false)?;
            }
            transaction.commit()?;
        }

        let mut stale_version_drops = 0;
        let version_text = RECORD_VERSION.to_string();
        let sweeps = matches!(verdict, ManifestVerdict::Compatible | ManifestVerdict::CacheOnly { .. })
            && stored_record_version.as_deref().is_some_and(|stored| stored != version_text);
        if sweeps {
            for model in inner.models.keys().cloned().collect::<Vec<_>>() {
                let transaction = inner.connection.unchecked_transaction()?;
                stale_version_drops += drop_stale_versions(&transaction, &model)?;
                transaction.commit()?;
            }
        }
        inner.counters.stale_version_drops += stale_version_drops;
        let tombstone_drops = prune_tombstones(&inner.connection, now, &TombstoneRetention::DEFAULT)?;
        Ok(BootReport { verdict, finished_interrupted_reset, stale_version_drops, tombstone_drops })
    }

    pub fn drop_stale_versions(&self, model: &str) -> Result<usize, StoreError> {
        let mut inner = self.lock();
        inner.ensure_healthy()?;
        inner.model(model)?;
        let transaction = inner.connection.unchecked_transaction()?;
        let dropped = drop_stale_versions(&transaction, model)?;
        transaction.commit()?;
        inner.counters.stale_version_drops += dropped;
        Ok(dropped)
    }
}
