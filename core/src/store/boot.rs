use std::collections::BTreeMap;

use rusqlite::params;

use crate::ddl::{create_model_tables, fingerprint_key, manifest_stamp, manifest_verify, rebuild_cache_tables, table_name, ManifestVerdict};
use crate::operation::QuarantineReason;
use crate::schema::ModelSpec;

use super::reset::{
    clear_intent, current_fingerprints, finish_interrupted_reset, models_to_reset, restore_local_values, retire_operations, stash_local_values, wipe_all_cache,
    wipe_model_cache, write_intent,
};
use super::tombstones::{prune_tombstones, TombstoneRetention};
use super::{ModelInfo, Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootReport {
    pub verdict: ManifestVerdict,
    pub finished_interrupted_reset: bool,
    pub tombstone_drops: usize,
    pub reset_models: Vec<String>,
}

impl Store {
    pub fn boot(&self, models: &[ModelSpec], data_version: Option<&str>) -> Result<BootReport, StoreError> {
        let mut reading_models = self.models.write().unwrap_or_else(|error| error.into_inner());
        let mut inner = self.lock();
        inner.ready = false;
        inner.data_version = data_version.map(str::to_string);
        inner.models = std::sync::Arc::new(models.iter().map(|spec| (spec.key.as_str().to_string(), ModelInfo::new(spec))).collect());
        *reading_models = inner.models.clone();

        self.session_end_replay.store(super::reset::pending_intent(&inner.connection)?.as_deref() == Some("session_end"), std::sync::atomic::Ordering::SeqCst);
        let finished_interrupted_reset = finish_interrupted_reset(&mut inner)?;
        let fingerprints: BTreeMap<String, String> = current_fingerprints(&inner);
        let verdict = manifest_verify(&inner.connection, data_version, &fingerprints)?;
        let now = inner.now_ms();

        let mut reset_models: Vec<String> = Vec::new();
        match &verdict {
            ManifestVerdict::Fresh | ManifestVerdict::Compatible => {
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
                        transaction.execute("DELETE FROM manifest WHERE key = ?1", params![fingerprint_key(model)])?;
                    }
                }
                restore_local_values(&transaction, &stash)?;
                retire_operations(&transaction, Some(&reset), Some(QuarantineReason::SchemaChanged))?;
                transaction.commit()?;
                create_model_tables(&inner.connection, models)?;
                let transaction = inner.connection.unchecked_transaction()?;
                manifest_stamp(&transaction, data_version, &fingerprints)?;
                transaction.commit()?;
                reset_models = reset;
            }
            ManifestVerdict::Incompatible => {
                write_intent(&inner.connection, "incompatible", &[], now)?;
                let transaction = inner.connection.unchecked_transaction()?;
                wipe_all_cache(&transaction, true)?;
                reset_models = models.iter().filter(|spec| !spec.is_memory()).map(|spec| spec.key.as_str().to_string()).collect();
                rebuild_cache_tables(&transaction)?;
                retire_operations(&transaction, None, Some(QuarantineReason::SchemaChanged))?;
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

        let tombstone_drops = prune_tombstones(&inner.connection, now, &TombstoneRetention::DEFAULT)?;
        Ok(BootReport { verdict, finished_interrupted_reset, tombstone_drops, reset_models })
    }
}
