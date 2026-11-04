use std::collections::BTreeSet;

use super::super::error::SyncError;
use super::super::{locked, Inner};
use std::sync::Arc;

impl Inner {
    pub(crate) fn invalidate_after(&self, model: &str, action: &str) -> Result<(), SyncError> {
        let spec = self.db.spec(model)?;
        let mut names: BTreeSet<String> = spec
            .actions
            .iter()
            .find(|declared| declared.name == action)
            .map(|declared| declared.refetch.iter().cloned().collect())
            .unwrap_or_default();
        if matches!(action, "create" | "destroy") {
            let takes_response = action == "create";
            names.extend(spec.relations.iter().filter(|relation| relation.remote.is_some() && !(takes_response && relation.joins_events)).map(|relation| relation.name.clone()));
        }
        let foreign: Vec<String> = names.iter().filter(|name| name.contains('.')).cloned().collect();
        for name in &foreign {
            names.remove(name);
        }
        for (target, relation) in foreign.iter().filter_map(|name| name.split_once('.')) {
            self.mark_stale(target, BTreeSet::from([relation.to_string()]))?;
        }
        self.mark_stale(model, names)
    }

    fn mark_stale(&self, model: &str, names: BTreeSet<String>) -> Result<(), SyncError> {
        for name in &names {
            self.db.shared.store.invalidate_family(&self.db.authority()?, model, name)?;
        }
        locked(&self.pending_reloads).push((model.to_string(), names));
        Ok(())
    }

    pub(crate) async fn reload_marked(self: &Arc<Self>) -> Result<(), SyncError> {
        let marked = std::mem::take(&mut *locked(&self.pending_reloads));
        let mut errors = Vec::new();
        for (model, names) in marked {
            if let Err(error) = self.reload_stale(&model, &names).await { errors.push(error.to_string()); }
        }
        if errors.is_empty() { Ok(()) } else { Err(SyncError::Event(errors.join("\n"))) }
    }

    pub(crate) async fn reload_lists_of_absent_parent(self: &Arc<Self>, model: &str) -> Result<(), SyncError> {
        self.invalidate_model(model).await
    }

    pub(crate) fn mark_lists_after_refused_destroy(&self, model: &str) -> Result<(), SyncError> {
        self.mark_model_stale(model)
    }

    async fn invalidate_model(self: &Arc<Self>, model: &str) -> Result<(), SyncError> {
        self.mark_model_stale(model)?;
        self.reload_marked().await
    }

    fn mark_model_stale(&self, model: &str) -> Result<(), SyncError> {
        let spec = self.db.spec(model)?;
        let names: BTreeSet<String> = spec.relations.iter().filter(|relation| relation.remote.is_some() && !relation.joins_events).map(|relation| relation.name.clone()).collect();
        self.mark_stale(model, names)
    }
}
