use std::collections::BTreeSet;

use super::super::error::SyncError;
use super::super::Inner;

impl Inner {
    pub(crate) async fn invalidate_after(&self, model: &str, action: &str) -> Result<(), SyncError> {
        let spec = self.db.spec(model)?;
        let mut names: BTreeSet<String> = spec
            .actions
            .iter()
            .find(|declared| declared.name == action)
            .map(|declared| declared.refetch.iter().cloned().collect())
            .unwrap_or_default();
        if matches!(action, "create" | "destroy") {
            names.extend(spec.relations.iter().filter(|relation| relation.remote.is_some()).map(|relation| relation.name.clone()));
        }
        let foreign: Vec<String> = names.iter().filter(|name| name.contains('.')).cloned().collect();
        for name in &foreign {
            names.remove(name);
        }
        for (target, relation) in foreign.iter().filter_map(|name| name.split_once('.')) {
            self.mark_and_reload(target, &BTreeSet::from([relation.to_string()])).await?;
        }
        self.mark_and_reload(model, &names).await
    }

    pub(crate) async fn reload_lists_of_absent_parent(&self, model: &str) -> Result<(), SyncError> {
        self.invalidate_model(model).await
    }

    pub(crate) async fn reload_lists_after_refused_destroy(&self, model: &str) -> Result<(), SyncError> {
        self.invalidate_model(model).await
    }

    async fn invalidate_model(&self, model: &str) -> Result<(), SyncError> {
        let spec = self.db.spec(model)?;
        let names = spec.relations.iter().filter(|relation| relation.remote.is_some() && !relation.joins_events).map(|relation| relation.name.clone()).collect();
        self.mark_and_reload(model, &names).await
    }

    async fn mark_and_reload(&self, model: &str, names: &BTreeSet<String>) -> Result<(), SyncError> {
        for name in names {
            self.db.shared.store.invalidate_family(model, name)?;
        }
        self.reload_watched(model, Some(names), true).await;
        Ok(())
    }
}
