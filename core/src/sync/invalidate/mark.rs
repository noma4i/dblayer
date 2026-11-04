use std::collections::BTreeSet;

use super::super::error::SyncError;
use super::super::{locked, Inner};
use super::StaleMark;
use crate::task::Carry;
use std::sync::Arc;

impl Inner {
    pub(crate) fn invalidate_after(&self, admitted: &crate::lifecycle::Admitted, model: &str, action: &str) -> Result<(), SyncError> {
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
        let mut marks = Vec::new();
        for (target, relation) in foreign.iter().filter_map(|name| name.split_once('.')) {
            marks.push(self.mark_stale(admitted, target, BTreeSet::from([relation.to_string()]))?);
        }
        marks.push(self.mark_stale(admitted, model, names)?);
        locked(&self.pending_reloads).extend(marks);
        Ok(())
    }

    fn mark_stale(&self, admitted: &crate::lifecycle::Admitted, model: &str, names: BTreeSet<String>) -> Result<StaleMark, SyncError> {
        let since = self.next_issued();
        for name in &names {
            self.db.shared.store.invalidate_family(&admitted.authority(), model, name)?;
        }
        let admitted = admitted.life().admit_passive(crate::lifecycle::Origin::Explicit, "reload", Some(admitted.value()))?;
        Ok(StaleMark { model: model.to_string(), names, since, admitted })
    }

    pub(crate) fn reload_marked(self: &Arc<Self>) {
        let marked = std::mem::take(&mut *locked(&self.pending_reloads));
        for mark in marked {
            let inner = self.clone();
            let carry = Carry::from(mark.admitted.clone());
            crate::task::spawn(carry, async move { let _ = inner.reload_stale(&mark).await; });
        }
    }

    pub(crate) async fn reload_lists_of_absent_parent(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<(), SyncError> {
        self.invalidate_model(admitted, model).await
    }

    pub(crate) fn mark_lists_after_refused_destroy(&self, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<(), SyncError> {
        let mark = self.mark_model_stale(admitted, model)?;
        locked(&self.pending_reloads).push(mark);
        Ok(())
    }

    async fn invalidate_model(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<(), SyncError> {
        let mark = self.mark_model_stale(admitted, model)?;
        self.reload_stale(&mark).await.map(|_| ())
    }

    fn mark_model_stale(&self, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<StaleMark, SyncError> {
        let spec = self.db.spec(model)?;
        let names: BTreeSet<String> = spec.relations.iter().filter(|relation| relation.remote.is_some() && !relation.joins_events).map(|relation| relation.name.clone()).collect();
        self.mark_stale(admitted, model, names)
    }
}
