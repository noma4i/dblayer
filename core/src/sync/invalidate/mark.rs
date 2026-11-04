use std::collections::BTreeSet;

use super::super::error::SyncError;
use super::super::{locked, Inner};
use super::StaleMark;
use crate::task::Carry;
use std::sync::Arc;

impl Inner {
    pub(crate) fn mutation_debts(&self, model: &str, action: &str) -> Result<Vec<crate::commit::WindowDebt>, SyncError> {
        let spec = self.db.spec(model)?;
        let mut names: BTreeSet<String> = spec.actions.iter().find(|declared| declared.name == action)
            .map(|declared| declared.refetch.iter().cloned().collect()).unwrap_or_default();
        if matches!(action, "create" | "destroy") {
            names.extend(spec.relations.iter().filter(|relation| relation.remote.is_some() && !(action == "create" && relation.joins_events)).map(|relation| relation.name.clone()));
        }
        self.debts_for(model, names)
    }

    pub(crate) fn refused_destroy_debts(&self, model: &str) -> Result<Vec<crate::commit::WindowDebt>, SyncError> {
        let names = self.db.spec(model)?.relations.iter().filter(|relation| relation.remote.is_some() && !relation.joins_events).map(|relation| relation.name.clone()).collect();
        self.debts_for(model, names)
    }

    fn debts_for(&self, model: &str, names: BTreeSet<String>) -> Result<Vec<crate::commit::WindowDebt>, SyncError> {
        if names.is_empty() { return Ok(Vec::new()); }
        let since = self.next_issued()?;
        Ok(names.into_iter().map(|name| {
            let (target, relation) = name.split_once('.').unwrap_or((model, &name));
            crate::commit::WindowDebt { model: target.to_string(), relation: relation.to_string(), since }
        }).collect())
    }

    pub(crate) fn queue_committed_debts(&self, admitted: &crate::lifecycle::Admitted, effects: &super::super::send::DeliveryEffects, debts: &[crate::commit::WindowDebt]) -> Result<(), SyncError> {
        let retained = admitted.life().admit_passive(crate::lifecycle::Origin::Explicit, "reload", Some(admitted.value()))?;
        for debt in debts {
            let mark = StaleMark { model: debt.model.clone(), names: BTreeSet::from([debt.relation.clone()]), since: debt.since, admitted: retained.clone() };
            let inner = effects.inner.clone();
            locked(&effects.pending).push(move || {
                let Some(inner) = inner.upgrade() else { return };
                let carry = Carry::from(mark.admitted.clone());
                crate::task::spawn(carry, async move { let _ = inner.reload_stale(&mark).await; });
            });
        }
        Ok(())
    }

    fn mark_stale(&self, admitted: &crate::lifecycle::Admitted, model: &str, names: BTreeSet<String>) -> Result<StaleMark, SyncError> {
        let since = self.next_issued()?;
        for name in &names {
            self.db.shared.store.invalidate_family(&admitted.authority(), model, name, since)?;
        }
        let admitted = admitted.life().admit_passive(crate::lifecycle::Origin::Explicit, "reload", Some(admitted.value()))?;
        Ok(StaleMark { model: model.to_string(), names, since, admitted })
    }

    pub(crate) async fn reload_lists_of_absent_parent(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<(), SyncError> {
        self.invalidate_model(admitted, model).await
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
