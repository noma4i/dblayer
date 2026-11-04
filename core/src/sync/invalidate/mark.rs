use std::collections::BTreeSet;

use super::super::error::SyncError;
use super::super::{locked, Inner};
use super::{WindowEvent, WindowMark, WindowTrigger};
use std::sync::Arc;

impl WindowMark {
    pub(crate) fn debts(&self) -> Vec<crate::commit::WindowDebt> {
        self.families.iter().map(|(model, relation)| crate::commit::WindowDebt { model: model.clone(), relation: relation.clone(), since: self.since }).collect()
    }
}

impl Inner {
    pub(crate) fn window_event(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, event: WindowTrigger) -> Result<(), SyncError> {
        if !Arc::ptr_eq(admitted.life(), &self.db.shared.store.lifecycle) || !admitted.is_current() { return Err(SyncError::Cancelled); }
        let (model, joins_events) = match event {
            WindowTrigger::Lost { model } => (model, true),
            WindowTrigger::ParentMissing { model } => (model, false),
        };
        let names = self.db.spec(&model)?.relations.iter().filter(|relation| relation.remote.is_some() && (joins_events || !relation.joins_events)).map(|relation| relation.name.clone()).collect();
        let Some(mark) = self.prepare_window_families(model, names)? else { return Ok(()) };
        for (model, relation) in &mark.families {
            self.db.shared.store.invalidate_family(&admitted.authority(), model, relation, mark.since)?;
        }
        self.window_mark_committed(admitted, mark)
    }

    pub(crate) fn prepare_window_event(&self, event: WindowEvent) -> Result<Option<WindowMark>, SyncError> {
        let (model, names) = match event {
            WindowEvent::Mutation { model, action } => {
                let spec = self.db.spec(&model)?;
                let mut names: BTreeSet<String> = spec.actions.iter().find(|declared| declared.name == action)
                    .map(|declared| declared.refetch.iter().cloned().collect()).unwrap_or_default();
                if matches!(action.as_str(), "create" | "destroy") {
                    names.extend(spec.relations.iter().filter(|relation| relation.remote.is_some() && !(action == "create" && relation.joins_events)).map(|relation| relation.name.clone()));
                }
                (model, names)
            }
            WindowEvent::DestroyRefused { model } => {
                let names = self.db.spec(&model)?.relations.iter().filter(|relation| relation.remote.is_some() && !relation.joins_events).map(|relation| relation.name.clone()).collect();
                (model, names)
            }
        };
        self.prepare_window_families(model, names)
    }

    fn prepare_window_families(&self, model: String, names: BTreeSet<String>) -> Result<Option<WindowMark>, SyncError> {
        if names.is_empty() { return Ok(None); }
        let since = self.next_issued()?;
        let epoch = self.owner.epoch();
        let families = names.into_iter().map(|name| {
            let (target, relation) = name.split_once('.').unwrap_or((&model, &name));
            (target.to_string(), relation.to_string())
        }).collect();
        Ok(Some(WindowMark { since, epoch, families }))
    }

    /// Hands the debt that the commit of an outcome wrote to the window machine once the effects of that commit run.
    pub(crate) fn queue_window_mark(&self, admitted: &crate::lifecycle::Admitted, effects: &super::super::send::DeliveryEffects, mark: Option<WindowMark>) {
        let Some(mark) = mark else { return };
        let (inner, admitted) = (effects.inner.clone(), admitted.clone());
        locked(&effects.pending).push(move || {
            let Some(inner) = inner.upgrade() else { return };
            let _ = inner.window_mark_committed(&admitted, mark);
        });
    }

    pub(crate) fn window_mark_committed(&self, admitted: &crate::lifecycle::Admitted, mark: WindowMark) -> Result<(), SyncError> {
        if !Arc::ptr_eq(admitted.life(), &self.db.shared.store.lifecycle) || !admitted.is_current() { return Err(SyncError::Cancelled); }
        for (model, relation) in mark.families {
            self.owner.submit(super::super::load_machine::Event::StaleFamily { model, relation, since: mark.since, epoch: mark.epoch });
        }
        Ok(())
    }

    /// Marks the lists of a model whose parent row is absent and returns once every observed list of it was read again. The read of the window machine and this one share one request.
    pub(crate) async fn reload_lists_of_absent_parent(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, model: &str) -> Result<(), SyncError> {
        self.window_event(admitted, WindowTrigger::ParentMissing { model: model.to_string() })?;
        self.reload_watched(admitted, model, None, false).await.map(drop)
    }
}
