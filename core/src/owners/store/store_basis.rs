use super::*;

pub(crate) fn basis_for(entries: impl IntoIterator<Item = (String, u64)>) -> BTreeMap<String, u64> {
    let mut basis = BTreeMap::new();
    for (model, generation) in entries {
        basis.entry(model).and_modify(|value: &mut u64| *value = (*value).min(generation)).or_insert(generation);
    }
    basis
}

impl StoreBatch {
    pub(crate) fn models(&self) -> BTreeSet<String> {
        let mut models: BTreeSet<String> = self.rows.iter().map(|(model, _)| model.clone()).collect();
        for op in &self.ops {
            match op {
                StoreOp::Rows { model, .. } | StoreOp::LocalRows { model, .. } | StoreOp::EphemeralRows { model, .. }
                | StoreOp::LocalPending { model, .. } | StoreOp::RemoveRows { model, .. } | StoreOp::AttachFile { model, .. }
                | StoreOp::DetachFile { model, .. } | StoreOp::Numbers { model, .. } | StoreOp::Swap { model, .. } | StoreOp::ExpireRow { model, .. } | StoreOp::ResetEphemeral { model, .. } => { models.insert(model.clone()); }
                StoreOp::Window { model, related, .. } => {
                    models.insert(model.clone());
                    models.extend(related.iter().map(|(model, _)| model.clone()));
                }
                StoreOp::WindowMarker { model, .. } => { models.insert(model.clone()); }
                StoreOp::Intent { model, effects, .. } | StoreOp::Effects { model, effects, .. } => {
                    models.insert(model.clone());
                    effect_models(&mut models, effects);
                }
                StoreOp::Planned { effects } => effect_models(&mut models, effects),
                StoreOp::Upload { row, .. } => key_model(&mut models, &row.key),
                StoreOp::RemoveUpload { key } | StoreOp::UploadRoundError { key, .. } | StoreOp::UploadOrphan { key, .. } => key_model(&mut models, key),
                _ => {}
            }
        }
        models
    }
}

pub(super) fn effect_models(models: &mut BTreeSet<String>, effects: &IntentEffects) {
    models.extend(effects.rows.iter().map(|row| row.model.clone()));
    models.extend(effects.members.iter().map(|row| row.model.clone()));
    models.extend(effects.tombstones.iter().map(|row| row.model.clone()));
    models.extend(effects.touches.iter().map(|row| row.model.clone()));
}

pub(super) fn key_model(models: &mut BTreeSet<String>, key: &str) {
    if let Some((model, _)) = key.split_once(':') { models.insert(model.to_owned()); }
}
