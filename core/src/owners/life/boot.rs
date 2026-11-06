use super::*;

pub(super) fn boot_found(db: &Store, saved_unowned_pending: bool) -> Result<BootFound, LifeError> {
    let (saved_epoch, saved_generation, saved_identity) = db.saved().map_err(|error| match error {
        crate::store::StoreError::CorruptManifest(detail) => LifeError::CorruptManifest(detail),
        other => LifeError::Storage(other.to_string()),
    })?;
    let revision = db.revision_seq().ok().and_then(|revision| u64::try_from(revision).ok()).ok_or_else(|| LifeError::Storage("the stored revision is not a revision".into()))?;
    let report = db.open_report();
    let server_model_changed = report.changed_models.iter().any(|model| db.specs().get(model).is_none_or(|spec| !spec.is_local()));
    Ok(if report.recreated == Some(crate::ddl::RecreateReason::UnreadableFile) {
        BootFound::Fresh
    } else if report.recreated.is_some() {
        BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision }
    } else if server_model_changed {
        BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision }
    } else if !report.changed_models.is_empty() {
        BootFound::LocalModelChanged { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision }
    } else {
        BootFound::Same { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision }
    })
}
