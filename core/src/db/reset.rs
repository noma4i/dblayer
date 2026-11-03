use super::error::DbError;
use super::open::Db;

impl Db {
    pub(crate) fn testing_busy(&self) -> Result<Option<&'static str>, DbError> {
        let expiry = match self.shared.expiry_gate.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return Ok(Some("expiry")),
        };
        let ephemeral = self.shared.ephemeral.lock();
        if ephemeral.next_due().is_some_and(|due| due <= ephemeral.clock.now()) {
            return Ok(Some("expiry"));
        }
        drop(ephemeral);
        drop(expiry);
        let revision = self.revision_seq()?;
        if !self.shared.dispatch.quiet_through(revision) || !self.shared.reached_through(revision) {
            return Ok(Some("listeners"));
        }
        if !super::locked(&self.shared.upload_round).is_idle() {
            return Ok(Some("upload round"));
        }
        Ok(None)
    }

    pub fn reset(&self, models: &[&str]) -> Result<(), DbError> {
        let effects = {
            let _gate = super::locked(&self.shared.files_gate);
            for model in models {
                self.shared.spec(model)?;
            }
            self.shared.store.reset_cache_only_held(models)?
        };
        effects.run();
        Ok(())
    }

    pub fn reset_all(&self) -> Result<(), DbError> {
        let effects = {
            let _gate = super::locked(&self.shared.files_gate);
            self.shared.store.reset_incompatible_held()?
        };
        effects.run();
        Ok(())
    }
}
