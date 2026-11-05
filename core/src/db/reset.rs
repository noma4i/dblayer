#[cfg(test)]
use super::error::DbError;
#[cfg(test)]
use super::open::Db;

#[cfg(test)]
impl Db {
    /// The clearing of `ClearModels` for the tests that run without the owners.
    pub(crate) fn clear_models(&self, models: &[&str]) -> Result<(), DbError> {
        let generation = self.shared.store.generation().next();
        let names: Vec<String> = models.iter().map(|model| model.to_string()).collect();
        {
            let _gate = super::locked(&self.shared.files_gate);
            for model in models {
                self.shared.spec(model)?;
            }
            self.shared.store.erase_models(generation, &names)?;
        }
        self.shared.store.lifecycle.models_reset(generation.value(), &names);
        Ok(())
    }

    /// The clearing of `ClearAccount` for the tests that run without the owners.
    pub(crate) fn clear_account(&self) -> Result<(), DbError> {
        let generation = self.shared.store.generation().next();
        self.shared.store.lifecycle.epoch_ended(crate::runtime::Cause::IdentityChanged);
        {
            let _gate = super::locked(&self.shared.files_gate);
            self.shared.store.erase_account(generation, false)?;
        }
        let epoch = self.shared.store.lifecycle.snapshot().session_epoch + 1;
        self.shared.store.lifecycle.published(&crate::owners::contract::Published { epoch, identity: None, generation: generation.value(), closed: false });
        Ok(())
    }
}
