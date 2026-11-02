use super::error::DbError;
use super::open::Db;

pub(crate) use crate::store::ResetListener;

impl Db {
    pub(crate) fn on_reset(&self, listener: ResetListener) {
        self.shared.store.on_reset(listener);
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
