use crate::lifecycle::{Boundary, Permit};
use crate::store::StoreError;

use super::error::DbError;
use super::open::Db;

impl Db {
    pub fn reset(&self, models: &[&str]) -> Result<(), DbError> {
        let permit = self.shared.store.lifecycle.begin_blocking(Boundary::Models, None).map_err(StoreError::refused)?;
        self.reset_in(&permit, models)?;
        permit.finish();
        Ok(())
    }

    pub fn reset_all(&self) -> Result<(), DbError> {
        let permit = self.shared.store.lifecycle.begin_blocking(Boundary::Content, None).map_err(StoreError::refused)?;
        self.reset_all_in(&permit)?;
        permit.finish();
        Ok(())
    }

    /// Clears the cache of these models inside the transition the permit owns.
    pub(crate) fn reset_in(&self, permit: &Permit, models: &[&str]) -> Result<(), DbError> {
        let effects = {
            let _gate = super::locked(&self.shared.files_gate);
            for model in models {
                self.shared.spec(model)?;
            }
            self.shared.store.reset_cache_only_held(permit, models)?
        };
        effects.run();
        Ok(())
    }

    /// Clears every cache and retires the outbox inside the transition the permit owns.
    pub(crate) fn reset_all_in(&self, permit: &Permit) -> Result<(), DbError> {
        let effects = {
            let _gate = super::locked(&self.shared.files_gate);
            self.shared.store.reset_incompatible_held(permit)?
        };
        effects.run();
        Ok(())
    }
}
