use super::error::DbError;
use super::open::Db;

use std::sync::Arc;

pub(crate) type ResetListener = Arc<dyn Fn(Option<&[String]>) + Send + Sync>;

impl Db {
    pub(crate) fn on_reset(&self, listener: ResetListener) {
        super::open::locked(&self.shared.reset_listeners).push(listener);
    }

    pub fn reset(&self, models: &[&str]) -> Result<(), DbError> {
        let _gate = super::locked(&self.shared.files_gate);
        for model in models {
            self.shared.spec(model)?;
        }
        self.shared.store.reset_cache_only(models)?;
        Ok(())
    }

    pub fn reset_all(&self) -> Result<(), DbError> {
        let _gate = super::locked(&self.shared.files_gate);
        self.shared.store.reset_incompatible()?;
        Ok(())
    }
}
