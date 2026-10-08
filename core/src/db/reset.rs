use super::error::DbError;
use super::open::Db;

impl Db {
    pub fn reset(&self, models: &[&str]) -> Result<(), DbError> {
        for model in models {
            self.shared.spec(model)?;
        }
        self.shared.store.reset_cache_only(models)?;
        Ok(())
    }

    pub fn reset_all(&self) -> Result<(), DbError> {
        self.shared.store.reset_incompatible()?;
        Ok(())
    }
}
