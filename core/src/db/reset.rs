#[cfg(test)]
use super::error::DbError;
#[cfg(test)]
use super::open::Db;

#[cfg(test)]
impl Db {
    /// The clearing of `ClearModels` for the tests that run without the owners.
    pub(crate) fn clear_models(&self, models: &[&str]) -> Result<(), DbError> {
        let _gate = super::locked(&self.shared.files_gate);
        for model in models {
            self.shared.spec(model)?;
        }
        Ok(self.shared.store.clear_models_in_tests(models)?)
    }

    /// The clearing of `ClearAccount` for the tests that run without the owners.
    pub(crate) fn clear_account(&self) -> Result<(), DbError> {
        let _gate = super::locked(&self.shared.files_gate);
        Ok(self.shared.store.clear_account_in_tests()?)
    }
}
