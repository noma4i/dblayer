use super::{Store, StoreError};

impl Store {
    pub fn scope_members(&self, model: &str, scope_key: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.read(|reader| reader.scope_members(model, scope_key))
    }

    #[cfg(test)]
    pub fn count_members(&self, model: &str, scope_key: &str) -> Result<i64, StoreError> {
        self.read(|reader| reader.count_members(model, scope_key))
    }
}
