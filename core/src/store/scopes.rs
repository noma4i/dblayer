use super::{Store, StoreError};

impl Store {
    pub fn scope_members(&self, model: &str, scope_key: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.read(&[model], |reader| reader.scope_members(model, scope_key))
    }
}
