use rusqlite::params;

use super::{Store, StoreError};

impl Store {
    pub fn scope_members(&self, model: &str, scope_key: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.read(|reader| reader.scope_members(model, scope_key))
    }

    pub fn count_members(&self, model: &str, scope_key: &str) -> Result<i64, StoreError> {
        self.read(|reader| reader.count_members(model, scope_key))
    }

    pub fn drop_scope(&self, model: &str, scope_key: &str) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        Ok(inner.connection.execute(
            "DELETE FROM scope_member WHERE model = ?1 AND scope_key = ?2",
            params![model, scope_key],
        )?)
    }

    pub fn drop_row_memberships(&self, model: &str, id: &str) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        Ok(inner
            .connection
            .execute("DELETE FROM scope_member WHERE model = ?1 AND row_id = ?2", params![model, id])?)
    }
}
