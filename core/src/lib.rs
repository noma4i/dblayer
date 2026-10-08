uniffi::setup_scaffolding!();

pub mod identity;
pub mod pacing;
pub mod transport;
pub mod value;
pub mod schema;
pub mod ddl;

use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum DbError {
    #[error("sqlite: {detail}")]
    Sqlite { detail: String },
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        DbError::Sqlite { detail: error.to_string() }
    }
}

#[uniffi::export]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[derive(uniffi::Object)]
pub struct Store {
    connection: Mutex<Connection>,
}

#[uniffi::export]
impl Store {
    #[uniffi::constructor]
    pub fn open_in_memory() -> Result<Arc<Self>, DbError> {
        let connection = Connection::open_in_memory()?;
        connection.execute(
            "create table rows (model text not null, id text not null, payload text not null, primary key (model, id))",
            [],
        )?;
        Ok(Arc::new(Self { connection: Mutex::new(connection) }))
    }

    pub fn put(&self, model: String, id: String, payload: String) -> Result<(), DbError> {
        let connection = self.connection.lock().expect("store lock");
        connection.execute(
            "insert into rows (model, id, payload) values (?1, ?2, ?3) on conflict (model, id) do update set payload = excluded.payload",
            params![model, id, payload],
        )?;
        Ok(())
    }

    pub fn get(&self, model: String, id: String) -> Result<Option<String>, DbError> {
        let connection = self.connection.lock().expect("store lock");
        Ok(connection
            .query_row("select payload from rows where model = ?1 and id = ?2", params![model, id], |row| row.get(0))
            .optional()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_payload_reads_back() {
        let store = Store::open_in_memory().unwrap();
        store.put("moments".into(), "m1".into(), "{\"a\":1}".into()).unwrap();
        assert_eq!(store.get("moments".into(), "m1".into()).unwrap(), Some("{\"a\":1}".to_string()));
        assert_eq!(store.get("moments".into(), "m2".into()).unwrap(), None);
    }

    #[test]
    fn version_is_package_version() {
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
    }
}
