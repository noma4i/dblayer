//! Reads of the SQLite snapshot for `Deliveries`: a reader connection (WAL) sees a commit once its revision is stored.
//! The pool keeps connections for the next read; a read holds no lock of the library.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use rusqlite::{Connection, OpenFlags};

use crate::ddl::{manifest_read, REVISION_SEQ_KEY};
use crate::owners::read::deliveries::{Kind, ReadData, StreamRow, View};
use crate::query::{fetch_count, fetch_keys, CompiledStatement};

/// The statement of a stream: `(id, revision)` rows for `List` and `Entity`, one number for `Count`.
pub trait SnapshotStatements: Send + Sync + 'static {
    fn statement(&self, view: &View) -> Result<CompiledStatement, String>;
}

pub trait Snapshots: Send + Sync + 'static {
    /// The data of `view` in the snapshot of the read and the revision of that snapshot, never older than `revision`.
    fn read(&self, view: &View, revision: i64) -> Result<(i64, ReadData), String>;
}

pub struct SqliteSnapshots {
    path: PathBuf,
    statements: Arc<dyn SnapshotStatements>,
    pool: Mutex<Vec<Connection>>,
}

impl SqliteSnapshots {
    pub fn new(path: PathBuf, statements: Arc<dyn SnapshotStatements>) -> Self { Self { path, statements, pool: Mutex::new(Vec::new()) } }

    fn take(&self) -> Result<Connection, String> {
        if let Some(connection) = self.pool.lock().unwrap_or_else(PoisonError::into_inner).pop() {
            return Ok(connection);
        }
        Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX).map_err(|error| error.to_string())
    }
}

impl Snapshots for SqliteSnapshots {
    fn read(&self, view: &View, revision: i64) -> Result<(i64, ReadData), String> {
        let statement = self.statements.statement(view)?;
        let connection = self.take()?;
        let result = (|| {
            let snapshot = connection.unchecked_transaction().map_err(|error| error.to_string())?;
            let stored = manifest_read(&connection, REVISION_SEQ_KEY).map_err(|error| error.to_string())?;
            let seen = stored.map_or(Ok(0), |text| text.parse::<i64>().map_err(|error| error.to_string()))?;
            if seen < revision {
                return Err(format!("the snapshot is at revision {seen}, older than {revision}"));
            }
            let data = match view.kind {
                Kind::Events | Kind::Blob | Kind::RetryStatus => return Err("a stream of this kind reads no rows".to_string()),
                Kind::Count => ReadData::Count(u64::try_from(fetch_count(&connection, &statement).map_err(|error| error.to_string())?).unwrap_or(0)),
                Kind::List | Kind::Entity { .. } => ReadData::Rows { rows: fetch_keys(&connection, &statement).map_err(|error| error.to_string())?.into_iter().map(|(id, stamp)| StreamRow { id, stamp, row: None }).collect(), replaced: Vec::new() },
            };
            drop(snapshot);
            Ok((seen, data))
        })();
        self.pool.lock().unwrap_or_else(PoisonError::into_inner).push(connection);
        result
    }
}

/// Reads of the snapshot through `Db`: the query of the view runs in the read pool, the rows of the answer are ids with their stamps.
pub struct DbSnapshots {
    db: crate::db::Db,
    wires: Mutex<std::collections::HashMap<String, Arc<crate::ffi::wire::WireModel>>>,
}

impl DbSnapshots {
    pub fn new(db: crate::db::Db) -> Self { Self { db, wires: Mutex::new(std::collections::HashMap::new()) } }

    fn wire(&self, model: &str) -> Result<Arc<crate::ffi::wire::WireModel>, String> {
        if let Some(wire) = self.wires.lock().unwrap_or_else(PoisonError::into_inner).get(model) {
            return Ok(wire.clone());
        }
        let wire = Arc::new(crate::ffi::wire::WireModel::of(self.db.spec(model).map_err(|error| error.to_string())?));
        self.wires.lock().unwrap_or_else(PoisonError::into_inner).insert(model.to_string(), wire.clone());
        Ok(wire)
    }
}

impl Snapshots for DbSnapshots {
    fn read(&self, view: &View, revision: i64) -> Result<(i64, ReadData), String> {
        let (query, count) = match view.kind {
            Kind::List => (self.db.parse_query(&view.subject).map_err(|error| error.to_string())?.0, false),
            Kind::Count => (self.db.parse_query(&view.subject).map_err(|error| error.to_string())?.0, true),
            Kind::Entity { .. } => (crate::query::Query::new(&view.model).filtered(crate::query::Where::equal("id", serde_json::json!(view.subject))), false),
            Kind::Events | Kind::Blob | Kind::RetryStatus => return Err("a stream of this kind reads no rows".to_string()),
        };
        let (seen, read) = self.db.read_view(&query, count, revision).map_err(|error| error.to_string())?;
        Ok((seen, match read {
            crate::db::ViewRead::Count(count) => ReadData::Count(u64::try_from(count).unwrap_or(0)),
            crate::db::ViewRead::Rows(rows) => {
                let wire = self.wire(&view.model)?;
                ReadData::Rows { rows: rows.into_iter().map(|(id, stamp, row)| StreamRow { id, stamp, row: Some(wire.row(&row)) }).collect(), replaced: Vec::new() }
            }
        }))
    }
}
