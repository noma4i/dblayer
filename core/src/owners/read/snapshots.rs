use crate::store::Store;
use std::sync::{Arc, Mutex, PoisonError};

use crate::owners::read::deliveries::{Kind, ReadData, ReadMode, StreamRow, View};

pub trait Snapshots: Send + Sync + 'static {
    /// The data of `view` in the snapshot of the read and the revision of that snapshot, never older than `revision`. `replaced` lists the temporary ids swapped for server ids after the revision `after`.
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(i64, ReadData), String>;
}

/// Reads of the snapshot through `Db`: the query of the view runs in the read pool, the rows of the answer are ids with their stamps.
pub struct DbSnapshots {
    db: std::sync::Arc<Store>,
    wires: Mutex<std::collections::HashMap<String, Arc<crate::ffi::wire::WireModel>>>,
}

impl DbSnapshots {
    pub fn new(db: std::sync::Arc<Store>) -> Self { Self { db, wires: Mutex::new(std::collections::HashMap::new()) } }

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
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(i64, ReadData), String> {
        let (query, count) = match view.kind {
            Kind::List => (self.db.parse_query(&view.subject).map_err(|error| error.to_string())?.0, false),
            Kind::Count => (self.db.parse_query(&view.subject).map_err(|error| error.to_string())?.0, true),
            Kind::Entity { .. } => (crate::store::query::Query::new(&view.model).filtered(crate::store::query::Where::equal("id", serde_json::json!(view.subject))), false),
            Kind::Events | Kind::Blob | Kind::RetryStatus => return Err("a stream of this kind reads no rows".to_string()),
        };
        let held = match mode {
            ReadMode::Snapshot => None,
            ReadMode::Changes { from_revision, from_position, limit } => Some(crate::store::HeldVersion { from: (from_revision, from_position), limit }),
        };
        let (seen, read) = self.db.read_view(&query, count, revision, view.remote.then_some(&view.window), held).map_err(|error| error.to_string())?;
        let replaced = |db: &Store| db.replaced().replaced_between(after, seen).into_iter().map(|(from, to)| (from.to_string(), to.to_string())).collect();
        Ok((seen, match read {
            crate::store::ViewRead::Count(count) => ReadData::Count(u64::try_from(count).unwrap_or(0)),
            crate::store::ViewRead::Overflow => ReadData::Overflow,
            crate::store::ViewRead::Rows(rows, total) => {
                let wire = self.wire(&view.model)?;
                ReadData::Rows { rows: rows.into_iter().map(|(id, stamp, row)| StreamRow { id, stamp, row: Some(wire.row(&row)) }).collect(), replaced: replaced(&self.db), total }
            }
            crate::store::ViewRead::Changes { keys, rows, total } => {
                let wire = self.wire(&view.model)?;
                ReadData::Changes { keys, rows: rows.into_iter().map(|(id, stamp, row)| StreamRow { id, stamp, row: Some(wire.row(&row)) }).collect(), replaced: replaced(&self.db), total }
            }
        }))
    }
}
