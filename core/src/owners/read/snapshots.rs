use crate::store::Store;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::owners::read::deliveries::{Kind, ReadData, ReadMode, StreamRow, View};

pub trait Snapshots: Send + Sync + 'static {
    /// The data of `view` in the snapshot of the read and the revision of that snapshot, never older than `revision`. `replaced` lists the temporary ids swapped for server ids after the revision `after`.
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(i64, ReadData), String>;
}

/// Reads of the snapshot through `Db`: the query of the view runs in the read pool, the rows of the answer are ids with their stamps.
pub struct DbSnapshots {
    db: std::sync::Arc<Store>,
    wires: BTreeMap<String, Arc<crate::ffi::wire::WireModel>>,
}

impl DbSnapshots {
    pub fn new(db: std::sync::Arc<Store>) -> Self { Self { wires: db.specs().iter().map(|(key, spec)| (key.clone(), Arc::new(crate::ffi::wire::WireModel::of(spec)))).collect(), db } }

    fn wire(&self, model: &str) -> Result<Arc<crate::ffi::wire::WireModel>, String> {
        match self.wires.get(model) {
            Some(wire) => Ok(wire.clone()),
            None => Err(self.db.spec(model).err().map_or_else(|| format!("model {model} has no wire form"), |error| error.to_string())),
        }
    }
}

impl Snapshots for DbSnapshots {
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(i64, ReadData), String> {
        if view.kind == Kind::Operations {
            let (seen, operations) = self.db.read_operations(revision).map_err(|error| error.to_string())?;
            return Ok((seen, ReadData::Operations(crate::ffi::queue_of(&operations))));
        }
        let (query, count) = match view.kind {
            Kind::List | Kind::Count => (view.query.as_deref().cloned().ok_or_else(|| "a list or count view has no query".to_string())?, view.kind == Kind::Count),
            Kind::Entity { .. } => (crate::store::query::Query::new(&view.model).filtered(crate::store::query::Where::equal("id", serde_json::json!(view.subject))), false),
            Kind::Events | Kind::Blob | Kind::RetryStatus | Kind::Operations => return Err("a stream of this kind reads no rows".to_string()),
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
