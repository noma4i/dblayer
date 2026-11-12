use crate::store::Store;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::owners::read::deliveries::{Kind, ReadData, ReadMode, ReadVersion, StreamRow, View};

pub trait Snapshots: Send + Sync + 'static {
    /// The data of `view` in the snapshot of the read and the revision of that snapshot, never older than `revision`. `replaced` lists the temporary ids swapped for server ids after the revision `after`.
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(ReadVersion, ReadData), String>;
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
    fn read(&self, view: &View, revision: i64, after: i64, mode: ReadMode) -> Result<(ReadVersion, ReadData), String> {
        let from = match mode { ReadMode::Snapshot { from_revision, from_position } | ReadMode::Changes { from_revision, from_position, .. } => (from_revision, from_position) };
        if view.kind == Kind::Operations {
            let (version, operations, transfers) = self.db.read_queue(revision, from, view).map_err(|error| error.to_string())?;
            let mut items: Vec<crate::ffi::OperationInfo> = crate::owners::write::operation_queue::queue_of(&operations)?.into_iter().map(Into::into).collect();
            let at: std::collections::HashMap<i64, usize> = items.iter().enumerate().map(|(index, item)| (item.seq, index)).collect();
            for transfer in transfers {
                let Some(info) = transfer.info.as_ref() else {
                    let index = transfer.op_id.and_then(|op_id| at.get(&op_id)).copied();
                    let failed = crate::ffi::OperationInfo::of_failed_transfer(transfer);
                    if let Some(index) = index { items[index].status = failed.status; items[index].error = failed.error; }
                    else { items.push(failed); }
                    continue;
                };
                let shown = crate::ffi::wire::upload(info);
                match transfer.op_id.and_then(|op_id| at.get(&op_id)) {
                    Some(index) => items[*index].upload = Some(shown),
                    None => items.push(crate::ffi::OperationInfo::of_transfer(transfer, shown)),
                }
            }
            items.sort_by_key(|item| item.seq);
            return Ok((version, ReadData::Operations(items)));
        }
        if view.kind == Kind::Blob {
            let (model, id, action) = crate::store::split_transfer_key(&view.subject).ok_or_else(|| format!("{} is no transfer key", view.subject))?;
            let id = crate::identity::RowId::new(id).map_err(|error| error.to_string())?;
            let (version, upload) = self.db.read_transfer(model, &id, action, revision, from, view).map_err(|error| error.to_string())?;
            let (wait_reason, local_url, processing) = upload.map_or((None, None, None), |upload| { let wire = crate::ffi::wire::upload(&upload); (upload.wait_reason, wire.local_url, wire.processing) });
            return Ok((version, ReadData::Upload { wait_reason, local_url, processing }));
        }
        let (query, count) = match view.kind {
            Kind::List | Kind::Count => (view.query.as_deref().cloned().ok_or_else(|| "a list or count view has no query".to_string())?, view.kind == Kind::Count),
            Kind::Entity { .. } => (crate::store::query::Query::new(&view.model).filtered(crate::store::query::Where::equal("id", serde_json::json!(view.subject))), false),
            Kind::Events | Kind::CableCloses | Kind::Changes | Kind::Blob | Kind::Operations => return Err("a stream of this kind reads no rows".to_string()),
        };
        let held = match mode {
            ReadMode::Snapshot { .. } => None,
            ReadMode::Changes { from_revision, from_position, limit } => Some(crate::store::HeldVersion { from: (from_revision, from_position), limit }),
        };
        let (revision, position, relevant, read, swapped) = self.db.read_view(&query, count, revision, after, view.remote.then_some(&view.window), held, view, from).map_err(|error| error.to_string())?;
        let replaced = || swapped.into_iter().map(|(from, to)| (from.to_string(), to.to_string())).collect();
        Ok((ReadVersion { revision, position, relevant }, match read {
            crate::store::ViewRead::Count(count) => ReadData::Count(u64::try_from(count).unwrap_or(0)),
            crate::store::ViewRead::Overflow => ReadData::Overflow,
            crate::store::ViewRead::Rows(rows, total, owed) => {
                let wire = self.wire(&view.model)?;
                ReadData::Rows { rows: rows.into_iter().map(|(id, stamp, row)| StreamRow { owes: owed.contains(&id), id, stamp, row: Some(wire.row(&row)) }).collect(), replaced: replaced(), total }
            }
            crate::store::ViewRead::Changes { keys, rows, total, owed } => {
                let wire = self.wire(&view.model)?;
                ReadData::Changes { keys, rows: rows.into_iter().map(|(id, stamp, row)| StreamRow { owes: owed.contains(&id), id, stamp, row: Some(wire.row(&row)) }).collect(), replaced: replaced(), total }
            }
        }))
    }
}
