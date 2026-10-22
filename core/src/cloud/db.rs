use serde_json::Value;

use crate::db::{Db, DbError};
use crate::ddl::table_name;

use super::{journal, record_stamped, CloudChange, MergeSource, Hlc};

impl Db {
    pub fn cloud_replica(&self) -> Result<String, DbError> {
        Ok(journal::replica(&self.shared.store.lock().connection)?)
    }

    pub fn cloud_observe(&self, seen: &Hlc) -> Result<(), DbError> {
        Ok(journal::observe(&self.shared.store.lock().connection, seen)?)
    }

    pub fn cloud_pending(&self) -> Result<u64, DbError> {
        Ok(journal::pending(&self.shared.store.lock().connection)?)
    }

    pub fn cloud_cursor(&self) -> Result<Option<Vec<u8>>, DbError> {
        Ok(journal::cursor(&self.shared.store.lock().connection)?)
    }

    pub fn cloud_batch(&self, limit: u32) -> Result<Vec<(i64, CloudChange)>, DbError> {
        Ok(journal::batch(&self.shared.store.lock().connection, limit)?)
    }

    pub fn cloud_ack(&self, seqs: &[i64]) -> Result<(), DbError> {
        let inner = self.shared.store.lock();
        let transaction = inner.connection.unchecked_transaction()?;
        journal::ack(&transaction, seqs)?;
        Ok(transaction.commit()?)
    }

    pub fn cloud_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.cloud_changes.subscribe()
    }

    pub fn cloud_unstamped(&self) -> Result<Vec<(String, String)>, DbError> {
        let inner = self.shared.store.lock();
        let mut found = Vec::new();
        for spec in self.shared.specs.values().filter(|spec| spec.is_local() && !spec.cloud.is_empty()) {
            let sql = format!(
                "SELECT id FROM {} AS t WHERE NOT EXISTS (SELECT 1 FROM cloud_field AS f WHERE f.model = ?1 AND f.row_id = t.id) AND NOT EXISTS (SELECT 1 FROM cloud_row AS r WHERE r.model = ?1 AND r.row_id = t.id)",
                table_name(spec.key.as_str())
            );
            let mut statement = inner.connection.prepare(&sql)?;
            let ids: Vec<String> = statement.query_map([spec.key.as_str()], |row| row.get(0))?.collect::<Result<_, _>>()?;
            found.extend(ids.into_iter().map(|id| (spec.key.as_str().to_string(), id)));
        }
        Ok(found)
    }

    pub fn cloud_stamp_unstamped(&self, source: MergeSource) -> Result<(), DbError> {
        let unstamped = self.cloud_unstamped()?;
        let mut rows = Vec::new();
        for (model, id) in unstamped {
            if let Some(row) = self.shared.read(&[model.as_str()], |reader| Ok(reader.point(&model, &id)?))? {
                rows.push((model, id, row));
            }
        }
        let inner = self.shared.store.lock();
        let now_ms = inner.now_ms();
        let transaction = inner.connection.unchecked_transaction()?;
        let replica = journal::replica(&transaction)?;
        for (model, id, row) in rows {
            let spec = self.shared.spec(&model)?;
            for name in &spec.cloud {
                let stamp = match source {
                    MergeSource::UseLocal => journal::next_stamp(&transaction, now_ms)?,
                    MergeSource::UseCloud => Hlc::new(0, 0, &replica),
                };
                record_stamped(&transaction, &model, &id, name, &stamp, &row.get(name).cloned().unwrap_or(Value::Null))?;
            }
        }
        transaction.commit()?;
        self.shared.cloud_changes.send_modify(|version| *version += 1);
        Ok(())
    }
}
