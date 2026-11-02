use serde_json::Value;

use crate::db::{Db, DbError};
use crate::ddl::table_name;
use crate::identity::Generation;

use super::{journal, record_stamped, CloudChange, MergeSource, Hlc};

impl Db {
    pub fn cloud_replica(&self, generation: Generation) -> Result<String, DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
        Ok(journal::replica(&inner.connection)?)
    }

    pub fn cloud_observe(&self, generation: Generation, seen: &Hlc) -> Result<(), DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
        Ok(journal::observe(&inner.connection, seen)?)
    }

    pub fn cloud_pending(&self) -> Result<u64, DbError> {
        Ok(journal::pending(&self.shared.store.lock().connection)?)
    }

    pub fn cloud_cursor(&self, generation: Generation) -> Result<Option<Vec<u8>>, DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
        Ok(journal::cursor(&inner.connection)?)
    }

    pub fn cloud_batch(&self, generation: Generation, limit: u32) -> Result<Vec<(i64, CloudChange)>, DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
        Ok(journal::batch(&inner.connection, limit)?)
    }

    pub fn cloud_ack(&self, generation: Generation, seqs: &[i64]) -> Result<(), DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
        let transaction = inner.connection.unchecked_transaction()?;
        journal::ack(&transaction, seqs)?;
        Ok(transaction.commit()?)
    }

    pub fn cloud_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.cloud_changes.subscribe()
    }

    pub fn cloud_unstamped(&self, generation: Generation) -> Result<Vec<(String, String)>, DbError> {
        let inner = self.shared.store.lock();
        inner.ensure_generation(generation)?;
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

    pub fn cloud_stamp_rows(&self, generation: Generation, rows: &[(String, String)], source: MergeSource) -> Result<(), DbError> {
        {
            let inner = self.shared.store.lock();
            inner.ensure_generation(generation)?;
            let now_ms = inner.now_ms();
            let transaction = inner.connection.unchecked_transaction()?;
            let replica = journal::replica(&transaction)?;
            let reader = crate::store::Reader::new(&transaction, &inner.models);
            for (model, id) in rows {
                let Some(row) = reader.point(model, id)? else { continue };
                let spec = self.shared.spec(model)?;
                for name in &spec.cloud {
                    let stamp = match source {
                        MergeSource::UseLocal => journal::next_stamp(&transaction, now_ms)?,
                        MergeSource::UseCloud => Hlc::new(0, 0, &replica),
                    };
                    record_stamped(&transaction, model, id, name, &stamp, &row.get(name).cloned().unwrap_or(Value::Null))?;
                }
            }
            transaction.commit()?;
        }
        self.shared.cloud_changes.send_modify(|version| *version += 1);
        Ok(())
    }
}
