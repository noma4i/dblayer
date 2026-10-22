use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::db::{Db, DbError};
use crate::identity::{ModelKey, RowId};
use crate::store::Reader;

use super::{journal, outranks, CloudApplied, CloudBook, CloudChange, Hlc};

struct RowWork {
    tombstone: Option<Hlc>,
    tombstone_dirty: bool,
    fields: BTreeMap<String, (Hlc, Value)>,
    dirty: BTreeSet<String>,
}

impl Db {
    pub(crate) fn resolve_cloud_conflicts(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        for operation in &mut envelope.entity_ops {
            let EntityOp::Upsert { model, id, row } = operation else { continue };
            let spec = self.shared.spec(model.as_str())?;
            if spec.cloud.is_empty() {
                continue;
            }
            let Some(stored) = reader.point(model.as_str(), id.as_str())? else { continue };
            for name in &spec.cloud {
                let candidate = row.get(name).cloned().unwrap_or(Value::Null);
                if candidate == stored.get(name).cloned().unwrap_or(Value::Null) {
                    continue;
                }
                let Some((_, held)) = journal::read_field(reader.connection(), model.as_str(), id.as_str(), name)? else { continue };
                let rule = spec.field(name).and_then(|field| field.merge.as_ref());
                if rule.is_some_and(|rule| rule.rank(&candidate, &held) == Ordering::Less) {
                    match held {
                        Value::Null => row.remove(name),
                        held => row.insert(name.clone(), held),
                    };
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_cloud_page(&self, own: &str, changes: &[CloudChange], cursor: &[u8]) -> Result<(), DbError> {
        let generation = self.generation();
        let now_ms = self.shared.store.lock().now_ms();
        self.commit_following(generation, |reader| {
            let connection = reader.connection();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Cloud, generation);
            envelope.writes_local = true;
            let mut book = CloudBook { cursor: Some(cursor.to_vec()), ..CloudBook::default() };
            let mut works: BTreeMap<(String, String), RowWork> = BTreeMap::new();
            for change in changes.iter().filter(|change| change.replica != own) {
                let Ok(spec) = self.shared.spec(&change.model) else { continue };
                if spec.cloud.is_empty() || (!change.field.is_empty() && !spec.cloud.contains(&change.field)) {
                    continue;
                }
                let stamp = Hlc::new(change.clock_ms, change.clock_count, &change.replica);
                if book.observed.as_ref().is_none_or(|seen| *seen < stamp) {
                    book.observed = Some(stamp.clone());
                }
                let key = (change.model.clone(), change.row_id.clone());
                if !works.contains_key(&key) {
                    let tombstone = journal::tombstone(connection, &change.model, &change.row_id)?;
                    works.insert(key.clone(), RowWork { tombstone, tombstone_dirty: false, fields: BTreeMap::new(), dirty: BTreeSet::new() });
                }
                let Some(work) = works.get_mut(&key) else { continue };
                if change.field.is_empty() {
                    if work.tombstone.as_ref().is_none_or(|held| *held < stamp) {
                        work.tombstone = Some(stamp);
                        work.tombstone_dirty = true;
                    }
                    continue;
                }
                if work.tombstone.is_some() {
                    continue;
                }
                let value = change.value.clone();
                if !work.fields.contains_key(&change.field) {
                    if let Some(held) = journal::read_field(connection, &change.model, &change.row_id, &change.field)? {
                        work.fields.insert(change.field.clone(), held);
                    }
                }
                let rule = spec.field(&change.field).and_then(|field| field.merge.as_ref());
                let wins = work.fields.get(&change.field).is_none_or(|(held_stamp, held)| outranks(rule, (&value, &stamp), (held, held_stamp)));
                if wins {
                    work.fields.insert(change.field.clone(), (stamp, value));
                    work.dirty.insert(change.field.clone());
                }
            }
            for ((model, row_id), work) in works {
                let id = RowId::new(&row_id)?;
                let key = ModelKey::new(&model);
                let spec = self.shared.spec(&model)?;
                let stored = reader.point(&model, &row_id)?;
                if work.tombstone_dirty {
                    let Some(stamp) = work.tombstone.clone() else { continue };
                    book.applied.push(CloudApplied::Tombstone { model, row_id, stamp });
                    if stored.is_some() {
                        envelope.entity_ops.push(EntityOp::Destroy { model: key, id, reason: DestroyReason::Server });
                    }
                    continue;
                }
                if work.dirty.is_empty() {
                    continue;
                }
                for name in &work.dirty {
                    let (stamp, value) = work.fields[name].clone();
                    book.applied.push(CloudApplied::Field { model: model.clone(), row_id: row_id.clone(), field: name.clone(), stamp, value });
                }
                let row = match stored {
                    Some(mut row) => {
                        for name in &work.dirty {
                            match &work.fields[name].1 {
                                Value::Null => row.remove(name),
                                value => row.insert(name.clone(), value.clone()),
                            };
                        }
                        row
                    }
                    None if spec.is_local() => {
                        let mut input: Map<String, Value> = journal::row_fields(connection, &model, &row_id)?.into_iter().map(|(name, _, value)| (name, value)).collect();
                        input.extend(work.fields.iter().map(|(name, (_, value))| (name.clone(), value.clone())));
                        input.insert("id".into(), Value::String(row_id.clone()));
                        match spec.normalize_local(&input, now_ms) {
                            Ok(row) => row,
                            Err(_) => continue,
                        }
                    }
                    None => continue,
                };
                let mut row = row;
                row.insert("id".into(), Value::String(row_id));
                envelope.entity_ops.push(EntityOp::Upsert { model: key, id, row });
            }
            envelope.cloud = Some(book);
            Ok::<_, DbError>(Some(envelope))
        })
        .map_err(crate::db::plan_fault)?;
        Ok(())
    }
}
