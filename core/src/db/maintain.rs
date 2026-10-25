use std::collections::HashSet;

use crate::commit::{CommitEnvelope, FileOp, WriteOrigin};
use crate::files::{all_references, is_partial, FileDescriptor, FileReference, Location};
use crate::identity::{ModelKey, RowId};
use crate::store::{MaintenanceReport, TempRowPolicy};

use super::error::DbError;
use super::local_write::{plan_fault, read_outbox};
use super::open::{locked, Db};
use super::prior::Prior;

impl Db {
    pub fn maintain(&self, temp_rows: &[TempRowPolicy]) -> Result<MaintenanceReport, DbError> {
        let report = self.shared.store.maintain(temp_rows)?;
        self.sweep_files()?;
        Ok(report)
    }

    pub fn sweep_files(&self) -> Result<usize, DbError> {
        let _gate = locked(&self.shared.files_gate);
        let orphans = self.shared.read(&[], |reader| {
            let mut held: HashSet<String> = read_outbox(reader)?
                .iter()
                .filter_map(Prior::of)
                .flat_map(|prior| prior.held_files())
                .map(|file| file.id)
                .collect();
            let mut statement = reader.connection().prepare("SELECT file_id FROM op_item WHERE file_id IS NOT NULL")?;
            held.extend(statement.query_map([], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?);
            let mut orphans = Vec::new();
            for reference in all_references(reader.connection())? {
                let described = held.contains(&reference.id)
                    || self.shared.specs.contains_key(&reference.model)
                    && reader
                        .point(&reference.model, &reference.row_id)?
                        .and_then(|row| row.get(&reference.field).and_then(FileDescriptor::from_value))
                        .is_some_and(|descriptor| descriptor.id == reference.id);
                if !described {
                    orphans.push(reference);
                }
            }
            Ok(orphans)
        })?;
        let mut removed = orphans.len();
        if !orphans.is_empty() {
            self.remove_references(&orphans)?;
        }
        let live = self.shared.read(&[], |reader| Ok(all_references(reader.connection())?))?;
        let live_ids: HashSet<&str> = live.iter().map(|reference| reference.id.as_str()).collect();
        let directory_ids: HashSet<&str> =
            live.iter().filter(|reference| reference.location == Location::Directory).map(|reference| reference.id.as_str()).collect();
        for name in self.shared.files.directory_entries().map_err(super::files::io_fault)? {
            if is_partial(&name) || !directory_ids.contains(name.as_str()) {
                self.shared.files.remove_directory_entry(&name).map_err(super::files::io_fault)?;
                removed += 1;
            }
        }
        for name in self.shared.files.cache_entries().map_err(super::files::io_fault)? {
            if !live_ids.contains(name.as_str()) {
                self.shared.files.remove_cache_entry(&name).map_err(super::files::io_fault)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn remove_references(&self, references: &[FileReference]) -> Result<(), DbError> {
        let generation = self.generation();
        let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
        for reference in references {
            envelope.file_ops.push(FileOp::Remove {
                model: ModelKey::new(&reference.model),
                row_id: RowId::new(&reference.row_id)?,
                field: reference.field.clone(),
            });
        }
        self.commit_following(generation, |_| Ok::<_, DbError>(Some(envelope.clone()))).map_err(plan_fault)?;
        let ids: Vec<String> = references.iter().map(|reference| reference.id.clone()).collect();
        self.shared.files.discard(&ids).map_err(super::files::io_fault)?;
        Ok(())
    }
}
