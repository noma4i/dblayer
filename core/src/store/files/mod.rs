use rusqlite::{params, Connection, OptionalExtension, Row};

mod descriptor;
mod directories;

pub use descriptor::FileDescriptor;
pub(crate) use descriptor::sha256_hex;
pub(crate) use directories::{CopiedSource, FileDirectories};

pub(crate) const TABLE_LIMIT: usize = 256 * 1024;

/// The copy of a source file the library owns: a file above `TABLE_LIMIT` waits in `<id>.partial` until its batch commits, a smaller one is held in `data` for the table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopiedFile {
    pub id: String,
    pub size: u64,
    pub sha256: String,
    pub data: Vec<u8>,
    pub location: Location,
}

/// Copies `source` in pieces of one chunk through the admission of the process. A source that cannot be read is a storage error.
pub(crate) fn copy_attachment(files: &FileDirectories, source: &std::path::Path) -> std::io::Result<CopiedFile> {
    let id = uuid::Uuid::new_v4().to_string();
    match files.copy_source_partial(&id, source)? {
        CopiedSource::Ready { size, sha256, data, location } => Ok(CopiedFile { id, size, sha256, data, location }),
        CopiedSource::Unreadable(error) => Err(std::io::Error::other(error)),
    }
}

pub(crate) fn placement(length: usize, has_directory: bool) -> Location {
    if length <= TABLE_LIMIT || !has_directory {
        Location::Table
    } else {
        Location::Directory
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    Table,
    Directory,
}

impl Location {
    fn as_sql(self) -> &'static str {
        match self {
            Location::Table => "inline",
            Location::Directory => "disk",
        }
    }

    fn from_sql(text: &str) -> Self {
        if text == "disk" {
            Location::Directory
        } else {
            Location::Table
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    pub id: String,
    pub model: String,
    pub row_id: String,
    pub field: String,
    pub descriptor: FileDescriptor,
    pub location: Location,
    pub data: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileReference {
    pub id: String,
    pub model: String,
    pub row_id: String,
    pub field: String,
    pub location: Location,
}

const COLUMNS: &str = "id, model, row_id, field, name, mime, size, sha256, location, data, created_at";

fn record_from(row: &Row<'_>) -> rusqlite::Result<FileRecord> {
    let id: String = row.get(0)?;
    let location: String = row.get(8)?;
    Ok(FileRecord {
        descriptor: FileDescriptor {
            id: id.clone(),
            size: row.get::<_, i64>(6)? as u64,
            name: Some(row.get(4)?),
            mime: Some(row.get(5)?),
            sha256: Some(row.get(7)?),
        },
        id,
        model: row.get(1)?,
        row_id: row.get(2)?,
        field: row.get(3)?,
        location: Location::from_sql(&location),
        data: row.get::<_, Option<Vec<u8>>>(9)?.unwrap_or_default(),
        created_at: row.get(10)?,
    })
}

fn reference_from(row: &Row<'_>) -> rusqlite::Result<FileReference> {
    let location: String = row.get(4)?;
    Ok(FileReference { id: row.get(0)?, model: row.get(1)?, row_id: row.get(2)?, field: row.get(3)?, location: Location::from_sql(&location) })
}

pub(crate) fn put(connection: &Connection, record: &FileRecord) -> rusqlite::Result<()> {
    remove_pair(connection, &record.model, &record.row_id, &record.field)?;
    let data = (record.location == Location::Table).then_some(record.data.as_slice());
    connection.execute(
        &format!(include_str!("../sql/files/put.sql"), COLUMNS = COLUMNS),
        params![
            record.id,
            record.model,
            record.row_id,
            record.field,
            record.descriptor.name.clone().unwrap_or_default(),
            record.descriptor.mime.clone().unwrap_or_default(),
            record.descriptor.size as i64,
            record.descriptor.sha256.clone().unwrap_or_default(),
            record.location.as_sql(),
            data,
            record.created_at,
        ],
    )?;
    Ok(())
}

pub(crate) fn find(connection: &Connection, model: &str, row_id: &str, field: &str) -> rusqlite::Result<Option<FileRecord>> {
    connection
        .query_row(
            &format!(include_str!("../sql/files/find.sql"), COLUMNS = COLUMNS),
            params![model, row_id, field],
            record_from,
        )
        .optional()
}

pub(crate) fn remove_pair(connection: &Connection, model: &str, row_id: &str, field: &str) -> rusqlite::Result<usize> {
    connection.execute(include_str!("../sql/files/remove_pair.sql"), params![model, row_id, field])
}

pub(crate) fn remove_row(connection: &Connection, model: &str, row_id: &str) -> rusqlite::Result<usize> {
    connection.execute(include_str!("../sql/files/remove_row.sql"), params![model, row_id])
}

pub(crate) fn move_row(connection: &Connection, model: &str, from: &str, to: &str) -> rusqlite::Result<usize> {
    connection.execute(include_str!("../sql/files/move_row.sql"), params![model, from, to])
}

pub(crate) fn references_of_row(connection: &Connection, model: &str, row_id: &str) -> rusqlite::Result<Vec<FileReference>> {
    connection
        .prepare(include_str!("../sql/files/references_of_row.sql"))?
        .query_map(params![model, row_id], reference_from)?
        .collect()
}


pub(crate) fn clear_model(connection: &Connection, model: &str) -> rusqlite::Result<usize> {
    connection.execute(include_str!("../sql/files/clear_model.sql"), [model])
}

pub(crate) fn clear_all(connection: &Connection) -> rusqlite::Result<usize> {
    connection.execute(include_str!("../sql/files/clear_all.sql"), [])
}

#[cfg(test)]
mod tests;
