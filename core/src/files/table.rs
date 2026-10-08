use rusqlite::{params, Connection, OptionalExtension, Row};

use super::descriptor::FileDescriptor;

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

pub(crate) fn put(connection: &Connection, record: &FileRecord) -> rusqlite::Result<()> {
    remove_pair(connection, &record.model, &record.row_id, &record.field)?;
    let data = (record.location == Location::Table).then_some(record.data.as_slice());
    connection.execute(
        &format!("INSERT OR REPLACE INTO file ({COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"),
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
            &format!("SELECT {COLUMNS} FROM file WHERE model = ?1 AND row_id = ?2 AND field = ?3"),
            params![model, row_id, field],
            record_from,
        )
        .optional()
}

pub(crate) fn remove_pair(connection: &Connection, model: &str, row_id: &str, field: &str) -> rusqlite::Result<usize> {
    connection.execute("DELETE FROM file WHERE model = ?1 AND row_id = ?2 AND field = ?3", params![model, row_id, field])
}

pub(crate) fn remove_row(connection: &Connection, model: &str, row_id: &str) -> rusqlite::Result<usize> {
    connection.execute("DELETE FROM file WHERE model = ?1 AND row_id = ?2", params![model, row_id])
}

pub(crate) fn move_row(connection: &Connection, model: &str, from: &str, to: &str) -> rusqlite::Result<usize> {
    connection.execute("UPDATE OR REPLACE file SET row_id = ?3 WHERE model = ?1 AND row_id = ?2", params![model, from, to])
}

pub(crate) fn ids_of_rows(connection: &Connection, rows: &[(String, String)]) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("SELECT id FROM file WHERE model = ?1 AND row_id = ?2")?;
    let mut ids = Vec::new();
    for (model, row_id) in rows {
        for id in statement.query_map(params![model, row_id], |row| row.get::<_, String>(0))? {
            ids.push(id?);
        }
    }
    Ok(ids)
}

pub(crate) fn all_references(connection: &Connection) -> rusqlite::Result<Vec<FileReference>> {
    connection
        .prepare("SELECT id, model, row_id, field, location FROM file ORDER BY created_at, id")?
        .query_map([], |row| {
            let location: String = row.get(4)?;
            Ok(FileReference {
                id: row.get(0)?,
                model: row.get(1)?,
                row_id: row.get(2)?,
                field: row.get(3)?,
                location: Location::from_sql(&location),
            })
        })?
        .collect()
}

pub(crate) fn clear_model(connection: &Connection, model: &str) -> rusqlite::Result<usize> {
    connection.execute("DELETE FROM file WHERE model = ?1", [model])
}

pub(crate) fn clear_all(connection: &Connection) -> rusqlite::Result<usize> {
    connection.execute("DELETE FROM file", [])
}
