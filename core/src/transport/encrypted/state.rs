use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub credential_digest: String,
    pub sequence: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    pub credential_digest: String,
    pub request_id: String,
    pub response_key: String,
    pub sequence: i64,
    pub path: String,
    pub body: String,
    pub owner: Option<crate::transport::RequestOwner>,
}

pub fn session(connection: &Connection) -> rusqlite::Result<Option<Session>> {
    connection
        .query_row("SELECT credential_digest, sequence FROM encrypted_session WHERE id = 1", [], |row| {
            Ok(Session { credential_digest: row.get(0)?, sequence: row.get(1)? })
        })
        .optional()
}

pub fn start(connection: &Connection, credential_digest: &str) -> rusqlite::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute("DELETE FROM encrypted_pending", [])?;
    transaction.execute(
        "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, ?1, 0)
         ON CONFLICT(id) DO UPDATE SET credential_digest = excluded.credential_digest, sequence = 0",
        [credential_digest],
    )?;
    transaction.commit()
}

pub fn pending(connection: &Connection) -> rusqlite::Result<Option<Pending>> {
    connection
        .query_row("SELECT credential_digest, request_id, response_key, sequence, path, body, owner_seq, item_position, stage FROM encrypted_pending WHERE id = 1", [], |row| {
            Ok(Pending {
                credential_digest: row.get(0)?,
                request_id: row.get(1)?,
                response_key: row.get(2)?,
                sequence: row.get(3)?,
                path: row.get(4)?,
                body: row.get(5)?,
                owner: match (row.get::<_, Option<i64>>(6)?, row.get::<_, Option<i64>>(7)?, row.get::<_, Option<String>>(8)?) {
                    (Some(seq), Some(item_position), Some(stage)) => Some(crate::transport::RequestOwner { seq, item_position, stage }),
                    _ => None,
                },
            })
        })
        .optional()
}

pub fn store_pending(connection: &Connection, pending: &Pending) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, owner_seq, item_position, stage) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![pending.credential_digest, pending.request_id, pending.response_key, pending.sequence, pending.path, pending.body, pending.owner.as_ref().map(|owner| owner.seq), pending.owner.as_ref().map(|owner| owner.item_position), pending.owner.as_ref().map(|owner| &owner.stage)],
    )?;
    Ok(())
}

pub fn commit(connection: &Connection, sequence: i64) -> rusqlite::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute("UPDATE encrypted_session SET sequence = ?1 WHERE id = 1", [sequence])?;
    transaction.execute("DELETE FROM encrypted_pending", [])?;
    transaction.commit()
}

pub fn clear(connection: &Connection) -> rusqlite::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute("DELETE FROM encrypted_pending", [])?;
    transaction.execute("DELETE FROM encrypted_session", [])?;
    transaction.commit()
}

pub fn drop_pending(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute("DELETE FROM encrypted_pending", [])?;
    Ok(())
}
