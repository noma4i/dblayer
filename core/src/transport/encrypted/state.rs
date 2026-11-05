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
    pub purpose: crate::transport::NetworkPurpose,
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
    if session(&transaction)?.is_some_and(|session| session.credential_digest == credential_digest) {
        return transaction.commit();
    }
    transaction.execute("DELETE FROM encrypted_pending", [])?;
    transaction.execute(
        "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, ?1, 0)
         ON CONFLICT(id) DO UPDATE SET credential_digest = excluded.credential_digest, sequence = 0",
        [credential_digest],
    )?;
    transaction.commit()
}

pub(crate) fn restore_session(connection: &Connection, session: &Session) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, ?1, ?2)",
        params![session.credential_digest, session.sequence],
    )?;
    Ok(())
}

pub fn pending(connection: &Connection) -> rusqlite::Result<Option<Pending>> {
    connection
        .query_row("SELECT credential_digest, request_id, response_key, sequence, path, body, owner_kind, owner_operation_id, owner_item_id, owner_step, purpose FROM encrypted_pending WHERE id = 1", [], |row| {
            Ok(Pending {
                credential_digest: row.get(0)?,
                request_id: row.get(1)?,
                response_key: row.get(2)?,
                sequence: row.get(3)?,
                path: row.get(4)?,
                body: {
                    let body: String = row.get(5)?;
                    serde_json::from_str::<serde_json::Value>(&body).map_err(|error| rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(error)))?;
                    body
                },
                owner: match (row.get::<_, Option<String>>(6)?, row.get::<_, Option<i64>>(7)?, row.get::<_, Option<String>>(8)?, row.get::<_, Option<String>>(9)?) {
                    (Some(kind), Some(seq), None, None) if kind == "operation" => Some(crate::transport::RequestOwner::Operation { seq }),
                    (Some(kind), Some(operation_id), None, None) if kind == "unmatched" => Some(crate::transport::RequestOwner::Unmatched { operation_id }),
                    (Some(kind), Some(operation_id), Some(item_id), Some(step)) if kind == "transfer" => Some(crate::transport::RequestOwner::Transfer { operation_id, item_id, step: crate::operation::enum_from(&step)? }),
                    (None, None, None, None) => None,
                    _ => return Err(rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Null, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "incomplete pending request owner")))),
                },
                purpose: match row.get::<_, i64>(10)? { 0 => crate::transport::NetworkPurpose::Standard, 1 => crate::transport::NetworkPurpose::Transfer, value => return Err(rusqlite::Error::IntegralValueOutOfRange(10, value)) },
            })
        })
        .optional()
}

pub fn store_pending(connection: &Connection, pending: &Pending) -> rusqlite::Result<()> {
    let (kind, operation_id, item_id, step) = match &pending.owner {
        Some(crate::transport::RequestOwner::Operation { seq }) => (Some("operation"), Some(*seq), None, None),
        Some(crate::transport::RequestOwner::Unmatched { operation_id }) => (Some("unmatched"), Some(*operation_id), None, None),
        Some(crate::transport::RequestOwner::Transfer { operation_id, item_id, step }) => (Some("transfer"), Some(*operation_id), Some(item_id.as_str()), crate::operation::enum_text(step)),
        None => (None, None, None, None),
    };
    connection.execute(
        "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, owner_kind, owner_operation_id, owner_item_id, owner_step, purpose) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![pending.credential_digest, pending.request_id, pending.response_key, pending.sequence, pending.path, pending.body, kind, operation_id, item_id, step, match pending.purpose { crate::transport::NetworkPurpose::Standard => 0, crate::transport::NetworkPurpose::Transfer => 1 }],
    )?;
    Ok(())
}

#[cfg(test)]
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

