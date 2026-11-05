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
    /// The request id of the author (or of the call) that the owner Session keeps for the pair, restored as stored.
    pub req_id: u64,
}

/// The terminal outcome of a `Mutate` that the server answered: it stays until its author took it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredAnswer {
    Ok(String),
    Http,
    Unauthorized,
    Revoked,
    Session(crate::transport::SessionCode),
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
    transaction.execute_batch("DELETE FROM encrypted_pending; DELETE FROM encrypted_answer;")?;
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
        .query_row("SELECT credential_digest, request_id, response_key, sequence, path, body, owner_kind, owner_operation_id, owner_item_id, owner_step, purpose, req_id FROM encrypted_pending WHERE id = 1", [], |row| {
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
                req_id: u64::try_from(row.get::<_, i64>(11)?).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(11, -1))?,
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
        "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, owner_kind, owner_operation_id, owner_item_id, owner_step, purpose, req_id) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![pending.credential_digest, pending.request_id, pending.response_key, pending.sequence, pending.path, pending.body, kind, operation_id, item_id, step, match pending.purpose { crate::transport::NetworkPurpose::Standard => 0, crate::transport::NetworkPurpose::Transfer => 1 }, i64::try_from(pending.req_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(11, i64::MAX))?],
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
    transaction.execute_batch("DELETE FROM encrypted_pending; DELETE FROM encrypted_answer;")?;
    transaction.execute("DELETE FROM encrypted_session", [])?;
    transaction.commit()
}


pub fn store_answer(connection: &Connection, request_id: u64, answer: &StoredAnswer) -> rusqlite::Result<()> {
    let (outcome, detail) = match answer {
        StoredAnswer::Ok(body) => ("ok", Some(body.as_str())),
        StoredAnswer::Http => ("http", None),
        StoredAnswer::Unauthorized => ("unauthorized", None),
        StoredAnswer::Revoked => ("revoked", None),
        StoredAnswer::Session(code) => ("session", Some(code.name())),
    };
    let request_id = i64::try_from(request_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX))?;
    connection.execute("INSERT OR REPLACE INTO encrypted_answer (request_id, outcome, detail) VALUES (?1, ?2, ?3)", params![request_id, outcome, detail])?;
    Ok(())
}

pub fn remove_answer(connection: &Connection, request_id: u64) -> rusqlite::Result<()> {
    let request_id = i64::try_from(request_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX))?;
    connection.execute("DELETE FROM encrypted_answer WHERE request_id = ?1", [request_id])?;
    Ok(())
}

pub fn answers(connection: &Connection) -> rusqlite::Result<std::collections::BTreeMap<u64, StoredAnswer>> {
    let mut statement = connection.prepare("SELECT request_id, outcome, detail FROM encrypted_answer")?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?)))?;
    let mut found = std::collections::BTreeMap::new();
    for row in rows {
        let (request_id, outcome, detail) = row?;
        let invalid = |what: &str| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("the stored answer {request_id} has {what}"))));
        let answer = match (outcome.as_str(), detail) {
            ("ok", Some(body)) => StoredAnswer::Ok(body),
            ("http", None) => StoredAnswer::Http,
            ("unauthorized", None) => StoredAnswer::Unauthorized,
            ("revoked", None) => StoredAnswer::Revoked,
            ("session", Some(name)) => StoredAnswer::Session(crate::transport::SessionCode::from_wire(&name).ok_or_else(|| invalid("an unknown session code"))?),
            _ => return Err(invalid("an outcome that does not match its detail")),
        };
        found.insert(u64::try_from(request_id).map_err(|_| invalid("a negative request id"))?, answer);
    }
    Ok(found)
}
