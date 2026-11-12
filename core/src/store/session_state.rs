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
    pub document: Option<String>,
    pub owner: Option<crate::transport::RequestOwner>,
    pub purpose: crate::transport::NetworkPurpose,
    /// The request id of the author (or of the call) that the owner Session keeps for the pair, restored as stored.
    pub req_id: u64,
}

/// The terminal outcome of a `Mutate` that the server answered: it stays until its author took it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredAnswer {
    Ok(String),
    Http { status: u16 },
    TooLarge { size: u64, limit: u64 },
    Unauthorized,
    Security { code: crate::transport::SecurityCode, detail: String },
    Revoked { body: String },
    Session { code: crate::transport::SessionCode, body: String },
}

pub fn session(connection: &Connection) -> rusqlite::Result<Option<Session>> {
    connection
        .query_row(include_str!("sql/session_state/session.sql"), [], |row| {
            Ok(Session { credential_digest: row.get(0)?, sequence: row.get(1)? })
        })
        .optional()
}

pub fn dropped_pending(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(include_str!("sql/session_state/dropped_pending.sql"), [], |row| row.get(0))
}

pub fn unowned_pending(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(include_str!("sql/session_state/unowned_pending.sql"), [], |row| row.get(0))
}

#[cfg(test)]
pub fn start(connection: &Connection, credential_digest: &str) -> rusqlite::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    if session(&transaction)?.is_some_and(|session| session.credential_digest == credential_digest) {
        return transaction.commit();
    }
    transaction.execute_batch(include_str!("sql/session_state/start_delete_encrypted_pending.sql"))?;
    transaction.execute(
        include_str!("sql/session_state/start_insert_encrypted_session.sql"),
        [credential_digest],
    )?;
    transaction.commit()
}

pub fn pending(connection: &Connection) -> rusqlite::Result<Option<Pending>> {
    connection
        .query_row(include_str!("sql/session_state/pending.sql"), [], |row| {
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
                    (Some(kind), Some(operation_id), Some(item_id), Some(step)) if kind == "transfer" => Some(crate::transport::RequestOwner::Transfer { operation_id, item_id, step: crate::store::operation::enum_from(&step)? }),
                    (Some(kind), None, Some(upload_id), None) if kind == "orphan" => Some(crate::transport::RequestOwner::Orphan { upload_id }),
                    (Some(kind), None, None, None) if kind == "detached" => Some(crate::transport::RequestOwner::Detached),
                    (None, None, None, None) => None,
                    _ => return Err(rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Null, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "incomplete pending request owner")))),
                },
                purpose: match row.get::<_, i64>(10)? { 0 => crate::transport::NetworkPurpose::Standard, 1 => crate::transport::NetworkPurpose::Transfer, value => return Err(rusqlite::Error::IntegralValueOutOfRange(10, value)) },
                req_id: u64::try_from(row.get::<_, i64>(11)?).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(11, -1))?,
                document: row.get(12)?,
            })
        })
        .optional()
}

pub fn store_pending(connection: &Connection, pending: &Pending) -> rusqlite::Result<()> {
    let (kind, operation_id, item_id, step) = match &pending.owner {
        Some(crate::transport::RequestOwner::Operation { seq }) => (Some("operation"), Some(*seq), None, None),
        Some(crate::transport::RequestOwner::Unmatched { operation_id }) => (Some("unmatched"), Some(*operation_id), None, None),
        Some(crate::transport::RequestOwner::Transfer { operation_id, item_id, step }) => (Some("transfer"), Some(*operation_id), Some(item_id.as_str()), crate::store::operation::enum_text(step)),
        Some(crate::transport::RequestOwner::Orphan { upload_id }) => (Some("orphan"), None, Some(upload_id.as_str()), None),
        Some(crate::transport::RequestOwner::Detached) => (Some("detached"), None, None, None),
        None => (None, None, None, None),
    };
    connection.execute(
        include_str!("sql/session_state/store_pending.sql"),
        params![pending.credential_digest, pending.request_id, pending.response_key, pending.sequence, pending.path, pending.body, kind, operation_id, item_id, step, match pending.purpose { crate::transport::NetworkPurpose::Standard => 0, crate::transport::NetworkPurpose::Transfer => 1 }, i64::try_from(pending.req_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(11, i64::MAX))?, pending.document],
    )?;
    Ok(())
}

#[cfg(test)]
pub fn commit(connection: &Connection, sequence: i64) -> rusqlite::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute(include_str!("sql/session_state/commit_update_encrypted_session.sql"), [sequence])?;
    transaction.execute(include_str!("sql/session_state/commit_delete_encrypted_pending.sql"), [])?;
    transaction.commit()
}

fn encode_stored(answer: &StoredAnswer) -> (&'static str, Option<String>) {
    match answer {
        StoredAnswer::Ok(body) => ("ok", Some(body.clone())),
        StoredAnswer::Http { status } => ("http", Some(status.to_string())),
        StoredAnswer::TooLarge { size, limit } => ("too_large", Some(format!("{size}:{limit}"))),
        StoredAnswer::Unauthorized => ("unauthorized", None),
        StoredAnswer::Security { code, detail } => ("security", Some(format!("{}:{detail}", match code { crate::transport::SecurityCode::ServerKeyMismatch => "server_key_mismatch", crate::transport::SecurityCode::ResponseAuthentication => "response_authentication", crate::transport::SecurityCode::TlsTrust => "tls_trust" }))),
        StoredAnswer::Revoked { body } => ("revoked", Some(body.clone())),
        StoredAnswer::Session { code, body } => ("session", Some(format!("{}:{body}", code.name()))),
    }
}

pub fn store_answer(connection: &Connection, request_id: u64, operation: Option<i64>, answer: &StoredAnswer) -> rusqlite::Result<()> {
    let (outcome, detail) = encode_stored(answer);
    let request_id = i64::try_from(request_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX))?;
    connection.execute(include_str!("sql/session_state/store_answer.sql"), params![request_id, outcome, detail, operation])?;
    Ok(())
}

pub fn release_pending_owner(connection: &Connection, req_id: u64) -> rusqlite::Result<()> {
    let req_id = i64::try_from(req_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX))?;
    connection.execute(include_str!("sql/session_state/release_pending_owner.sql"), [req_id])?;
    Ok(())
}

pub fn remove_answer(connection: &Connection, request_id: u64) -> rusqlite::Result<()> {
    let request_id = i64::try_from(request_id).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX))?;
    connection.execute(include_str!("sql/session_state/remove_answer.sql"), [request_id])?;
    Ok(())
}

pub fn answers(connection: &Connection) -> rusqlite::Result<std::collections::BTreeMap<u64, StoredAnswer>> {
    Ok(answer_rows(connection)?.into_iter().map(|(request_id, _, answer)| (request_id, answer)).collect())
}

/// The answers that belong to an operation of the queue, by the operation: `(request_id, answer)`.
pub fn answers_of_operations(connection: &Connection) -> rusqlite::Result<std::collections::BTreeMap<i64, (u64, StoredAnswer)>> {
    Ok(answer_rows(connection)?.into_iter().filter_map(|(request_id, operation, answer)| Some((operation?, (request_id, answer)))).collect())
}

fn answer_rows(connection: &Connection) -> rusqlite::Result<Vec<(u64, Option<i64>, StoredAnswer)>> {
    let mut statement = connection.prepare(include_str!("sql/session_state/answer_rows.sql"))?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, Option<i64>>(3)?)))?;
    let mut found = Vec::new();
    for row in rows {
        let (request_id, outcome, detail, operation) = row?;
        let answer = decode_stored(&outcome, detail)?;
        let invalid = || rusqlite::Error::IntegralValueOutOfRange(0, request_id);
        found.push((u64::try_from(request_id).map_err(|_| invalid())?, operation, answer));
    }
    Ok(found)
}

fn invalid_answer(what: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, what.to_owned())))
}

fn decode_stored(outcome: &str, detail: Option<String>) -> rusqlite::Result<StoredAnswer> {
        let invalid = |what: &str| invalid_answer(what);
        let answer = match (outcome, detail) {
            ("ok", Some(body)) => StoredAnswer::Ok(body),
            ("http", Some(status)) => StoredAnswer::Http { status: status.parse().map_err(|_| invalid("a status that is not a number"))? },
            ("too_large", Some(detail)) => {
                let (size, limit) = detail.split_once(':').ok_or_else(|| invalid("an invalid size limit"))?;
                StoredAnswer::TooLarge { size: size.parse().map_err(|_| invalid("an invalid size"))?, limit: limit.parse().map_err(|_| invalid("an invalid limit"))? }
            }
            ("unauthorized", None) => StoredAnswer::Unauthorized,
            ("security", Some(detail)) => {
                let (code, detail) = detail.split_once(':').ok_or_else(|| invalid("an invalid security outcome"))?;
                let code = match code { "server_key_mismatch" => crate::transport::SecurityCode::ServerKeyMismatch, "response_authentication" => crate::transport::SecurityCode::ResponseAuthentication, "tls_trust" => crate::transport::SecurityCode::TlsTrust, _ => return Err(invalid("an unknown security code")) };
                StoredAnswer::Security { code, detail: detail.into() }
            }
            ("revoked", Some(body)) => StoredAnswer::Revoked { body },
            ("session", Some(detail)) => {
                let (name, body) = detail.split_once(':').unwrap_or((&detail, &detail));
                StoredAnswer::Session { code: crate::transport::SessionCode::from_wire(name).ok_or_else(|| invalid("an unknown session code"))?, body: body.to_string() }
            },
            _ => return Err(invalid("an outcome that does not match its detail")),
        };
        Ok(answer)
}

const RESUME_OUTCOME: &str = "resume_answer_outcome";
const RESUME_DETAIL: &str = "resume_answer_detail";

fn encode_resume(answer: &crate::owners::session::SessionAnswer) -> rusqlite::Result<(&'static str, String)> {
    use crate::owners::contract::RejectReason;
    use crate::owners::session::SessionError;
    let pair = match answer {
        Ok(body) => encode_stored(&StoredAnswer::Ok(body.clone())),
        Err(SessionError::Http { status }) => encode_stored(&StoredAnswer::Http { status: *status }),
        Err(SessionError::TooLarge { size, limit }) => encode_stored(&StoredAnswer::TooLarge { size: *size, limit: *limit }),
        Err(SessionError::Unauthorized) => encode_stored(&StoredAnswer::Unauthorized),
        Err(SessionError::Security { code, detail }) => encode_stored(&StoredAnswer::Security { code: *code, detail: detail.clone() }),
        Err(SessionError::Revoked { body }) => encode_stored(&StoredAnswer::Revoked { body: body.clone() }),
        Err(SessionError::Session { code, body }) => encode_stored(&StoredAnswer::Session { code: *code, body: body.clone() }),
        Err(SessionError::Graphql { code, body }) => ("graphql", Some(serde_json::to_string(&(code, body)).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?)),
        Err(SessionError::Rejected(RejectReason::StaleGeneration { models, generations })) => ("rejected_stale", Some(serde_json::to_string(&(models, generations)).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?)),
        Err(SessionError::Rejected(RejectReason::Storage(detail))) => ("rejected_storage", Some(detail.clone())),
        Err(SessionError::Rejected(RejectReason::NoSession)) => ("rejected_no_session", None),
        Err(SessionError::Rejected(RejectReason::Closed)) => ("rejected_closed", None),
        Err(SessionError::Rejected(RejectReason::Invalid(detail))) => ("rejected_invalid", Some(detail.clone())),
        Err(SessionError::Storage(detail)) => ("storage", Some(detail.clone())),
        Err(SessionError::ResponseTooLarge { limit }) => ("response_too_large", Some(limit.to_string())),
        Err(SessionError::Invalid) => ("invalid", None),
        Err(SessionError::SessionExists) => ("session_exists", None),
        Err(SessionError::NoCredential) => ("no_credential", None),
        Err(SessionError::StateLost) => ("state_lost", None),
        Err(SessionError::SequenceConflict) => ("sequence_conflict", None),
        Err(SessionError::Network) => ("network", None),
        Err(SessionError::Interrupted) => ("interrupted", None),
        Err(SessionError::NoSession) => ("no_session", None),
        Err(SessionError::Closed) => ("closed", None),
    };
    Ok((pair.0, pair.1.unwrap_or_default()))
}

fn decode_resume(outcome: &str, detail: String) -> rusqlite::Result<crate::owners::session::SessionAnswer> {
    use crate::owners::contract::RejectReason;
    use crate::owners::session::{session_answer, SessionError};
    let error = match outcome {
        "ok" | "http" | "too_large" | "security" | "revoked" | "session" => return Ok(session_answer(decode_stored(outcome, Some(detail))?)),
        "unauthorized" => return Ok(session_answer(decode_stored(outcome, None)?)),
        "graphql" => { let (code, body) = serde_json::from_str(&detail).map_err(|_| invalid_answer("invalid graphql answer"))?; SessionError::Graphql { code, body } },
        "rejected_stale" => { let (models, generations) = serde_json::from_str(&detail).map_err(|_| invalid_answer("invalid rejected generations"))?; SessionError::Rejected(RejectReason::StaleGeneration { models, generations }) },
        "rejected_storage" => SessionError::Rejected(RejectReason::Storage(detail)),
        "rejected_no_session" => SessionError::Rejected(RejectReason::NoSession),
        "rejected_closed" => SessionError::Rejected(RejectReason::Closed),
        "rejected_invalid" => SessionError::Rejected(RejectReason::Invalid(detail)),
        "storage" => SessionError::Storage(detail),
        "response_too_large" => SessionError::ResponseTooLarge { limit: detail.parse().map_err(|_| invalid_answer("invalid response limit"))? },
        "invalid" => SessionError::Invalid,
        "session_exists" => SessionError::SessionExists,
        "no_credential" => SessionError::NoCredential,
        "state_lost" => SessionError::StateLost,
        "sequence_conflict" => SessionError::SequenceConflict,
        "network" => SessionError::Network,
        "interrupted" => SessionError::Interrupted,
        "no_session" => SessionError::NoSession,
        "closed" => SessionError::Closed,
        _ => return Err(invalid_answer("unknown resume outcome")),
    };
    Ok(Err(error))
}

pub fn store_resume_answer(connection: &Connection, answer: &crate::owners::session::SessionAnswer) -> rusqlite::Result<()> {
    let (outcome, detail) = encode_resume(answer)?;
    connection.execute(include_str!("sql/ddl/manifest_write.sql"), params![RESUME_OUTCOME, outcome])?;
    connection.execute(include_str!("sql/ddl/manifest_write.sql"), params![RESUME_DETAIL, detail])?;
    Ok(())
}

pub fn remove_resume_answer(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(include_str!("sql/boot/boot_delete_manifest.sql"), [RESUME_OUTCOME])?;
    connection.execute(include_str!("sql/boot/boot_delete_manifest.sql"), [RESUME_DETAIL])?;
    Ok(())
}

pub fn resume_answer(connection: &Connection) -> rusqlite::Result<Option<crate::owners::session::SessionAnswer>> {
    let outcome: Option<String> = connection.query_row(include_str!("sql/ddl/manifest_read.sql"), [RESUME_OUTCOME], |row| row.get(0)).optional()?;
    let detail: Option<String> = connection.query_row(include_str!("sql/ddl/manifest_read.sql"), [RESUME_DETAIL], |row| row.get(0)).optional()?;
    match (outcome, detail) {
        (None, None) => Ok(None),
        (Some(outcome), Some(detail)) => decode_resume(&outcome, detail).map(Some),
        _ => Err(invalid_answer("incomplete resume answer")),
    }
}
