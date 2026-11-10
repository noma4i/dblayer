//! The error of a refused operation as the text Outbox keeps: the JSON of `OperationFailure`, so the kind, the session code and the extensions reach the outcome and the row.

use crate::owners::contract::ErrorClass;
use crate::owners::session::SessionError;
use crate::value::sync::{FailureKind, OperationFailure};
use crate::transport::{GraphqlExtensions, SessionCode, TransportError};

fn text(failure: &OperationFailure) -> String { serde_json::to_string(failure).unwrap_or_else(|_| failure.message.clone()) }

/// A refusal of Session: its code when it has one, the class of the error otherwise.
pub fn of_session(error: &SessionError, class: ErrorClass) -> String {
    if let SessionError::Session { code, body } = error {
        return text(&OperationFailure::new(FailureKind::Session(*code), body.clone()));
    }
    if let SessionError::Revoked { body } = error {
        return text(&OperationFailure::new(FailureKind::Session(SessionCode::SessionEnded), body.clone()));
    }
    if let SessionError::ResponseTooLarge { limit } = error {
        return text(&OperationFailure::new(FailureKind::ResponseTooLarge { limit: *limit }, format!("the answer is above the limit of {limit} bytes")));
    }
    if let SessionError::TooLarge { size, limit } = error {
        return text(&OperationFailure::new(FailureKind::RequestTooLarge { size: *size, limit: *limit }, format!("the request is {size} bytes, the limit is {limit} bytes")));
    }
    let code = match error {
        SessionError::SequenceConflict => Some(SessionCode::SequenceConflict),
        SessionError::Unauthorized => Some(SessionCode::Unauthenticated),
        SessionError::StateLost => Some(SessionCode::StateLost),
        SessionError::Revoked { .. } => Some(SessionCode::SessionEnded),
        SessionError::NoCredential => Some(SessionCode::NoCredential),
        _ => None,
    };
    let kind = match (code, class) {
        (Some(code), _) => FailureKind::Session(code),
        (None, ErrorClass::Network) => FailureKind::Network,
        (None, ErrorClass::Server) => FailureKind::Server,
        (None, _) => FailureKind::Rejected,
    };
    text(&OperationFailure::new(kind, code.map_or_else(|| format!("{error:?}"), |code| code.name().to_string())))
}

/// The server refused with GraphQL errors.
pub fn of_graphql(message: String, extensions: Option<GraphqlExtensions>) -> String {
    text(&OperationFailure { kind: FailureKind::Rejected, message, extensions })
}

/// The error of a step of a transfer: the refusal of the server keeps its message and extensions, a failure of the connection keeps its class.
pub fn of_transport(error: &TransportError) -> String {
    match error {
        TransportError::Graphql { messages, extensions } => of_graphql(messages.join("; "), extensions.clone()),
        TransportError::GraphqlAnswer { response } => of_graphql(response.errors.iter().map(|error| error.message.as_str()).collect::<Vec<_>>().join("; "), response.extensions()),
        TransportError::Session { code, detail } => text(&OperationFailure::new(FailureKind::Session(*code), if detail.is_empty() { code.name().to_string() } else { detail.clone() })),
        TransportError::RequestTooLarge { size, limit } => text(&OperationFailure::new(FailureKind::RequestTooLarge { size: *size, limit: *limit }, error.to_string())),
        TransportError::ResponseTooLarge { limit } | TransportError::SentAnswerTooLarge { limit } => text(&OperationFailure::new(FailureKind::ResponseTooLarge { limit: *limit }, error.to_string())),
        TransportError::Network(_) | TransportError::Interrupted(_) => text(&OperationFailure::new(FailureKind::Network, error.to_string())),
        TransportError::Http { status, .. } if *status >= 500 => text(&OperationFailure::new(FailureKind::Server, error.to_string())),
        _ => text(&OperationFailure::new(FailureKind::Rejected, error.to_string())),
    }
}

/// The kept cause of a failed attempt as the text of its failure.
pub fn of_cause(kind: FailureKind) -> String {
    match kind {
        FailureKind::ResponseTooLarge { limit } => text(&OperationFailure::new(kind, format!("the answer is above the limit of {limit} bytes"))),
        _ => text(&OperationFailure::new(kind, "refused")),
    }
}

/// The step had no live connection to run on.
pub fn no_link() -> String { text(&OperationFailure::new(FailureKind::Network, "the transfer has no connection")) }

/// The kept text as a failure; a plain text is a refusal with that message.
pub fn parse(error: &str) -> OperationFailure {
    if error == "missing_file" { return OperationFailure::new(FailureKind::MissingFile, error); }
    if let Some(message) = error.strip_prefix("source_failed:") { return OperationFailure::new(FailureKind::SourceFailed, message); }
    serde_json::from_str(error).unwrap_or_else(|_| OperationFailure::new(FailureKind::Rejected, error))
}

/// The answer lacks what the action promised.
pub fn protocol(message: &str) -> String { text(&OperationFailure::new(FailureKind::Protocol, message)) }

/// The operation waited in the queue longer than its `expires_after`.
pub fn expired() -> String { text(&OperationFailure::new(FailureKind::Expired, "expired")) }
