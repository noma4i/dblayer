//! The error of a refused operation as the text Outbox keeps: the JSON of `OperationFailure`, so the kind, the session code and the extensions reach the outcome and the row.

use crate::owners::contract::ErrorClass;
use crate::owners::session::SessionError;
use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphqlExtensions, SessionCode};

fn text(failure: &OperationFailure) -> String { serde_json::to_string(failure).unwrap_or_else(|_| failure.message.clone()) }

/// A refusal of Session: its code when it has one, the class of the error otherwise.
pub fn of_session(error: &SessionError, class: ErrorClass) -> String {
    if let SessionError::TooLarge { size, limit } = error {
        return text(&OperationFailure::new(FailureKind::RequestTooLarge { size: *size, limit: *limit }, format!("the request is {size} bytes, the limit is {limit} bytes")));
    }
    let code = match error {
        SessionError::Session(code) => Some(*code),
        SessionError::SequenceConflict => Some(SessionCode::SequenceConflict),
        SessionError::Unauthorized => Some(SessionCode::Unauthenticated),
        SessionError::StateLost => Some(SessionCode::StateLost),
        SessionError::Revoked => Some(SessionCode::SessionEnded),
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

/// The kept text as a failure; a plain text is a refusal with that message.
pub fn parse(error: &str) -> OperationFailure {
    if error == NETWORK { return OperationFailure::new(FailureKind::Network, NETWORK); }
    serde_json::from_str(error).unwrap_or_else(|_| OperationFailure::new(FailureKind::Rejected, error))
}

/// The answer lacks what the action promised.
pub fn protocol(message: &str) -> String { text(&OperationFailure::new(FailureKind::Protocol, message)) }

/// The request never reached the server and the action does not wait for the network.
pub fn network() -> String { NETWORK.into() }

const NETWORK: &str = "network";
