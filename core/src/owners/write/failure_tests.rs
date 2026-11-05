use crate::owners::contract::ErrorClass;
use crate::owners::session::SessionError;
use crate::syncstate::FailureKind;
use crate::transport::{GraphqlExtensions, SessionCode, TransactionDisposition};

use super::failure::{network, of_graphql, of_session, parse};

#[test]
fn a_refusal_keeps_its_kind_code_and_extensions_in_the_text_outbox_stores() {
    for code in [SessionCode::Unauthenticated, SessionCode::AccountUnavailable, SessionCode::RateLimited] {
        let failure = parse(&of_session(&SessionError::Session(code), ErrorClass::Permanent));
        assert_eq!((failure.kind, failure.message), (FailureKind::Session(code), code.name().to_string()));
    }
    let extensions = GraphqlExtensions { code: Some("REFUSED".into()), transaction_disposition: Some(TransactionDisposition::Keep), fields: Default::default() };
    let failure = parse(&of_graphql("purchase refused".into(), Some(extensions.clone())));
    assert_eq!((failure.kind, failure.message, failure.extensions), (FailureKind::Rejected, "purchase refused".to_string(), Some(extensions)));
    assert_eq!(parse(&network()).kind, FailureKind::Network);
    assert_eq!(parse("plain text").kind, FailureKind::Rejected);
}
