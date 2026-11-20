use async_trait::async_trait;

pub(crate) mod answer_kind;
mod cancel;
pub(crate) mod encrypted;
mod error;
mod extensions;
#[cfg(test)]
mod mock;
mod online;
mod put;
mod raw;
mod request;
mod response;
pub(crate) mod retry;
mod session_code;

pub use answer_kind::{classify as classify_answer, rate_limit_pause, refusal, refusal_name, AnswerKind, Refusal, WireOutcome};
pub use cancel::CancelToken;
pub use encrypted::cable::{CableConnectError, CableCredential, CableError, CableKeys, CableLine, CableLines, CableLink, CableSocket, EncryptedLines, Incoming, SocketEvent};
pub use encrypted::{repeats, response_from_answer, Answer, AppPlatform, ConfigCode, Country, CredentialSlot, EncryptedConfig, EncryptedEndpoints, EncryptedOperationClass, EncryptedSessionOperation, EncryptedTransport, Exchange, HttpExchange, HttpReply, SecurityCode, ServerConfig};
pub use error::TransportError;
pub use extensions::{graphql_extensions, GraphqlExtensions, TransactionDisposition};
#[cfg(test)]
pub use mock::{RecordedKind, MockTransport, PutScript, RecordedCall, RecordedPut, Reply};
pub use online::Connectivity;
pub use raw::{classify, classify_upgrade, encrypted_call, graphql_call, graphql_endpoint, graphql_text, merge_headers, user_agent, ExchangeKind, RawFailureKind, HttpRequest, Author, WireIds, RawFailure, RawOutcome, CONNECT_TIMEOUT_MS, REQUEST_TIMEOUT_MS, MAX_HTTP_RESPONSE_BYTES, MAX_PUT_RESPONSE_BYTES};
pub use put::{ByteSink, PutFile, PutRequest, PutReply, TransferIdentity, TransferStep};
pub use request::{GraphQLError, GraphQLRequest, NetworkPurpose, RequestOwner, RequestReason, RequestRecord, Response};
pub use response::{ResponseOutcome, ResponseShape};
pub use retry::{RetryClass, RetryCalls, RetryEntry, RetryOwner, RetrySnapshot};
pub use session_code::SessionCode;

#[async_trait]
pub trait Upstream: Send + Sync {
    fn set_online(&self, _online: bool) {}
    /// D-492: the next request and the next cable open use a new connection; requests in flight finish as they are.
    fn rotate_connections(&self) {}
    fn session(&self) -> Result<Option<String>, TransportError> { Ok(Some(String::new())) }
    async fn open_cable(&self, _url: &str) -> Result<std::sync::Arc<dyn CableSocket>, TransportError> {
        Err(TransportError::Network("encrypted cable socket is not connected".into()))
    }
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn put(&self, request: PutRequest) -> Result<PutReply, TransportError>;
}

#[cfg(test)]
mod tests;
