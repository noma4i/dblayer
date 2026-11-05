use async_trait::async_trait;

mod cancel;
pub(crate) mod encrypted;
mod error;
mod extensions;
mod events;
mod fetch;
#[cfg(any(test, feature = "stress"))]
mod mock;
mod online;
mod put;
mod request;
mod response;
pub(crate) mod retry;
mod session_code;
mod subscription;

pub use cancel::CancelToken;
pub use encrypted::cable::{CableConnect, CableCredential, CableError, CableKeys, CableLine, CableLines, CableLink, CableSocket, CableSubscriptions, EncryptedLines, Incoming, SocketEvent};
pub use encrypted::{missing_from_registry, repeats, response_of, Answer, AppPlatform, ConfigCode, Country, CredentialSlot, EncryptedClient, EncryptedConfig, EncryptedEndpoints, EncryptedSessionOperation, EncryptedTransport, HttpExchange, HttpReply, SecurityCode, ServerConfig};
pub use error::TransportError;
pub use extensions::{graphql_extensions, GraphqlExtensions, TransactionDisposition};
pub use events::{EventFault, EventSpec};
pub use fetch::{FetchAttempt, FetchError, FetchReport};
#[cfg(any(test, feature = "stress"))]
pub use mock::{CallKind, MockTransport, PutReply, RecordedCall, RecordedPut, Reply};
pub use online::Connectivity;
pub use put::{ByteSink, PutFile, PutRequest, PutResponse, TransferIdentity, TransferStep};
pub use request::{GraphQLError, GraphQLRequest, NetworkPurpose, RequestOwner, Response};
pub use response::{ResponseOutcome, ResponseShape};
pub use retry::{RetryAttempt, RetryClass, RetryCalls, RetrySnapshot};
pub use session_code::SessionCode;
pub use subscription::{
    ServerSubscription, SubscriptionClose, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind,
    ServerSubscriptionHandle, SubscriptionSender, SubscriptionReceiver, subscription_channel,
};

#[async_trait]
pub trait Transport: Send + Sync {
    fn retries_queries(&self) -> bool { false }
    fn set_online(&self, _online: bool) {}
    fn resume_subscriptions(&self) {}
    fn session(&self) -> Result<Option<String>, TransportError> { Ok(Some(String::new())) }
    async fn open_cable(&self, _url: &str) -> Result<std::sync::Arc<dyn CableSocket>, TransportError> {
        Err(TransportError::Network("encrypted cable socket is not connected".into()))
    }
    async fn query(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn mutate(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn subscribe(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<ServerSubscription, TransportError>;
    async fn put(&self, admitted: &crate::ffi::legacy::Admitted, request: PutRequest) -> Result<PutResponse, TransportError>;
}

#[cfg(test)]
pub(crate) fn test_admission() -> Result<crate::ffi::legacy::Admitted, crate::ffi::legacy::Ended> {
    std::sync::Arc::new(crate::ffi::legacy::Leases::default()).admit(crate::ffi::legacy::Origin::Explicit, "test transport")
}

#[cfg(test)]
mod cancel_tests;
#[cfg(test)]
mod events_tests;
#[cfg(test)]
mod fetch_tests;
#[cfg(test)]
mod mock_tests;
#[cfg(test)]
mod put_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod respond_tests;
#[cfg(test)]
mod response_tests;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod subscription_tests;
