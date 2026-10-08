use async_trait::async_trait;

mod cancel;
mod error;
mod events;
mod fetch;
mod mock;
mod online;
mod put;
mod request;
mod response;
mod retry;
mod subscription;

pub use cancel::CancelToken;
pub use error::TransportError;
pub use events::{event_names, max_debounce, routable, EventFault, EventSpec, PublishVerdict};
pub use fetch::{FetchAttempt, FetchError, FetchReport};
pub use mock::{CallKind, MockTransport, PutReply, RecordedCall, RecordedPut, Reply};
pub use online::Connectivity;
pub use put::{ByteSink, PutRequest, PutResponse};
pub use request::{GraphQLError, GraphQLRequest, Nullable, Response};
pub use response::{ResponseOutcome, ResponseShape};
pub use retry::{retriable_codes, RetryClass, RETRY_BACKOFF};
pub use subscription::{
    Subscription, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind, SubscriptionFault,
    SubscriptionHandle, SubscriptionOutcome, SubscriptionRetry,
};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError>;
    async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError>;
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
mod response_tests;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod subscription_tests;
