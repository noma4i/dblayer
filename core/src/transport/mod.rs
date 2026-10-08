use async_trait::async_trait;

mod error;
mod events;
mod fetch;
mod mock;
mod request;
mod response;
mod retry;
mod subscription;

pub use error::TransportError;
pub use events::{event_names, max_debounce, routable, EventFault, EventSpec, PublishVerdict};
pub use fetch::{FetchAttempt, FetchError, FetchReport};
pub use mock::{CallKind, MockTransport, RecordedCall, Reply};
pub use request::{GraphQLError, GraphQLRequest, Nullable, Response};
pub use response::{ResponseOutcome, ResponseShape};
pub use retry::{retriable_codes, retry_delay, RetryClass};
pub use subscription::{
    Subscription, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind, SubscriptionFault,
    SubscriptionHandle, SubscriptionOutcome, SubscriptionRetry,
};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError>;
    async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError>;
}

#[cfg(test)]
mod events_tests;
#[cfg(test)]
mod fetch_tests;
#[cfg(test)]
mod mock_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod response_tests;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod subscription_tests;
