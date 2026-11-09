use async_trait::async_trait;
use crate::transport::TransportError;

pub enum SocketEvent {
    Text(String),
    Closed { code: Option<u16>, reason: Option<String>, failure: Option<crate::transport::RawFailure> },
}

#[async_trait]
pub trait CableSocket: Send + Sync {
    async fn send(&self, text: String) -> Result<(), TransportError>;
    async fn next(&self) -> Option<SocketEvent>;
    async fn close(&self);
    async fn cancel(&self);
}
