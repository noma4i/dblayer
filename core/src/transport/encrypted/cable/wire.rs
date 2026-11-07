use async_trait::async_trait;
use crate::transport::TransportError;

pub enum SocketEvent {
    Text(String),
    Closed { code: u16, reason: String },
}

#[async_trait]
pub trait CableSocket: Send + Sync {
    async fn send(&self, text: String) -> Result<(), TransportError>;
    async fn next(&self) -> Option<SocketEvent>;
    async fn close(&self);
}
