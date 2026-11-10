// The session tables live in the project database: the credential stays with the app and never enters SQLite.
use async_trait::async_trait;

use super::error::TransportError;

pub mod cable;
pub(crate) mod credential;
pub(crate) mod exchange;
mod config;
pub(crate) mod bootstrap;
#[cfg(test)]
pub(crate) mod tests;
mod graphql;
pub(crate) mod jwe;
pub(crate) mod key;
mod origin;

pub(crate) use exchange::SendOnce;
pub(crate) use exchange::checked_path;
pub use exchange::{Answer, EncryptedConfig, EncryptedEndpoints, EncryptedOperationClass, EncryptedSessionOperation, Exchange};
pub use config::{AppPlatform, ConfigCode, Country, SecurityCode, ServerConfig};
pub use graphql::{graphql_body, hash_only, repeats, response_from_answer, EncryptedTransport};
pub(crate) use graphql::response_from_answer_with_kind;
#[cfg(test)]
use {jwe::{open_response, seal_request, RequestPlain, ResponsePlain}, key::ServerKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpReply {
    pub status: u16,
    pub body: String,
}

#[async_trait]
pub trait HttpExchange: Send + Sync {
    async fn post(&self, path: &str, body: String, purpose: crate::transport::NetworkPurpose, wire: crate::transport::WireIds) -> Result<HttpReply, TransportError>;
}

pub trait CredentialSlot: Send + Sync {
    fn get(&self) -> Result<Option<String>, TransportError>;
    fn set(&self, credential: &str) -> Result<(), TransportError>;
    fn clear(&self) -> Result<(), TransportError>;
}


