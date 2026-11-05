// The session tables live in the project database: the credential stays with the app and never enters SQLite.
use async_trait::async_trait;

use super::error::TransportError;

pub(crate) mod client;
pub mod cable;
mod config;
pub(crate) mod bootstrap;
#[cfg(test)]
pub(crate) mod fake_server_tests;
mod graphql;
pub(crate) mod jwe;
mod key;
mod origin;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod vectors_tests;

pub use client::{Answer, EncryptedClient, EncryptedConfig, EncryptedEndpoints, EncryptedSessionOperation};
pub(crate) use client::permanently_refused;
pub use config::{AppPlatform, ConfigCode, Country, SecurityCode, ServerConfig};
pub use graphql::{missing_from_registry, repeats, response_of, EncryptedTransport};
pub(crate) use graphql::pending_matches;
#[cfg(test)]
use {jwe::{open_response, seal_request, RequestPlain, ResponsePlain}, key::ServerKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpReply {
    pub status: u16,
    pub body: String,
}

#[async_trait]
pub trait HttpExchange: Send + Sync {
    async fn post(&self, path: &str, body: String, purpose: crate::transport::NetworkPurpose) -> Result<HttpReply, TransportError>;
}

pub trait CredentialSlot: Send + Sync {
    fn get(&self) -> Result<Option<String>, TransportError>;
    fn set(&self, credential: &str) -> Result<(), TransportError>;
    fn clear(&self) -> Result<(), TransportError>;
}

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod credential_reenter_tests;
#[cfg(test)]
mod bootstrap_tests;
#[cfg(test)]
mod graphql_tests;
#[cfg(test)]
mod jwe_tests;
#[cfg(test)]
mod key_tests;
#[cfg(test)]
mod origin_tests;
#[cfg(test)]
mod persisted_query_retry_tests;
#[cfg(test)]
mod persisted_query_tests;
#[cfg(test)]
mod refused_sequence_tests;
#[cfg(test)]
mod sealed_errors_tests;
#[cfg(test)]
mod session_guards_tests;
#[cfg(test)]
mod retry_tests;

#[cfg(test)]
pub(crate) mod protocol_tests;
