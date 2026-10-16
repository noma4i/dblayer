// The session tables live in the project database: the credential stays with the app and never enters SQLite.
use async_trait::async_trait;

use super::error::TransportError;

mod client;
mod config;
#[cfg(test)]
pub(crate) mod fake_server_tests;
mod graphql;
mod jwe;
mod key;
mod state;
#[cfg(test)]
pub(crate) mod vectors;

pub use client::{Answer, EncryptedClient, EncryptedConfig, LOGOUT_PATH};
pub use config::{ConfigError, ServerConfig, ServerKeyInfo};
pub use graphql::EncryptedTransport;
#[cfg(test)]
use {jwe::{open_response, seal_request, RequestPlain, ResponsePlain}, key::ServerKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpReply {
    pub status: u16,
    pub body: String,
}

#[async_trait]
pub trait HttpExchange: Send + Sync {
    async fn post(&self, path: &str, body: String) -> Result<HttpReply, TransportError>;
    async fn get(&self, path: &str) -> Result<HttpReply, TransportError>;
}

pub trait CredentialSlot: Send + Sync {
    fn get(&self) -> Result<Option<String>, TransportError>;
    fn set(&self, credential: &str) -> Result<(), TransportError>;
    fn clear(&self) -> Result<(), TransportError>;
}

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod graphql_tests;
#[cfg(test)]
mod jwe_tests;
#[cfg(test)]
mod key_tests;
