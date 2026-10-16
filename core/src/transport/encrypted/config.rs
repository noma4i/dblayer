use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Deserialize;

use super::key::{ServerKey, MIN_KEY_BITS};
use crate::transport::TransportError;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct ServerKeyInfo {
    pub kid: String,
    pub public_key_der: String,
    pub alg: String,
    pub enc: String,
    pub bits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct ServerConfig {
    pub protocol_version: u32,
    pub min_app_version: String,
    pub graphql_url: String,
    pub cable_url: String,
    pub server_key: ServerKeyInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Transport(TransportError),
    KeyMismatch(String),
}

impl ServerConfig {
    pub fn parse(text: &str, key: &ServerKey) -> Result<Self, ConfigError> {
        let config: ServerConfig = serde_json::from_str(text).map_err(|error| ConfigError::Transport(TransportError::Decode(error.to_string())))?;
        let announced = &config.server_key;
        let same_der = STANDARD.decode(&announced.public_key_der).is_ok_and(|der| der == key.der());
        let reason = match () {
            () if announced.kid != key.pin() => "the announced key id differs from the pin",
            () if !same_der => "the announced key differs from the key of the app",
            () if announced.alg != "RSA-OAEP" || announced.enc != "A256GCM" => "the announced algorithms are not RSA-OAEP and A256GCM",
            () if (announced.bits as usize) < MIN_KEY_BITS => "the announced key is too short",
            () => return Ok(config),
        };
        Err(ConfigError::KeyMismatch(reason.to_string()))
    }
}
