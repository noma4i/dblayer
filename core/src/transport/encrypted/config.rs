use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::collections::HashMap;
use serde::{Deserialize, Deserializer};

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

fn null_default<'de, D: Deserializer<'de>, T: Deserialize<'de> + Default>(deserializer: D) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct ServerConfig {
    pub protocol_version: u32,
    pub min_app_version: String,
    pub graphql_url: String,
    pub cable_url: String,
    pub server_key: ServerKeyInfo,
    pub country: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub country_disabled: bool,
    #[serde(default, deserialize_with = "null_default")]
    pub countries: Vec<Country>,
    #[serde(default, deserialize_with = "null_default")]
    pub update_required: bool,
    #[serde(default, deserialize_with = "null_default")]
    pub tracking_prompt_enabled: bool,
    pub ios_store_url: Option<String>,
    pub android_store_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
pub struct Country {
    #[serde(default, deserialize_with = "null_default")]
    pub name: String,
    #[serde(default, deserialize_with = "null_default")]
    pub code: String,
    pub flag: Option<String>,
    pub phone_code: Option<String>,
    pub phone_mask: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub phone_formatter: HashMap<String, String>,
    #[serde(default, deserialize_with = "null_default")]
    pub enabled: bool,
    #[serde(default, deserialize_with = "null_default")]
    pub phone_auth_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Transport(TransportError),
    KeyMismatch(String),
}

impl ServerConfig {
    pub fn parse(text: &str, key: &ServerKey) -> Result<Self, ConfigError> {
        let decode = |message: String| ConfigError::Transport(TransportError::Decode(message));
        let value: serde_json::Value = serde_json::from_str(text).map_err(|error| decode(error.to_string()))?;
        let countries = value.get("countries").and_then(|countries| countries.as_array()).into_iter().flatten();
        for country in countries {
            let formatter_ok = match country.get("phone_formatter") {
                None | Some(serde_json::Value::Null) => true,
                Some(serde_json::Value::Object(map)) => map.values().all(serde_json::Value::is_string),
                Some(_) => false,
            };
            if !formatter_ok {
                let name = country.get("name").and_then(|name| name.as_str()).unwrap_or_default();
                return Err(decode(format!("phone_formatter of the country {name} must map strings to strings")));
            }
        }
        let config: ServerConfig = serde_json::from_value(value).map_err(|error| decode(error.to_string()))?;
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
