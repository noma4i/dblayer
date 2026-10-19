use std::collections::HashMap;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Deserialize, uniffi::Enum)]
#[serde(rename_all = "lowercase")]
pub enum AppPlatform { Ios, Android }

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SecurityCode { ServerKeyMismatch, ResponseAuthentication, TlsTrust }

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum ConfigCode { InvalidFormat, UnsupportedVersion, InvalidRequest }

fn flags<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
    Ok(serde_json::Value::Object(value).to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub config_version: u32,
    pub protocol_version: u32,
    pub min_app_version: String,
    pub update_required: bool,
    pub country: Option<String>,
    pub country_disabled: bool,
    pub countries: Vec<Country>,
    pub tracking_prompt_enabled: bool,
    pub store_url: Option<String>,
    #[serde(rename = "flags", deserialize_with = "flags")]
    pub flags_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, uniffi::Record)]
#[serde(rename_all = "camelCase")]
pub struct Country {
    pub name: String,
    pub code: String,
    pub flag: Option<String>,
    pub phone_code: Option<String>,
    pub phone_mask: Option<String>,
    pub phone_formatter: HashMap<String, String>,
    pub enabled: bool,
    pub phone_auth_enabled: bool,
}
