use std::sync::Arc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use crate::runtime::Clock;
use super::{AppPlatform, ConfigCode, EncryptedConfig, HttpExchange, SecurityCode, ServerConfig, TransportError};
use super::jwe::{open_plain, random_token, seal_plain};
use super::key::ServerKey;
use crate::transport::retry::{RetryCalls, Retrying};


fn invalid(detail: impl ToString) -> TransportError {
    TransportError::Config { code: ConfigCode::InvalidRequest, detail: detail.to_string() }
}

fn authentication(detail: impl ToString) -> TransportError {
    TransportError::Security { code: SecurityCode::ResponseAuthentication, detail: detail.to_string() }
}

fn body_text(plain: &Value) -> String {
    let body = &plain["body"];
    let build = body.get("build").map(|value| format!(",\"build\":{value}")).unwrap_or_default();
    format!("{{\"platform\":{},\"version\":{}{build}}}", body["platform"], body["version"])
}

pub(super) fn request_text(plain: &Value) -> String {
    format!("{{\"audience\":{},\"kid\":{},\"method\":{},\"path\":{},\"requestId\":{},\"issuedAt\":{},\"responseKey\":{},\"body\":{}}}", plain["audience"], plain["kid"], plain["method"], plain["path"], plain["requestId"], plain["issuedAt"], plain["responseKey"], body_text(plain))
}

pub(super) fn request_plain(config: &EncryptedConfig, platform: AppPlatform, version: &str, build: Option<&str>, clock: &dyn Clock, request_id: &str, response_key: &str) -> Result<Value, TransportError> {
    let segments: Vec<_> = version.split('.').collect();
    if !(2..=3).contains(&segments.len()) || segments.iter().any(|segment| segment.is_empty() || !segment.bytes().all(|byte| byte.is_ascii_digit())) {
        return Err(invalid("version must have 2 or 3 numeric segments"));
    }
    if build.is_some_and(|build| build.chars().count() > 32) { return Err(invalid("build exceeds 32 characters")); }
    let mut body = json!({ "platform": platform, "version": version });
    if let Some(build) = build { body["build"] = build.into(); }
    Ok(json!({ "audience": config.origin.trim_end_matches('/'), "kid": config.pin, "method": "POST", "path": config.endpoints.bootstrap, "requestId": request_id, "issuedAt": clock.now().as_secs(), "responseKey": response_key, "body": body }))
}

pub(super) fn request_digest(plain: &Value) -> String {
    let text = format!("{{\"audience\":{},\"body\":{},\"kid\":{},\"method\":{},\"path\":{},\"requestId\":{},\"responseKey\":{}}}", plain["audience"], body_text(plain), plain["kid"], plain["method"], plain["path"], plain["requestId"], plain["responseKey"]);
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn accept(token: &str, response_key: &str, request_id: &str, digest: &str) -> Result<ServerConfig, TransportError> {
    let response = open_plain(token, response_key).map_err(authentication)?;
    if response["requestId"].as_str() != Some(request_id) || response["requestDigest"].as_str() != Some(digest) || response.get("sequence") != Some(&Value::Null) {
        return Err(authentication("the response does not match the bootstrap request"));
    }
    let body = &response["body"];
    if let Some(error) = crate::transport::answer_kind::transient_failure(body) { return Err(error); }
    if body.get("error").is_some() { return Err(invalid("the server refused the bootstrap request")); }
    let config: ServerConfig = serde_json::from_value(body.clone()).map_err(|error| TransportError::Config { code: ConfigCode::InvalidFormat, detail: error.to_string() })?;
    if config.config_version != 1 {
        return Err(TransportError::Config { code: ConfigCode::UnsupportedVersion, detail: "unsupported configVersion".into() });
    }
    Ok(config)
}

pub(crate) struct AppRelease<'a> {
    pub platform: AppPlatform,
    pub version: &'a str,
    pub build: Option<&'a str>,
}

pub(crate) async fn fetch(config: &EncryptedConfig, release: &AppRelease<'_>, http: Arc<dyn HttpExchange>, clock: &dyn Clock, progress: &RetryCalls, online: &crate::transport::Connectivity) -> Result<ServerConfig, TransportError> {
    config.validate()?;
    let audience = config.origin.trim_end_matches('/');
    if !super::origin::allowed(audience) { return Err(invalid("the origin must be https or a development host without a path")); }
    let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| TransportError::Security { code: SecurityCode::ServerKeyMismatch, detail: error.to_string() })?;
    let mut retry = Retrying::new(clock, progress, online, crate::runtime::RetryPolicy::Http);
    loop {
        online.online().await;
        let request_id = uuid::Uuid::new_v4().to_string();
        let response_key = zeroize::Zeroizing::new(random_token());
        let plain = request_plain(config, release.platform, release.version, release.build, clock, &request_id, &response_key)?;
        let digest = request_digest(&plain);
        let token = seal_plain(&key, request_text(&plain).as_bytes()).map_err(authentication)?;
        crate::lock::assert_no_library_lock("HttpExchange::post");
        let result = match http.post(&config.endpoints.bootstrap, token, crate::transport::NetworkPurpose::Standard, crate::transport::WireIds::bootstrap()).await {
            Ok(reply) if reply.status == 200 => accept(&reply.body, &response_key, &request_id, &digest),
            Ok(reply) => Err(TransportError::Http { status: reply.status, body: String::new() }),
            Err(error) => Err(error),
        };
        match result { Ok(config) => return Ok(config), Err(error) => retry.failed(error).await? }
    }
}
