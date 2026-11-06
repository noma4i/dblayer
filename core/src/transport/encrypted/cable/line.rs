use std::sync::Arc;
use async_trait::async_trait;
use serde_json::{json, Value};
use crate::transport::{Transport, TransportError};
use super::{CableError, CableKeys, CableSocket, EncryptedConfig, SocketEvent};

/// What a connection gives to its reader: a decrypted frame with its number, the end of the socket, or bytes that are no frame.
#[derive(Debug)]
pub enum Incoming {
    Frame { counter: u64, body: Value },
    Closed { code: u16, reason: String },
    Unreadable(CableError),
}

/// One connection after its handshake. It holds a key and no counter: the owner of the counters gives the counter of every frame it sends.
#[async_trait]
pub trait CableLine: Send + Sync {
    async fn send(&self, counter: u64, body: Value) -> Result<(), TransportError>;
    async fn next(&self) -> Incoming;
    async fn close(&self);
}

/// Opens a socket and runs the handshake of the cable.
#[async_trait]
pub trait CableLines: Send + Sync {
    async fn connect(&self) -> Result<Arc<dyn CableLine>, TransportError>;
}

pub type CableCredential = Arc<dyn Fn() -> Result<String, TransportError> + Send + Sync>;

pub struct EncryptedLines {
    transport: Arc<dyn Transport>,
    config: EncryptedConfig,
    credential: CableCredential,
}

impl EncryptedLines {
    pub fn new(transport: Arc<dyn Transport>, config: EncryptedConfig, credential: CableCredential) -> Self { Self { transport, config, credential } }
}

impl Drop for EncryptedLines {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("EncryptedLines drop"); }
}

fn command(channel: &str, command: &str, data: Option<Value>) -> String {
    let mut body = json!({"command":command, "identifier":json!({"channel":channel}).to_string()});
    if let Some(data) = data { body["data"] = json!(data.to_string()); }
    body.to_string()
}

fn decode_fault(error: impl std::fmt::Display) -> TransportError { TransportError::Decode(error.to_string()) }

async fn handshake(socket: &dyn CableSocket, config: &EncryptedConfig, credential: &str) -> Result<CableKeys, TransportError> {
    config.validate()?;
    if credential.is_empty() { return Err(TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "empty cable credential".into() }); }
    let mut agreed: Option<(CableKeys, String)> = None;
    loop {
        let text = match socket.next().await {
            Some(SocketEvent::Text(text)) => text,
            Some(SocketEvent::Closed { code, reason }) => return Err(TransportError::Network(format!("cable {code}: {reason}"))),
            None => return Err(TransportError::Network("cable closed".into())),
        };
        let body: Value = serde_json::from_str(&text).map_err(decode_fault)?;
        match body["type"].as_str() {
            Some("challenge") => {
                agreed = Some(CableKeys::agree(config, &body, credential).map_err(decode_fault)?);
                crate::lock::effect(|| socket.send(command(&config.cable_channel, "subscribe", None))).await?;
            }
            Some("confirm_subscription") => {
                let (_, hello) = agreed.as_ref().ok_or_else(|| decode_fault("confirmation before challenge"))?;
                crate::lock::effect(|| socket.send(command(&config.cable_channel, "message", Some(json!({"action":"hello", "hello":hello}))))).await?;
            }
            Some("reject_subscription") => return Err(TransportError::Session { code: crate::transport::SessionCode::AccountUnavailable, detail: "cable subscription refused".into() }),
            _ => if let Some(token) = body["message"]["frame"].as_str() {
                let (keys, _) = agreed.take().ok_or_else(|| decode_fault("frame before challenge"))?;
                let (counter, ready) = keys.open(token).map_err(decode_fault)?;
                keys.confirms(counter, &ready).map_err(decode_fault)?;
                return Ok(keys);
            },
        }
    }
}

/// A socket whose handshake is cancelled mid-way (the connect future is dropped) closes in the background.
struct OpenSocket(Option<Arc<dyn CableSocket>>);

impl Drop for OpenSocket {
    fn drop(&mut self) {
        let (Some(socket), Ok(handle)) = (self.0.take(), tokio::runtime::Handle::try_current()) else { return };
        crate::task::spawn_on(crate::task::Carry::Detached, &handle, async move { socket.close().await });
    }
}

struct SocketLine {
    socket: Arc<dyn CableSocket>,
    keys: CableKeys,
    channel: String,
}

#[async_trait]
impl CableLine for SocketLine {
    async fn send(&self, counter: u64, body: Value) -> Result<(), TransportError> {
        let token = self.keys.seal(counter, body).map_err(decode_fault)?;
        crate::lock::effect(|| self.socket.send(command(&self.channel, "message", Some(json!({"action":"frame", "frame":token}))))).await
    }

    async fn next(&self) -> Incoming {
        loop {
            match self.socket.next().await {
                Some(SocketEvent::Text(text)) => {
                    let Ok(body) = serde_json::from_str::<Value>(&text) else { return Incoming::Unreadable(CableError::Frame) };
                    let Some(token) = body["message"]["frame"].as_str() else { continue };
                    return match self.keys.open(token) {
                        Ok((counter, body)) => Incoming::Frame { counter, body },
                        Err(error) => Incoming::Unreadable(error),
                    };
                }
                Some(SocketEvent::Closed { code, reason }) => return Incoming::Closed { code, reason },
                None => return Incoming::Closed { code: 1006, reason: String::new() },
            }
        }
    }

    async fn close(&self) { self.socket.close().await; }
}

#[async_trait]
impl CableLines for EncryptedLines {
    async fn connect(&self) -> Result<Arc<dyn CableLine>, TransportError> {
        let credential = (self.credential)()?;
        let socket = self.transport.open_cable(&self.config.cable_url()?).await?;
        let mut open = OpenSocket(Some(socket.clone()));
        let keys = handshake(socket.as_ref(), &self.config, &credential).await;
        open.0 = None;
        match keys {
            Ok(keys) => Ok(Arc::new(SocketLine { socket, keys, channel: self.config.cable_channel.clone() })),
            Err(error) => {
                socket.close().await;
                Err(error)
            }
        }
    }
}
