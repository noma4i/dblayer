use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use crate::transport::{Connectivity, GraphQLRequest, Subscription, TransportError};
use super::{CableLink, CableSession, EncryptedConfig};

const IDENTIFIER: &str = r#"{"channel":"V3::EncryptedGraphqlChannel"}"#;

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

#[async_trait]
pub trait CableConnect: Send + Sync {
    async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError>;
}

struct Add {
    request: GraphQLRequest,
    reply: oneshot::Sender<Subscription>,
}

enum Command {
    Add(Add),
    Resume,
    #[cfg(test)]
    Settled(oneshot::Sender<()>),
}

pub struct CableSubscriptions {
    commands: mpsc::UnboundedSender<Command>,
    wakes: Arc<AtomicUsize>,
    online: Connectivity,
}

impl CableSubscriptions {
    pub fn new(config: EncryptedConfig, credential: Arc<dyn Fn() -> Result<String, TransportError> + Send + Sync>, connect: Arc<dyn CableConnect>, online: Connectivity) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        tokio::spawn(run(config, credential, connect, online.clone(), receiver, wakes.clone()));
        Self { commands, wakes, online }
    }

    pub async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> {
        let (reply, receiver) = oneshot::channel();
        self.commands.send(Command::Add(Add { request, reply })).map_err(|_| TransportError::Network("cable stopped".into()))?;
        receiver.await.map_err(|_| TransportError::Network("cable stopped".into()))
    }

    pub fn wake_count(&self) -> usize { self.wakes.load(Ordering::Relaxed) }
    pub fn set_online(&self, online: bool) { self.online.set(online); }

    pub fn resume(&self) { let _ = self.commands.send(Command::Resume); }

    #[cfg(test)]
    pub async fn settled(&self) -> Result<(), TransportError> {
        let (sender, receiver) = oneshot::channel();
        self.commands.send(Command::Settled(sender)).map_err(|_| TransportError::Network("cable stopped".into()))?;
        tokio::time::timeout(Duration::from_secs(1), receiver).await.map_err(|_| TransportError::Network("cable busy".into()))?.map_err(|_| TransportError::Network("cable stopped".into()))
    }
}

fn command(command: &str, data: Option<Value>) -> String {
    let mut body = json!({"command":command, "identifier":IDENTIFIER});
    if let Some(data) = data { body["data"] = json!(data.to_string()); }
    body.to_string()
}

async fn send(socket: &dyn CableSocket, link: &mut CableLink, session: &mut CableSession, body: Value) -> Result<(), TransportError> {
    let token = link.seal(body).map_err(|error| TransportError::Decode(error.to_string()))?;
    socket.send(command("message", Some(json!({"action":"frame", "frame":token})))).await?;
    session.traffic();
    Ok(())
}

async fn disconnect(connection: &mut Option<(Arc<dyn CableSocket>, CableLink)>, session: &mut CableSession, online: bool) -> Option<Instant> {
    if let Some((socket, mut link)) = connection.take() { link.close(); socket.close().await; }
    session.closed(1006, "connection lost");
    session.failed_open(online).map(|delay| Instant::now() + delay)
}

async fn handshake(socket: &dyn CableSocket, config: &EncryptedConfig, credential: &str, session: &mut CableSession, commands: &mut mpsc::UnboundedReceiver<Command>, online: &Connectivity) -> Result<CableLink, TransportError> {
    let mut link = None;
    let next = socket.next();
    tokio::pin!(next);
    loop {
        let event = tokio::select! {
            event = &mut next => event,
            _ = session.cancelled() => {
                if session.is_empty() { return Err(TransportError::Interrupted("cable has no subscribers".into())); }
                continue;
            }
            command = commands.recv() => {
                match command {
                    Some(Command::Add(add)) => { let (_, subscription) = session.subscribe(add.request); let _ = add.reply.send(subscription); }
                    Some(Command::Resume) => session.resume(),
                    #[cfg(test)]
                    Some(Command::Settled(sender)) => { let _ = sender.send(()); }
                    None => return Err(TransportError::Interrupted("cable stopped".into())),
                }
                continue;
            }
            _ = online.offline() => return Err(TransportError::Network("offline".into())),
        };
        next.set(socket.next());
        let text = match event {
            Some(SocketEvent::Text(text)) => text,
            Some(SocketEvent::Closed { code, reason }) => {
                session.closed(code, &reason);
                return Err(TransportError::Network(format!("cable {code}: {reason}")));
            }
            None => return Err(TransportError::Network("cable closed".into())),
        };
        let body: Value = serde_json::from_str(&text).map_err(|error| TransportError::Decode(error.to_string()))?;
        match body["type"].as_str() {
            Some("challenge") => {
                let (opened, hello) = CableLink::open(config, &body, credential).map_err(|error| TransportError::Decode(error.to_string()))?;
                link = Some((opened, hello));
                socket.send(command("subscribe", None)).await?;
            }
            Some("confirm_subscription") => {
                let (_, hello) = link.as_ref().ok_or_else(|| TransportError::Decode("confirmation before challenge".into()))?;
                socket.send(command("message", Some(json!({"action":"hello", "hello":hello})))).await?;
            }
            Some("reject_subscription") => return Err(TransportError::Decode("cable subscription refused".into())),
            _ => if let Some(token) = body["message"]["frame"].as_str() {
                let (mut opened, _) = link.take().ok_or_else(|| TransportError::Decode("frame before challenge".into()))?;
                let ready = opened.receive(token).map_err(|error| TransportError::Decode(error.to_string()))?;
                if ready["t"] != "ready" { return Err(TransportError::Decode("expected cable ready".into())); }
                return Ok(opened);
            },
        }
    }
}

async fn run(config: EncryptedConfig, credential: Arc<dyn Fn() -> Result<String, TransportError> + Send + Sync>, connect: Arc<dyn CableConnect>, online: Connectivity, mut commands: mpsc::UnboundedReceiver<Command>, wakes: Arc<AtomicUsize>) {
    let mut session = CableSession::new();
    let mut connection: Option<(Arc<dyn CableSocket>, CableLink)> = None;
    let mut retry_at: Option<Instant> = None;
    let mut incoming = None;
    loop {
        if session.is_empty() {
            if let Some((socket, mut link)) = connection.take() { link.close(); socket.close().await; }
            retry_at = None;
        }
        if connection.is_none() { incoming = None; }
        if !session.is_empty() && connection.is_none() && retry_at.is_none() && online.is_online() {
            let attempt = async {
                let credential = credential()?;
                if credential.is_empty() { return Err(TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "empty cable credential".into() }); }
                let socket = tokio::time::timeout(Duration::from_secs(10), connect.open()).await.map_err(|_| TransportError::Network("cable opening timeout".into()))??;
                let result = tokio::time::timeout(Duration::from_secs(10), handshake(socket.as_ref(), &config, &credential, &mut session, &mut commands, &online)).await.map_err(|_| TransportError::Network("cable handshake timeout".into())).and_then(|result| result);
                match result {
                    Ok(link) => Ok((socket, link)),
                    Err(error) => { socket.close().await; Err(error) }
                }
            }.await;
            match attempt {
                Ok((socket, mut link)) => {
                    let mut success = true;
                    for body in session.ready() {
                        if send(socket.as_ref(), &mut link, &mut session, body).await.is_err() { success = false; break; }
                    }
                    if success {
                        incoming = Some(Box::pin(receive(socket.clone())));
                        connection = Some((socket, link));
                    }
                    else { socket.close().await; retry_at = session.failed_open(online.is_online()).map(|delay| Instant::now() + delay); }
                }
                Err(error) => {
                    if matches!(error, TransportError::Session { .. }) { session.refuse(crate::transport::SubscriptionFailure::from_error(error)); }
                    retry_at = session.failed_open(online.is_online()).map(|delay| Instant::now() + delay);
                }
            }
        }
        let deadline = retry_at.or_else(|| session.deadline().filter(|_| connection.is_some()));
        tokio::select! {
            add = commands.recv() => {
                let add = match add {
                    Some(Command::Add(add)) => add,
                    Some(Command::Resume) => { session.resume(); continue; }
                    #[cfg(test)]
                    Some(Command::Settled(sender)) => { let _ = sender.send(()); continue; }
                    None => break,
                };
                let (id, subscription) = session.subscribe(add.request);
                let _ = add.reply.send(subscription);
                if let Some((socket, link)) = connection.as_mut() {
                    if let Some(body) = session.frame(&id) {
                        if send(socket.as_ref(), link, &mut session, body).await.is_err() {
                            retry_at = disconnect(&mut connection, &mut session, online.is_online()).await;
                        }
                    }
                }
            }
            body = session.cancelled() => {
                if let (Some(body), Some((socket, link))) = (body, connection.as_mut()) {
                    if send(socket.as_ref(), link, &mut session, body).await.is_err() {
                        retry_at = disconnect(&mut connection, &mut session, online.is_online()).await;
                    }
                }
            }
            event = async { match incoming.as_mut() { Some(next) if connection.is_some() => next.await, _ => std::future::pending().await } } => {
                incoming = connection.as_ref().map(|(socket, _)| Box::pin(receive(socket.clone())));
                let mut closed = None;
                match event {
                    Some(SocketEvent::Text(text)) => {
                        let body = serde_json::from_str::<Value>(&text).ok();
                        if let Some(token) = body.as_ref().and_then(|body| body["message"]["frame"].as_str()) {
                            if let Some((_, link)) = connection.as_mut() {
                                match link.receive(token) {
                                    Ok(body) if body["t"] == "closing" => { closed = Some((body["code"].as_u64().unwrap_or(4400) as u16, body["reason"].as_str().unwrap_or("").to_string())); }
                                    Ok(body) => { if session.receive(body).is_err() { closed = Some((4400, String::new())); } }
                                    Err(_) => { closed = Some((4400, String::new())); }
                                }
                            }
                        }
                    }
                    Some(SocketEvent::Closed { code, reason }) => closed = Some((code, reason)),
                    None => closed = Some((1006, String::new())),
                }
                if let Some((code, reason)) = closed {
                    session.closed(code, &reason);
                    if let Some((socket, mut link)) = connection.take() { link.close(); socket.close().await; }
                    retry_at = session.failed_open(online.is_online()).map(|delay| Instant::now() + delay);
                }
            }
            _ = async { match deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } } => {
                wakes.fetch_add(1, Ordering::Relaxed);
                if retry_at.take().is_none() {
                    match session.tick() {
                        Ok(Some(body)) => if let Some((socket, link)) = connection.as_mut() {
                            if send(socket.as_ref(), link, &mut session, body).await.is_err() {
                                retry_at = disconnect(&mut connection, &mut session, online.is_online()).await;
                            }
                        },
                        Err(_) => {
                            retry_at = disconnect(&mut connection, &mut session, online.is_online()).await;
                        }
                        _ => {}
                    }
                }
            }
            _ = online.online(), if !online.is_online() && !session.is_empty() => {}
            _ = online.offline(), if online.is_online() && connection.is_some() => {
                session.closed(1001, "offline");
                if let Some((socket, mut link)) = connection.take() { link.close(); socket.close().await; }
                retry_at = None;
            }
        }
    }
    if let Some((socket, _)) = connection { socket.close().await; }
}

async fn receive(socket: Arc<dyn CableSocket>) -> Option<SocketEvent> { socket.next().await }
