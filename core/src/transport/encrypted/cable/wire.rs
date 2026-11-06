use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use crate::pacing::{Clock, timeout};
use crate::transport::{Connectivity, GraphQLRequest, ServerSubscription, TransportError};
use super::{CableLink, CableSession, EncryptedConfig};


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
    admitted: crate::ffi::admission::Admitted,
    reply: oneshot::Sender<ServerSubscription>,
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
}

impl CableSubscriptions {
    pub fn new(config: EncryptedConfig, credential: Arc<dyn Fn(&crate::ffi::admission::Admitted) -> Result<String, TransportError> + Send + Sync>, connect: Arc<dyn CableConnect>, online: Arc<Connectivity>, clock: Arc<dyn Clock>) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        crate::task::spawn(crate::task::Carry::Detached, run(config, credential, connect, online.clone(), receiver, wakes.clone(), clock));
        Self { commands, wakes }
    }

    pub async fn subscribe(&self, admitted: &crate::ffi::admission::Admitted, request: GraphQLRequest) -> Result<ServerSubscription, TransportError> {
        let (reply, receiver) = oneshot::channel();
        crate::lock::effect(|| self.commands.send(Command::Add(Add { request, admitted: admitted.clone(), reply })).map_err(|_| TransportError::Network("cable stopped".into())))?;
        receiver.await.map_err(|_| TransportError::Network("cable stopped".into()))
    }

    pub fn wake_count(&self) -> usize { self.wakes.load(Ordering::Relaxed) }

    pub fn resume(&self) { crate::lock::effect(|| { let _ = self.commands.send(Command::Resume); }); }

    #[cfg(test)]
    pub async fn settled(&self) -> Result<(), TransportError> {
        let (sender, receiver) = oneshot::channel();
        crate::lock::effect(|| self.commands.send(Command::Settled(sender)).map_err(|_| TransportError::Network("cable stopped".into())))?;
        tokio::time::timeout(Duration::from_secs(1), receiver).await.map_err(|_| TransportError::Network("cable busy".into()))?.map_err(|_| TransportError::Network("cable stopped".into()))
    }
}

fn command(channel: &str, command: &str, data: Option<Value>) -> String {
    let mut body = json!({"command":command, "identifier":json!({"channel":channel}).to_string()});
    if let Some(data) = data { body["data"] = json!(data.to_string()); }
    body.to_string()
}

async fn send(channel: &str, socket: &dyn CableSocket, link: &mut CableLink, session: &mut CableSession, body: Value) -> Result<(), TransportError> {
    let token = link.seal(body).map_err(|error| TransportError::Decode(error.to_string()))?;
    socket.send(command(channel, "message", Some(json!({"action":"frame", "frame":token})))).await?;
    session.sent();
    Ok(())
}

async fn settle(channel: &str, connection: &mut Option<(Arc<dyn CableSocket>, CableLink)>, session: &mut CableSession) {
    loop {
        let frames = session.take_frames();
        let Some((socket, link)) = connection.as_mut() else { break };
        if frames.is_empty() { break; }
        for body in frames {
            if send(channel, socket.as_ref(), link, session, body).await.is_err() { session.lost(); break; }
        }
    }
    if !session.is_open() {
        if let Some((socket, mut link)) = connection.take() { link.close(); socket.close().await; }
    }
}

async fn await_connected<T>(next: impl std::future::Future<Output = T>, session: &mut CableSession, commands: &mut mpsc::UnboundedReceiver<Command>, online: &Connectivity) -> Result<T, TransportError> {
    tokio::pin!(next);
    loop {
        tokio::select! {
            event = &mut next => return Ok(event),
            _ = session.cancelled() => {}
            command = commands.recv() => {
                match command {
                    Some(Command::Add(add)) => { session.discard_ended(); let (_, subscription) = session.subscribe(add.admitted, add.request); crate::lock::effect(|| { let _ = add.reply.send(subscription); }); }
                    Some(Command::Resume) => session.resume(),
                    #[cfg(test)]
                    Some(Command::Settled(sender)) => { crate::lock::effect(|| { let _ = sender.send(()); }); }
                    None => return Err(TransportError::Interrupted("cable stopped".into())),
                }
            }
            _ = online.offline() => {}
        }
        session.connectivity();
        if !session.is_connecting() { return Err(TransportError::Interrupted("cable is no longer connecting".into())); }
    }
}

async fn handshake(socket: &dyn CableSocket, config: &EncryptedConfig, credential: &str, session: &mut CableSession, commands: &mut mpsc::UnboundedReceiver<Command>, online: &Connectivity) -> Result<CableLink, TransportError> {
    let mut link = None;
    loop {
        let event = await_connected(socket.next(), session, commands, online).await?;
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
                socket.send(command(&config.cable_channel, "subscribe", None)).await?;
            }
            Some("confirm_subscription") => {
                let (_, hello) = link.as_ref().ok_or_else(|| TransportError::Decode("confirmation before challenge".into()))?;
                socket.send(command(&config.cable_channel, "message", Some(json!({"action":"hello", "hello":hello})))).await?;
            }
            Some("reject_subscription") => return Err(TransportError::Session { code: crate::transport::SessionCode::AccountUnavailable, detail: "cable subscription refused".into() }),
            _ => if let Some(token) = body["message"]["frame"].as_str() {
                let (mut opened, _) = link.take().ok_or_else(|| TransportError::Decode("frame before challenge".into()))?;
                let ready = opened.receive(token).map_err(|error| TransportError::Decode(error.to_string()))?;
                if ready["t"] != "ready" { return Err(TransportError::Decode("expected cable ready".into())); }
                return Ok(opened);
            },
        }
    }
}

async fn run(config: EncryptedConfig, credential: Arc<dyn Fn(&crate::ffi::admission::Admitted) -> Result<String, TransportError> + Send + Sync>, connect: Arc<dyn CableConnect>, online: Arc<Connectivity>, mut commands: mpsc::UnboundedReceiver<Command>, wakes: Arc<AtomicUsize>, clock: Arc<dyn Clock>) {
    let mut session = CableSession::new(clock.clone(), online.clone());
    let mut connection: Option<(Arc<dyn CableSocket>, CableLink)> = None;
    let mut incoming = None;
    loop {
        session.discard_ended();
        session.connectivity();
        settle(&config.cable_channel, &mut connection, &mut session).await;
        if connection.is_none() { incoming = None; }
        if session.is_connecting() && connection.is_none() {
            let attempt = async {
                config.validate()?;
                let admitted = session.admitted().cloned().ok_or_else(|| TransportError::Interrupted("cable has no current subscription".into()))?;
                let credential = credential(&admitted)?;
                if credential.is_empty() { return Err(TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "empty cable credential".into() }); }
                let socket = timeout(clock.as_ref(), Duration::from_secs(10), admitted.run(await_connected(connect.open(), &mut session, &mut commands, &online))).await.map_err(|_| TransportError::Network("cable opening timeout".into()))????;
                let result = timeout(clock.as_ref(), Duration::from_secs(10), admitted.run(handshake(socket.as_ref(), &config, &credential, &mut session, &mut commands, &online))).await.map_err(|_| TransportError::Network("cable handshake timeout".into())).and_then(|result| result.map_err(TransportError::from)).and_then(|result| result);
                match result {
                    Ok(link) => Ok((socket, link)),
                    Err(error) => { socket.close().await; Err(error) }
                }
            }.await;
            match attempt {
                Ok((socket, link)) => {
                    incoming = Some(Box::pin(receive(socket.clone())));
                    connection = Some((socket, link));
                    session.opened();
                    settle(&config.cable_channel, &mut connection, &mut session).await;
                }
                Err(error) => {
                    let error = match error {
                        TransportError::Http { status: 401, body } => TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: body },
                        TransportError::Http { status: 403, body } => TransportError::Session { code: crate::transport::SessionCode::AccountUnavailable, detail: body },
                        error => error,
                    };
                    if matches!(error, TransportError::Session { .. }) { session.refuse(crate::transport::SubscriptionFailure::from_error(error)); }
                    else { session.open_failed(); }
                }
            }
        }
        let deadline = session.deadline();
        let waits_online = session.has_demand() && !online.is_online();
        let waits_offline = session.has_demand() && online.is_online();
        let admission = session.admitted().cloned();
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::Add(add)) => { session.discard_ended(); let (_, subscription) = session.subscribe(add.admitted, add.request); crate::lock::effect(|| { let _ = add.reply.send(subscription); }); }
                    Some(Command::Resume) => session.resume(),
                    #[cfg(test)]
                    Some(Command::Settled(sender)) => { crate::lock::effect(|| { let _ = sender.send(()); }); }
                    None => break,
                }
            }
            _ = session.cancelled() => {}
            event = async { match incoming.as_mut() { Some(next) if connection.is_some() => next.await, _ => std::future::pending().await } } => {
                incoming = connection.as_ref().map(|(socket, _)| Box::pin(receive(socket.clone())));
                match event {
                    Some(SocketEvent::Text(text)) => {
                        let body = match serde_json::from_str::<Value>(&text) {
                            Ok(body) => body,
                            Err(_) => { session.closed(4400, "invalid cable response"); continue; }
                        };
                        if let Some(token) = body["message"]["frame"].as_str() {
                            if let Some((_, link)) = connection.as_mut() {
                                match link.receive(token) {
                                    Ok(body) if body["t"] == "closing" => session.closed(body["code"].as_u64().unwrap_or(4400) as u16, body["reason"].as_str().unwrap_or("")),
                                    Ok(body) => { if session.receive(body).is_err() { session.closed(4400, ""); } }
                                    Err(_) => session.closed(4400, ""),
                                }
                            }
                        }
                    }
                    Some(SocketEvent::Closed { code, reason }) => session.closed(code, &reason),
                    None => session.closed(1006, ""),
                }
            }
            _ = async { match deadline { Some(deadline) => clock.sleep(deadline.saturating_sub(clock.now())).await, None => std::future::pending().await } } => {
                wakes.fetch_add(1, Ordering::Relaxed);
                session.tick();
            }
            _ = online.online(), if waits_online => {}
            _ = online.offline(), if waits_offline => {}
            _ = async { match admission { Some(admitted) => { let _ = admitted.run(std::future::pending::<()>()).await; }, None => std::future::pending().await } } => {}
        }
    }
    if let Some((socket, _)) = connection { socket.close().await; }
}

async fn receive(socket: Arc<dyn CableSocket>) -> Option<SocketEvent> { socket.next().await }
