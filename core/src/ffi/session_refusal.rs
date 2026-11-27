use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, watch, Mutex};
use super::Database;

#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRefusalReason { Unauthenticated, AccountUnavailable, SessionEnded, Http401, Http403 }

#[derive(uniffi::Record, Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionRefusal { pub epoch: u64, pub reason: SessionRefusalReason }

#[derive(uniffi::Object)]
pub struct SessionRefusalSubscription {
    gate: Weak<crate::runtime::EpochGate>,
    id: u64,
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<SessionRefusal>>>,
    stopped: watch::Sender<bool>,
}

impl SessionRefusalSubscription {
    pub(crate) fn new(gate: &Arc<crate::runtime::EpochGate>, id: u64, receiver: mpsc::UnboundedReceiver<SessionRefusal>, stopped: watch::Sender<bool>) -> Arc<Self> {
        Arc::new(Self { gate: Arc::downgrade(gate), id, receiver: Arc::new(Mutex::new(receiver)), stopped })
    }
}

#[uniffi::export]
impl SessionRefusalSubscription {
    pub async fn next(&self) -> Option<SessionRefusal> {
        let mut stopped = self.stopped.subscribe();
        let receiver = self.receiver.clone();
        super::database::on_core(super::database::Scope::root(), async move {
            if *stopped.borrow() { return None; }
            tokio::select! { biased; _ = stopped.wait_for(|stopped| *stopped) => None, value = async { receiver.lock().await.recv().await } => value }
        }).await.ok().flatten()
    }

    pub fn cancel(&self) {
        if let Some(gate) = self.gate.upgrade() { gate.remove_refusal_reader(self.id); }
        crate::lock::effect(|| self.stopped.send_replace(true));
    }
}

impl Drop for SessionRefusalSubscription {
    fn drop(&mut self) { self.cancel(); }
}

#[uniffi::export]
impl Database {
    pub fn session_refusals(&self) -> Result<Arc<SessionRefusalSubscription>, super::DbError> {
        self.not_closed()?;
        self.life.gate().refusal_reader()
    }
}
